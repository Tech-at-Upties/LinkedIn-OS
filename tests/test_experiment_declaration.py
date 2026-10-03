"""Explicit repair declaration preserves failed attempts and source holds."""
from datetime import UTC, datetime, timedelta
import json
import sqlite3
from threading import Barrier, Thread

import pytest

from test_live_startup import configuration, spy, invoke
from xingestion.linkedin.live import BoundedBrowserCapture, declare_startup_repair_successor
from nos_linkedin.acquisition import Journal, RoutePermission


def predecessor(tmp_path, monkeypatch, intervention=None):
    config, old = configuration(tmp_path)
    capture = BoundedBrowserCapture(old)
    if intervention:
        spy(monkeypatch, [], lambda receipt: receipt.update(stopped={'reason':intervention},session_verified=False))
    else:
        monkeypatch.setattr('xingestion.linkedin.live.subprocess.run', lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError()))
    try: invoke(capture)
    except (TimeoutError,ValueError): pass
    root = capture.root
    source = Journal(root / '.local/linkedin-source.sqlite')
    generation = source.generation('linkedin.company-feed')
    def fail(): raise TimeoutError()
    source.capture(scope='linkedin.company-feed', generation=generation, attempt_id='source-one',
        feed_publisher_id=old['feed_publisher_id'], permission=RoutePermission(True,True,datetime.now(UTC)+timedelta(minutes=30),'allowlisted_source_projection'),
        now=datetime.now(UTC), send=fail)
    # Advance the declared predecessor clock without refunding its attempt.
    old['expires_at'] = (datetime.now(UTC)-timedelta(seconds=1)).isoformat()
    from hashlib import sha256
    old_sha = sha256(json.dumps(old, sort_keys=True, allow_nan=False).encode()).hexdigest()
    with sqlite3.connect(capture.budget_path) as connection:
        connection.execute('UPDATE capture_allowance SET declaration_sha=?', (old_sha,))
    previous = root / '.local/previous.json'; previous.write_text(json.dumps(old),encoding='utf-8')
    new = {**old, 'expires_at':(datetime.now(UTC)+timedelta(minutes=10)).isoformat(), 'source_journal':str(source.path)}
    future = root / '.local/next.json'; future.write_text(json.dumps(new),encoding='utf-8')
    witness = root / '.local/witness.json'
    witness.write_text(json.dumps({'source_navigations':0,'outcomes':[
        {'mode':'old_filtered','result':{'chrome_install_not_found':True}},
        {'mode':'with_install_paths','result':{'launched':True,'only_navigation':'about:blank'}}]}))
    return dict(previous_manifest=previous,next_manifest=future,previous_journal=source.path,witness=witness), capture, source, old, new


def test_explicit_successor_preserves_history_and_old_attempt_no_resend(tmp_path, monkeypatch):
    arguments, original, source, old, new = predecessor(tmp_path, monkeypatch)
    result = declare_startup_repair_successor(**arguments)
    with sqlite3.connect(original.budget_path) as connection:
        history = connection.execute('SELECT * FROM capture_allowance_history').fetchone()
        assert history[1] == 'source-one' and history[3] == result['declaration_sha']
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is None
    calls = []; spy(monkeypatch,calls)
    selected = BoundedBrowserCapture(new)
    assert invoke(selected,'new-source').status == 200
    with pytest.raises(ValueError,match='already consumed'): invoke(BoundedBrowserCapture(new),'third-source')
    assert len(calls) == 1
    original_calls = []
    assert source.capture(scope='linkedin.company-feed',generation=source.generation('linkedin.company-feed'),attempt_id='source-one',
        feed_publisher_id=old['feed_publisher_id'],permission=RoutePermission(True,True,datetime.now(UTC)+timedelta(minutes=30),'allowlisted_source_projection'),
        now=datetime.now(UTC),send=lambda:original_calls.append('forbidden'))
    assert original_calls == []


@pytest.mark.parametrize('fault',['active','company','scope','global_hold','source_hold','wrong_digest','successful','crash_unknown','witness','other_journal'])
def test_invalid_successor_never_changes_consumed_allowance(tmp_path, monkeypatch, fault):
    args, original, source, old, new = predecessor(tmp_path, monkeypatch)
    if fault == 'active': old['expires_at'] = (datetime.now(UTC)+timedelta(minutes=1)).isoformat()
    if fault == 'company': new['company_url'] = 'https://www.linkedin.com/company/other/posts/'
    if fault == 'scope': new['capture_budget'] = 2
    if fault == 'global_hold': original.hold.write_text('{}')
    if fault == 'source_hold': source.stop('linkedin.company-feed','operator_stop')
    if fault == 'wrong_digest': old['probe_sha256'] = 'f'*64
    if fault in {'successful','crash_unknown'}:
        with source.connect() as connection:
            connection.execute('UPDATE responses SET outcome=?',('bounded' if fault=='successful' else 'dispatch_unknown',))
    if fault == 'witness': args['witness'].write_text('{}')
    if fault == 'other_journal': new['source_journal'] = str(original.root / '.local/missing.sqlite')
    args['previous_manifest'].write_text(json.dumps(old)); args['next_manifest'].write_text(json.dumps(new))
    with pytest.raises(ValueError): declare_startup_repair_successor(**args)
    with sqlite3.connect(original.budget_path) as connection:
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is not None
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='capture_allowance_history'").fetchone()


def test_competing_explicit_declarations_install_once(tmp_path,monkeypatch):
    args, original, source, old, new = predecessor(tmp_path,monkeypatch)
    barrier=Barrier(2); results=[]
    def declare():
        barrier.wait(3)
        try: declare_startup_repair_successor(**args); results.append('declared')
        except ValueError: results.append('refused')
    threads=[Thread(target=declare),Thread(target=declare)]
    for thread in threads: thread.start()
    for thread in threads: thread.join(10)
    assert not any(thread.is_alive() for thread in threads) and sorted(results)==['declared','refused']
    with sqlite3.connect(original.budget_path) as connection:
        assert connection.execute('SELECT COUNT(*) FROM capture_allowance_history').fetchone()[0]==1


def test_successor_still_obeys_source_stop_and_exact_bootstrap_journal(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from xingestion.linkedin.bootstrap import build_linkedin_runtime
    args, original, source, old, new = predecessor(tmp_path,monkeypatch)
    declare_startup_repair_successor(**args)
    calls=[]; spy(monkeypatch,calls)
    selected=BoundedBrowserCapture(new)
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN','local-http-test-token')
    config=SimpleNamespace(linkedin_live_path=args['next_manifest'],data_dir=tmp_path,
                           linkedin_m2_ingest_url='http://127.0.0.1:1/v1/linkedin/results')
    with pytest.raises(ValueError,match='original source Journal'): build_linkedin_runtime(config)
    config.data_dir=source.path.parent
    runtime=build_linkedin_runtime(config)
    assert runtime.journal.path==source.path
    source.stop('linkedin.company-feed','operator_stop')
    with pytest.raises(ValueError,match='source hold'): invoke(selected,'successor-after-stop')
    assert calls==[]


def test_intervention_committed_before_parent_stop_still_blocks_declaration(tmp_path,monkeypatch):
    args, original, source, old, new=predecessor(tmp_path,monkeypatch,intervention='authentication_required')
    with source.connect() as connection:
        assert not connection.execute("SELECT stopped FROM route_state WHERE scope='linkedin.company-feed'").fetchone()[0]
    assert original.intervention_for('source-one')=='authentication_required'
    with pytest.raises(ValueError,match='intervention'): declare_startup_repair_successor(**args)
    with sqlite3.connect(original.budget_path) as connection:
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is not None
