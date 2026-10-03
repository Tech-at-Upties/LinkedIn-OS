"""Erase expired WR-025 application copies and inspect metadata, never replay."""
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / '.local/nos-integration/M2/src'))
from nos_linkedin.acquisition import Journal
from nos_m2.store import Store


def main():
    receipt_path = ROOT / 'docs/results/native-startup-01ad32e497f24673a3a5c395387867a0.json'
    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    previous_receipt = json.loads((ROOT / 'docs/results/native-startup-7f6a3ade60914c06a3ebb2f7634be783-resume-04a0e5249dc34971b1c8f12a33bffdd0.json').read_text(encoding='utf-8'))
    previous_task = previous_receipt['root_job_id']
    now = datetime.now(UTC)
    assert now >= datetime.fromisoformat(receipt['expires_at']), 'original deadline has not elapsed'
    directory = (ROOT / '.local/native-startup-7f6a3ade60914c06a3ebb2f7634be783').resolve()
    source_path, sink_path = [(ROOT / receipt[key]).resolve() for key in ('source_journal', 'sink_database')]
    assert source_path.parent == sink_path.parent == directory
    assert source_path.is_file() and sink_path.is_file()
    journal = Journal(source_path)
    newly_erased = journal.purge_expired(now)
    with journal.connect() as connection:
        attempts = [dict(row) for row in connection.execute(
            'SELECT attempt_id, outcome, body_sha256, body IS NULL AS erased FROM responses')]
        assert len(attempts) == 2 and all(row['erased'] for row in attempts)
        assert {row['outcome'] for row in attempts} == {'policy_expired', 'transport_failure'}
    store = Store('sqlite:///' + sink_path.as_posix())
    store.initialize()
    try:
        count = store.purge_expired_evidence(limit=100)
        with sqlite3.connect(sink_path) as connection:
            copies = {}
            for table in ('evidence_occurrences', 'source_versions', 'observations', 'deliveries'):
                rows = connection.execute(f"SELECT count(*), sum(json_extract(envelope, '$.retention_state') = 'policy_expired') FROM {table}").fetchone()
                assert rows == (10, 10), (table, rows)
                # SQL checks keys without loading former publication content.
                for key in ('text', 'author', 'raw_payload', 'metrics', 'references', 'media'):
                    assert connection.execute(f"SELECT count(*) FROM {table} WHERE json_type(envelope, '$.{key}') IS NOT NULL").fetchone()[0] == 0
                copies[table] = rows[0]
        assert store.list_evidence() == [] and store.claim_work('expiry-verifier') is None
    finally:
        store.close()
    with psycopg.connect('postgresql://nos_test@127.0.0.1:15432/nos_linkedin_startup_jobs') as connection:
        assert Path(connection.execute('SHOW data_directory').fetchone()[0]).resolve() == (ROOT / '.local/verification-postgres').resolve()
        tasks = connection.execute('SELECT task_id, state FROM capability_tasks WHERE task_id = ANY(%s)',
            ([receipt['root_job_id'], previous_task],)).fetchall()
        assert dict(tasks) == {receipt['root_job_id']: 'DONE', previous_task: 'DEAD_LETTER'}
    report = {'checked_at': now.isoformat(), 'expires_at': receipt['expires_at'],
        'source_newly_erased': newly_erased, 'source_metadata': attempts,
        'm2_newly_purged_identities': count, 'expired_envelope_copies': copies,
        'successful_and_failed_task_states_preserved': True, 'source_reads': 0,
        'limits': 'Application copies only; no physical remnants or backup erasure claim.'}
    output = ROOT / 'docs/results/native-startup-expiry-verification.json'
    if output.exists():
        output = output.with_name(output.stem + '-' + now.strftime('%Y%m%dT%H%M%S%fZ') + '.json')
    output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
