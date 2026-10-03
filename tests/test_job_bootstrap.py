"""Explicit saved-projection runtime through actual local M2 HTTP admission."""
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
from threading import Thread
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
import uvicorn

from test_nos_integration import convert, store, wire
from test_parser import COMPANY, envelope
from nos_m2.api import create_app
from nos_m2.service import M2Service
from nos_m2.settings import Settings
from xingestion.linkedin.bootstrap import build_linkedin_runtime
from xingestion.linkedin.pages import execute_leased_company_page
from nos_linkedin.acquisition import RoutePermission
from xingestion.capabilities import LinkedInCompanyFeedInput
from test_job_runtime import ledger, worker, create


@pytest.fixture
def m2(store, tmp_path):
    settings = Settings(provider='unavailable', api_token=SecretStr('local-http-test-token'),
                        api_request_log_path=str(tmp_path / 'api-requests.jsonl'))
    service = M2Service(settings, store=store)
    app = create_app(service=service, settings=settings)
    yield app, service
    service.close()


def manifest_config(tmp_path, endpoint, *, change=None):
    now = datetime.now(UTC)
    value = {'company_url': 'https://www.linkedin.com/company/linkedin/posts/',
             'feed_publisher_id': COMPANY, 'evidence_class': 'synthetic_fixture',
             'captured_at': (now - timedelta(seconds=1)).isoformat(),
             'expires_at': (now + timedelta(minutes=30)).isoformat(), 'body': envelope()}
    if change:
        change(value)
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(value), encoding='utf-8')
    return SimpleNamespace(linkedin_replay_path=path, linkedin_m2_ingest_url=endpoint,
                           data_dir=tmp_path)


def test_m2_batch_auth_validation_and_duplicate_ack(m2, store):
    app, service = m2
    with TestClient(app) as client:
        value = wire()
        assert client.post('/v1/linkedin/results', json=value).status_code == 401
        headers = {'Authorization': 'Bearer local-http-test-token'}
        bad = {**value, 'items': value['items'] + [{'item_id': 'invalid'}]}
        assert client.post('/v1/linkedin/results', json=bad, headers=headers).status_code == 400
        assert store.get_evidence('linkedin:urn:li:activity:123') is None
        first = client.post('/v1/linkedin/results', json=value, headers=headers)
        second = client.post('/v1/linkedin/results', json=value, headers=headers)
        assert first.status_code == second.status_code == 200
        assert not first.json()['receipts'][0]['duplicate_delivery']
        assert second.json()['receipts'][0]['duplicate_delivery']
        assert len(store.list_observations('linkedin:urn:li:activity:123')) == 1


def test_saved_projection_bootstrap_real_m2_http(m2, tmp_path, monkeypatch, store, ledger):
    app, service = m2
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(10)
    endpoint = f'http://127.0.0.1:{listener.getsockname()[1]}/v1/linkedin/results'
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', lifespan='off'))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(.01)
    assert server.started
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    try:
        selected = build_linkedin_runtime(manifest_config(tmp_path, endpoint))
        assert selected.evidence_class == 'synthetic_fixture'
        # Keep legacy unrestricted transport probes on a separate synthetic
        # identity. An existing unrestricted corpus cannot be upgraded safely.
        transport_probe = wire()
        transport_probe['items'][0]['item_id'] = 'urn:li:activity:999'
        transport_probe['items'][0]['source_fields']['occurrence_id'] = 'urn:li:activity:999'
        result = selected.deliver(transport_probe)
        assert result['receipts'][0]['duplicate_delivery'] is False
        assert selected.deliver(transport_probe)['receipts'][0]['duplicate_delivery'] is True
        assert store.admit(convert(transport_probe)[0]).duplicate_delivery
        selected.runs.create(run_id='bootstrapped-page', feed_publisher_id=COMPANY,
            evidence_class='synthetic_fixture', first_start=3, count=10, page_budget=1)
        lease = selected.runs.claim('bootstrapped-page', owner='actual-m1', now=datetime.now(UTC))
        result = execute_leased_company_page(runs=selected.runs, lease=lease,
            permission=RoutePermission(True, True, selected.expires_at, 'synthetic_fixture'),
            send=lambda selected_page: selected.send(LinkedInCompanyFeedInput(
                'https://www.linkedin.com/company/linkedin/posts/', COMPANY), selected_page),
            deliver=lambda batch: selected.deliver(selected._wire(batch, 'bootstrapped-page', lease.attempt_id, lease.ordinal)))
        assert result.delivery_outcome == 'delivery_acknowledged'
        assert selected.runs.summary('bootstrapped-page')['completed_pages'] == 1
        task = create(ledger)
        assert worker(ledger, selected)._process_delivery(task.task_id).state.value == 'DONE'
        northbound_result = selected.result_for_task(ledger.get_task(task.task_id))
        assert northbound_result['job_id'] == task.task_id
        assert store.admit(convert(northbound_result)[0]).duplicate_delivery
    finally:
        server.should_exit = True
        thread.join(5)
        listener.close()
    assert not thread.is_alive()


@pytest.mark.parametrize('change', ['native_class', 'expired', 'wrong_paging', 'other_host', 'missing_token'])
def test_bootstrap_refuses_unproved_configuration(tmp_path, monkeypatch, change):
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    def mutate(value):
        if change == 'native_class':
            value['evidence_class'] = 'native_response'
        if change == 'expired':
            value['expires_at'] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        if change == 'wrong_paging':
            next(iter(value['body']['data']['data'].values()))['paging']['start'] = 0
    config = manifest_config(tmp_path, 'http://127.0.0.1:1/v1/linkedin/results', change=mutate)
    if change == 'other_host':
        config.linkedin_m2_ingest_url = 'https://example.com/v1/linkedin/results'
    if change == 'missing_token':
        monkeypatch.delenv('LINKEDIN_M2_API_TOKEN')
    with pytest.raises(ValueError):
        build_linkedin_runtime(config)
    assert not (tmp_path / 'linkedin-source.sqlite').exists()


def test_disabled_bootstrap_uses_no_optional_runtime(tmp_path):
    assert build_linkedin_runtime(SimpleNamespace(data_dir=tmp_path)) is None


@pytest.mark.parametrize('mode', ['redirect', 'wrong_ack'])
def test_sink_does_not_follow_redirect_or_accept_wrong_ack(tmp_path, monkeypatch, mode):
    destinations = []
    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != '/v1/linkedin/results':
                destinations.append(self.path)
            if mode == 'redirect':
                self.send_response(307)
                self.send_header('Location', '/redirected')
                self.end_headers()
            else:
                body = json.dumps({'job_id': 'wrong', 'receipts': []}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    try:
        selected = build_linkedin_runtime(manifest_config(tmp_path,
                    f'http://127.0.0.1:{server.server_port}/v1/linkedin/results'))
        with pytest.raises(Exception):
            selected.deliver(wire())
        assert destinations == []
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
