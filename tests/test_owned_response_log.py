"""Actual northbound HTTP logging must not create retained publication copies."""
import json
from http.server import ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from urllib.parse import urlparse

import httpx
import pytest

from test_job_runtime import BODY, Handler, ledger, runtime, worker
from xingestion.api_request_log import ApiRequestLogger, log_body, truncate_body
from xingestion.capabilities import CapabilityPlanner
from xingestion.tasks import TaskState
from xingestion.web import live_server


class LoggingHandler(Handler):
    _record_api_response = live_server.LiveAppHandler._record_api_response
    _set_api_key_log_context = live_server.LiveAppHandler._set_api_key_log_context

    def do_GET(self):
        self._begin_api_request_log(urlparse(self.path))
        super().do_GET()

    def do_POST(self):
        self._begin_api_request_log(urlparse(self.path))
        super().do_POST()


def config(root, max_bytes=12000):
    return SimpleNamespace(api_keys={'test-key-a': 'client-a'}, max_active_tasks_per_capability=2,
        api_request_log_enabled=True, api_request_log_dir=root / 'logs', data_dir=root,
        api_request_log_max_body_bytes=max_bytes)


def test_actual_http_result_does_not_log_publication(ledger, tmp_path, monkeypatch):
    cfg = config(tmp_path)
    logger = ApiRequestLogger(cfg)
    calls = []
    state = SimpleNamespace(ledger=ledger, planner=CapabilityPlanner(None), api_key_store=None,
        linkedin_runtime=runtime(tmp_path, lambda wire: None, calls), config=cfg,
        api_request_logger=logger)
    monkeypatch.setattr(live_server, 'STATE', state)
    server = ThreadingHTTPServer(('127.0.0.1', 0), LoggingHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with httpx.Client(base_url=f'http://127.0.0.1:{server.server_port}',
                          headers={'Authorization': 'Bearer test-key-a'}, timeout=5) as client:
            job = client.post('/v1/jobs', json=BODY).json()['job_id']
            assert worker(ledger, state.linkedin_runtime)._process_delivery(job).state == TaskState.DONE
            response = client.get(f'/v1/results/{job}')
            assert response.status_code == 200
            assert 'Own commentary' in response.text
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
    text = '\n'.join(path.read_text(encoding='utf-8') for path in cfg.api_request_log_dir.glob('*.jsonl'))
    assert 'Own commentary' not in text
    assert 'LinkedIn collaborated on this' not in text
    assert 'test-key-a' not in text
    entry = logger.list_recent()[0]
    assert entry['response']['status'] == 200 and entry['path'] == f'/v1/results/{job}'
    assert len(calls) == 1


@pytest.mark.parametrize('shape', ['direct', 'nested', 'json_string', 'json_bytes', 'oversized'])
def test_append_defends_owned_payloads_before_truncation(tmp_path, shape):
    owned = {'source_type': 'linkedin', 'items': [{'publication': 'private-publication-marker',
             'author': 'private-author-marker', 'metrics': {'secret_metric_marker': 12}}]}
    body = owned
    if shape == 'nested':
        body = {'results': [owned], 'label': 'wrapper'}
    elif shape == 'json_string':
        body = json.dumps(owned)
    elif shape == 'json_bytes':
        body = json.dumps(owned).encode()
    elif shape == 'oversized':
        owned['items'][0]['publication'] += 'x' * 20000
    logger = ApiRequestLogger(config(tmp_path, 256))
    path = logger.append({'path': '/v1/results/test-job', 'response': {'status': 200, 'body': body}})
    text = path.read_text(encoding='utf-8')
    assert 'private-publication-marker' not in text and 'private-author-marker' not in text
    summary = logger.list_recent()[0]['response']['body']
    assert summary['omitted'] == 'owned_source_payload'
    assert len(summary['sha256']) == 64 and summary['original_bytes'] > 0


def test_unrelated_logging_stays_visible(tmp_path):
    logger = ApiRequestLogger(config(tmp_path))
    path = logger.append({'path': '/v1/results/x-job', 'response': {'body': {'source_type': 'x', 'items': ['visible']}}})
    assert 'visible' in path.read_text(encoding='utf-8')


@pytest.mark.parametrize('body', [
    {'retention': {'mode': 'ingress_only'}, 'text': 'publication-marker'},
    {'items': [{'retention': {'mode': 'ingress_only'}, 'text': 'publication-marker'}]},
    {'capability_id': 'LINKEDIN_COMPANY_FEED', 'payload': {'text': 'publication-marker'}},
    {'nested': json.dumps({'source_type': 'linkedin', 'text': 'publication-marker'}), 'copied_text': 'publication-marker'},
])
def test_owned_detection_precedes_truncation_and_omits_wrapper(body):
    safe, truncated = truncate_body(log_body(body), max_bytes=256)
    assert not truncated and safe['omitted'] == 'owned_source_payload'
    assert 'publication-marker' not in json.dumps(safe)


@pytest.mark.parametrize('binary', [False, True])
def test_opaque_result_string_never_keeps_a_prefix(tmp_path, binary):
    logger = ApiRequestLogger(config(tmp_path, 256))
    body = 'publication-marker' + 'x' * 20000
    if binary:
        body = body.encode()
    path = logger.append({'path': '/v1/results/test-job',
                          'response': {'body': body}})
    assert 'publication-marker' not in path.read_text(encoding='utf-8')


def test_append_does_not_mutate_response_or_request(tmp_path):
    body = {'source_type': 'linkedin', 'items': [{'text': 'publication-marker'}]}
    entry = {'path': '/v1/results/test-job', 'request_body': body, 'response': {'body': body}}
    logger = ApiRequestLogger(config(tmp_path))
    logger.append(entry)
    assert entry['request_body'] is body and entry['response']['body'] is body
