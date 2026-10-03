"""Backfill WR-017's omitted deadline from its original receipt, then erase copies."""
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / '.local/nos-integration/M2/src'))
from nos_m2.retention import backfill_expired_ingress
from nos_m2.store import Store

receipt = json.loads((ROOT / 'docs/results/native-job-8be615ad6b0d4e83bee99ac70088f39f.json').read_text())
path = (ROOT / receipt['sink_database']).resolve()
assert path.parent == (ROOT / '.local').resolve()
with sqlite3.connect(path) as connection:
    identities = [row[0] for row in connection.execute('SELECT evidence_id FROM evidence_occurrences')]
store = Store('sqlite:///' + path.as_posix())
store.initialize()
try:
    count = backfill_expired_ingress(store, evidence_ids=identities,
        expires_at=datetime.fromisoformat(receipt['projection_expires_at']), expected_job_id=receipt['root_job_id'])
    assert store.list_evidence() == []
    assert store.claim_work('expiry-verifier') is None
    with sqlite3.connect(path) as connection:
        for table in ('evidence_occurrences', 'source_versions', 'observations', 'deliveries'):
            rows = connection.execute(f'SELECT envelope FROM {table}').fetchall()
            assert len(rows) == 10
            for encoded, in rows:
                value = json.loads(encoded)
                assert value['retention_state'] == 'policy_expired'
                assert not set(value) & {'text', 'author', 'raw_payload', 'metrics', 'references', 'media'}
    report = {'checked_at': datetime.now(UTC).isoformat(), 'job_id': receipt['root_job_id'],
        'original_expires_at': receipt['projection_expires_at'], 'newly_purged_evidence': count,
        'erased_envelope_copies': 40, 'metadata_receipts_preserved': True, 'network_reads': 0,
        'limits': 'Application envelopes only; private research receipts, backups and physical database remnants are separate.'}
    (ROOT / 'docs/results/native-owned-job-sink-expiry.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))
finally:
    store.close()
