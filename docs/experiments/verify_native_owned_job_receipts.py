"""Check stored metadata and erase expired source bytes; never replay a capture."""
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from nos_linkedin.acquisition import Journal

ROOT = Path(__file__).resolve().parents[2]
receipt = json.loads((ROOT / 'docs/results/native-job-8be615ad6b0d4e83bee99ac70088f39f.json').read_text())
with psycopg.connect('postgresql://nos_test@127.0.0.1:15432/nos_linkedin_owned_jobs') as connection:
    assert Path(connection.execute('SHOW data_directory').fetchone()[0]).resolve() == (ROOT / '.local/verification-postgres').resolve()
    task = connection.execute('SELECT state, attempt_count, result_json FROM capability_tasks WHERE task_id=%s',
                              (receipt['root_job_id'],)).fetchone()
    assert task[0] == 'DONE' and task[1] == 2
    result = task[2] if isinstance(task[2], dict) else json.loads(task[2])
    assert 'items' not in result
with sqlite3.connect(ROOT / receipt['sink_database']) as connection:
    # Count provenance only. Do not read or print retained publication text.
    counts = {table: connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
              for table in ('deliveries', 'observations', 'evidence_occurrences')}
    assert counts == dict.fromkeys(counts, 10)
    jobs = connection.execute('SELECT DISTINCT m1_job_id FROM evidence_origins').fetchall()
    assert jobs == [(receipt['root_job_id'],)]
journal = Journal(ROOT / receipt['source_journal'])
purged = journal.purge_expired(datetime.now(UTC))
with journal.connect() as connection:
    sources = connection.execute('SELECT attempt_id, body IS NULL AS body_erased, outcome FROM responses').fetchall()
    assert len(sources) == 1 and sources[0]['body_erased']
report = {'verified_at': datetime.now(UTC).isoformat(), 'root_job_id': receipt['root_job_id'],
          'task_state': task[0], 'task_attempt_count': task[1], 'metadata_only_task_result': True,
          'm2_counts': counts, 'source_bytes_erased': True, 'newly_purged': purged,
          'network_reads': 0, 'limitation': 'Existing M2 copies lack a retention deadline; WR-018 must repair the boundary.'}
(ROOT / 'docs/results/native-owned-job-receipt-verification.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
