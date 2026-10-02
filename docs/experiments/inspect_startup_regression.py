"""Record the unavailable infrastructure in the existing M1 preflight test."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
NOS = ROOT / '.local/nos-integration/M1'
sys.path[:0] = [str(NOS / 'src'), str(NOS / 'tests')]
import test_northbound_api
from xingestion.web import live_server

run = live_server.DeploymentPreflight.run
checks = []
def record(self):
    result = run(self)
    checks.extend({'name': check.name, 'status': check.status} for check in result.checks)
    return result
live_server.DeploymentPreflight.run = record
case = test_northbound_api.NorthboundApiTests('test_startup_route_returns_preflight_checks')
case.setUp()
try:
    try:
        case.test_startup_route_returns_preflight_checks()
    except AssertionError:
        status = 'existing_preflight_rejects_unavailable_infrastructure'
    else:
        status = 'passed'
finally:
    case.tearDown()
print(json.dumps({'status': status, 'checks': checks}, indent=2))
