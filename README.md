# NOS LinkedIn evidence components

Standalone LinkedIn parsing and evidence components for NOS. This is the primary development repository. NOS integration lives on [`feat/linkedin-m1` in NOS-V1](https://github.com/Tech-at-Upties/NOS-V1/tree/feat/linkedin-m1).

The current implementation includes conservative normalized company-feed parsing, source-specific wire serialization, and a permission-gated original-byte journal with expiry, replay and durable stop handling. It does not register a live collector or recurring workload.

Attempt admission is committed before transport runs. If the process exits before a response is committed, replay returns `dispatch_unknown` and never resends that attempt. This includes a possible crash before any request was sent: the journal cannot distinguish it from a transmitted request whose response was lost. Transport failures and invalid or oversized responses also retain a durable attempt outcome. These states do not establish source absence or successful collection.

## Install and test

Use Python 3.11 or later. Verification was run locally on Python 3.12.

```powershell
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e .
./.venv/Scripts/python.exe -m pip install -r requirements-verification.txt
./.venv/Scripts/python.exe -m pytest tests -q
```

Synthetic tests verify parser, field-state, failure and durability behavior. A private projected-receipt check skips when its local artifact is absent. Synthetic/projected checks do not prove live source semantics.

For controlled NOS integration, check out its `feat/linkedin-m1` branch separately and set `NOS_INTEGRATION_ROOT` to that checkout before running tests. Install this package in the same verification environment. The actual M1 serializer, M2 converter/model and Store are exercised; no live task or HTTP route is enabled.

`NOS_M2_TEST_DATABASE_URL` enables the integration suite's real Postgres storage cases using temporary schemas. Use a dedicated test instance; those schemas are removed by the tests. On a machine with PostgreSQL 17 already installed, `scripts/Local-VerificationPostgres.ps1` can create an isolated project-owned cluster on localhost port 15432. Check ownership with `scripts/check-verification-postgres.py` before running fixtures that truncate their dedicated test database, and stop that cluster with the helper's `-Stop` option.

## Evidence semantics

- Occurrence activity, UGC content, author, feed publisher and metric target remain separate native IDs.
- Company-feed membership and collaboration header text do not establish company authorship or a repost.
- Missing, null, invalid and zero fields remain distinct. Native numLikes is not silently treated as LIKE-only reactions.
- Explicit graph references define membership and original relationships. Conflicting entities are not resolved by last-write map behavior or numeric suffix.
- Relative age text is not an absolute publication timestamp. Relevance order and reported totals do not prove chronological completeness or source exhaustion.
- Original-byte receipts, allowlisted source projections and synthetic fixtures remain different evidence classes.
- Raw retention expiry does not assert source deletion. Downstream derived-data policy enforcement is still open.

## Access and current limits

Collection and retention require an established permitted route. A browser login alone does not establish that permission. [LinkedIn's current website automation guidance](https://www.linkedin.com/help/linkedin/answer/a1341387/prohibited-software-and-extensions) restricts automated collection; [official organization post reads](https://learn.microsoft.com/en-us/linkedin/marketing/community-management/shares/posts-api?view=li-lms-2026-06) require appropriate API permissions and page roles.

The native browser probe refuses before launch without a confirmed, unexpired permission record for its exact route and target. Do not create that record to bypass the access requirement. Session profiles, credentials, source receipts and local controller instructions are excluded from this repository.

Current gaps include recipe-specific termination, production transport/access/retention, task/session registration, Redis delivery/canonical fencing, live error precision and sustained workload measurement. The SQLite dispatch primitive serializes stop commits with an in-flight call; it does not cancel a transmitted request or satisfy the full NOS runtime fence by itself.
