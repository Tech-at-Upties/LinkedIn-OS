# NOS LinkedIn evidence components

Standalone LinkedIn parsing and evidence components for NOS. This is the primary development repository. NOS integration lives on [`feat/linkedin-m1` in NOS-V1](https://github.com/Tech-at-Upties/NOS-V1/tree/feat/linkedin-m1).

The current implementation includes conservative normalized company-feed parsing, source-specific wire serialization, and a original-byte journal with explicit collection/retention flags with expiry, replay and durable stop handling. It does not register a live collector or recurring workload.

Attempt admission is committed before transport runs. If the process exits before a response is committed, replay returns `dispatch_unknown` and never resends that attempt. This includes a possible crash before any request was sent: the journal cannot distinguish it from a transmitted request whose response was lost. Transport failures and invalid or oversized responses also retain a durable attempt outcome. These states do not establish source absence or successful collection.

Saved responses must be classified before another attempt can dispatch on the source scope. `classification_pending` blocks new requests until the original attempt is replayed without a resend. Expired unclassified bytes leave `classification_expired` and keep that barrier; retained HTTP access status can still establish a durable hold. Replay is bound to the exact original feed publisher and evidence class. `Journal.capture` requires `feed_publisher_id`; older receipts without that context remain readable but cannot be relabeled during replay. Permission flags require actual boolean `True` values.

The NOS branch's opt-in `xingestion.linkedin.executor.execute_company_page` connects one owned source callback to the M1 serializer and a validated M2 admission callback. It uses the source attempt ID as the stable job identity. The Journal checks the source fence, pending classification and expiry around physical delivery; a committed stop suppresses delivery, while a stop arriving during delivery waits for that callback. Explicit replay after an ambiguous delivery can deduplicate in M2 without rereading the source. The callback must acknowledge validated admission or raise. Task/queue registration and a durable delivery outbox remain open.

Delivery callbacks must use a separate destination database and must not reenter the Journal to write. The guard serializes local callback invocation; it cannot cancel a transmitted write or prove that a remote write will not commit later after a timeout. Full runtime/canonical quarantine remains unverified.

The Journal can retain native responses, allowlisted source projections and synthetic fixtures. It preserves each capture's evidence class through replay. Its digest covers exactly the callback's bytes: retaining a projection does not retain the native body or prove native byte fidelity. The probe keeps the original native-body digest separately in its private receipt.

`CompanyPageRuns` stores bounded run context, exact page requests and expiring owner tokens in the source Journal DB. Its stable page attempt survives reservation/reclaim and crash recovery. The NOS branch's opt-in `execute_leased_company_page` checks ownership under the physical source/delivery lock, validates returned paging before M2 admission and advances only from a matching committed delivery acknowledgement. Crash recovery or ambiguous delivery reuses the page/job without rereading the source. Short/empty/partial pages, unproved range boundaries and budget exhaustion keep source completeness unknown. The next relevance offset is a bounded candidate, not a complete-history cursor.

Ownership guards are read-only; callbacks must not reenter Journal/run writes. Durable acknowledgement receipts support cursor checkpoints, not automatic delivery retries or a distributed outbox. A source admission followed by owner loss can remain `dispatch_unknown` even if the guard prevented transport; recovery does not invent a safe resend.

`Journal.purge_expired` removes expired retained bytes independently of result requests while preserving unresolved classification barriers. The NOS branch now has a controlled source-specific generic job runtime with actual PostgreSQL ownership, authenticated northbound results and local M2 HTTP admission. Generic task storage holds coverage/expiry metadata without duplicating source text; results are rebuilt only from eligible acknowledged bytes. See NOS `M1/docs/LINKEDIN_CONTROLLED_RUNTIME.md` for explicit saved-projection setup. This mode makes no LinkedIn requests. Real Redis delivery, live startup transport, downstream derived retention and remote late canonical fencing remain open.

## Install and test

Use Python 3.11 or later. Verification was run locally on Python 3.12.

```powershell
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e .
./.venv/Scripts/python.exe -m pip install -r requirements-verification.txt
./.venv/Scripts/python.exe -m pytest tests -q
```

Synthetic tests verify parser, field-state, failure and durability behavior. A private projected-receipt check skips when its local artifact is absent. Synthetic/projected checks do not prove live source semantics.

For controlled NOS integration, check out its `feat/linkedin-m1` branch separately and set `NOS_INTEGRATION_ROOT` to that checkout before running tests. Install this package in the same verification environment. Tests exercise actual M1 serialization, generic task ownership, northbound/local M2 HTTP and M2 service/storage. LinkedIn runtime remains disabled until explicitly configured; Redis delivery is tested separately.

`NOS_M2_TEST_DATABASE_URL` enables the integration suite's real Postgres storage cases using temporary schemas. Use a dedicated test instance; those schemas are removed by the tests. On a machine with PostgreSQL 17 already installed, `scripts/Local-VerificationPostgres.ps1` can create an isolated project-owned cluster on localhost port 15432. Check ownership with `scripts/check-verification-postgres.py` before running fixtures that truncate their dedicated test database, and stop that cluster with the helper's `-Stop` option.

`XINGESTION_TEST_POSTGRES_DSN` enables the real generic job/lease cases. Point it at a separate disposable task database: the existing NOS task fixture migrates and truncates its task/outbox tables. Do not run fixtures against a live stack database or run independent truncating suites concurrently.

## Evidence semantics

- Occurrence activity, UGC content, author, feed publisher and metric target remain separate native IDs.
- Company-feed membership and collaboration header text do not establish company authorship or a repost.
- Missing, null, invalid and zero fields remain distinct. Native numLikes is not silently treated as LIKE-only reactions.
- Explicit graph references define membership and original relationships. Conflicting entities are not resolved by last-write map behavior or numeric suffix.
- Relative age text is not an absolute publication timestamp. Relevance order and reported totals do not prove chronological completeness or source exhaustion.
- Original-byte receipts, allowlisted source projections and synthetic fixtures remain different evidence classes.
- Raw retention expiry does not assert source deletion. Downstream derived-data policy enforcement is still open.

## Access and current limits

The native browser experiment uses a dedicated local session, one company page, at most one continuation and a cap of 12 scoped native reads. It blocks non-read requests and stops on challenge/restriction. It checks the landed page before continuation and preserves each run in a unique allowlisted receipt. Session profiles, credentials, source receipts and local controller instructions are excluded from publication.

The opt-in local experiment `docs/experiments/execute_bounded_company_read.py` connects one current first-page capture to the actual M1 executor and local M2 SQLite admission, then checks restart replay without another browser launch. It uses exact source collection roots and explicitly retains a projection. `verify_projected_company_boundary.py` checks older private projections without network requests. Both need the NOS integration checkout at `.local/nos-integration`; browser reads also need its dedicated local Chrome session and existing Playwright installation in the sibling NOS checkout. Offline probe contract checks run with `node --test tests/test_probe_contract.cjs`.

Current gaps include recipe-specific termination, production transport/access/retention, task/session registration, Redis delivery/canonical fencing, live error precision and sustained workload measurement. The SQLite dispatch primitive serializes stop commits with an in-flight call; it does not cancel a transmitted request or satisfy the full NOS runtime fence by itself.
