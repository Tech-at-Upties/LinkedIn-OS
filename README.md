# NOS LinkedIn evidence components

Standalone LinkedIn parsing and evidence components for NOS. This is the primary development repository. NOS integration lives on [`feat/linkedin-m1` in NOS-V1](https://github.com/Tech-at-Upties/NOS-V1/tree/feat/linkedin-m1).

The current implementation includes conservative normalized company-feed parsing, source-specific wire serialization, and an original-byte journal with explicit collection/retention flags, expiry, replay and durable stop handling. The NOS integration explicitly enables a bounded saved/live company-page runtime. Recurring collection is not registered.

Attempt admission is committed before transport runs. If the process exits before a response is committed, replay returns `dispatch_unknown` and never resends that attempt. This includes a possible crash before any request was sent: the journal cannot distinguish it from a transmitted request whose response was lost. Transport failures and invalid or oversized responses also retain a durable attempt outcome. These states do not establish source absence or successful collection.

Saved responses must be classified before another attempt can dispatch on the source scope. `classification_pending` blocks new requests until the original attempt is replayed without a resend. Expired unclassified bytes leave `classification_expired` and keep that barrier; retained HTTP access status can still establish a durable hold. Replay is bound to the exact original feed publisher and evidence class. `Journal.capture` requires `feed_publisher_id`; older receipts without that context remain readable but cannot be relabeled during replay. Permission flags require actual boolean `True` values.

The NOS branch's opt-in `xingestion.linkedin.executor.execute_company_page` connects one owned source callback to the M1 serializer and a validated M2 admission callback. Its source attempt remains distinct from the generic runtime's root task. The Journal checks the source fence, pending classification and expiry around physical delivery; a committed stop suppresses delivery, while a stop arriving during delivery waits for that callback. Explicit replay after an ambiguous delivery can deduplicate in M2 without rereading the source. Generic PostgreSQL task/outbox and actual Redis worker dispatch are implemented; bounded delivery-only retries preserve the original source attempt and expiry.

Delivery callbacks must use a separate destination database and must not reenter the Journal to write. The guard serializes local callback invocation. NOS integration adds committed M2 source/task-generation acknowledgements to reject delayed arrivals after source stop, pending-task cancellation or acknowledged takeover. A local stop alone leaves the remote acknowledgement window unresolved. Transport-generation metadata does not create another source observation or delivery identity.

The Journal can retain native responses, allowlisted source projections and synthetic fixtures. It preserves each capture's evidence class through replay. Its digest covers exactly the callback's bytes: retaining a projection does not retain the native body or prove native byte fidelity. The probe keeps the original native-body digest separately in its private receipt.

`CompanyPageRuns` stores bounded run context, exact page requests and expiring owner tokens in the source Journal DB. Its stable page attempt survives reservation/reclaim and crash recovery. The NOS branch's opt-in `execute_leased_company_page` checks ownership under the physical source/delivery lock, validates returned paging before M2 admission and advances only from a matching committed delivery acknowledgement. Crash recovery or ambiguous delivery reuses the page/job without rereading the source. Short/empty/partial pages, unproved range boundaries and budget exhaustion keep source completeness unknown. The next relevance offset is a bounded candidate, not a complete-history cursor.

Ownership guards are read-only; callbacks must not reenter Journal/run writes. Durable acknowledgement receipts support cursor checkpoints, not automatic delivery retries or a distributed outbox. A source admission followed by owner loss can remain `dispatch_unknown` even if the guard prevented transport; recovery does not invent a safe resend.

Overfull pages also pause progress without claiming completeness. Actor fields retain missing, null and invalid states through the NOS boundary. Malformed metric targets remain unknown; duplicate reaction kinds are omitted as ambiguous while independently reported native aggregates remain available.

`Journal.purge_expired` removes expired retained bytes independently of result requests while preserving unresolved classification barriers. The NOS branch has a controlled source-specific generic job runtime with actual PostgreSQL ownership, Redis delivery, authenticated northbound results and local M2 HTTP admission. Generic task storage holds coverage/expiry metadata without duplicating source text; results are rebuilt only from eligible acknowledged bytes. See NOS `M1/docs/LINKEDIN_CONTROLLED_RUNTIME.md` for explicit saved-projection setup. Saved mode makes no LinkedIn requests. Separately declared live startup and remote source/task fencing have controlled verification; sustained operation and enabled derived processing remain open.

## Company publication jobs

The NOS branch accepts `LINKEDIN_COMPANY_FEED` through authenticated `POST /v1/jobs`. Supplying `requested_count` selects the count contract, version 2. Omitting it preserves the existing version-1 single-page job. The API chooses this version from the payload; callers constructing `CapabilityRequest` directly must supply the matching contract version.

```json
{
  "capability_id": "LINKEDIN_COMPANY_FEED",
  "idempotency_key": "company-publications-15",
  "payload": {
    "company_url": "https://www.linkedin.com/company/linkedin/posts/",
    "feed_publisher_id": "urn:li:fsd_company:1337",
    "page_mode": "initial_document",
    "requested_count": 15,
    "max_pages": 3,
    "ordering": "relevance"
  }
}
```

`requested_count` targets unique occurrence IDs returned to the caller. It is separate from `max_pages`, source page sizes and the native read cap. The implemented batch path preserves initial start 0/count 3, API start 3/count 10, and optional continuation start 13/count 10 as separate source pages. It selects at most the requested count before M2 admission and preserves the same ordered IDs through delivery-only replay. Saved fixtures verify count and recovery behavior; native execution of this batch path is still pending.

The initial route currently supports this fixed LinkedIn company URL and its independently observed feed ID. A URL alone does not resolve the publisher ID, and arbitrary company URLs are not established by this example. Keyword search, member-account feeds, particular-post lookup, recent ordering and date filters are not enabled. Current ordering is relevance.

For an installed version-3 observation grant, `XINGESTION_LINKEDIN_GRANT_PATH` selects one bounded browser batch of two or three pages within the grant's page cap. Configure exactly one of grant, saved replay or legacy live mode. The worker consumes one capture grant, persists all returned projected pages before delivery, and classifies every saved member before the first M2 admission. A missing continuation becomes a shortfall, never another browser launch. Grant installation is operator configuration; ordinary jobs neither issue grants nor replenish capture allowances. See the [NOS controlled runtime guide](https://github.com/Tech-at-Upties/NOS-V1/blob/feat/linkedin-m1/M1/docs/LINKEDIN_CONTROLLED_RUNTIME.md).

Fetch the job's returned `result_url` while its acknowledged source bodies remain eligible. Result `coverage` contains `requested_count`, `returned_count`, `fulfilled` and `shortfall_reason`. A succeeded bounded job can return fewer items with `fulfilled: false`; `source_complete` remains null even when the count is fulfilled. Empty/short pages, structural parse gaps, budget exhaustion and missing continuation remain distinct. Expiry or a source hold suppresses retained results rather than triggering another scrape.

## Install and test

Use Python 3.11 or later. Verification was run locally on Python 3.12.

```powershell
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e .
./.venv/Scripts/python.exe -m pip install -r requirements-verification.txt
./.venv/Scripts/python.exe -m pytest tests -q
```

Synthetic tests verify parser, field-state, failure and durability behavior. A private projected-receipt check skips when its local artifact is absent. Synthetic/projected checks do not prove live source semantics.

With the NOS integration checkout available, these focused checks use synthetic pages and isolated local stores:

```powershell
./.venv/Scripts/python.exe -m pytest tests/test_company_collection.py tests/test_company_batch.py tests/test_company_count_runtime.py tests/test_company_batch_runtime.py tests/test_company_batch_transport.py -q
node --test tests/test_company_batch_probe.cjs
```

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

Current observation identified the initial three-item collection inside an embedded response: its body references an inert element containing entity-encoded JSON. This start-0/count-3 page remains distinct from the following API start-3/count-10 page. The explicit initial runtime mode preserves those roots and separate HTML, wrapper, decoded-body and projection digests. Its current scope is the observed LinkedIn company page; complete history and other account routes remain unproved.

`docs/experiments/observe_company_bootstrap.py` provides separately declared metadata-only discovery modes with fixed consumed markers. A new declaration filename cannot rerun a consumed mode or replenish the ordinary runtime allowance. Offline checks use `node --test tests/test_probe_contract.cjs tests/test_bootstrap_contract.cjs` and `python -m pytest tests/test_bootstrap_observer.py -q`. Discovery receipts contain no replayable publication bodies.

The opt-in local experiment `docs/experiments/execute_bounded_company_read.py` connects one current first-page capture to the actual M1 executor and local M2 SQLite admission, then checks restart replay without another browser launch. It uses exact source collection roots and explicitly retains a projection. `verify_projected_company_boundary.py` checks older private projections without network requests. Both need the NOS integration checkout at `.local/nos-integration`; browser reads also need its dedicated local Chrome session and existing Playwright installation in the sibling NOS checkout. Offline probe contract checks run with `node --test tests/test_probe_contract.cjs`.

One explicitly declared current configured worker/Redis/M2 HTTP startup observed two scoped native reads and ten admissions, followed by ten source-free reconstruction duplicates. Its original source and forty M2 application envelope copies were purged at expiry, preserving provenance. This is bounded integration evidence; sustained cadence and recipe-specific termination remain unproved.

Owned M2 ingress_only mode inherits the original capture deadline, blocks analysis/derived records and runs independent expiry maintenance. M1 HTTP logs omit owned bodies before truncation and store only digests. Offline loopback tests observed browser descendants exit after owner termination and timeout; this does not cover every crash mode. Enabled derived processing/retention, fresh observations after earlier expiry, broader native semantics and production deployment remain open. See the NOS branch's M1/docs/LINKEDIN_CONTROLLED_RUNTIME.md and M2/docs/OWNED-INGRESS-RETENTION.md for acknowledgement and maintenance limits.
