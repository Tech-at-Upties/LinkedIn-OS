# WR-006 — Response diagnosis and durable stop boundaries

**Task:** Owned read-only LinkedIn collection for NOS M1.  
**Investigation date:** 2 October 2026.  
**Incoming checkpoint:** LI-M1-S005.  
**Outgoing checkpoint:** LI-M1-S006.  
**Operation:** Inspect source classifiers, pause/caller behavior and HTTP semantics; execute copied bodies against synthetic responses; implement and falsify a bounded persistent stop-state model.  
**Status:** Completed bounded research/prototype pass. No live LinkedIn request, supplier account connection, production M1 change, selected production transport or final blueprint.

## 1. A stopped account does not necessarily mean a stopped acquisition task

The most consequential new failure was in **our first local model**, not in M1: after a synthetic challenge it held the affected account, but admitted the same task when supplied with a different fictional account. That violated the investigation's existing prohibition on continuing blocked activity through identity rotation. The failing probe is retained. The repaired model records a separate task hold alongside the account/session hold, so reassignment cannot silently remove the task's stop condition.

That finding emerged after44 initial tests passed. Three additional tests exercise challenged-task reassignment, rate-limited-task reassignment and clearing an account hold without clearing the task. The final suite has **47 passing Python tests**. This is a local logical-admission guarantee under trusted task identities, not a whole-production scheduler or physical network barrier.

The related source inspection explains why a single failure label is insufficient. A classifier must preserve **what was observed**, **what diagnosis that supports**, **which work must stop**, **what can still be preserved**, and **who or what can resume it**. A security-pattern hypothesis may justify conservative stopping without proving a current native challenge signal. A valid JSON object is not evidence that a particular collection capability succeeded.

### Why this operation was selected

S005 left current response classification and real pause behavior untested after several parser/provenance passes. More supplier feature descriptions would not resolve that gap. Existing source files made classifier and control-flow testing possible without credentials or deliberately triggering an upstream security check. H06 was the main target; H01/H02/H09 production checks and H04/H05/H10 native captures remained separate.

## 2. The source authors explicitly describe uncaptured security-response hypotheses

The toolkit limitations document says its project had not captured a live security check on an API request. The assumed final paths, English text and JSON carriers were based on code reasoning/general API expectations, not captures from the relevant endpoints. This qualification matters more than the recency of the classifier's September30 hardening. New tests here remain **synthetic**, and passing them does not estimate current LinkedIn signal precision. [R71](sources006/inspection-notes.md#r71--the-source-itself-discloses-that-its-security-response-shapes-were-not-captured)

Primary sources actually inspected:

| Record | Inspected source | Use and limit |
|---|---|---|
| R68 | Toolkit `classify-response.js`, complete returned functions |18 copied-body classifier cases; no full-file byte-identity claim |
| R69 | Toolkit `voyager-core.js`, lines1–155 and150–310 | Relevant request/helper control flow;8 transport/state cases use simulated dependencies |
| R70 | Toolkit `quota.js`, lines1–340 | Pause/backoff/clear functions and stored-object scope; Chrome persistence/locking not run |
| R71 | Toolkit security-check limitations, lines150–300 | Explicit provenance downgrade for path/JSON/wording hypotheses |
| R72 | mayai CLI client, lines215–305 | Discovery catch and candidate construction inspected, not executed |
| R73 | mayai CLI search, lines1–230 | Query-loop failure propagation and route/docstring distinctions inspected, not executed |
| R74 | RFC6585 §§4,6 |429 scope and network-auth semantics, not platform-specific measurements |
| R75 | RFC9110 §10.2.3 | Retry-After seconds/date contract |
| R76 | RFC7725 §3 |451's standard meaning; nonstandard platform use still needs evidence |

Exact source URLs, repository refs, blob IDs and ranges are in `sources006/source-registry.json`. Toolkit ref is `e2a34b5e3c51a2328a51994e153f74fbb8431805`; mayai ref is `cac3356c736707073b31984c7357e37d0b1ae189`. Neither is asserted to be the version deployed by LinkedIn, M1 or any source user.

## 3. Copied classifier and request bodies expose different failure boundaries

**E021:** `prototype006/upstream_probe.mjs` executed18 classifier cases and8 transport/state cases. The classifier's executable declarations/bodies were copied with comments omitted and formatting compacted. Selected transport/quota functions were copied with local dependency adapters; message-only constants were shortened. No complete upstream module/test suite was imported. The harness's `fetch` only invokes a stub; its default throws `NO_NETWORK`.

### A body-read exception can become an apparent transport success

The source `readBody` returns an empty string when `.text()` throws. The request function later returns `{ok:true}` for empty text. A synthetic200 response whose `.text()` throws therefore produced `{ok:true}` in the copied read function. This is an actual executed output, not a claim that LinkedIn returned such a response here. [R69]

The later capability parser may reject that object. Consequently the supported finding is **loss of the body-read failure at this interface**, not proven admission of fabricated evidence by the entire toolkit. NOS should retain body completeness and failure class separately from empty-body semantics. A data-read recipe does not become successful merely because an empty response is valid for an unrelated write operation.

### A latched challenge blocks a new call but not every already-entered call

The source checks pause state and then awaits cookie retrieval before calling fetch. The probe started that function, stalled the injected cookie lookup, committed a challenge through the copied pause functions, and released the lookup. Mock-fetch calls changed from0 to1 while the challenge latch remained set. A separately initiated call after the latch was set was correctly rejected before mock fetch. [R69–R70]

This identifies a specific **preflight-to-fetch interval**. It does not show actual traffic after a real challenge, global bypass, or automatic alternate-account selection. A worker that already passed a gate is not the same as a new invocation. Actual final dispatch/cancellation/fencing needs its own test; the new local model intentionally claims only logical-admission ordering.

### A fixed backoff can undercut a supplied delay, and clearing a challenge can remove it

With a synthetic429 carrying `Retry-After:7200`, the copied transport/quota path stored a900-second timer. It passes only the status to `noteBackoff`, not the header. After setting a challenge, `clearChallenge` removed both the challenge and that timer. [R69–R70]

These are source choices, not measurements of a safe request interval. RFC9110 allows seconds or an HTTP date; a local default must not silently shorten a valid server-specified wait. RFC6585 does not reveal whether a429 applies per resource, identity, server or some combination. The new model preserves the not-before value and records unknown upstream scope rather than copying X's capability-specific assumption. [R74–R75]

### Classification patterns must not be promoted into native truth

Selected18-case outcomes:

| Synthetic input | Copied helper outcome | Meaning for NOS |
|---|---|---|
| Normal nested data discussing captcha/security checks | `ok` | Positive control: ordinary source words are not always mistaken for system signals |
| Nested `challengeUrl` field in otherwise normal data | `ok` | Root-key distinction works for this case |
| Word `challenge` only in query string | `ok` | Query text does not trigger the path rule |
| Root `challengeUrl:null` | `challenge` | Key presence alone exceeds what the value establishes |
| Foreign-origin URL with a checkpoint path | `challenge` | Path-only diagnosis does not establish which service issued it |
| `/login-help` | `signed_out` | Prefix matching is broader than a login-path boundary |
| HTTP403 plus challenge text inside nested content | `challenge` | Generic refusal plus prose is insufficient evidence of a native security challenge |
| HTTP451 without any challenge evidence | `challenge` | Standard code meaning differs; platform-specific override needs evidence |
|200 JSON with root status429 | `ok` | Body carrier and transport status are distinct; an item validator is still needed |
| Malformed JSON or nonredirected plain text | `ok` | This helper does not perform JSON/capability validation |
| Unrecognized-language HTML or marker after prefix limit | `error` | Still refused, but no confirmed sign-out/challenge diagnosis |

The remaining controls cover a known-pattern checkpoint path, generic403,401 and429. Full exact inputs/outputs are in `results006/final-run/upstream-probe.json`. No frequency or current-platform accuracy claim follows from these inputs.

### Sign-out and discovery errors need caller-level propagation

Two explicit calls to the copied request function, each receiving401, reached the mock fetch twice: the selected code throws sign-out but does not set a persistent session hold. Other caller guards were not executed. That is an interface requirement for NOS, not proof of automatic repeated requests in the real application. [R69]

In the separate direct client, HTML query-ID discovery catches all `LinkedInAPIError` values and returns no discovered identifier; the outer builder still supplies cached/static candidates. The search-query loop itself only continues on500 and rethrows other query failures. A discovery failure can therefore lose a stand-down meaning before the narrower query loop runs. This is **inspected control-flow inference**, not an executed mayai client experiment. Its file-level REST description also differs from the actual people-GraphQL/company-REST branches; no universal current route verdict was derived. [R72–R73]

## 4. The new boundary separates diagnosis, preservation and stop scope

**E022:** `prototype006/response_boundary.py` supplies a bounded original implementation, not a production replacement for M1 orchestration. Its input is a `Context` with local account/session/route/task labels and a `Response` containing already acquired permitted bytes, HTTP status, final URL, content type, receipt time, explicit body completeness and optional Retry-After. It never receives or obtains a live cookie and never sends a request.

The normal sequence is:

1. `GateStore.begin` checks active holds and records one **logical attempt admission** in a SQLite transaction. Reusing the same attempt ID is not permission to send again.
2. `Boundary.observe` attempts immutable raw-byte storage before JSON interpretation. The stored body hash/reference remains separate from diagnosis.
3. The classifier records transport facts, a bounded diagnostic interpretation and an appropriate local hold. Known-path/JSON-pattern rules are explicitly labeled **source hypotheses**, not live-validated native detectors.
4. The event and its hold changes commit together. Duplicate event identity with identical contents is a no-op; conflicting reuse fails closed.
5. Only a persisted, complete JSON candidate may proceed toward a separate capability parser. **No result from this laboratory claims capability success or advances a source/consumer cursor.**

If raw storage fails, a challenge pattern still produces its stop state and no data candidate. If safety/event persistence fails, the Boundary instance stops admitting more work. That process-local emergency stop is not a claim of distributed/fleet coordination. A crash between separate raw and database stores can leave an orphan file; reconciliation and power-loss guarantees remain unresolved.

### Different failures have different consequences

| Observation or pattern | Primary local hold | Additional protection and limits |
|---|---|---|
| Trusted-origin checkpoint pattern | Account+origin | Same task also held; hypothesis label remains; no identity substitution |
|401 or trusted sign-in path | Session | Same task held; not a suspension claim or automated sign-in |
|429 | Conservative account+origin | Same task held, supplied not-before preserved, true upstream limit scope unknown |
|999 | Account+origin review | Same task held; neither a measured rate limit nor confirmed challenge |
|403 or404 | Task | No account ban or source-deletion inference |
|451 alone | Task restriction review | Standard reason recorded; challenge not established |
| Foreign final origin / network authentication | Route | Source-account health not inferred from network conditions |
| Invalid/incomplete/empty data-read body | Task/capture review | Never equivalent to an empty collection or successful evidence |
| Valid JSON without error carrier | No hold from this classifier | Capability parser, source coverage, permissions and release approval still required |

Recognizing an ordinary source string is not diagnosing a challenge. Unknown HTML is refused without asserting its meaning. A root body-error carrier preserves partial data but does not pretend the HTTP status changed. The exact supported envelope checks remain conservative and experimental.

### Holds cannot silently clear one another

Each hold has a scope key, reason, revision, first-observed time, optional not-before and clear state. An acknowledgement names one exact reason/revision plus caller-supplied operator/evidence labels; stale acknowledgements fail. Authentication of that operator is outside the laboratory. Rate holds cannot be cleared through the challenge-acknowledgement method. Multiple rate observations cannot shorten a known later reset; an unknown reset remains blocked pending external policy. A late successful response from already-admitted work is stored without clearing newer holds.

In the reproduced timeline, rate and challenge create account and task holds. A same-task/different-account admission is denied. Clearing only the reviewed challenge reasons leaves the two rate holds until their specified expiry. This tests separate reasons, not a live resume workflow or a safe workload.

## 5. The first model failed an important test despite a green initial suite

**E023:** after44 tests passed, the probe changed only the account/session labels for the same challenged task. The initial model admitted it. `results006/task-reassignment-before-fix.json` records `contract_pass:false` and zero network calls. The entire pre-fix model and tests remain available.

The repair adds task holds alongside account/session holds. It does not ban every account or merge unrelated tasks. The three new tests prove the intended local rule under trusted task identity: reassignment cannot erase a challenged or rate-limited task's state, and clearing an account reason alone does not clear the task. Existing tests were updated to expect both independent scope records. `results006/task-hold-fix.diff` records the exact changes.

This is **our prototype defect and repair**. It does not establish an M1 production bug or an upstream toolkit flaw. An actual scheduler must preserve task/acquisition lineage when it creates retries; issuing a new task ID to disguise a blocked retry is outside what a local table can identify. That remains an integration requirement.

## 6. The executed results include recovery but not a physical network guarantee

| Check actually run | Outcome |
|---|---|
| Initial new Python suite |44 passed |
| Additional task-reassignment probe |Failed intended rule, preserved |
| Final new Python suite |47 passed,0 failures/errors/skips |
| Copied classifier cases |18 passed |
| Copied request/pause cases |8 passed |
| Unchanged decoder dependency |33 Python tests passed separately |
| Child-process recovery |Exit74 after committed challenge; reopened database denied another logical admission |
| Network/LinkedIn/provider traffic |None from the laboratories |

Recorded environment: Python3.13.5, Nodev22.16.0, SQLite3.46.1, Linux. The inherited runner terminal warning appears after successful test output and exit0; it is not hidden or counted as a failed test. No old Node, post, meaning, provider-receipt or original history suite was rerun in this pass. Their earlier code/results remain unchanged.

The47 tests also cover exact raw bytes, malformed/duplicate/nonfinite JSON, explicit empty versus incomplete bodies, trusted origin/path boundaries, null/invalid challenge fields, nested source text, distinct HTTP/body status, non-English HTML, unknown rate reset, HTTP-date/seconds waits, task/session/account/route isolation, duplicate/conflicting event IDs, late-success behavior, raw-store failure, safety-store failure, stale acknowledgements and logical-admission replay refusal. The executable test names, sample verdicts and hold timeline travel in `results006/final-run/`.

### The physical dispatch gap remains deliberately open

A SQLite transaction can order **logical admission** against a committed hold. It cannot itself prove that an already-admitted request will not later leave a socket, cancel a request already in flight, or coordinate every production worker. The copied deferred-cookie probe makes this deficit concrete. Actual worker/transport staging must check the latest state at the last practical dispatch boundary and define cancellation/quarantine of already-admitted work without discarding permitted raw evidence.

A later isolated test should suspend a worker between selection and actual transport invocation, commit a stop from another worker, then release the first. Record logical ticket, gate generation, actual transport-call count and raw/admission outcome. Passing the new logical model is not a substitute for that test. Do not provoke a platform challenge; use a recorded sanitized signal or injected local response.

## 7. M1 remains the benchmark rather than being redesigned by this laboratory

The project source describes source-specific one-attempt calls, production-owned retries, raw-before-parse, account/session separation, immediate human handling of challenges and distinct failure scopes. These obligations remain intact. The laboratory demonstrates a small set of required behaviors; it does not certify M1's existing implementation or replace its PostgreSQL/outbox/fencing machinery with SQLite.

The earlier documented missing-history routes, candidate-versus-approved planning ambiguity, change-only engagement policy, RSS qualifications, source-policy variants and provider route/retention findings remain in the checkpoint. No first production capability, account count, polling interval, supplier, framework or transport was selected. No existing production repository was modified.

### Reversal and refinement conditions

Current sanitized source responses can replace assumed classifier patterns with actual endpoint-specific evidence. A real native error schema may distinguish challenge, suspension, permission failure and protocol drift more precisely; generic codes/words alone do not. A verified scope for a rate limit can narrow the conservative hold, but must not become an identity-rotation workaround. Whole-caller evidence may show additional upstream guards, narrowing reuse concerns without changing the copied-function outputs. Production fencing/dispatch tests can advance the physical stop guarantee; the local admission test cannot.

## 8. The next operation should inspect the real downstream boundary

The owned-route safety gap now has executed source-model evidence and a tested local component. The next high-value operation is to attempt read-only inspection of the **named NOS-M2 repository and its actual `EvidenceEnvelope` intake/correction paths**, using the preserved report's `ahansardar/NOS-M2` reference as a source lead. Its current accessibility and deployment status are **not established**. A readable source tree can settle concrete source enum, partial-input, clock, identity and correction constraints before a larger integrated prototype is built. The actual M1 production repository and target deployment still need verification.

H04/H05/H10 current original/reshare/page-two evidence remains active. The classifier experiment does not repair missing native captures or make guessed post fields implementable. H12–H15 acquisition agreement, supplier clock/sidecar/retained-history/exit questions remain active; additional vendor feature pages are lower value than those exact deficits. The physical-dispatch experiment above is new explicit H16 debt, not a reason to declare the entire research blocked.

## 9. The cumulative artifact remains the restart basis

The incoming pass005 archive passed CRC checking and all164 manifest entries matched. Incoming STATE matched the separately mounted file. The initial extraction retained the ZIP's top directory and the root STATE check consequently failed; moving the recovered contents up one level corrected the path without changing baseline bytes. That administrative recovery is recorded separately from source/model findings. Incoming STATE, README and manifest are archived under `checkpoints/` before replacement.

The final source-copy check initially included all17 source-manifest entries rather than only the12 current `source_snapshots`. Two historical Library files have no current root mount, and the deliberately older Source of Truth has different bytes. That failed check was corrected to compare the intended12 current snapshots; all match. Historical inputs remain verified against their incoming hashes, not replaced with current same-named files. `results006/packaging-checker-correction.json` preserves this administrative scope error; no source changed.

New files:

- `sources006/`: exact source pointers/ranges/blob IDs, attributed inspection qualifications, failed download/DNS results and the toolkit MIT notice.
- `prototype006/`: original classifier/hold model, copied-body harnesses,47-test suite, reproduction runner and full limits.
- `results006/`: initial44 and final47 outputs, actual failed reassignment probe, pre-fix files, exact patch,18+8 copied-body outputs, decoder regression, sample verdicts/hold timeline and continuity audits.
- `STATE.md`: complete LI-M1-S006 replacement; unchanged historical findings and their qualifications remain accessible.

Byte/text audits establish package continuity, not perfect semantic retention, current platform truth, source rights or distributed operational reliability.

**OPERATION:** Classifier/caller inspection and durable stop-boundary testing.  
**DELTA:**18 classifier cases,8 request/pause cases,47 final local tests and33 unchanged decoder regressions; body-read/cooldown/preflight boundaries exposed; same-task reassignment failure repaired and retained.  
**OPEN HINGE:** Live signal precision, physical dispatch fencing, current native original/reshare/pagination evidence, actual M1/M2 mapping, supplier fit and sustained operation.  
**ROUTE:** NEXT. No final blueprint or readiness certification.
