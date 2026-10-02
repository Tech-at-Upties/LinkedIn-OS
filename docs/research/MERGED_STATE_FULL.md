# STATE — Canonical merge of the two LinkedIn/M1 investigation branches

**Checkpoint ID:** `LI-M1-MERGED-001`  
**Merge date:** 2 October 2026  
**Inputs:** Branch A `NOS-LINKEDIN-STATE-006`; Branch B `LI-M1-S006`  
**Status:** **ACTIVE / NOT READY FOR FINAL BLUEPRINT. READY FOR CODEX ENVIRONMENT-BACKED CONTINUATION.**  
**Purpose:** combine the two six-pass investigations without losing branch-specific evidence, failed experiments, uncertainty, or provenance.

## 1. Merge semantics

This file is a synthesis checkpoint, not a claim that either branch independently proved a production LinkedIn collector.

Rules for continuation:

- A finding does **not** gain confidence merely because both branches discuss the same idea; check whether they used independent evidence or shared prior art.
- Branch-specific experiment IDs remain branch-specific. Do not renumber them into one fake continuous experiment history.
- When this file compresses a finding, the original Branch A/Branch B STATE and WR files remain authoritative for the exact scope, test setup, limitations, source names, and historical sequence.
- If the two branches differ, preserve the difference until source/code/live evidence resolves it.
- Neither branch had a real authenticated LinkedIn collector, sustained live workload, or verified production M1→M2 LinkedIn integration.
- The branch documents refer to cumulative ZIP/archive artifacts that were apparently internal to those chats and were **never delivered to the user**. Treat those paths as referenced-but-undelivered. Do not make continuation depend on recovering them.

## 2. Objective and hard scope

Build an **owned, production-grade, read-only LinkedIn collection capability for NOS M1**.

Use the existing X/M1 implementation as the engineering benchmark for durability, evidence preservation, typed failures, retries, release discipline and coverage honesty, but do not force LinkedIn into X identities, endpoints, pagination, cadence, account counts, browser assumptions, or capability boundaries.

Production scope remains read-only. Do not build posting, messaging, connection requests, comments, reactions, follow/unfollow, or other interaction automation.

Account/security constraints remain binding:

- legitimate authenticated sessions and permitted reads only;
- no password/cookie/token sharing into chat/project artifacts;
- stop on challenge/suspension/access restriction rather than automate around it;
- no CAPTCHA solving, ban evasion, identity rotation, or proxy/account substitution to continue blocked work;
- distinguish keeping a valid session healthy from defeating a platform action.

The preferred end-state is an owned collector. Official APIs, providers, public-page acquisition, exports, and other legitimate routes remain supplements/fallbacks where evidence supports them.

## 3. Evidence vocabulary

Use these statuses when carrying findings forward:

- **DEMONSTRATED_LOCAL** — code was actually executed locally, only within its stated fixture/model.
- **INSPECTED_CODE** — source code was read; live success is not thereby proved.
- **VERIFIED_DOCUMENT** — primary/official documentation was inspected; service behavior or entitlement is not thereby proved.
- **AUTHOR_REPORTED / HISTORICAL_EXECUTION** — maintainer/source author reports a live result; this project did not reproduce it.
- **SUPPORTED** — multiple relevant sources or strong project documentation support the claim, but not direct live verification.
- **PROVISIONAL** — architecture/design inference awaiting stronger evidence.
- **DISPUTED / UNRESOLVED_SCOPE** — sources, versions, programs, or meanings differ.
- **REFUTED_AS_STATED** — a particular proposition failed a recorded counterexample.
- **UNTESTED** — no relevant executed check.
- **BLOCKED_BY_ENVIRONMENT** — current environment lacked a named dependency; not evidence of impossibility.
- **DEFERRED** — useful work not selected in the bounded pass.

## 4. Current combined result

The research phase **worked as pre-build uncertainty reduction**, but it has reached the point where further high-value progress increasingly depends on Codex/local/cloud execution with real repository access and an authorized LinkedIn test environment.

What the combined work has established:

1. LinkedIn acquisition is not one route. Official organization APIs, authenticated/private web-client reads, public organizational pages, providers, exports, portability/research programs and stored datasets have materially different access, provenance, history, retention and failure semantics.
2. A production adapter cannot be designed around a single endpoint name or a generic "LinkedIn API" abstraction. Recipe/representation semantics, roots, referenced entities, field sufficiency, population meaning, pagination, session state and release/version evidence have to be explicit.
3. Raw/source evidence, DOM, provider response, provider-normalized record, export and derived feed are different evidence classes. Do not relabel one as another.
4. Publication time, source modification time, source capture time, provider crawl/processing time, NOS receipt time and replay time must stay distinct.
5. Unknown is not zero; unavailable is not deleted; a short page is not automatically terminal; provider-terminal is not source-complete; selector miss is not an empty successful collection.
6. Original/reshare/commentary/author relationships need source-native evidence. Text equality, display names, trailing numeric IDs and convenient URL rewrites are not sufficient identity rules.
7. M1's generic reliability mechanisms are useful starting points, but actual LinkedIn integration still needs the real M1/M2 code path verified.
8. No safe LinkedIn polling rate, account count, concurrency, soak duration, or completeness percentage has been measured.
9. No first production capability subset or acquisition winner has been selected.
10. Retention/use policy is route-specific and can constrain architecture. "Raw is immutable" means unchanged while retained, not automatically retained forever.
11. Stop/diagnosis semantics must be durable and scoped. A local prototype caught and fixed same-task reassignment after a challenge/rate hold, but physical last-dispatch fencing in the real worker remains untested.

## 5. What is actually demonstrated offline

### 5.1 Evidence/history and replay semantics

Both branches built synthetic/local witnesses showing that latest-only state cannot substitute for ordered history, raw evidence must be preserved independently from interpretation, duplicate delivery must not mint duplicate authoritative evidence, and collection/receipt/replay clocks must remain distinct.

Branch A additionally exercised capture/snapshot/observation/delivery distinctions, quarantine, parser-revision safety, lease/fencing counterexamples and persistent demo state. Branch B exercised process-exit recovery, replay, history cursors and explicit acquisition-lineage semantics.

These are **generic evidence-model results**, not proof of LinkedIn transport or production M1 implementation.

### 5.2 Structured response/reference reconstruction

Both branches inspected and tested normalized `data`/`included`-style or related structured representations. The experiments show why production parsing must retain:

- root slots separately from resolved entities;
- missing and conflicting references;
- full native URNs/namespaces;
- field sufficiency separately from identity resolution;
- raw pointers/locations where useful;
- explicit unknown/null/zero/invalid states;
- response-profile/recipe versioning.

Several local defects were intentionally preserved and repaired during the branch experiments; fixture success remains fixture-scoped.

### 5.3 Public-page/DOM evidence

Branch A materially inspected RSSHub and `RSS-linkedin-app` organizational-page approaches and tested an offline HTML auditor. It demonstrated risks such as processing-time-relative timestamps drifting, nested text/author boundaries being flattened, height-stagnation not proving history completion, and generated-feed freshness not equaling source freshness.

No current raw LinkedIn HTTP/DOM pair was obtained, and browser-vs-direct equivalence remains untested.

### 5.4 Company/post prior art and pagination/failure behavior

Branch A inspected a Ruby company-updates implementation and a Python ancestor and executed them against fake acquisition. It reproduced mutable/default accumulation problems, budget/termination ambiguity, partial-result loss, duplicates and convenient zero synthesis. Branch B independently built/inspected post/excerpt/comment/reshare and meaning-contract experiments around other implementations and fixtures.

The useful production consequence is a typed run outcome with separate raw/parsed/unique counts, explicit continuation state, preserved committed pages after later failure, and pre-request budgets. It is **not** evidence that the historical route works live today.

### 5.5 Provider receipt/provenance semantics

Branch B executed supplier-envelope receipt models showing that:

- whole provider-response hashes can change while the provider-projected payload is unchanged;
- provider receipt time is not native source-observation time;
- replay/idempotency applies only within its actual bounded scope;
- supplier terminal state can coexist with known dropped inputs and unknown source completeness.

Branch A materially broadened provider/dataset documentation and retention/history comparison.

### 5.6 Response diagnosis and durable stop state

Branch B executed copied classifier/request/pause bodies and an original local response-boundary model. It reproduced:

- body-read errors being lost at a selected interface;
- new calls being blocked after a hold while an already-entered call can cross a preflight-to-fetch interval;
- a fixed cooldown undercutting a supplied Retry-After;
- different hold reasons accidentally clearing one another in upstream-style state;
- overbroad synthetic challenge diagnoses;
- an actual defect in the first local model where the same task could be reassigned to a new fictional account after a stop.

The local fix added independent task holds and passed the expanded suite. This demonstrates **logical admission behavior only**. It does not prove physical socket dispatch is fenced in M1.

## 6. What remains unproven

The following are still **not demonstrated** by either branch:

- a current authorized authenticated LinkedIn session used by a working collector;
- a current live private/web-client request recipe for the required production capabilities;
- current safe session lifecycle and challenge/error signal precision;
- a live original/reshare pair with verified authorship/text ownership/metric target;
- a real two-page/continuation sequence proving current paging/termination semantics;
- current arbitrary-topic/post search coverage;
- current comment/reply paging and parent/root semantics for required scope;
- complete edit/deletion/unavailability behavior;
- sustained recurring collection, safe cadence, concurrency, health, recovery and completeness characteristics;
- physical last-dispatch fencing across real workers;
- actual production M1 source/worker/storage/release integration for LinkedIn;
- actual M2 `EvidenceEnvelope`/converter/schema compatibility for LinkedIn;
- provider conformance, contract fit, India/deployment/egress fit, provenance sidecars and retained-run recovery;
- production retention/expiry implementation;
- a selected first production capability subset;
- a selected production transport or provider fallback;
- a final Codex implementation blueprint.

## 7. M1/X benchmark that survives both branches

Reuse only after actual source verification:

- source-specific protocol/acquisition logic separated from durable production orchestration;
- production-owned retries rather than hidden protocol retries;
- durable task intent/outbox/worker state;
- raw-before-parse evidence discipline;
- typed failure classes and explicit partial outcomes;
- account/session/route/capability failure isolation;
- human intervention on challenges;
- replay/idempotent downstream effects;
- release validation, explicit promotion, drift detection and rollback/fallback discipline;
- source/coverage health that distinguishes failure from quiet activity;
- standing collection and targeted/on-demand requests as different intents;
- stable northbound evidence contract independent of platform-specific session details.

Do **not** copy by assumption:

- X numeric-ID rules;
- X operation IDs/headers/features;
- X transaction-header mechanics;
- X cursor/page behavior;
- X five-minute cadence or other polling defaults;
- X page limits;
- X account-pool size;
- X browser/fingerprint setup;
- X rate classifications;
- X source-type enums or current M1→M2 translator assumptions.

Important existing integration debt preserved by both branches: the supplied M1/M2 handbook reports missing ordered monitor-run/RSS-event routes relative to M2 expectations. Current deployment/source code must be checked before claiming lossless history compatibility.

## 8. Combined acquisition-route map

No route below is selected. Every route must pass **access**, **permitted use/retention**, **representation/provenance integrity**, and **operational coverage/reliability** separately.

| Route | Combined evidence | Current status / key gap |
|---|---|---|
| Owned official organization/community-management APIs | Official docs and offline decoders inspected | Scoped entitlement/roles; development restrictions; use/retention fit and live access unverified; not arbitrary platform coverage |
| Owned authenticated structured web-client reader | Several implementations/fixtures/operation registries inspected; fake/local tests | Current recipe/session/fields/paging and live reliability unverified |
| Owned public organizational page/DOM reader | RSSHub and RSS-linkedin-app inspected; offline HTML tests | Bounded preview/window, timestamp/author/provenance issues; raw-vs-DOM and sustained completeness unverified |
| LinkedIn Member Data Portability | Primary docs examined in Branch B | Consenting-member/program-specific route, not arbitrary-member monitoring; combination/retention/eligibility constraints |
| LinkedIn Research Tools/programmatic research access | Primary docs examined in Branch B | Approved-project/use restrictions; not a general production feed; NOS eligibility unverified |
| Unipile | Documentation inspected in both branches | Connected-account dependency remains; provider/native ID/time/error/retention semantics and live conformance unverified |
| Bright Data | Documentation inspected in both branches | Managed extraction/job/snapshot route; request-shape/version discrepancies, provenance, stop-on-challenge fit and exact retention/product scope unresolved |
| Coresignal Company Posts | Branch A documentary lead | Potential historical publication supplement; current dictionary/endpoints, source clocks, edit/deletion history and license unverified |
| Coresignal Multi-Source Company | Branch A documentary lead | Enriched organization history, not native publication history; per-field provenance/cadence unresolved |
| SocialCrawl | Branch B documentary lead | Normalized/cache/run-history candidate; native provenance, retention, source clock and conformance unverified |
| Authorized Page/admin exports / independently supplied publications | Both branches treat as distinct import route | Preserve declared origin/window; does not become recurring native LinkedIn observation automatically |
| Negotiated/government-facilitated platform data | Mentioned as a possible distinct route in project/provider context | No concrete feed/agreement was available; do not assume it exists or fits |

Provider/open-source routes are evidence and alternatives, not automatic architecture choices.

## 9. Combined capability map

No capability is production-certified.

| Capability family | Current evidence | Main missing proof |
|---|---|---|
| Organization identity/metadata | Official/provider docs + structured fixtures + graph/reference tests | Current aliases, field provenance, scope and live route |
| Known organization publication lookup/refresh | Official/provider docs; historical implementations | Current accepted identifier, native identity, unavailable/delete semantics |
| Organization publication listing/history | Historical web-client/public-page/provider implementations | Current paging, stable ordering under change, completeness/window semantics, sustained refresh |
| Member posts / feed entries | Branch B handler/fixture/excerpt work; legacy richer fixture | Current full author/original/reshare relations and native paging |
| Original/reshare/commentary relationships | Official parent/root concepts + prior-art/source fixtures | Genuine current pair with actor/text/time/metric target and continuation |
| Comments/replies | Historical/alternative implementations and synthetic occurrence preservation | Current response JSON, scope, parent/root, ordering, pagination, edits/deletes |
| Aggregate engagement | Official/provider fields and offline null/zero tests | Native definitions, target object, visibility, history, provider defaults |
| Organization admin analytics/exports | Official docs | Actual entitlement/export schema/retention; separate from public post evidence |
| Organization notifications/mentions | Official documented surfaces | Scope, authorization, hydration gaps, relationship to edit history |
| Organization metric history | Provider/doc leads | Per-field provenance and native-vs-enriched distinction |
| Articles/newsletters/document/media references | Official and provider leads | Full body vs preview, media bytes/reference semantics, history |
| Events/jobs/other org context | Leads only | Mechanism, history, utility and scope remain incomplete |
| Profiles / experience / education / skills | Branch B source inspection | Current field projection, visibility, paging, relevance to first production scope |
| Search/discovery | People-search/activity-tab/query-registry leads | Post/topic search mechanics, facets, root membership, historical truncation/ordering, completeness |
| Relationship/network views | Source-reported account-scoped views | Only consider permitted bounded read context; no global/private graph or dossier claim |
| Edits/deletions/unavailability | Evidence model supports distinctions | Current live source signals and complete history not established |
| Session/request/drift | Code inspection + local stop/classifier models | Live signal precision, actual session daemon, current recipe discovery/promotion, distributed dispatch fence |

### Scope boundary that must not drift

Branch A explicitly excludes personal political dossiers, population-scale person enumeration and private relationship graphs. Branch B mentions account-scoped relationship views as a possible observable surface while also rejecting a global/private graph claim. The merged interpretation is:

> investigate only read-only, permitted, bounded relationship/context data that is actually needed for M1 evidence; do not turn this project into person-level mass tracking, private-network collection or political dossier construction.

## 10. Prior-art landscape already examined

Across the branches, the following classes/projects were materially inspected or traced to varying depth:

- OpenRecruiterTools/linkedin-toolkit and associated operation registry/normalized response/session/classifier material;
- mayai direct-client discovery/query paths;
- alternative comments/read-client material referenced by Branch B;
- RSSHub LinkedIn company-page path and maintenance history;
- `RSS-linkedin-app` organizational public-page reader;
- `marostr/linkedin-voyager_api` Ruby company-updates path;
- `nsandman/linkedin-api` Python ancestor;
- official LinkedIn Python SDK/client behavior in selected files;
- source-authored reshare/fallback examples and richer legacy fixtures in Branch B;
- Unipile, Bright Data, Coresignal and SocialCrawl documentation;
- official LinkedIn organization, portability, research, analytics/notification and retention/use documentation.

Do not count ports/forks/shared ancestry as independent live confirmation. README claims are lower value than inspected code, dated issues/commits, actual fixtures and live observations.

## 11. Provisional combined architecture

The current best responsibility model is:

```text
semantic collection intent / target / reason
  -> acquisition-route eligibility + use/retention policy
  -> source-specific request/DOM/provider job representation profile
  -> last-moment stop/session/rate/permission gate
  -> acquisition attempt
  -> immutable permitted raw artifact + capture context
  -> transport/diagnosis facts + typed run/page/item outcome
  -> source-specific reconstruction (roots/references/fields/relationships)
  -> occurrence/content-version/observation/metric/provenance objects
  -> durable history / replay-equivalent source
  -> M1 northbound mapping
  -> idempotent M2 admission with coverage and raw lineage
```

### Required behavioral boundaries

1. **Intent is durable.** A standing watch, targeted refresh, replay and provider backfill are not the same reason even when they retrieve the same object.
2. **Acquisition route is explicit.** Provider/public/API/web-client/export observations do not lose their route identity after normalization.
3. **Policy travels with evidence.** Applicable permitted-use/retention/expiry state cannot be retrofitted from a successful HTTP status.
4. **Raw before interpretation where permitted.** If the route does not permit durable raw retention, record the actual allowed artifact/derivative lineage rather than pretending native raw custody.
5. **Representation is versioned.** Operation/query/decorations/root/types/field expectations/paging meaning form a capability release profile; a query ID alone is insufficient.
6. **Response facts are separate from diagnosis.** HTTP/body/read errors, challenge hypotheses, permission/access failures and capability-empty results must not collapse.
7. **Stops are scoped and durable.** Account/session/route/task reasons remain separate; late success does not clear a newer stop; a retry cannot silently change identity to escape a task stop.
8. **Physical dispatch still needs a fence.** Logical SQLite experiments do not prove a production worker cannot emit a request after another worker commits a stop.
9. **Parsing preserves uncertainty.** Missing/null/zero/invalid/conflict/unresolved references remain distinct.
10. **Pagination is durable and typed.** Store page receipts and continuation state together; retain committed earlier pages after later failure; short/empty pages are terminal only when the recipe proves it.
11. **Source object identity is not text identity.** Preserve full namespaces/URNs, occurrence context and source relationships.
12. **Observation history is independent from content version.** Engagement changes, unchanged recaptures, corrections and redelivery need separate semantics.
13. **Coverage honesty is first class.** Failed collection never becomes "nothing happened"; provider terminal never automatically means source complete.
14. **M2 sees a stable source-independent evidence contract.** LinkedIn session/route internals stay in M1 while provenance, clocks, relationships, partiality and coverage cross the boundary.

This is a behavioral architecture. Database/framework/deployment choices remain open until actual M1 code and workload make them load-bearing.

## 12. Cross-branch decisions that can be treated as confirmed task intent

- owned/self-built collector is primary objective;
- broad LinkedIn-native discovery, not X parity, defines the exploration space;
- read-only production scope;
- no automated challenge solving/ban evasion/identity rotation;
- open-source and providers must be studied as evidence/prior art/fallbacks;
- raw/provenance/history/missingness honesty is mandatory;
- real code/tests should be built whenever they answer a real uncertainty;
- fixture/synthetic results must not be promoted to live truth;
- one master implementation blueprint is the eventual final handoff, not pre-split tickets;
- persistent STATE/work products must preserve failed tests and untested items;
- actual M1/M2 source verification and live LinkedIn evidence are now higher value than more speculative offline schema work.

## 13. Differences/tensions that remain visible

### 13.1 What to do next

Branch A selected a generic **expiry/replay lifecycle test (T17)** after its retention research. Branch B selected **actual M2 evidence-intake source inspection** after its stop-state work.

Merged priority for Codex: inspect the real M1/M2 code and run environment-backed integration/live experiments first. T17 remains useful, but it should not displace the now-available opportunity to resolve the real production boundary. Route-specific expiry implementation becomes higher value once an acquisition route and actual policy are selected.

### 13.2 Relationship/network scope

Branch A explicitly places personal-history/person-enumeration/private graphs outside scope. Branch B lists relationship/network views as a possible account-scoped surface. Preserve the narrower production interpretation in §9.

### 13.3 Provider sets

The provider landscapes differ rather than conflict. Combine them: Unipile and Bright occur in both; Branch A adds Coresignal products; Branch B adds SocialCrawl and more official-program comparison. None has production acceptance.

### 13.4 Official retention/access rules

Different official programs and field classes carry different restrictions. Do not merge six-week/six-month organization-content rules with 24/48-hour member-data rules into one universal LinkedIn retention policy. Keep route, program and field class explicit.

### 13.5 Branch test counts

Do not add test counts from both branches and present the total as independent confidence. Many tests exercise similar generic evidence principles and sometimes related prior art. Each count proves only its own code/fixture contract.

## 14. Canonical open experiments/dependencies

### P0 — Recover the actual production code boundary

Read-only inspect the actual M1 and M2 source/revision that Codex will modify/deploy:

- M1 planner/release router/protocol attempt/worker/lease/fence/raw store/canonical store/northbound serializer;
- M2 `EvidenceEnvelope`, converters, source enums, identity/relationship fields, observation/version keys, correction/quarantine logic and permissions;
- current ordered history routes and deployment configuration.

Resolve the documented ordered-history gap and candidate-vs-approved planning ambiguity from source, not from summaries.

### P1 — Establish one current owned acquisition path

Using an authorized local LinkedIn test account/session with secrets kept outside artifacts, perform the smallest current read needed to establish one real organizational publication or known post.

Record:

- target/intent;
- request method/path/operation family and nonsecret parameter structure;
- response status/content type;
- representation root/types/fields;
- full relevant native identifiers;
- capture clock;
- source time if actually supplied;
- continuation evidence;
- challenge/access state;
- raw bytes retained locally only as permitted.

Stop on challenge/restriction. Do not sweep endpoints.

### P2 — Verify native relationships and pagination

Obtain bounded authorized examples for:

- original + reshare/repost;
- one comment/reply context if required for the first scope;
- at most one real continuation/second page.

Determine actor/text ownership, original/root/parent IDs, metric target, source/capture times, ordering and terminal semantics. Do not infer missing relationships from text or display names.

### P3 — Verify physical stop/fencing

In an isolated real worker/transport path, suspend after selection but before actual network dispatch, commit an injected stop/hold from another worker/process, then release the first. Count actual transport calls and inspect task/account/session/route generations and completion/quarantine behavior. Do not induce an actual platform challenge.

### P4 — Build the LinkedIn protocol adapter around verified shapes

Only after P1/P2 provide current shapes:

- capability/recipe profile;
- request construction;
- parser/reconstructor;
- pagination state;
- typed page/item/run outcomes;
- raw/provenance pointers;
- session/stop/error mapping;
- fixtures and replay tests;
- candidate/approved release discipline and drift detection.

Reuse generic M1 orchestration where source inspection confirms compatibility.

### P5 — Integrate with M1→M2 and history

Exercise at minimum:

- one publication;
- repeat unchanged capture;
- metric-only change;
- changed representation/extraction without false source edit;
- unavailable/access-denied object;
- original/reshare reference;
- duplicate/redelivery;
- process restart;
- partial page then recovery;
- coverage gap.

Prove the actual northbound contract rather than inventing a parallel LinkedIn schema.

### P6 — Measure sustained operation

After one route is working, predeclare a small permitted workload and measure:

- request counts;
- latency;
- duplicates;
- continuation behavior;
- source freshness;
- failures and recovery;
- session health;
- memory/CPU/network cost;
- missed/partial windows;
- stop/resume behavior.

Do not invent a safe cadence before this measurement.

### P7 — Validate fallback/provider routes only where needed

If owned collection is insufficient for a required capability/history window, run a bounded conformance trial for the relevant provider/export route. Verify native/provider identity mapping, source/crawl/processing/receipt clocks, dropped/warning fields, raw/sidecar lineage, retained-run recovery, contract/use/retention/egress fit and exit/export behavior.

### P8 — Implement retention/expiry against the chosen route policy

Run/adapt Branch A T17 against the real selected route policy: expiry, duplicate removal, late delivery, replay, restart, derivatives/backups/references. Never represent policy expiry as LinkedIn deletion.

## 15. First production subset remains open

The research deliberately did not freeze the first production feature set. Codex should select it only after P0–P2 establish actual feasibility and M1/M2 fit.

A reasonable selection decision should consider:

- NOS/M2 usefulness;
- reliability of the current source route;
- source-native provenance/identity quality;
- achievable history/refresh semantics;
- session/account operational burden;
- implementation/maintenance cost;
- policy/retention fit;
- fallback availability;
- measurable sustained behavior.

Do not select solely because an old library exposes a method of the same name.

## 16. Artifact continuity and missing inputs

This merged handoff includes the loose final artifacts that were supplied to this chat:

- Branch A final STATE;
- Branch A WR-006 acquisition/retention report;
- Branch A acquisition-route comparison;
- Branch A Iteration-006 package receipt;
- Branch B final STATE;
- Branch B WR-006 response/stop report;
- Branch B Pass-006 verification receipt;
- the current repository link text supplied in the NOS Project.

It does **not** contain many historical prototype/source/result files named inside the two STATES, because those files were not delivered by the chats. Their receipts describe internal archive/manifests, but the user did not receive those archives.

Continuation rule:

- do not ask the user for the internally referenced ZIPs;
- do not mark this as a user handoff failure;
- do not claim to have inspected or rerun referenced-but-undelivered code/results;
- if a prior experimental result becomes load-bearing, reproduce the smallest relevant experiment in Codex from the documented method and current source/fixtures, and record that new result independently.

## 17. Current route

**OPERATION COMPLETED BY THIS MERGE:** combine both six-pass research branches without collapsing confidence/provenance.  
**DELTA:** one canonical model, acquisition map, capability map, architecture, disagreement register and environment-backed experiment sequence; original branch final artifacts preserved separately.  
**OPEN HINGE:** actual production M1/M2 code, current authorized LinkedIn source evidence, real physical dispatch fencing and sustained operation.  
**ROUTE:** hand to Codex/local environment for continued research → implementation → testing. **NOT READY FOR FINAL BLUEPRINT YET.**
