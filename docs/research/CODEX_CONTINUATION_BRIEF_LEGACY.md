# CODEX CONTINUATION BRIEF — LinkedIn adapter for NOS M1

## Purpose

Continue the LinkedIn/M1 investigation from two independent six-pass ChatGPT research branches. Do not restart from generic LinkedIn scraping research. The branches already performed substantial documentation/code inspection and offline experimentation; your value now is to resolve the environment-dependent unknowns, implement against verified current behavior, and integrate with the real NOS codebase.

Read first:

1. `MERGED_STATE.md`
2. `EVIDENCE_BRANCH_CROSSWALK.md`
3. Branch A and Branch B final STATE/WR files under `branch_a/` and `branch_b/`
4. The actual current NOS M1/M2 repository/source tree you will modify

The merged STATE is the working checkpoint. The branch files remain provenance authorities for exact experiment details.

## What not to do

Do not:

- assume a production LinkedIn collector already works;
- treat offline/synthetic test success as live LinkedIn proof;
- repeat broad research that the branches already completed unless a specific current uncertainty requires it;
- copy old Voyager/Rest.li/GraphQL request recipes blindly;
- patch an old library directly into production because it has a `company_updates`/posts method;
- invent a safe request rate, page limit, account count or soak duration;
- copy X IDs/cadence/session behavior into LinkedIn;
- treat provider data as native LinkedIn raw evidence;
- flatten source time, capture time, provider time and NOS receipt time;
- continue a challenged/restricted task through another identity;
- build interaction/write automation;
- ask the user to paste live LinkedIn credentials into project/chat artifacts.

## What the research already gives you

The two branches already established useful contracts around:

- source-specific acquisition vs generic M1 orchestration;
- raw/provenance/history distinctions;
- occurrence/content-version/observation/delivery separation;
- reference-graph reconstruction and conflict/missingness handling;
- pagination receipts and partial outcomes;
- public-page/DOM timestamp and attribution pitfalls;
- provider receipt/cache/source-clock distinctions;
- route-specific use/retention concerns;
- response diagnosis vs stop state;
- task/account/session/route hold separation;
- failure cases in old/open-source collectors;
- a broad acquisition-route and capability landscape.

Use those as testable requirements, not unquestioned production designs.

## Highest-value execution sequence

### 1. Verify the real NOS integration boundary before coding a parallel architecture

Inspect current M1 and M2 source read-only first.

Find the exact code for:

- M1 planning/admission;
- protocol/source attempt boundary;
- release/candidate/approved routing;
- worker lease/fence and retry ownership;
- raw persistence;
- canonical observation/history store;
- source health/coverage;
- targeted/on-demand collection;
- northbound serializer/API;
- M2 EvidenceEnvelope/domain converter;
- source enums/identity/reference/media validation;
- source-version/observation/delivery identity;
- correction/reprocessing rules;
- quarantine/partial input;
- permissions/raw-proof handling;
- ordered history routes actually present in the code/deployment.

Record exact paths/commits. Resolve documentary conflicts rather than silently choosing one.

### 2. Reproduce prior prototypes only where they reduce risk

The branch documents reference many historical prototype/code/result files that were not actually delivered to the user. Treat those paths as **referenced-but-undelivered**. Do not ask the user to supply internal ZIPs/archives that were never provided.

Treat the delivered STATE/WR/verification documents as the evidence record of what the chats report they executed. If a historical prototype result becomes load-bearing for production, rebuild the smallest necessary experiment from its documented method and current source/fixtures, then record the new locally reproducible result. Do not rerun every historical experiment as ceremony.

Prioritize only components that directly inform the production path: representation reconstruction, page receipt semantics, provider receipt semantics, response/stop boundaries, and actual M1/M2 compatibility.

### 3. Establish one current authorized owned read

Use an authorized local LinkedIn account/session. Keep secrets in the local secret/session mechanism, never in generated reports.

Choose the smallest read that answers a production question—prefer a known organization publication or known post relevant to the first likely M1 capability.

Capture locally, where permitted:

- exact method/path/operation family;
- parameter names/types and representation/decorations/version data;
- HTTP status/content type/final origin;
- response root/types/fields;
- full native IDs/URNs;
- source publication/modification times if actually present;
- actual capture time;
- paging/continuation metadata;
- access/session/error state;
- raw bytes and hash subject to the route's applicable policy.

Do not endpoint-sweep. Stop on challenge/restriction.

### 4. Resolve the load-bearing native semantics

Before broad collector coding, get bounded evidence for:

- author/publisher identity;
- current occurrence vs original/reshare/repost;
- authored vs embedded/quoted/commentary text;
- metric target and null/zero/missing meanings;
- one real next-page transition and end behavior;
- current comment/reply parent/root/paging only if required by the chosen first scope;
- unavailable/access-denied/deleted distinctions where naturally observable.

Version the recipe/representation contract around the verified behavior. Do not make query ID alone the release identity.

### 5. Test the physical stop boundary

Branch B proved only logical admission. In an isolated M1 worker/transport test:

1. admit/select a request;
2. pause immediately before actual transport dispatch;
3. commit an injected stop/hold from another worker/process;
4. release the first worker;
5. record actual transport-call count and state generation;
6. verify how already-acquired raw data is retained/quarantined;
7. repeat across restart.

Use injected/sanitized signals; do not trigger a real LinkedIn challenge on purpose.

### 6. Implement the LinkedIn protocol/source adapter against verified fixtures

Build only the smallest current production slice. It should have:

- explicit semantic capability/target;
- request/representation profile;
- session attachment contract;
- one-attempt source call;
- raw-before-parse behavior where permitted;
- typed transport/access/challenge/rate/parser/paging outcomes;
- conservative reconstruction preserving missing/conflicting refs;
- durable page/continuation state;
- separate raw/parsed/unique counts;
- source-native IDs/relationships;
- no invented timestamp/metric/author values;
- candidate/validation/approval/release lifecycle compatible with M1;
- replay fixtures and drift tests.

Do not hide protocol retries inside the adapter if M1 owns retry policy.

### 7. Integrate through real M1 → M2 contracts

Use the existing production data model instead of creating a new parallel LinkedIn envelope unless a verified gap requires an extension.

Exercise:

- initial post/publication;
- duplicate/redelivery;
- repeat unchanged observation;
- engagement-only update;
- changed extraction/representation without false source edit;
- partial page then recovery;
- access denied/unavailable;
- reshare/original relationship;
- restart/replay;
- collection gap/coverage state;
- targeted on-demand request if in first scope.

Resolve the ordered-history mismatch before claiming lossless M2 intake.

### 8. Measure a small recurring workload

Only after the route works:

- predeclare a small target set and page budget;
- run repeatedly;
- measure request/latency/cursor/duplicate/error/freshness/session-health/resource behavior;
- retain failures and missing windows;
- stop on challenge/restriction;
- use the measurements to derive—not guess—initial cadence/concurrency.

A one-shot live success is not completion.

### 9. Evaluate fallback/provider routes only when they solve a real gap

If the owned route cannot satisfy a required capability/history window, test the relevant provider/export route against the same evidence contract.

Do not compare vendor names only. Test:

- exact target coverage;
- native/provider ID mapping;
- source/crawl/processing/receipt clocks;
- raw/sidecar provenance;
- pagination/dropped/warnings/errors;
- retained-run/event recovery;
- caching/idempotency behavior;
- contract/use/retention/egress fit;
- data export/exit path.

### 10. Implement route-specific retention/expiry

The chosen route must carry its applicable policy. Test expiration, redelivery, replay, restart, derivatives, backups/replicas and removed-artifact references. Policy expiry must never be emitted downstream as source deletion.

## Iteration discipline

After every substantive Codex pass:

- create/update a full work record;
- update the canonical STATE;
- preserve failed experiments and pre-fix versions when they change understanding;
- distinguish live, source-inspected, fixture-tested and inferred facts;
- record exact repo commit/path/environment;
- stop after one bounded high-value operation rather than sprawling across many unverified changes.

Do not produce the final master blueprint until the current live/source/integration questions are sufficiently closed that synthesis is genuinely the highest-value operation.

## Completion target

The final result should be one master LinkedIn/M1 implementation blueprint plus working, tested production code for the agreed V1 subset, with explicit residual uncertainty and measured operational limits. The blueprint should describe what actually survived testing, not what the research initially hoped would work.
