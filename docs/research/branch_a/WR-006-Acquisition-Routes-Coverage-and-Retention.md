# WR-006 — Acquisition routes must preserve scope, provenance and permitted retention

**Task:** Owned, read-only LinkedIn collection research for NOS M1.  
**Pass:** 006 · 2 October 2026, Asia/Kolkata.  
**Operation:** Broaden the capability/acquisition comparison using primary access documentation, provider documentation and a further inspectable organizational public-page implementation.  
**State:** Active investigation, not the final implementation blueprint. No production repository, account, subscription or provider configuration was changed.

## 1. The next decision depended on acquisition evidence, not another pagination simulation

Iteration 005 had traced actual company-update implementations and executed their failure behavior with fake acquisition. It did not establish a current source response, sustainable acquisition, source-native attribution or a production integration. Its selected next operation was to address the neglected provider/capability landscape. That is the operation performed here.

The investigation now has eight route records and fifteen capability-family records in `research/ACQUISITION_ROUTE_COMPARISON.md` and its machine-readable companion `research/acquisition_catalog_006.json`. These are not fifteen implemented features or eight proven acquisition options. Every route explicitly has `live_tested_here=false`, `implemented_here=false` and `production_ready=false`. Some capabilities are unresolved or outside the supported organizational scope, rather than silently omitted or called impossible.

The baseline was the complete iteration-005 checkpoint and cumulative ZIP. Earlier experimental findings remain useful; their tests were not rerun, repaired or reclassified in this pass. The current continuation retains the launch, all prior work products, code, fixtures, failures, receipts and qualifications beneath `history/NOS_LinkedIn_Iteration_005/`.

### What counts as evidence in this record

Source IDs S006-01 through S006-27 resolve in `sources/SOURCE_REGISTER.md` and `sources/source_register_006.json`. Each records the actual inspection scope, absolute source date where available, effective URL, retrieval reference, supported facts and limitations. A provider's documentation is direct evidence of its advertised contract, not direct evidence of upstream service behavior. Search-only entries remain marked search-only. A source code read is not execution. A date in a generated-feed commit is not a successful current LinkedIn capture.

No source or provider collection API was invoked. No raw LinkedIn response, authenticated browser, paid job, data export or real M1/M2 admission was obtained. The only new executable work is research-catalog authoring and artifact verification. It must not be counted as another collector experiment.

## 2. Official access has both tier and data-use constraints

The served Community Management access page lists a Development tier with 500 calls per application and 100 per member per 24 hours. BATCH_GET calls and Social Actions push are unavailable in that tier. Standard access requires the applicable review; the table's broad wording is not proof of unlimited upstream quotas. These limits concern official API access, not a safe rate for LinkedIn's web client. [S006-01]

That changes the implementation dependency of the existing iteration-002 decoder. Its offline batch tests remain valid parser tests, but do not demonstrate that a Development application may execute those requests. Similarly, a design that depends on push for its initial release would be depending on an unverified entitlement. Capability availability must be checked independently from whether request construction and parsing code exist.

The currently served Marketing API storage documentation distinguishes organization content, embedded member content, profile fields, reporting and identifiers. Organization social activity is ordinarily limited to six weeks, or six months for the documented authenticated-organization case. Member activity has a 48-hour limit; other-member profile caching has a 24-hour limit. Organization-profile and nonindividual reporting data have different rules. The most restrictive applicable duration governs overlapping fields. The document exempts independently client-provided data not obtained through Marketing APIs from these particular requirements. That exemption is not a general grant of rights. [S006-02]

The separate restricted-use page also restricts uses of member data and derivatives and disallows the documented Community Management social-feed use case. It does not establish approval for NOS's proposed archive or analysis. [S006-03]

**Decision changed, provisionally:** official API entitlement alone is no longer an adequate eligibility test for an archival route. Before depending on it, the actual approved use, field classes, retention, deletion requirements and downstream processing have to fit. These served documents do not settle a legal interpretation of the whole NOS project, and their rules must not be silently transplanted to every other route.

### Raw evidence can remain immutable without being retained forever

The architectural response proposed here is policy-carrying evidence, not an invented universal deletion schedule:

1. Before acquisition, associate the proposed route and intended purpose with its approved use policy. An unverified policy is not permission inferred from a successful HTTP response.
2. At capture, bind the raw artifact, route, representation, provenance and applicable retention policy. A policy must specify what starts its clock; processing the same artifact again must not silently restart retention.
3. Keep the artifact unchanged while it exists. Expiry is a separate governed removal operation, not editing the original into a new source version.
4. If a single raw blob mixes fields with different limits, preserving the whole blob beyond the earliest applicable expiry cannot be justified merely because one field has a longer limit. A separately retained permitted extraction would be a derivative artifact with its own lineage, not the original raw response.
5. Apply removal policy to replicas, backups, exports, caches, quotations and derived records that embed affected content. An immutable snapshot schema does not by itself make its embedded data exempt.
6. Preserve only the removal/audit information the policy permits. Neither a retained identifier nor a hash automatically authorizes keeping the expired underlying content or proves anonymization.

These steps have not been implemented. They identify a real conflict to test before binding an acquisition route to NOS history. Earlier archived research fixtures and source-code examples are not newly acquired Marketing API content; this finding does not authorize silently deleting prior project artifacts.

**Important semantic distinction:** source deletion, access loss, capture absence and local retention expiry are different events. A downstream consumer must not see policy expiry labeled as “LinkedIn deleted this post.” An eventual implementation may need a separate artifact/accessibility record rather than changing M2's source-availability enum. No M2 field or database migration is frozen in this pass.

## 3. Provider access describes three different products, not one fallback

### A connected-session provider still depends on the upstream account

Unipile's inspected v1 company-post route requires a connected `account_id`, a company identifier and the company-specific flag. Its guide distinguishes provider-facing identifiers from a `social_id` used for related read operations. The connection documentation describes account-state and reconnect handling. [S006-09, S006-10, S006-11]

**Inference:** this outsources maintenance of some source-specific machinery; it does not remove the dependence on what the connected account can access. If the same upstream account is restricted, a managed session is not an independent recovery source merely because its API hostname differs. A security challenge must still stop the affected use. Alternative-account rotation is not an acceptable substitute in this task.

The organization example includes both a relative date and `parsed_datetime`, as well as a null author identifier and a zero impression counter. Their presence is not enough to verify native publication precision, resolved publisher identity or a genuine observed zero. These are unresolved field meanings, not a claim that the provider fabricated them. [S006-10]

A v2 reference also appeared in search. Only the returned search text was inspected; it is not evidence that v1 is obsolete or that the two identifier/pagination contracts can be mixed. A later implementation must pin its exact API version. [S006-12]

For this route, the minimum useful evidence is a permitted organizational sample with the meaning of each time/identity/metric field, plus an account-failure receipt. Provider branding and a schema-shaped JSON example cannot supply those missing facts.

### On-demand extraction adds a remote job lifecycle

Bright Data documents company-post lookup and company/date-window discovery, separately from its stored datasets. Its first-request guide describes a synchronous operation that can return a `snapshot_id` after a one-minute timeout. The async guide then distinguishes collecting, digesting, ready and failed states, and explicitly allows individual inputs to fail even when the overall job succeeds. [S006-13 through S006-17]

Two source discrepancies remain deliberately unresolved: the tutorial and generated reference examples differ in body shape, and the company-discovery reference illustrates a synchronous endpoint while the guide directs discovery to async. Their coexistence is not evidence that either request form actually works in the current provider environment. The examples are research leads, not executable NOS specifications. [S006-13, S006-14, S006-16]

**Proposed behavior:** retain a provider job receipt separately from the source publication. A pending `snapshot_id` is neither an empty successful collection nor an immutable LinkedIn content version. After a transport timeout, first reconcile a known submitted job; do not automatically create another paid acquisition job unless the provider's idempotency or reconciliation contract makes that safe. When ready, validate individual input outcomes before acknowledging the batch. A failed terminal state must not be trapped in a loop that waits only for ready. This behavior is not implemented here.

The company-discovery example includes `top_visible_comments`. That label does not establish complete comment coverage. A date-window filter similarly does not prove all publications inside the interval were discoverable. Source observation time, crawl time, normalization time and result-delivery time must remain distinguishable. [S006-16]

The introduction advertises challenge-solving/bypass behavior. That operational behavior is **not adopted**. Before considering this candidate usable, an acceptable route must be shown to meet the task's stop-on-challenge constraint; paying a provider does not silently remove it. [S006-13]

### A stored company-publication dataset is different from an enriched company profile

Coresignal's Multi-Source Company Data describes enriched organization records and aggregate company-metric history. Its returned summary table says monthly/quarterly delivery. A dated September 10 announcement offers daily delivery for that product. The dated announcement advances the documented availability claim; it does not establish a fresh source observation for every field or an identical cadence for every product. [S006-18, S006-19]

A separate June 4 announcement describes a **Company Posts** dataset with publication content, dates, engagement, company information and file/API access. This is a substantive new organizational-publication supplement lead. It prevents an incorrect conclusion that Coresignal offers only company metadata. However, no Company Posts endpoint dictionary, live sample, archival start, per-item source-capture clock, complete edit/deletion history or NOS-specific usage agreement was obtained. The announcement also advertises commenter data, which is not imported or treated as a default requirement here. [S006-20]

Its cached release notes are useful negative evidence: schema changes and third-party source removal can affect a managed dataset too. The returned index ends at April 2026, so it cannot certify the current October schema. [S006-21]

**Inference:** a licensed stored dataset may cover earlier observations the owned collector never made, but only if the contract and sample establish that coverage. It cannot reconstruct an unrecorded intermediate edit merely because it is called historical. A provider's organization enrichment and a source-native publication belong to different provenance classes. Merge them only through an evidenced relationship, not by replacing missing source fields with enriched values.

## 4. The added public-page implementation has activity but not demonstrated fresh acquisition

A second organizational public-page implementation was inspected through the GitHub connector: `afonsogcardoso/RSS-linkedin-app`, pinned to `839dbd159bfa6dd167d6a1bc18e6a6926e1c2042`. The latest returned commit is a generated-feed update at **2026-10-02 12:25:54Z**. The last returned commit touching `src/scrape.js` is `bc93706db6114aa1a4d6440d5863f76383a8bea9`, **2026-03-26 10:08:10Z**. These are two different kinds of maintenance evidence. [S006-23, S006-24]

Actually read:

| File | Scope | Relevant mechanism |
|---|---|---|
| `src/scrape.js` | Lines 1–200 | Bounded scrolling and height-stagnation stop; company-page candidates; DOM selectors; activity-ID-derived timestamp candidate |
| `src/state.js` | Lines 1–260 | String/attachment merge, fallback identity, date/observation fields |
| `src/index.js` | Complete returned file | Normalization, missing-author/date fallbacks, zero-item refusal, merged feed generation |

The scroll loop and zero-item refusal are useful choices to understand, not proof of completeness or correctness. Source inspection suggests specific limitations: stopping when height stops growing is not a source-confirmed end; selecting longer text may miss a shortening edit; a configured organization name substituted for a missing author is not source-observed attribution; and recording a time before acquisition is not a per-response capture timestamp. These are inspection-based implications, not newly executed counterexamples. [S006-25 through S006-27]

The timestamp bit calculation remains unvalidated source inference. No assertion that it is always wrong or always right is made. It must not become an exact native publication timestamp simply because the resulting year is plausible. Prior T11 identity/time validation still governs.

The entry point rewrites generation metadata when it successfully merges output, so a generated commit alone cannot establish a newly published item. The actual commit diff, workflow logs and original source response were not examined. Likewise, a preserved old feed can be useful but must be accompanied by source-health status; otherwise a failure may look like quiet activity. [S006-27]

No new upstream code was executed, installed or vendored. Returned Git blob IDs are recorded as repository metadata, not claimed locally verified hashes. The candidate's license and full dependency behavior remain unreviewed. This is an additional inspectable owned-path candidate, not the selected implementation.

## 5. LinkedIn-native organizational capabilities extend beyond a tweet-shaped feed

The comparison distinguishes a known post, an organization's listing, an original/reshare relationship, discussion context, aggregate engagement, admin reporting, notifications, long-form editions/media references, organizational metadata history, event/job context and historical availability. Some are mapped to documented surfaces; others remain explicit leads. The matrix also records arbitrary topic/home-feed completeness as unresolved, and personal dossiers/private graphs as outside this package's supported scope.

Three distinctions deserve implementation consequences:

**Organization notifications are not global search.** The official schema includes mention and comment edit/delete actions, with notification IDs separate from source-object URNs. Authorizations and administrator roles govern lifetime; decorated payloads can be missing. These notification objects need their own delivery identity and hydration/access state, not conversion into publication IDs. This does not prove complete publication-edit history. [S006-05]

**An article or newsletter is not only its feed card.** Official help describes separate newsletter-edition measures such as article views and impressions. It establishes a native surface worth understanding, not a general-purpose export or API for all newsletter bodies. The current prototypes still do not fully decode structured `little` commentary or native long-form content. Preserve media/body references and distinct clocks rather than claim a short preview is complete. [S006-06, inherited WR-002]

**Admin-provided reporting is a different supplement.** Official help documents Page analytics export. Such an export can support authorized organization-level reporting, but no actual file or content-history guarantee was obtained. A separately supplied first-party article or RSS publication also remains its own source occurrence, even when it resembles a LinkedIn post. It is not relabeled a LinkedIn capture. [S006-07]

The first production subset remains unselected. R01–R08 are comparison records, not a ranking. The old “provider research unexamined” gap is partly closed by documentation and this inspection; provider behavior, contracts and sustained reliability remain open. The entire broad done condition is not certified complete.

## 6. The proposed acquisition contract now has four independent acceptance dimensions

The new route comparison produces a concrete behavioral change: evaluate each candidate against **access**, **permitted use/retention**, **representation integrity**, and **operational coverage** separately. A pass on one dimension cannot silently stand in for another.

### Before a request

A proposed operation identifies a semantic target and purpose, the acquisition route, granted capability/tier, applicable use policy and a bounded work budget. It pins the request/response representation profile only after the source or provider contract is supported. Sensitive session material remains referenced outside task data. An unresolved route is research-only, not marked production-ready because a library exposes a function of the same name.

The existing M1 separation is still the intended engineering starting point: source-specific request/response behavior below durable orchestration. The September 30 handbook documents the recipe composition, task/outbox authority, single-attempt protocol calls and typed failure handling. Those are reported implementation facts; no live code was inspected here. [P10, sections 1.3, 2.2 and 2.4–2.5]

### After a response

Keep separate evidence classes:

| Artifact class | What may honestly be claimed | What must not be inferred |
|---|---|---|
| Original source response | Exact source bytes actually captured through the declared route | Universal visibility, correctness of every claim in the content, or permanent storage rights |
| Serialized browser DOM | The state that was serialized after the declared browser process | Identity with the original HTTP response or absence of client-side transformations |
| Raw provider response | Exact bytes the provider delivered | That these are the original LinkedIn bytes or were captured at receipt time |
| Provider extraction/dataset row | Fields returned with their documented origin and dataset version | Native-field status for enriched values, unknown counters turned to zero, or exact source time from a normalized date alone |
| Admin export / independently supplied publication | The supplied artifact, its declared origin and reporting window | Complete original platform history or automatic equivalence to another source occurrence |

Store the original field/value and the extraction's stated origin. Possible origin states are source-provided, provider-derived, independently supplied, or unexplained; these are proposed distinctions, not final schema names. Any validated transform retains an input pointer and method/version. Unknown native identity remains unknown rather than receiving a fictional URL or a configured author name.

Do not flatten all dates into `observed_at`. At minimum, distinguish publication time when source-supported, source modification time when supported, actual source capture time if known, provider processing/dataset time, NOS receipt time and replay time. Missing fields stay absent/unknown. A provider's precision formatting is not evidence that its origin was precise. A new fetch of an unchanged item can be a new observation; a redelivery of the same capture cannot become a new source event.

### At completion or failure

A collector operation reports the attempted scope, successful captures, rejected items, missing inputs, continuation state and stop reason. A successful provider batch with one missing input is partial. A retained prior feed during failure is stale preserved evidence, not a fresh all-clear. An upstream denial does not prove there were no publications.

Retries follow the actual operation semantics. An external POST creating a provider job is not automatically idempotent because its eventual purpose is read-only. Once a job ID exists, recovery should use it. If a response was lost before a job ID was received, the provider's reconciliation/idempotency capability must be known before repeated submission is safe. These are requirements for later implementation, not tested behavior this pass.

The M1→M2 history mismatch is unchanged. The supplied handbook says the local M1 checkout lacks ordered monitor-run and RSS-event routes expected by M2. None of the new providers or code inspections repairs that boundary. Current-item reads cannot recover a missed intermediate version. The actual serializer/converter/admission and deployed route checks remain prerequisites to claiming compatibility. [P10, sections 3.3–3.9]

## 7. The next experiments have exact decision-changing outputs

These add to, rather than replace, T1–T14.

### T15: Establish the route's use and retention policy before durable integration

**Question:** Does the granted route permit the proposed organizational evidence use and retention, including referenced content, raw artifacts, replay and downstream derivatives?

**Minimum observation:** the responsible local operator or data-contract owner records the product/tier, organization scope, permitted use, field classes, clock definitions, retention periods and deletion obligations. Provide only a nonsecret policy summary or sanitized applicable clause; no passwords, tokens, confidential unrelated terms or broad credentials.

**Interpretation:** confirmed fit permits that route-specific prototype to proceed. A shorter retention term requires expiry-aware architecture. Unsupported use disqualifies that route for that use, not every possible acquisition route. Unknown terms remain a blocker for relying on durable data, not proof of technical impossibility. A separate source's permission cannot be silently borrowed.

### T16: Inspect one company-publication response with provenance, not a profile sample

**Question:** What do a candidate provider's identity, date, counters, text and history actually mean?

**Minimum local experiment:** for one permitted known organization publication, obtain a minimized result and associated job/dataset receipt. Return only necessary original IDs/URLs or consistent pseudonyms, field names and values needed for structure, capture/receipt clocks, continuation, per-item error state and explicit derivation definitions. Strip credentials, unrelated items and unnecessary person/commenter information locally. Record original-versus-sanitized hash distinction; do not claim a sanitized artifact is byte-identical to the original.

For a stored dataset, require the **Company Posts** dictionary and sample, not company profiles or Employee Posts. For a connected session, include the source/account failure mapping. For an async provider, include a known pending/ready/failed example or documented receipt schema. No paid request or account connection is authorized by this work record itself.

**Interpretation:** well-proven native field origin advances only that source profile. Unexplained timestamps stay unassigned as source time. Partial comment windows limit context claims. A dataset with no intermediate edits remains a latest/history supplement, not a complete source-version archive.

### T17: Test expiry and replay without fabricating a source deletion

**Question:** Can the existing evidence distinctions survive policy expiry, a crash during removal, redelivery and replay without reviving expired content or altering source history?

**Reachable next experiment:** a standalone offline policy-driven test using existing synthetic fixtures and explicitly injected example policies. Separate immutable content identity, allowed artifact lifetime, derived copies and audit metadata. Exercise before/at/after expiry, repeated deletion work, late arrival, redelivery, restart and references to removed artifacts. Policy-clock and calendar assumptions remain explicit; this test cannot grant permissions or certify real storage erasure.

**Interpretation:** failures revise the candidate lifecycle before any production integration. Success advances generic mechanics only. Actual backups, remote stores, M2 snapshots, deployment and source terms still require their own checks. This is the next selected bounded operation because the newly discovered archival conflict is both material and partly testable without credentials.

### Still-required live and sustained work

T10/T11/T14 still define minimal permitted raw source/DOM and publication-identity observations. T1/T2 and C8 still govern real serializer/converter/capture-time/history-route verification. T6 still requires sustained operation after a proven scoped route. No source-safe cadence, account count, workload or soak interval was invented here.

## 8. Failed retrievals and untested leads are retained without turning them into platform failures

The source register and `sources/access_log_006.json` distinguish unsuccessful retrieval from negative evidence. Coresignal's dictionary link did not return readable content. The daily-delivery article's direct open failed even though a dated provider search result and blog index supplied relevant text. Some bare LinkedIn documentation URLs served older views until the July view was requested explicitly. The served view is not a guarantee of the latest enabled October API version. Bright Data's older broad URL redirected to a profile page; that profile sample was not used or copied into this organizational work.

A candidate Apify actor appeared in search with a limited recent-publication claim. Its underlying implementation was not inspected, so it is only a lead. Pages Data Portability was linked from official help; its terms and actual route were not investigated in this pass. Organization events/jobs remain incompletely examined. Do not turn these into implemented capabilities or discard them merely because this bounded pass ended.

The previously inaccessible M1/M2 repositories were not retried without new access information. No absent capture is evidence that a capability cannot exist. Conversely, no publicly visible preview establishes permitted automated recurring access.

## 9. New artifacts and actual checks are research continuity work, not collection tests

| Artifact | Purpose | Input and actual execution | Limitation |
|---|---|---|---|
| `research/build_catalog.py` | Reproducibly author source/route/capability records and their readable matrix | Authored research facts, scopes and proposed boundaries; run locally | Not a source client, parser, entitlement check or independent fact checker |
| `research/acquisition_catalog_006.json` | Inspectable comparison with explicit evidence and live-status flags | 27 source records, 8 routes, 15 capability families | Records comparison state, not implemented feature counts |
| `sources/SOURCE_REGISTER.md` and JSON | Preserve exact URLs, dates, evidence types and limitations | Inspected documentation/code metadata and marked search leads | Paraphrase/inspection notes, not archived source pages or byte authentication |
| `sources/access_log_006.json` | Preserve failed paths and productive search chains | Actual observed retrieval outcomes | No original source collection or network outage conclusion |
| `lab/verify_package_006.py` | Verify prior members, checkpoint coverage, catalog links and package pointers | Existing archived files and newly authored files; run with retained receipt | Mechanical evidence checks, not runtime collector tests |
| `results/baseline_members_006.json` | Record the cumulative ZIP extraction and hash comparison | All 296 prior files | A setup-path mistake was corrected before hash verification |
| `results/verification_006.json` | Record actually executed checks | Prior manifest, unchanged member hashes, source-ID/route/capability consistency and checkpoint coverage | Does not assert perfect semantic retention or source truth |

**Real setup failure:** the first extraction retained an extra archive-root directory, so the manifest lookup failed with FileNotFoundError. The contents were moved up exactly one level and all 296 source members were compared successfully afterward. This is preserved as an artifact-setup error, not an upstream or collector failure.

The resulting STATE includes all thirty-four baseline sections, with old operational directions clearly historical and a new active route. Original source and test files remain unchanged under the historical directory. Old loose links cannot transfer the full executable corpus; continuation needs this cumulative ZIP or all its members. The full archive and its manifest are checked separately after authoring. No earlier test count is presented as newly executed.

## 10. Surviving decisions remain conditional

An owned collector is still the intended end state. Official APIs, permitted web-client reads, public pages, providers and organizational exports remain distinct candidates and supplements. No vendor is selected, no first production feature subset is frozen and no final Codex blueprint is issued.

The new conclusions would change for concrete evidence: an applicable different approved agreement could resolve a retention/use mismatch; a genuine source sample could establish a timestamp or identifier mapping; current route/deployment evidence could qualify the missing-history finding; a documented provider contract could establish idempotency, coverage or field lineage; and a failing expiry/replay experiment could require a different storage boundary. No conclusion changes merely because another route has not yet been proven.

**OPERATION:** Broaden organizational capability/acquisition evidence and compare access, provenance, history and retention.  
**DELTA:** Provider routes are differentiated; official access/retention gates exposed; new Company Posts supplement and dated delivery change identified; additional owned-page code inspected; comparison and history preserved.  
**OPEN HINGE:** Route-specific use/retention fit, genuine source/provider publication evidence, deployed M1→M2 admission and sustained operation.  
**ROUTE:** NEXT — bounded offline expiry/replay lifecycle test with injected policies, while keeping the empirical source and integration gates open.
