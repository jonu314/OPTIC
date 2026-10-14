# OPTIC Attribute Health

The independent worker discovers active attribute/job/channel contracts, detects count-distribution
and quality changes, saves immutable evidence, obtains a separate trend review, and independently
extracts selected attributes from full transcripts with **exactly `gpt-6.1-sol`**. Neither a stable
distribution nor an AI disagreement is a human-confirmed accuracy result. No production values,
prompts, queues, emails or schedules are changed.

## Safe execution

Use Python 3.13 locally from the checkout root (Python 3.11 is the prepared MLRun image):

```powershell
py -3.13 -m unittest discover -s optic_streamlit_dashboard -p "test_health_*.py"
py -3.13 optic_streamlit_dashboard\OPTIC_ATTRIBUTE_MONITOR.py run --fixture --fixture-ai --fixture-disposition --no-render
py -3.13 optic_streamlit_dashboard\OPTIC_ATTRIBUTE_MONITOR.py run --fixture --dry-run --plan-only
py -3.13 optic_streamlit_dashboard\OPTIC_ATTRIBUTE_MONITOR.py detect --read-only
py -3.13 optic_streamlit_dashboard\OPTIC_ATTRIBUTE_MONITOR.py review --resume RUN_ID --enable-ai
```

`--fixture` is visibly synthetic; only `--fixture-ai` enables its fake client. There is no automatic
fixture fallback when a real integration fails. `--research-fixture FILE` uses recorded aggregate
dates and cannot fabricate call identities/transcripts. `--dry-run`, `--plan-only`, and `--read-only`
never make AI calls, even with an AI-enabled environment. They may save **local** SQLite state,
aggregate snapshots, chart specifications, manifests and planned stages; `--plan-only` also suppresses
PNG rendering. Later stages require an explicit `--resume RUN_ID`. Resume retains successful
checkpoints and judgments; input hashes invalidate downstream stages when evidence/manifests
change. Each audit freezes its approved rubric and exact judge prompt/model in an immutable context.
Draft rubrics do not invalidate extractions or trigger regrading; even later approvals/prompt
versions cannot silently rejudge a completed old run. `--reevaluate --resume RUN_ID` is an explicit
new grading context using the latest approved rubric/prompt, preserving immutable first judgments.
A human Resolved disposition is never automatically reopened by resume or reevaluation.
`--expand` adds to the saved pilot under its original grading context instead of replacing it.
`replay --replay-end DATE` accepts
repeated dates, runs chronologically, uses at most 90 preceding complete dates per evaluation, and
does not infer missing human calibration/audit-yield results.

Real Odin inference requires explicit `--enable-ai`; the CLI overrides an AI-enabled environment
back to disabled otherwise. Fixture/research source modes reject real inference. A POST retry in
the Console only writes a local queue; it cannot launch inference. An operator must resume an
authorized independent worker. No production setup/build/run has been performed for this feature.

## Configuration and storage

`attribute_health.config.Settings` reads `OPTIC_HEALTH_<FIELD>` settings. Defaults: Central calendar,
90 inclusive complete dates ending yesterday, source readiness may move that end earlier;
ACQUISITION/RETENTION/SERVICE voice (use `--all-active-jobs` to expand). Registry always exposes
all active contracts and capability states. State defaults to the user's `.optic/attribute-health`
directory and must be outside the checkout; set `OPTIC_HEALTH_STATE_DIR` to an authorized durable
location. Never commit state, generated PNGs, transcripts or credentials.

### Per-enabled-job runtime readiness

Readiness is an implemented source-arrival gate, not a nonempty latest-date shortcut or an
activation-only calibration recommendation. `evaluate_job_readiness(window, settings, jobs,
evidence, contracts)` consumes bounded daily per-job source counts/IDs/conflicts/watermarks and
per-contract nonblank-presence counts. Service or chat volume cannot establish Acquisition
completeness. Invalid **nonblank** outputs count as present here and remain a separate quality
detector concern; readiness does not grade classification correctness.

The proposed end is capped by the complete Central calendar and `SOURCE_LAG_DAYS`. Volume is
compared with the job's own supported prior same-weekday median across at most four weeks.
The baseline needs three supported days and at least 100 aggregate baseline interactions by
default. ID coverage must reach 99%, with identity conflicts at most the remaining 1%, and an
explicit usable timestamp watermark is required. Missing/malformed evidence is unknown, not
proof of completeness.

Required-presence checks apply only to active attribute/job bindings. Resolved legitimate
allowed-empty contracts are exempt from required-presence checks, **not** from volume/ID/
watermark checks. Required attributes need supported prior in-window high presence; unstable,
historically absent, ambiguous and unmapped contracts remain explicitly unknown rather than
inventing retrospective activation. Among supported required contracts, at least 90% must
currently reach 80% nonblank presence. Thus one missing-attribute surge among ten supported
attributes remains visible to quality detection; widespread partial fills fail the gate.
No usable required-attribute readiness evidence explicitly defers the job.

The policy scans at most seven recent dates. Demonstrably incomplete jobs can roll back to a
proven ready date; unknown jobs or jobs with no proven ready date are explicitly deferred.
The shared end must itself be ready for every retained job; jobs lacking that aligned-date
proof are deferred. The worker recomputes exactly 90 inclusive dates after rollback, persists
all diagnostics, and marks deferred contracts **Not yet evaluated**, with scan p/q set to one
while retaining the whole declared test family. Deferred jobs cannot enter review, sampling,
audit or sentinel dispatch. Resume applies saved readiness to the adapter and never
reprobes/reinterprets a successful frozen run.

All names below have the `OPTIC_HEALTH_` environment prefix and are explicitly passed through
the prepared deployment configuration, including defaults:

| Setting | Default |
|---|---:|
| `SOURCE_LAG_DAYS` | 1 |
| `READINESS_LOOKBACK_DAYS` | 7 |
| `READINESS_BASELINE_WEEKS` | 4 |
| `READINESS_MIN_BASELINE_DAYS` | 3 |
| `READINESS_MIN_BASELINE_VOLUME` | 100 |
| `READINESS_VOLUME_FRACTION` | 0.8 |
| `READINESS_ID_FRACTION` | 0.99 |
| `READINESS_REQUIRED_PRESENT_FRACTION` | 0.8 |
| `READINESS_ATTRIBUTE_READY_FRACTION` | 0.9 |
| `READINESS_ATTRIBUTE_BASELINE_DAYS` | 14 |

These configurable thresholds still require calibration, but the fail-safe gate and its unknown/
deferred states function now; no live smoke query is needed to test those behaviors.
Settings construction and the runtime readiness policy share one validator: source lag is 1–89
days, readiness scanning is 1–30 days, weekday baseline lookback is 1–12 weeks (default four),
attribute baseline lookback is 1–89 days, and minimum support is 1–12 days without exceeding
either baseline lookback. Minimum baseline volume is 1–1,000,000,000,000; fractions are in (0,1].
Every observation and baseline remains inside the run's maximum 90-date analytical window.

SQLite WAL + `BEGIN IMMEDIATE` protects compare-and-set writes, idempotency keys, fenced leases and
attempt/token reservations across workers. Run/aggregate/evidence/chart/manifest/extraction/history
snapshots are immutable; the incident/current-judgment projection may advance while immutable
`judgment_versions` retains first blinded judgments. A reclaimed lease increments its fence;
the old owner cannot reserve or checkpoint afterward. Worker/AI leases are bounded and renewed.
Expired request reservations remain conservatively charged; dispatch reconciles them, whereas GET
budget views are read-only. Local SQLite requires a filesystem with reliable shared locking.
**Do not put the database on an NFS/object mount that lacks correct SQLite locking.** For multiple
hosts use a validated durable shared filesystem or a future authorized transactional state adapter.

Daily defaults: 500 transcript request attempts, 20 trend-review attempts, four concurrent Odin
requests, 10 new reviews/run, five new pilots/run, 120 items/incident. **Retries count as attempts.**
Sentinel and dismissed audits share one persisted rotating reserve capped at 10% (50 attempts);
ordinary audit capacity is initially 450. The reserve never doubles because both queues exist.
One reserve pilot may be selected per run and it remains subject to total/day/token limits.
The 10-review and five-new-pilot limits are cumulative for the immutable run, **not** fresh
allowances per stage invocation or resume. SQLite atomically records distinct
`run_id + kind + incident_id` reservations under matching run/phase fences. Retries reuse their
incident reservation, expansion of an existing pilot consumes no new-pilot slot, and successful
work remains skipped. Excess stays queued explicitly for a **NEW run**. Changing a resumed
process's Settings cannot raise the frozen run envelope. Legacy partial runs conservatively
seed prior persisted work before admitting further reservations.
Input/output daily budgets are 2,000,000/500,000; per-request limits 24,000/2,000. Live text
reservation uses a deliberately conservative UTF-8-byte upper bound plus framing/image allowances;
large full packets/transcripts are queued rather than truncated. Operators can raise the explicit
request envelope after validating actual token usage; no unverified prices are assumed. Fake
clients use their deterministic simulated token counts, not a live-model estimator. A timeout,
missing usage envelope or HTTP failure charges the entire reservation; reported actual usage
settles successful calls. Central-day counters reset by ledger date, not process restart.
Attempts, retry count, unique transcript hashes, call-attribute items, successful requests and
input/output token usage are separate measures.

The audit-item ledger and each run/incident summary separately count selected source calls and
call-attribute items. Forty fixture calls can legitimately have two identical transcript contents,
two cached content extractions, **38 cache hits**, and only two judge requests (plus one trend
review); that is not a two-call audit. `unique_transcript_hashes` measures dispatched content hashes,
whereas `unique_transcripts` counts distinct processed source IDs. `audit_counts` also exposes
`unique_source_calls`, `saved_judgments` and `cache_hits`.

## Measurement, statistics and immutable charts

Coverage entries (including Watch, insufficient-data and outside-allowlist contracts) are
lightweight projections referencing the frozen run/daily snapshot. GET drilldown reconstructs
their packet and chart spec in memory, verifies snapshot hashes and windows, and does not write
state, query Snowflake, render PNGs or dispatch inference. Calibrated Candidates and selected
sentinels save immutable packets/specs once; incident rows retain references, not duplicate copies.
PNG exports occur only after a trend-review slot is acquired and image review is explicitly
verified/enabled. The Console renders saved/generated specs directly. The declared test family
is stored once by hash; this does not remove unsupported contracts from the correction family.

Large repetitive state payloads use a versioned lossless compressed representation. Store readers
accept both existing plain JSON and compressed rows; hashes and immutable-object comparisons are
based on their decoded canonical content. The listing index avoids sorting large payloads on disk.
Do not run an older Store implementation against a state database containing compressed rows.

Watch contracts may advance to Candidate when calibrated evidence arrives. Coverage status updates
remain current without duplicating tracking incidents; completed human dispositions are preserved.
A non-severe persistent movement may be deferred only when the validated reviewer recommends
Population-mix supported and measured evidence explains that label. Each unexplained persistent
label and every severe signal still overrides deferral. Legacy multi-label packets with no label-specific
mix evidence remain conservative. Deferred incidents retain the dismissed-audit reserve and seven-day
recheck. The Console shows Monitoring distinctly from statistical Watch.

Registry keys are attribute ID + job/channel + output-contract hash, never display name alone.
Daily eligible/valid/allowed-empty/missing/invalid counts must reconcile; the full calendar includes
zero-volume dates with undefined rates. Finite types get output-shift monitoring, all types get
completeness/validity, and qualifying text gets `TEXT_PRESENCE_3_WORDS` presence—not semantics.
Numeric distribution and general free-text semantic drift are deliberately deferred, never “healthy.”
Unbounded LIST contracts retain validity monitoring without pretending to have a finite label
distribution. INTEGER contracts receive numeric-validity coverage; their numeric distribution
monitoring is explicitly deferred.

`Settings.raw_dimensions=True` enables language/site from exact integrated-to-raw production ID,
connection, job and interaction-time joins. Tied conflicting raw dimension values stay unknown;
other OPTIC labels are not substituted as explanatory ground truth. Real registry discovery
retrieves attribute IDs dynamically. Recorded aggregate-only research snapshots label their
attribute IDs unverified instead of presenting synthetic/reconstructed identities as production IDs.

The core detector fits weekday/volume-aware residuals and overdispersion, and calibrates with
**moving weekday block vector null simulations that rerun the full category/date/recent/EWMA
scan**. Whole-declared-contract-family **Bonferroni** is conservative under arbitrary dependence,
including practical prescreen non-survivors. Empirical p-values are `(exceedances+1)/(simulations+1)`,
never zero. Adaptive work is bounded by configured simulation minimum/maximum; insufficient
precision is explicitly Watch, not significance. Two full non-overlapping seven-day blocks confirm
persistence; historical local segments/complexity penalty and EWMA Watch cover steps/gradual movement.
These are initial calibration assumptions, not lifetime false-incident guarantees. Replay and
held-out stable fixtures must assess false incidents, delay and suppression misses before activation.

An open incident freezes its original baseline/evidence. New runs link their latest detection
without silently replacing the prior decision or moving its baseline. If baseline dates leave the
current window, it explicitly says “pre-shift baseline no longer available in the current window.”
Independent dated regimes can create new linked incidents; old incidents remain historical.
If a new calibrated Candidate adds a supported, stable, persistent step at least 14 days from
already saved steps, the worker snapshots a linked **new-regime incident**, even when the
oldest onset and direction set are unchanged. Its packet/sample/audit queue covers the new step;
the predecessor's evidence, judgments, manifest and human resolution are preserved. Small
onset jitter or another category at an existing step does not create a duplicate. Watch or
unsupported new movement cannot reopen a resolved incident. Missing/invalid/allowed-empty
processing signals route to explicit processing investigation before semantic-label audits.

Evidence packets contain exact dates/watermark, contracts/provenance, daily numerators/denominators,
breakdowns, metric IDs, signals, calibrated statistics/mix and unknowns. Core ECharts specifies raw
rates, a faint seven-day volume-weighted smoother, volume and supporting categories with gaps and
bands. `evidence.save_evidence` exports PNG through local ECharts + headless Playwright/Chromium,
not an operator's browser/filter state. Packet, spec and PNG hashes are separate and validated.
`--no-render` explicitly keeps saved specs without PNG. Install Chromium separately when authorized;
`OPTIC_HEALTH_BROWSER_CHANNEL=chrome` is an explicit alternative to bundled Chromium.
Packet statistics reference the declared family by hash, size and run reference rather than
repeating a potentially enormous contract-key list; full family keys remain in the saved run.

## Verification and bounded read-only signoff

The final complete mocked selectors passed **351 tests**: 239 root health tests and 112 web/API/
Model Lab tests. Counts are actual runner results, not collection counts or sums of overlapping
selectors. The module totals are engine 70, repository 11, readiness 34, store 34, audit 35,
workflow 44, setup 11, API 60, Model Lab 46, Model Lab resume 3 and Model Lab identity 3.
Readiness coverage includes accepted minimum/maximum settings, all 1–12 weekday-baseline weeks,
rejected out-of-range values, and identical settings/runtime validation of cross-field support.
All 28 changed Python files compile.

The fresh corrected-code synthetic proof run `7ab6ea388f114e04b840e684dae3631a` used a separate
outside-source-control store and fake clients only. It saved a candidate, trend Audit recommendation,
40-item manifest with 15/15/5/5 strata, 40 immutable judgments, weighted summaries, 20 human-review
items, and an explicit fixture-only Inconclusive/Monitoring disposition visible through the
saved dashboard APIs. Four packet and four PNG hashes were verified. Two content extractions,
38 cache hits, one trend-review attempt and two judge attempts were recorded; cumulative run
envelopes consumed one of ten review slots and one of five pilot slots. `final_verification.json`
records the actual test results and fresh-flow assertions.

The original UI proof store for run `b4a3e34e492c43bc96f8d12a9cf8acc5` was retained byte-for-byte.
After an explicit UI human Resolved disposition and an unapproved rubric draft, its completed
resume left both request counts unchanged and preserved the resolution. `ui_verification.json`
records the desktop/mobile checks, zero unexpected JavaScript errors, preserved judgment
history and explicit 503 recovery behavior.

Focused tests cover partial Acquisition versus ready Service,
missing/lagging jobs, exact 90-day rollback, allowed-empty/inactive exemptions, absent IDs/
watermarks, partial required fills, unknown historical applicability, a resolved two-step incident
followed by a linked third regime, repeated-resume envelopes, two-worker fencing and frozen limits.

The same persisted-store mobile proof was recaptured with a 332 × 440 chart showing the real
90-point series, populated reviewer/mix data, reachable controls, and no document overflow or
unexpected JavaScript errors. It issued no POSTs and changed no budgets; the isolated preview was
stopped. Plain resume of the saved proof after the new gates preserved the human Resolved state,
40 judgments, two judge attempts and one review attempt. Its frozen run envelopes report one
of ten review slots and one of five pilot slots used.

Recorded default voice coverage declared 590 contracts and used 11,799 null simulations.
The recorded Acquisition Generic series produced data-derived August 24 and September 19 step
estimates; these are statistical estimates, not hardcoded required dates or confirmed defects.
Twenty synthetic stationary 90-day series produced zero Candidates. That small null fixture
result is not a production false-positive guarantee, a human-accuracy benchmark or a lifetime
calibration claim.

Bounded source checks used a **transient authorized local credential fallback** for
SELECT-only Snowflake smoke checks; no credential or workstation path is stored in this runbook
or source. Catalog/schema discovery returned 1,871 contracts across 23 jobs, 16 explicit conflicts,
exact member IDs, and 402 source columns. Per-job readiness for ACQUISITION plus **one** active
required field, `account_verification`, reached October 12. That deliberately narrow probe is
**not validation of all contracts or a production rollout**.

One selected-call identity smoke resolved the correct production ID → analysis.ID →
analysis.ANALYSISID → content.ANALYSISID relationship with one 3,456-character content row.
Full transcript text was not persisted. Generated finite-LIST SQL was also checked using
SELECT-only synthetic VALUES with the existing Python 3.11 connector: three positive JSON/comma
case variants and three valid negatives, including `[]`, matched; invalid/null/scalar inputs
were excluded. That SQL-shape test read no production calls.

No Odin inference, production mutation, deployment, schedule installation or commits were used
for these signoff checks. All-contract readiness calibration, rollout validation, human judge
calibration and any live-inference activation remain explicit prerequisites.

## AI review and independent audit

The Odin adapter follows Model Lab and NEC/IGL gateway conventions: HTTPS deployment chat
completions, `api-version=2025-04-01-preview`, JSON mode, `max_completion_tokens`, low reasoning
for verified GPT-6.1 Sol, and no temperature or silent fallback. Token precedence is MLRun secret/
environment `ODIN_API_TOKEN`, `ODIN_BEARER_TOKEN`, `ODIN_API_KEY`, with optional Bearer prefix stripped.
Base overrides: `ODIN_GATEWAY_BASE_URL`, then `ODIN_API_BASE_URL`. Only transport/429/5xx are retried
with bounded backoff/jitter; **401/403 fail immediately**. Raw gateway errors and secrets are never
persisted. Errors have safe codes and retryability.

Trend review validates exact packet hash, decision schema, metric IDs and covered ISO dates.
Invalid requests/JSON/references never clear a candidate. Severe or persistent candidates override
deferral. Monitor is due after seven new complete dates. Multimodal is disabled until explicitly
verified with `OPTIC_HEALTH_IMAGE_VERIFIED=1`; then the saved PNG hash/packet identity is checked.
Otherwise the reviewer honestly uses structured-data mode and humans still have the chart.

Pilot strata: 15 pre-flagged, 15 post-flagged, five pre-complement and five post-complement.
Seeded uniform metadata candidates are bounded to 120; full transcripts are fetched only after
selection. The manifest saves unique IDs, dates, population counts, probabilities, weights, brand
cells when daily/repository population totals reconcile, and explicit shortfalls. Expansion adds
toward 45/45/15/15 without discarding pilot calls. Unverified segment totals get stratum weights
only, not invented segment probabilities. Unsupported quality-only label samplers route to
data/processing review rather than grading classification semantics.

The judge sees **only** full transcript, system instructions, definition/options/empty rules and an
approved rubric. It does not see production answer, period/identity, detection rationale or model
identity. Returned quotes must occur exactly within the asserted speaker's turn. Missing, ambiguous,
inconsistent, multi-content-unresolved or truncated identities remain unresolved and are never
AI-graded. Identity is production ID → analysis.ID → analysis.ANALYSISID → content.ANALYSISID;
connection/job/time checks and unique fallback are repository responsibilities and revalidated
by the audit. Never silently choose an arbitrary latest connection/longest content row.

Programmatic comparison uses shared normalization; allowed empty is not missing/false. Insufficient
and ambiguous judgments count as neither correct nor incorrect. `call_type_acq` needs a versioned
human-approved rubric before supported semantic grading; draft text is never hidden ground truth.
Cache key: transcript hash + attribute ID + contract/rubric hash + exact judge model + prompt version.
Stored-output comparison is separate, so cached extraction can compare an independently changed
production result. Historical scope remains current-contract retrospective unless recovered.

Summaries post-stratify using selection weights, resample whole calls within cells, envelope boundary
results with weighted Wilson cell intervals, report pre/post category disagreement/confusions and
separate ambiguity/unresolved rates and unresolved-population bounds. Missing cells/population share
are explicit; there is no unweighted “accuracy” for an oversampled flagged corpus. Twenty human
examples rotate across period/agreement/disagreement/uncertain. Human accepts/rejects are stored
with authors, time and history, but are not silently promoted to calibrated truth.

## Store/Console interface

`Store(Settings_or_state_dir)` supports `put/get/list`, `record_disposition`, `save_rubric`,
`record_item_review`, `queue_retry`, and read-only `budget_summary`. Local mutations require author
and idempotency; changed content with a reused key raises `ConflictError(ValueError, status=409)`.
Disposition outcomes: Likely classification degradation / Real population change supported /
Ambiguous contract / Data/processing issue / Inconclusive. Actions: Monitoring / Resolved /
Needs human review. Rubrics have immutable versions + explicit approved/draft state.

Kinds include registry, runs (snapshot with progress overlay), incidents, manifests, judgments,
dispositions, rubrics, item_reviews, retries, evidence, charts, chart_images and safe errors.
Incident evidence references packet_hash/chart_hash and optional PNG artifact, with saved packet/
chart_spec also inline. Summaries expose `periods.pre/post`, `category_specific`,
`human_review_items`, `human_review`, `complete`, and `shortfalls`. Budget exposes attempts
{judge,review}, transcript_attempts/review_attempts, input_tokens/output_tokens, successful_requests,
retries, call_attribute_items, unique_transcripts, reserved_requests, uncertain_usage_requests,
reserve_attempts and limits including combined_reserve. Full transcripts are not stored in normal
manifests/judgments; intentional drilldown uses the protected repository path, never arbitrary files.

The Console view is `/attribute-health`. Loopback-only APIs are
`/api/attribute-health/{overview,registry,runs,incidents}` and
`/incidents/<id>/{evidence,chart,audits}`. A PNG GET at
`/incidents/<id>/chart-image/<64hex>.png` validates incident/packet/spec/PNG identity.
POST actions are incident dispositions/retry/items/<id>/review/transcript and `/rubrics`.
POST requires exact same-origin Origin, JSON no larger than 32 KiB, one Idempotency-Key and
explicit author. Transcript payload `{author,confirm:true}` retrieves only a saved selected item;
no transcript is leaked through listing or persisted. Health GET does not import a live repository,
change state, or launch a worker.

## Iguazio / ADO activation handoff

Prepared `OPTIC_ATTRIBUTE_MONITOR_Setup.py` is import-safe and defaults to plan-only. It does not
sync secrets, submit runs, create/update schedules or autorun. Future authorized activation requires:

1. Copy reviewed files into the **confirmed ADO deployment repository**, not a presumed GitHub path.
2. Set `OPTIC_HEALTH_ADO_SOURCE_URL` with explicit `#refs/heads/BRANCH` (or `IQUAZIO_SOURCE_BRANCH`),
   `OPTIC_HEALTH_JOB_PATH` to confirmed repo-relative worker path, `OPTIC_HEALTH_RACF_ID`,
   `OPTIC_HEALTH_STATE_DIR` to the mounted durable directory, and
   `OPTIC_HEALTH_SHARED_STATE_CONFIRMED=1` only after validating locking/durability.
3. Use the approved MLRun 1.10.1 / VistraDsml 1.10.1.6 Python 3.11 deployment client.
   Prepared setup uses `marketing-tech`, with_repo=True, explicit requirements/Chromium commands,
   matching PLAYWRIGHT_BROWSERS_PATH, persisted fn.set_env/requests/limits, and explicit build_function.
   It publishes/readbacks a versioned ready image and DB URI only after successful build.
4. Supply existing Snowflake key-pair and Odin secrets through the runtime secret mechanism.
   Verify read-only source privileges, complete-day readiness, identity policies and state access.
5. Approve business rubrics, independently verify multimodal if desired, calibrate judge against
   human items and replay thresholds. Only then authorize real inference and budgets.
6. After separate schedule authorization, recommend a daily source-refresh-aware tick **after**
   readiness checks, with concurrency one; inspect existing pinned function/task references. No
   schedule has been installed and no fixed UTC time is assumed to prove Central completeness.
