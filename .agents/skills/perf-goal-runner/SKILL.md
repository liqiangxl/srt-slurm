---
name: perf-goal-runner
description: Execute an authorized performance optimization or regression investigation toward measurable targets using matched baselines, request lifecycle evidence, falsifiable hypotheses and controlled experiments. Use to run the investigation, not merely draft a goal or summarize one saved job.
---

# Run a performance goal

Coordinate **Analyze → Hypothesize → Verify** until the agreed targets are
verified or a stopping condition is reached. Reuse the skills below; keep
parsing, query semantics, configuration changes and job sizing in their owners.
Loading this skill grants no additional execution authority.

## Establish or resume the campaign

Read the user’s goal and existing campaign records. Identify acceptance metrics,
percentiles, units, required load points, correctness/error/unfinished-request
guardrails, baseline identity, fixed controls, permitted variables and execution
scope. Preserve any specified time, GPU, run-count or other resource budget.
If these are unresolved, use [perf-goal-writer](../perf-goal-writer/SKILL.md) to
refine the goal; ask only for missing decisions that block dependent work.
Continue independent analysis of accessible evidence. Do not start a persistent
`/goal` merely because this skill is invoked.

Before further experiments, reconcile submission receipts, scheduler states and
saved results. Reuse completed evidence and active jobs. An ambiguous submission
must be reconciled before another is launched; never duplicate an active or
unknown attempt. Follow the user's connection/retry restrictions and site
workflow. Record each submitted job ID immediately. Drafting, monitoring or
analysis alone does not authorize submissions, cancellations, serving restarts
or changes to pinned model/image inputs.

Maintain a compact `root_cause.md` in the campaign directory with the goal,
controls, authority, budget consumed/remaining and next action. Keep an experiment
ledger there: hypothesis and predicted signal, exact change versus baseline,
recipe/source/image identities, job receipt/state, evidence paths and phase,
metrics/validity, result and next decision. Preserve failed experiments and
rejected hypotheses. Resume from that ledger, not from a repeated baseline run.

## Analyze the baseline

For SGLang serving queue or producer–consumer questions, load
[request-lifecycle-analysis](../request-lifecycle-analysis/SKILL.md). Generate
phase-qualified queue statistics and the request-flow diagram from saved worker
logs. Use aggregate or disaggregated flow as appropriate. Verify actual startup
limits and observed batches; keep worker/rank and reporting populations separate.

When matching DSight artifacts exist and finer request timing, metric or profiler
evidence would answer the question, load
[dsight-query](../dsight-query/SKILL.md) and its required guide. Query existing
artifacts before collecting new captures. Missing evidence is unavailable, not
zero. Select evidence relevant to the hypothesis rather than requiring every
possible profiler.

Source performance and validity separately from native benchmark exports.
Check clocks, phase coverage, warmup/drain, errors, cancellations, unfinished work
and workload/cache populations before comparing. Queue means do not establish
per-request residence time, hardware utilization or a blocking cause. Transfer
occupancy is not bandwidth; logical cached input is not computed input; native
interactivity is not reciprocal pooled ITL; assumed power is not measured power.

## Hypothesize and select the next experiment

Rank a small set of falsifiable bottleneck explanations. For each, record
supporting/conflicting evidence, a predicted lifecycle/performance signal and
the smallest comparison that separates it from alternatives. Collect missing
instrumentation within scope before attributing a cause.

Choose the next experiment by expected information or goal progress per resource
cost. Change one factor when isolating a cause; test combinations only after
individual effects are understood, or explicitly mark a compound exploratory
comparison. Retain matched model/image revisions, topology, workload, concurrency,
cache/warmup and benchmark settings except for the named variable. Do not alter
an official benchmark requirement to make a diagnostic run pass the target.

## Verify within the authorized scope

For repository implementation changes, follow the applicable subsystem rules.
Use [add-config-field](../add-config-field/SKILL.md) when introducing or changing
configuration fields, and
[validate-without-cluster](../validate-without-cluster/SKILL.md) for local dry-run,
mock/snapshot and required checks. Ordinary recipe tuning does not require a new
schema field. Validate the concrete candidate before a final approval gate when
one is required; reuse authorization already given.

Before preparing Slurm jobs, load
[slurm-job-sizing](../../../src/srtctl/dsight/skills/slurm-job-sizing/SKILL.md).
Use separate experiment/run directories, preserve the baseline and submit only
through the approved workflow within authorized aggregate resources. Short or
reduced tests are diagnostic; final acceptance requires the prescribed benchmark
and repetition/sample plan. Job-sizing advice never overrides pinned benchmark
warmup, duration or workload requirements.

Compare baseline/candidate under matched instrumentation. Use observability-on
for diagnosis and matched observability-off for final performance unless the
goal includes monitoring overhead; disclose functional settings changed by
observability. Collect and preserve native results and logs. Maintain each
actual cluster run's `perf-analysis.md` following
[per-run report requirements](../../../docs/perf-analysis.md), linking the skills,
queries and generated artifacts that supported the decision.

Evaluate the predicted signal alongside every acceptance metric, validity and
request population. Record regressions and variability; do not promote the best
single run, synthetic result or simulated acceptance into serving/correctness
validation. If throughput improves but a required interactivity/latency target
fails, the goal remains unmet. Update the ledger before choosing another arm.

## Stop and report accurately

Complete only when all required metrics, load points, repetitions and correctness
criteria pass with preserved evidence. Stop new dependent actions when the user
pauses/cancels, the budget is exhausted, required authority is absent, submission
state is unresolved, or evidence prevents a justified next experiment. Continue
unaffected analysis where useful. Do not silently expand the budget or launch a
new sweep. Mark any persistent goal status only under that goal tool's rules.

Report the best qualified result versus baseline, targets met/unmet, what the
lifecycle evidence supports, remaining uncertainty, resource use and the next
concrete decision. Distinguish a measured improvement from a validated root
cause. A blocked or budget-limited campaign is partial progress, not success.
