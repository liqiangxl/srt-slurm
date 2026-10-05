---
name: request-lifecycle-analysis
description: Read existing SGLang benchmark worker logs and dump request-count statistics for each lifecycle queue, separately by worker, rank and batch type. Use for aggregate or split prefill/decode queue analysis; no dashboard, external repository or job execution is required.
---

# Request lifecycle queue statistics

For "analyze a job", first read its benchmark/launcher log, resolved recipe and
worker logs. Verify job, role, worker, topology and measurement boundaries.
Dump observed request counts at each logged queue using the self-contained
standard-library script below. It is adapted from the request lifecycle method
in `sa-cache-dashboard-vis`; that repository is not needed at runtime.

## Load the logs

Find worker-log paths in the actual run's launcher metadata/logs. A client-only
`benchmark.out` normally contains no scheduler queue counts: use its worker-log
references, or report that worker evidence is missing. Do not infer absent
queues from client latency. Supply each log's verified role and worker identity;
do not assume node ordering, an unlabeled rank of zero, or aggregate topology.

Resolve `SKILL_HOME` from this `SKILL.md`, not the working directory:

```bash
python3 "$SKILL_HOME/scripts/queue_stats.py" \
  --job JOB --mode disaggregated \
  --log prefill PREFILL_WORKER /path/to/prefill.out \
  --log decode DECODE_WORKER /path/to/decode.out \
  --output /path/to/report/queue-statistics.md \
  --json-output /path/to/report/queue-statistics.json
```

Use `--mode aggregate --log aggregate WORKER /path/to/worker.out` for a shared
scheduler. Each log is frozen at its initial byte size; only complete lines are
read and hashed. The script streams summaries without storing all events.
It never contacts a cluster, changes jobs, scrapes endpoints or imports another
repository. Python 3.10+ is sufficient; no pip installation is needed.

By default the report covers the whole available log and explicitly includes
any warmup/drain. For phase-aligned statistics, independently verify native phase
epochs and the worker log's clock mapping, then add:

```bash
  --start 2026-10-01T21:13:25+00:00 \
  --end 2026-10-01T22:13:55+00:00 \
  --log-utc-offset-minutes VERIFIED_OFFSET \
  --clock-source 'Native epoch evidence and verified worker clock mapping'
```

Those dates illustrate syntax only; use this run's actual boundaries. Logs on
different clocks need separate invocations. Never guess a timezone or trim the
initial replay lull. Keep raw evidence and earlier reports. Source-prefix hashes,
retained/tail bytes, selected interval and source identities are in the JSON.

## Read the statistics

Each row identifies source log, worker role, native rank labels, Prefill/Decode
report type and the literal count field. The report gives valid sample count,
missing/invalid count, unweighted sample mean, minimum, sampled maximum and
latest valid value/timestamp. Expected but absent fields remain unavailable.

Include a fenced Mermaid request-flow graph before the statistics tables, using
`flowchart TB` for top-to-bottom direction. Follow
the producer/receiver flow: arrival -> prefill bootstrap; corresponding decode
preallocation -> receiver transfer reservation, with destination metadata/readiness
back to bootstrap; prefill waiting -> running prompt chunks and first token ->
handoff/in-flight -> sender completion and cleanup. Independently, decode transfer
-> ready waiting -> running subsequent tokens -> completion, with streamed output.
Use dotted cross-worker dependencies for KV/state chunks during computation and
final KV/recurrent state/first-token metadata handoff. Sender completion is a
milestone, not a queue or generation completion. Preserve transfer overlap with
prefill computation. These are conceptual dependencies, not universal handshake
ordering or traced transitions. In each flowchart block, briefly explain its waiting/admission gate; running
blocks describe computation/generation, completion blocks describe release, and
frontend/output blocks describe routing/delivery. These explain state semantics,
not a proven blocking cause for this run. In flowchart nodes, show only the sample mean rounded to one decimal (e.g. 7.1);
keep maxima and other statistics in the tables. Annotate only an unambiguous worker/rank series;
otherwise refer to separate tables. Aggregate flow uses a shared scheduler and
has no remote handoff. Keep this definition self-contained at runtime.

Always include prefill-running and handoff in the statistics report, not only in
prose. For current Prefill batch logs, show active prefill request occupancy as
**Unavailable / not logged**, and show `#new-seq` batch-entry statistics separately
as observations of the prefill-running stage. Never substitute `#running-req`
(existing work outside the current prefill batch) or `#new-seq` for active
occupancy. Show sender handoff/in-flight statistics from `#inflight-req`, including
missing evidence when absent. Keep sender handoff and receiver transfer separate:
they can overlap for the same request. An aggregate scheduler has no remote
handoff stage. Audit any future explicit occupancy field before mapping it.

Explain the queues using the deployed version's meanings:

- Aggregate: frontend/tokenization -> shared SGLang waiting -> prefill/chunk
  continuation -> decode on the same worker -> output/completion. No remote
  bootstrap, receiver preallocation or P/D transfer states apply.
- Disaggregated prefill: bootstrap (receiver metadata/readiness), waiting
  (scheduler admission), prefill running (active prompt computation), handoff/in-flight (sender completion).
- Disaggregated decode: preallocation (request/KV/recurrent/metadata capacity),
  transfer (reserved receiver awaiting usable prompt state), ready waiting,
  running generation, then release at completion.

Name each discussed state's owner, input, admission gate and release event.
Counts alone do not identify why a specific request is blocked. Decode transfer
can include waiting for prefill computation; it is not network saturation.
`#new-seq` counts prefill batch entries including chunk continuations, not unique
arrivals. Prefill `#running-req` can describe existing work outside the current
prefill batch. `#retracted-req` is an event batch size, not queue occupancy or a
cumulative retraction total. Completion counts remain unmeasured without logs.

Keep roles, TP/PP/DP/EP labels, source files and batch types separate. Do not sum
replicated ranks or prefill/decode records of the same request. Reporting-sample
means are not time-weighted occupancy; sampled maxima can miss true peaks.
Missing counts are not zero. No request IDs, residence times, per-request
transition counts, model/kernel timing, throughput or accuracy are inferred.

Follow [per-run performance report requirements](../../../docs/perf-analysis.md)
when analyzing a real run: link the statistics and skill in the actual run's
`perf-analysis.md`, and source client performance metrics separately. If work is
restricted to offline/read-only, retain a local draft and state that the cluster
report was not updated. This skill authorizes no submission or optimization sweep.

## Validate changes

```bash
python3 -m unittest discover -s "$SKILL_HOME/scripts/tests" -v
```

Tests use synthetic logs, not benchmark evidence. The script and skill are based
on the current SGLang aggregate/P-D field conventions used by
`sa-cache-dashboard-vis`; audit changed log formats before interpreting them.
