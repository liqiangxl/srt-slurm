Read `CLAUDE.md` for the development guide: commands, PR descriptions, style, and the Design Rules.
Subsystem rules are in the `AGENTS.md` file of each directory you touch; review criteria are in `REVIEW.md`; recurring procedures are skills in `.agents/skills/`.

For every Slurm run you launch or analyze, write `<run_dir>/perf-analysis.md` in
the actual run output directory on the cluster.
Follow [Per-run Performance Analysis](docs/perf-analysis.md)
for the required TTFT/ITL percentiles, throughput, agentic Pareto/SLO metrics,
sources and missing-data handling. For performance diagnosis, debugging or
improvement runs, explain how the collected metrics, files, DSight/dashboard
views and skills helped answer the goal.

Before using DSight, read [docs/dsight.md](docs/dsight.md) and load the applicable
DSight skills listed there by reading the linked skill files. This applies to
building or querying reports, analyzing existing results, preparing dashboard
views, and changing DSight code.

Before preparing or submitting Slurm jobs, read
[slurm-job-sizing](src/srtctl/dsight/skills/slurm-job-sizing/SKILL.md)
to size allocation time and benchmark traffic to the task.

For analysis of existing SGLang serving jobs, use
[request-lifecycle-analysis](.agents/skills/request-lifecycle-analysis/SKILL.md).
It uses a self-contained Python script to load benchmark worker logs and dump
per-queue request-count statistics by worker, rank and batch type. No external
repository or HTML bundle is required. Preserve the per-run report and evidence
requirements above.

For execution of an authorized performance optimization goal, use
[perf-goal-runner](.agents/skills/perf-goal-runner/SKILL.md). It coordinates
baseline analysis, falsifiable hypotheses and controlled verification through
the existing skills, preserving campaign evidence and authorization/budget limits.
Use perf-goal-writer when the request is only to draft or refine the goal.

## IBPL reporting for future runs

For every new or revised benchmark recipe, ensure this reporting block is
present in the recipe and in the effective cluster `srtslurm.yaml` selected by
`SRTSLURM_CONFIG`. The automatic post-run uploader reads the cluster config.
Preserve existing reporting settings when merging this block.

```yaml
reporting:
  s3:
    bucket: "ibpl-uploads-external"
    prefix: "SRT"
    secret_access_key: null
    access_key_id: "team-coreai-comparch-infbench-ibpl-uploads-external"
    region: "us-east-1"
    endpoint_url: "https://pdx.s8k.io"
```

The supplied upload secret is stored only in the owner-readable local file
`/Users/liqianglu/.config/srt-slurm/ibpl.env` (mode 0600). Source it in the
actual submit shell before `srtctl apply`; a local environment is not inherited
automatically by an SSH command. Keep secrets out of recipes, lockfiles,
documentation, logs and Git. Never print the credential file. Leave
`secret_access_key` null so the uploader resolves `AWS_SECRET_ACCESS_KEY`.

Before preparing a launch, read the installed
[ibpl-srt-tagging skill](/Users/liqianglu/.codex/skills/ibpl-srt-tagging/SKILL.md).
Every submission must include `ibdb_experiment_id`, `ibdb_study_id` and
`ibdb_author` (use `liqiangl` for this user's runs). Choose stable experiment
and distinct study IDs from the actual campaign; do not leave placeholders.
This checkout accepts a single comma-separated `--tags` value:

```bash
source /Users/liqianglu/.config/srt-slurm/ibpl.env
srtctl apply -f recipe.yaml \
  --tags "ibdb_experiment_id=<experiment>,ibdb_study_id=<study>,ibdb_author=liqiangl"
```

For disaggregated one-sided speed-of-light probes only, add
`ibdb_inference_serving_tag=disagg-ctx` when OSL=1, or `disagg-gen` when ISL=1,
as specified by the tagging skill. Do not tag normal serving points as SOL
probes. Validate the resolved reporting settings and launch tags before
submission, and preserve upload success/failure evidence after completion.
Adding reporting does not authorize new benchmarks or retrospective uploads.
