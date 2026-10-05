"""Streaming request-queue statistics from saved SGLang worker logs; no dependencies."""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import tempfile

FIELDS = {
    'bootstrap': '#bootstrap-req', 'preallocation': '#prealloc-req',
    'transfer': '#transfer-req', 'waiting / ready waiting': '#queue-req',
    'running (reported batch)': '#running-req', 'prefill batch entries': '#new-seq',
    'handoff / in-flight': '#inflight-req', 'grammar': '#grammar-req',
    'optimistic': '#optimistic-req', 'retracted (event batch size)': '#retracted-req',
}
EXPECTED = {
    'aggregate': ('waiting / ready waiting', 'running (reported batch)', 'retracted (event batch size)'),
    'prefill': ('bootstrap', 'waiting / ready waiting', 'running (reported batch)', 'prefill batch entries', 'handoff / in-flight'),
    'decode': ('preallocation', 'transfer', 'waiting / ready waiting', 'running (reported batch)', 'retracted (event batch size)'),
}
PREFIX = re.compile(r'\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)([^\]]*)\]')
VALUE = re.compile(r'(?<![\w-])(#(?:bootstrap|prealloc|transfer|queue|running|inflight|grammar|optimistic|retracted)-req|#new-seq)\s*:\s*([^,\s]+)')
LIMITS = [
    'Means are unweighted reporting-sample means, not time-weighted occupancy; maxima are sampled peaks.',
    'Worker/rank/batch/source identities stay separate; counts are not unique requests or traced transitions.',
    'Prefill #new-seq includes chunk continuations; prefill #running-req need not include the current prefill batch.',
    'Prefill running active occupancy is explicitly unavailable; batch entries and existing running work are separate observations.',
    'Sender handoff/in-flight and receiver transfer can overlap for the same requests; do not add their counts.',
    'Decode transfer can wait for prefill computation; occupancy does not measure network bandwidth.',
    'Retraction counts are event batch sizes, not occupancy or cumulative totals; missing fields are unavailable.',
]


def finite_count(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number >= 0 and number.is_integer() else None
    except ValueError:
        return None


def aware_time(value):
    result = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.utcoffset() is None:
        raise ValueError('Phase boundaries require explicit timezone offsets')
    return result


def reduce_log(path, role, worker, *, start=None, end=None, offset=None):
    path = Path(path).resolve()
    size, used, matched = path.stat().st_size, 0, 0
    digest, groups = hashlib.sha256(), {}
    with path.open('rb') as source:
        while source.tell() < size:
            raw = source.readline(size - source.tell())
            if not raw.endswith(b'\n'):
                break
            used += len(raw)
            digest.update(raw)
            line = raw.decode(errors='replace')
            prefix, values = PREFIX.search(line), dict(VALUE.findall(line))
            if not values:
                continue
            if role == 'aggregate' and any(label in values for label in ('#bootstrap-req', '#prealloc-req', '#transfer-req')):
                raise ValueError(f'{path}: handoff queues require disaggregated role metadata')
            if not prefix:
                raise ValueError(f'{path}: count-bearing log line has no supported dated timestamp')
            stamp = dt.datetime.fromisoformat(prefix[1])
            if start is not None:
                stamp = stamp.replace(tzinfo=dt.timezone(dt.timedelta(minutes=offset)))
                if not start <= stamp <= end:
                    continue
            batch = 'Prefill' if 'Prefill batch' in line else 'Decode' if 'Decode batch' in line else 'queue report'
            rank = ' '.join(re.findall(r'\b(?:TP|PP|DP|EP)\d+\b', prefix[2])) or 'unlabeled'
            key = (rank, batch)
            group = groups.setdefault(key, {'rank': rank, 'batch_type': batch, 'report_lines': 0, 'fields': {}})
            group['report_lines'] += 1
            matched += 1
            for name, label in FIELDS.items():
                if label not in values:
                    continue
                state = group['fields'].setdefault(name, {'field': label, 'samples': 0, 'invalid': 0,
                    'sample_mean': None, 'min': None, 'max': None, 'latest': None, 'latest_timestamp': None})
                number = finite_count(values[label])
                if number is None:
                    state['invalid'] += 1
                    continue
                n = state['samples'] + 1
                state['sample_mean'] = number if n == 1 else state['sample_mean'] + (number - state['sample_mean']) / n
                state.update(samples=n, min=number if n == 1 else min(state['min'], number),
                    max=number if n == 1 else max(state['max'], number))
                if state['latest_timestamp'] is None or stamp >= dt.datetime.fromisoformat(state['latest_timestamp']):
                    state.update(latest=number, latest_timestamp=stamp.isoformat())
    if not matched:
        raise ValueError(f'{path}: no queue/count reports in selected scope; supply worker logs, not client-only logs')
    for group in groups.values():
        expected = set(EXPECTED[role])
        if role == 'aggregate' and group['batch_type'] == 'Prefill':
            expected.add('prefill batch entries')
        for name in expected:
            group['fields'].setdefault(name, {'field': FIELDS[name], 'samples': 0, 'invalid': 0,
                'sample_mean': None, 'min': None, 'max': None, 'latest': None, 'latest_timestamp': None})
        # A Prefill batch report exposes entries, not active request occupancy.
        # Keep the absent lifecycle stage explicit instead of treating #running-req as it.
        if role == 'prefill' or (role == 'aggregate' and group['batch_type'] == 'Prefill'):
            group['fields']['prefill running (active request occupancy)'] = {
                'field': None, 'samples': 0, 'invalid': 0, 'sample_mean': None,
                'min': None, 'max': None, 'latest': None, 'latest_timestamp': None,
            }
        for name, state in group['fields'].items():
            state['evidence'] = (
                'Not logged; #running-req excludes the current prefill batch; do not substitute zero'
                if name == 'prefill running (active request occupancy)' else
                'Prefill-running batch entries; includes chunk continuations, not active occupancy'
                if name == 'prefill batch entries' else
                'Sender handoff/in-flight count; not completion rate or summed with decode transfer'
                if name == 'handoff / in-flight' else
                'Existing running work outside the current prefill batch; not active prefill occupancy'
                if name == 'running (reported batch)' and group['batch_type'] == 'Prefill' else
                'Literal logged count'
            )
            state['missing'] = group['report_lines'] - state['samples'] - state['invalid']
    return {'path': str(path), 'role': role, 'worker': worker, 'snapshot_bytes': size,
        'retained_bytes': used, 'omitted_tail_bytes': size-used, 'sha256_retained_prefix': digest.hexdigest(),
        'groups': list(groups.values())}



def lifecycle_mermaid(report):
    """Conceptual dependencies; annotate only an unambiguous worker/rank series."""
    def count(role, field):
        groups = [g for src in report['sources'] if src['role'] == role for g in src['groups']]
        if len(groups) != 1:
            return 'See separate worker/rank tables'
        state = groups[0]['fields'].get(field)
        if not state or state['sample_mean'] is None:
            return 'Not measured'
        return f"{state['sample_mean']:.1f}"

    if report['mode'] == 'aggregate':
        return '\n'.join([
            '```mermaid', 'flowchart TB',
            '  A["Arrival / frontend / tokenization<br/>Route and tokenize the prompt"] --> W["Shared scheduler waiting<br/>Wait for batch admission and KV/state capacity"]',
            '  W --> P["Prefill running / chunk continuation<br/>Compute prompt chunks and first token<br/>Active occupancy not measured; batch entries in tables"]',
            '  P --> D["Decode running on same worker<br/>Generate subsequent tokens"]',
            '  D --> O["Streamed output<br/>Deliver generated tokens to client"]',
            '  D --> C["Request completed<br/>Release request/KV/state resources<br/>Not measured"]',
            '```',
        ])
    return '\n'.join([
        '```mermaid', 'flowchart TB',
        '  A["Arrival / frontend<br/>Route prompt to serving workers<br/>Not measured"] --> B',
        '  subgraph P["Prefill producer"]',
        f'    B["Bootstrap<br/>Wait for decode receiver readiness and metadata<br/>{count("prefill", "bootstrap")}"] --> W["Prefill waiting<br/>Wait for batch admission and token/KV/state capacity<br/>{count("prefill", "waiting / ready waiting")}"]',
        f'    W --> R["Prefill running: prompt chunks + first token<br/>Active occupancy not measured<br/>Batch entries: {count("prefill", "prefill batch entries")}"]',
        f'    R --> H["Handoff / in-flight<br/>Wait for asynchronous sender completion<br/>{count("prefill", "handoff / in-flight")}"]',
        '    H --> PC["Prefill handoff complete<br/>Sender cleanup / release ownership<br/>Milestone; not measured"]',
        '  end',
        '  subgraph D["Decode receiver / consumer"]',
        f'    PA["Preallocation<br/>Wait for receiver handshake and request/KV/state slots<br/>{count("decode", "preallocation")}"] --> T["Transfer: receiver reserved<br/>Wait for usable prompt KV/state and first-token metadata<br/>{count("decode", "transfer")}"]',
        f'    T --> Q["Ready waiting<br/>Wait for decode batch admission and capacity<br/>{count("decode", "waiting / ready waiting")}"]',
        f'    Q --> DR["Decode running<br/>Generate subsequent tokens until EOS/output limit<br/>{count("decode", "running (reported batch)")}"]',
        '    DR --> DC["Request completed<br/>Release request/KV/state resources<br/>Not measured"]',
        '  end',
        '  B -. "Corresponding receiver preparation" .-> PA',
        '  PA -. "Destination metadata / receiver ready" .-> B',
        '  R -. "Earlier KV/state chunks can transfer during computation" .-> T',
        '  H -. "Final KV / recurrent state / first-token metadata" .-> T',
        '  DR --> O["Streamed output to client<br/>Deliver generated tokens<br/>Not queue occupancy"]',
        '```',
    ])


def markdown(report):
    def text(value):
        if value is None:
            return 'Unavailable'
        return (f'{value:.6g}' if isinstance(value, float) else str(value)).replace('|', '\\|').replace('\n', ' ')
    lines = [f"# Request lifecycle statistics: job {text(report['job'])}", '',
        f"Topology: {report['mode']}. Scope: {text(report['scope'])}.", '',
        'Values are request/count observations, not unique requests or time-weighted occupancy.', '',
        '## Request flow', '', lifecycle_mermaid(report), '',
        'Solid arrows show role-local progression; dotted arrows show cross-worker coordination/data dependencies.',
        'This is a conceptual flow, not a joined per-request trace. Sender completion and receiver readiness are independent.',
        'Node values are unweighted sample means rounded to one decimal; detailed statistics remain in separate tables.', '']
    for source in report['sources']:
        lines += [f"## {text(source['role'])} / {text(source['worker'])}", '', f"Source: {text(source['path'])}",
            f"SHA256: `{source['sha256_retained_prefix']}`; retained {source['retained_bytes']} bytes.", '']
        for group in source['groups']:
            lines += [f"### {group['rank']} / {group['batch_type']}", '',
                '| State / literal field | Samples | Missing | Invalid | Mean | Min | Max | Latest | Latest timestamp | Evidence / meaning |',
                '|---|---:|---:|---:|---:|---:|---:|---:|---|---|']
            for name, state in group['fields'].items():
                row = [f"{name} / {state['field'] or 'not logged'}"] + [state[k] for k in
                    ('samples', 'missing', 'invalid', 'sample_mean', 'min', 'max', 'latest', 'latest_timestamp', 'evidence')]
                lines.append('| ' + ' | '.join(map(text, row)) + ' |')
            lines.append('')
    lines += ['## Limits', ''] + ['- ' + item for item in LIMITS]
    return '\n'.join(lines) + '\n'


def write_output(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False, encoding='utf-8') as stream:
        temp = Path(stream.name)
        stream.write(content)
    temp.replace(path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job', required=True)
    parser.add_argument('--mode', choices=('aggregate', 'disaggregated'), required=True)
    parser.add_argument('--log', nargs=3, action='append', required=True, metavar=('ROLE', 'WORKER', 'PATH'))
    parser.add_argument('--start', type=aware_time)
    parser.add_argument('--end', type=aware_time)
    parser.add_argument('--log-utc-offset-minutes', type=int)
    parser.add_argument('--clock-source')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--json-output', type=Path)
    args = parser.parse_args(argv)
    try:
        bounded = args.start is not None or args.end is not None
        if bounded and (args.start is None or args.end is None or args.start >= args.end or
                args.log_utc_offset_minutes is None or not args.clock_source or not args.clock_source.strip()):
            raise ValueError('Phase selection requires increasing --start/--end, explicit log offset and clock evidence')
        if args.log_utc_offset_minutes is not None and abs(args.log_utc_offset_minutes) > 840:
            raise ValueError('Log UTC offset must be within +/-14 hours')
        allowed = {'aggregate'} if args.mode == 'aggregate' else {'prefill', 'decode'}
        if any(role not in allowed for role, _, _ in args.log):
            raise ValueError('Log roles must match the selected topology')
        inputs = {Path(path).resolve() for _, _, path in args.log}
        outputs = [p.resolve() for p in (args.output, args.json_output) if p is not None]
        if len(inputs) != len(args.log) or len(set(outputs)) != len(outputs) or set(outputs) & inputs:
            raise ValueError('Use distinct input logs and output paths; never overwrite source evidence')
        sources = [reduce_log(path, role, worker, start=args.start, end=args.end,
            offset=args.log_utc_offset_minutes) for role, worker, path in args.log]
        report = {'format': 'request-lifecycle-log-statistics-v2', 'job': args.job, 'mode': args.mode,
            'scope': f'{args.start.isoformat()} to {args.end.isoformat()}' if bounded else 'whole available log, including any warmup/drain',
            'clock_source': args.clock_source, 'log_utc_offset_minutes': args.log_utc_offset_minutes,
            'sources': sources, 'limitations': LIMITS}
        write_output(args.output, markdown(report))
        if args.json_output:
            write_output(args.json_output, json.dumps(report, indent=2, allow_nan=False) + '\n')
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
