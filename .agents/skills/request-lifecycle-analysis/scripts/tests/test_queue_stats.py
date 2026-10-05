"""Behavioral tests with synthetic log text; no cluster or original checkout."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('queue_stats', Path(__file__).resolve().parents[1] / 'queue_stats.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class QueueStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def log(self, text):
        path = self.root / 'worker.out'
        path.write_text(text)
        return path

    def test_rank_isolation_missing_fields_invalid_values_and_partial_tail(self):
        path = self.log('[2026-10-02 08:00:00 DP0 TP0] Decode batch, #running-req: 2, #queue-req: 0\n'
            '[2026-10-02 08:00:01 DP1 TP0] Decode batch, #running-req: 8, #prealloc-req: 4\n'
            '[2026-10-02 08:00:02 DP0 TP0] Decode batch, #running-req: 4, #queue-req: nan\n'
            '[2026-10-02 08:00:03 DP0 TP0] Decode batch, #running-req: 999')
        data = module.reduce_log(path, 'decode', 'd0')
        self.assertGreater(data['omitted_tail_bytes'], 0)
        groups = {g['rank']: g for g in data['groups']}
        d0 = groups['DP0 TP0']['fields']
        self.assertEqual(d0['running (reported batch)']['sample_mean'], 3)
        self.assertEqual(d0['running (reported batch)']['samples'], 2)
        self.assertEqual(d0['waiting / ready waiting']['invalid'], 1)
        self.assertIsNone(d0['preallocation']['sample_mean'])
        self.assertEqual(d0['preallocation']['missing'], 2)
        self.assertEqual(groups['DP1 TP0']['fields']['running (reported batch)']['sample_mean'], 8)

    def test_phase_filter_and_unknown_rank(self):
        path = self.log('[2026-10-02 08:00:00] Prefill batch, #bootstrap-req: 100\n'
            '[2026-10-02 08:01:00] Prefill batch, #bootstrap-req: 2, #new-seq: 3\n')
        data = module.reduce_log(path, 'prefill', 'p0',
            start=module.aware_time('2026-10-02T08:00:30Z'),
            end=module.aware_time('2026-10-02T08:01:30Z'), offset=0)
        self.assertEqual(data['groups'][0]['rank'], 'unlabeled')
        self.assertEqual(data['groups'][0]['fields']['bootstrap']['sample_mean'], 2)
        with self.assertRaises(ValueError):
            module.aware_time('2026-10-02T08:00:00')

    def test_handoff_metadata_and_out_of_order_latest(self):
        path = self.log('[2026-10-02 08:00:02 TP0] Decode batch, #running-req: 4, #prealloc-req: 1\n'
            '[2026-10-02 08:00:01 TP0] Decode batch, #running-req: 2\n')
        data = module.reduce_log(path, 'decode', 'd0')
        self.assertEqual(data['groups'][0]['fields']['running (reported batch)']['latest'], 4)
        with self.assertRaises(ValueError):
            module.reduce_log(path, 'aggregate', 'a0')

    def test_prefill_running_and_handoff_are_distinct_observations(self):
        path = self.log('[2026-10-02 08:00:00 TP0] Prefill batch, #running-req: 0, #new-seq: 2, #inflight-req: 3\n'
            '[2026-10-02 08:00:01 TP0] Prefill batch, #running-req: 0, #new-seq: 4\n')
        fields = module.reduce_log(path, 'prefill', 'p0')['groups'][0]['fields']
        self.assertIsNone(fields['prefill running (active request occupancy)']['sample_mean'])
        self.assertIsNone(fields['prefill running (active request occupancy)']['field'])
        self.assertEqual(fields['prefill batch entries']['sample_mean'], 3)
        self.assertEqual(fields['running (reported batch)']['sample_mean'], 0)
        self.assertEqual(fields['handoff / in-flight']['sample_mean'], 3)
        self.assertEqual(fields['handoff / in-flight']['missing'], 1)
        aggregate = module.reduce_log(path, 'aggregate', 'a0')['groups'][0]['fields']
        # An observed field remains literal evidence, but no remote handoff is synthesized.
        self.assertIsNone(aggregate['prefill running (active request occupancy)']['sample_mean'])
        no_handoff = self.log('[2026-10-02 08:00:00] Prefill batch, #new-seq: 2\n')
        aggregate = module.reduce_log(no_handoff, 'aggregate', 'a0')['groups'][0]['fields']
        self.assertNotIn('handoff / in-flight', aggregate)
        prefill = module.reduce_log(no_handoff, 'prefill', 'p0')['groups'][0]['fields']
        self.assertIsNone(prefill['handoff / in-flight']['sample_mean'])

    def test_flow_does_not_pool_worker_series_or_invent_prefill_occupancy(self):
        path = self.log('[2026-10-02 08:00:00 TP0] Prefill batch, #new-seq: 2, #inflight-req: 3\n')
        source = module.reduce_log(path, 'prefill', 'p0')
        report = {'mode': 'disaggregated', 'sources': [source]}
        graph = module.lifecycle_mermaid(report)
        self.assertIn('Handoff / in-flight<br/>Wait for asynchronous sender completion<br/>3.0', graph)
        self.assertIn('Active occupancy not measured', graph)
        self.assertIn('H --> PC', graph)
        self.assertIn('T --> Q', graph)
        report['sources'].append(dict(source, worker='p1'))
        graph = module.lifecycle_mermaid(report)
        self.assertNotIn('Handoff / in-flight<br/>Wait for asynchronous sender completion<br/>3.0', graph)
        self.assertIn('See separate worker/rank tables', graph)
        aggregate = module.lifecycle_mermaid({'mode': 'aggregate', 'sources': []})
        self.assertNotIn('Transfer:', aggregate)
        self.assertNotIn('Handoff', aggregate)

    def test_cli_outputs_and_preserves_input_on_bad_invocation(self):
        path = self.log('[2026-10-02 08:00:00 TP0] Decode batch, #running-req: 2\n')
        report, data = self.root/'report.md', self.root/'report.json'
        args = ['--job','synthetic','--mode','aggregate','--log','aggregate','a0',str(path)]
        module.main(args+['--output',str(report),'--json-output',str(data)])
        self.assertEqual(json.loads(data.read_text())['sources'][0]['groups'][0]['fields']['running (reported batch)']['latest'], 2)
        self.assertIn('Unavailable', report.read_text())
        original = path.read_bytes()
        with self.assertRaises(SystemExit):
            module.main(args+['--output',str(path)])
        self.assertEqual(path.read_bytes(), original)
        with self.assertRaises(ValueError):
            module.reduce_log(self.log('AIPerf: benchmark complete\n'), 'aggregate', 'a0')


if __name__ == '__main__':
    unittest.main()
