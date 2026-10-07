import importlib.util
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('run_validation', Path(__file__).resolve().parents[1] / 'run-validation.py')
RUN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUN)


class RunnerTests(unittest.TestCase):
    def run_case(self, live=False, allow=False, result=0, snapshots=None):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            details = root / 'details.json'
            details.write_text(json.dumps({'assertions': ['test-only'], 'limitations': [],
                                          'cleanup': 'no effects', 'environment': {},
                                          'binary_sha256': {}, 'fault': None}))
            argv = ['run-validation.py', '--catalog-id', 'unit', '--out-dir',
                    str(root / 'artifacts/runs'), '--details', str(details)]
            if allow:
                argv += ['--allow-live']
            argv += ['--', 'test-command']
            with patch.object(sys, 'argv', argv), patch.object(RUN, 'ROOT', root), \
                    patch.object(RUN.EVIDENCE, 'load_catalog', return_value={'unit': {'live': live}}), \
                    patch.object(RUN.EVIDENCE, 'snapshot', side_effect=snapshots or [{}, {}]), \
                    patch.object(RUN.EVIDENCE, 'digest', return_value='a' * 64), \
                    patch.object(RUN.EVIDENCE, 'check_record'), \
                    patch.object(RUN.subprocess, 'check_output', return_value=b'commit'), \
                    patch.object(RUN, 'execute', return_value=(result, False)) as child:
                with self.assertRaises(SystemExit) as exit:
                    RUN.main()
                records = list((root / 'artifacts/runs').glob('*.json'))
                return exit.exception.code, child.call_count, [json.loads(p.read_text()) for p in records]

    def test_timeout_stops_exact_group_and_waits_before_log_sealing(self):
        with patch.object(RUN.subprocess, 'Popen') as spawn, patch.object(RUN.os, 'killpg') as kill:
            process = spawn.return_value
            process.pid = 12345
            process.wait.side_effect = [subprocess.TimeoutExpired(['test'], 1), -9]
            self.assertEqual(RUN.execute(['test'], None, 1), (124, True))
            self.assertTrue(spawn.call_args.kwargs['start_new_session'])
            self.assertEqual([call.args[0] for call in kill.call_args_list], [12345, 12345])
            self.assertEqual(process.wait.call_count, 2)

    def test_live_requires_explicit_allow_before_execution(self):
        code, calls, records = self.run_case(live=True)
        self.assertIn('requires', code)
        self.assertEqual(calls, 0)
        self.assertEqual(records, [])

    def test_failure_retained_and_no_shell(self):
        code, calls, records = self.run_case(result=7)
        self.assertEqual(code, 7)
        self.assertEqual(calls, 1)
        self.assertEqual(records[0]['result'], 'failed')
        self.assertEqual(records[0]['command'], ['test-command'])

    def test_source_changes_explicitly_invalidate_frozen_evidence(self):
        code, _, records = self.run_case(snapshots=[{}, {'changed': 'a' * 64}])
        self.assertEqual(code, 0)
        self.assertTrue(records[0]['source_changed_during_execution'])
        self.assertTrue(records[0]['limitations'])


if __name__ == '__main__':
    unittest.main()
