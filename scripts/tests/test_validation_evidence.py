import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

SPEC = importlib.util.spec_from_file_location(
    'validation_evidence', Path(__file__).resolve().parents[1] / 'validation-evidence.py'
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.log = self.root / 'run.log'
        self.log.write_text('test result: ok\n')
        self.patcher = patch.object(MODULE, 'ROOT', self.root)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.catalog = {'unit': {}}
        self.record = {
            'version': 1, 'run_id': 'r1', 'catalog_id': 'unit',
            'provenance': 'historical-import', 'result': 'passed', 'exit_code': None,
            'source_sha256': None, 'binary_sha256': None,
            'logs': [{'path': 'run.log', 'sha256': MODULE.digest(self.log)}],
        }

    def test_historical_unknowns_not_promoted_to_candidate_provenance(self):
        MODULE.check_record(self.record, self.catalog)
        self.assertIsNone(self.record['exit_code'])
        self.assertIsNone(self.record['source_sha256'])

    def test_changed_log_refused(self):
        self.log.write_text('different\n')
        with self.assertRaisesRegex(ValueError, 'changed log'):
            MODULE.check_record(self.record, self.catalog)

    def test_escape_and_symlink_refused(self):
        for path in ('../outside', str(self.log)):
            with self.assertRaises(ValueError):
                MODULE.relative_file(path)
        with tempfile.TemporaryDirectory() as other:
            outside = Path(other) / 'secret'
            outside.write_text('not read')
            (self.root / 'link').symlink_to(outside)
            with self.assertRaises(ValueError):
                MODULE.relative_file('link')

    def test_result_exit_disagreement_refused(self):
        self.record['exit_code'] = 1
        with self.assertRaisesRegex(ValueError, 'disagree'):
            MODULE.check_record(self.record, self.catalog)

    def test_new_record_needs_actual_command_and_utc_interval(self):
        self.record.update(
            provenance='recorded-after-execution', exit_code=0,
            command='python3 -m unittest', started_at='2026-10-07T12:00:00Z',
            finished_at='2026-10-07T12:01:00Z', assertions=['assertion'],
            limitations=['unit only'], cleanup='no effects', environment={}, fault=None,
        )
        MODULE.check_record(self.record, self.catalog)
        self.record['finished_at'] = '2026-10-07T11:59:00Z'
        with self.assertRaisesRegex(ValueError, 'reversed'):
            MODULE.check_record(self.record, self.catalog)
        self.record['finished_at'] = '2026-10-07T12:01:00'
        with self.assertRaisesRegex(ValueError, 'UTC'):
            MODULE.check_record(self.record, self.catalog)

    def test_binary_digest_syntax_not_current_binary_equality(self):
        self.record['binary_sha256'] = {'old-grv': 'a' * 64}
        MODULE.check_record(self.record, self.catalog)
        self.record['binary_sha256'] = {'old-grv': 'unverified'}
        with self.assertRaisesRegex(ValueError, 'digest'):
            MODULE.check_record(self.record, self.catalog)

    def test_record_creates_distinct_private_files_and_binds_log(self):
        details = self.root / 'details.json'
        details.write_text(json.dumps({
            'command': ['test'], 'assertions': ['unit assertion'],
            'limitations': ['unit only'], 'cleanup': 'no effects',
            'environment': {}, 'binary_sha256': {}, 'fault': None,
        }))
        catalog = self.root / 'catalog.json'
        catalog.write_text('{}')
        args = SimpleNamespace(
            catalog_id='unit', log=str(self.log), details=str(details),
            result='passed', exit_code=0, started_at='2026-10-07T12:00:00Z',
            finished_at='2026-10-07T12:01:00Z',
            out_dir=str(self.root / 'artifacts/runs'),
        )
        with patch.object(MODULE, 'CATALOG', catalog), \
                patch.object(MODULE, 'snapshot', return_value={}), \
                patch.object(MODULE.subprocess, 'check_output', return_value=b'commit'):
            MODULE.record_run(args, self.catalog)
            MODULE.record_run(args, self.catalog)
        paths = list(Path(args.out_dir).glob('*.json'))
        self.assertEqual(len(paths), 2)
        for path in paths:
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            record = json.loads(path.read_text())
            self.assertEqual(record['logs'][0]['sha256'], MODULE.digest(self.log))
            self.assertTrue(record['source_dirty'])
            MODULE.check_record(record, self.catalog)

    def test_catalog_duplicate_id_refused(self):
        catalog = self.root / 'catalog.json'
        catalog.write_text(json.dumps({'version': 1, 'tests': [{'id': 'unit'}, {'id': 'unit'}]}))
        with patch.object(MODULE, 'CATALOG', catalog):
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                MODULE.load_catalog()


if __name__ == '__main__':
    unittest.main()
