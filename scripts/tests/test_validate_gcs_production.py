import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('validate_gcs', Path(__file__).resolve().parents[1] / 'validate-gcs-production.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)
MOD.AUTHORIZED = 'gs://synthetic-bucket/validation'
MOD.SF_USER = 'fixture@example.invalid'
MOD.SF_ID = '00D000000000001AAA'


class CloudRunner:
    def __init__(self, outputs):
        self.outputs = iter(outputs)
        self.calls = []

    def run(self, argv, phase):
        self.calls.append(argv)
        return next(self.outputs)


class GCSTests(unittest.TestCase):
    def setUp(self):
        config = {'gcs': {'root': MOD.AUTHORIZED, 'account': 'fixture@example.com', 'project': 'project'},
                  'sf': {'org': MOD.SF_USER, 'org_id': MOD.SF_ID}}
        patcher = patch.object(MOD, 'private_config', return_value=config)
        patcher.start()
        self.addCleanup(patcher.stop)

    def args(self):
        return SimpleNamespace(write_root=MOD.AUTHORIZED + '/12345678-1234-4123-8123-123456789abc',
                               account='fixture@example.com', project='project',
                               sf_org=MOD.SF_USER, sf_org_id=MOD.SF_ID)

    def test_no_parent_sibling_or_arbitrary_org(self):
        MOD.validate_args(self.args())
        for change in ({'write_root': MOD.AUTHORIZED}, {'write_root': MOD.AUTHORIZED + '/xyz/'},
                       {'sf_org': 'production'}, {'account': '--override'}):
            args = self.args()
            for key, value in change.items():
                setattr(args, key, value)
            with self.assertRaises(MOD.ValidationError):
                MOD.validate_args(args)

    def test_missing_inventory_is_only_exact_notfound(self):
        for error, accepted in [(b'ERROR: (gcloud.storage.ls) One or more URLs matched no objects.\n', True),
                                (b'permission denied', False)]:
            cloud = MOD.Cloud(self.args(), CloudRunner([(1, b'', error)]))
            if accepted:
                self.assertEqual(cloud.inventory(), [])
            else:
                with self.assertRaises(MOD.ValidationError):
                    cloud.inventory()

    def test_cleanup_never_deletes_escape(self):
        runner = CloudRunner([(0, b'gs://bucket/sibling/key#1\n', b'')])
        with self.assertRaises(MOD.ValidationError):
            MOD.Cloud(self.args(), runner).cleanup()
        self.assertEqual(len(runner.calls), 1)

    def test_exact_generation_cleanup_and_absence(self):
        url = self.args().write_root + '/key#123'
        runner = CloudRunner([(0, (url + '\n').encode(), b''), (0, b'', b''),
                             (1, b'', b'ERROR: (gcloud.storage.ls) One or more URLs matched no objects.')])
        self.assertTrue(MOD.Cloud(self.args(), runner).cleanup()['absence_proved'])
        self.assertIn(url, runner.calls[1])
        self.assertNotIn('--recursive', runner.calls[1])

    def test_numeric_oracle_never_adopts_float(self):
        with self.assertRaises(MOD.ValidationError):
            MOD.canonical('decimal_value', 1.25)
        self.assertEqual(MOD.canonical('decimal_value', '99999999.1250000000'),
                         MOD.Decimal('99999999.125'))


if __name__ == '__main__':
    unittest.main()
