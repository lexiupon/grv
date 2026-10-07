import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
import uuid

SPEC = importlib.util.spec_from_file_location('s3_production', Path(__file__).resolve().parents[1] / 'validate-s3-production.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class S3Tests(unittest.TestCase):
    def args(self):
        return SimpleNamespace(write_root=MOD.AUTHORIZED + '/' + str(uuid.uuid4()),
                               profile=MOD.PROFILE, region=MOD.REGION,
                               sf_org=MOD.PROOF.SF_USER, sf_org_id=MOD.PROOF.SF_ID)

    def test_scope_uuid_profile_region_org(self):
        args = self.args()
        MOD.validate_args(args)
        for key, value in [('write_root', MOD.AUTHORIZED), ('write_root', MOD.AUTHORIZED + '/not-uuid'),
                           ('profile', 'default'), ('region', 'us-east-1'), ('sf_org_id', 'production')]:
            bad = self.args()
            setattr(bad, key, value)
            with self.assertRaises(MOD.PROOF.ValidationError):
                MOD.validate_args(bad)

    def test_inventory_no_parent_effect_and_truncated_refusal(self):
        args = self.args()
        runner = Mock()
        cloud = MOD.Cloud(args, runner)
        runner.json.return_value = {'IsTruncated': False, 'Contents': [{'Key': cloud.prefix + 'grv.json'}]}
        self.assertEqual(len(cloud.inventory()), 1)
        command = runner.json.call_args.args[0]
        self.assertEqual(command[command.index('--prefix') + 1], cloud.prefix)
        runner.json.return_value = {'IsTruncated': True}
        with self.assertRaises(MOD.PROOF.ValidationError):
            cloud.inventory()
        runner.json.return_value = {'Contents': [{'Key': 'sibling/grv.json'}]}
        with self.assertRaises(MOD.PROOF.ValidationError):
            cloud.inventory()

    def test_cleanup_exact_owned_keys_and_absence(self):
        runner = Mock()
        cloud = MOD.Cloud(self.args(), runner)
        runner.json.side_effect = [{'Contents': [{'Key': cloud.prefix + 'grv.json'}]}, {}, {}, {}]
        runner.run.return_value = (0, b'', b'')
        result = cloud.cleanup()
        self.assertTrue(result['absence_proved'])
        self.assertEqual(result['deleted_objects'], 1)
        command = runner.run.call_args.args[0]
        self.assertEqual(command[command.index('--key') + 1], cloud.prefix + 'grv.json')


if __name__ == '__main__':
    unittest.main()
