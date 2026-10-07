import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('config', Path(__file__).resolve().parents[1] / 'release-validation-config.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'private.json'
        self.value = {'sf': {'org': 'fixture@example.invalid', 'org_id': '00D000000000001AAA'},
                      'gcs': {'root': 'gs://synthetic-bucket/validation', 'account': 'fixture@example.invalid', 'project': 'synthetic-project'},
                      's3': {'root': 's3://synthetic-bucket/validation', 'profile': 'synthetic-profile', 'region': 'eu-west-1'},
                      'archive': {'root': self.directory.name}}
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.value))
        self.path.chmod(0o600)

    def test_explicit_private_coordinates(self):
        self.assertEqual(MOD.load_config(self.path), self.value)
        self.path.chmod(0o644)
        with self.assertRaises(MOD.ConfigError):
            MOD.load_config(self.path)

    def test_symlink_and_extra_credentials_refused(self):
        link = self.path.with_name('alias')
        link.symlink_to(self.path)
        with self.assertRaises(MOD.ConfigError):
            MOD.load_config(link)
        self.value['sf']['token'] = 'synthetic-not-a-real-token'
        self.save()
        with self.assertRaises(MOD.ConfigError):
            MOD.load_config(self.path)

    def test_parent_scope_and_duplicate_keys_refused(self):
        self.value['s3']['root'] = 's3://synthetic-bucket'
        self.save()
        with self.assertRaises(MOD.ConfigError):
            MOD.load_config(self.path)
        self.path.write_text('{"sf":{},"sf":{}}')
        with self.assertRaises(MOD.ConfigError):
            MOD.load_config(self.path)


if __name__ == '__main__':
    unittest.main()
