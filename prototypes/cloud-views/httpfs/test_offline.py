"""No-cloud tests for auth selection and exact HTTP manifest rejection."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('httpfs_offline', Path(__file__).with_name('offline.py'))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
LIVE_SPEC = importlib.util.spec_from_file_location('httpfs_live', Path(__file__).with_name('live_oauth.py'))
live = importlib.util.module_from_spec(LIVE_SPEC)
LIVE_SPEC.loader.exec_module(live)


class SelectionTests(unittest.TestCase):
    def test_oauth_live_requires_explicit_metadata(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                probe.sanitized_env(True, True)

    def test_oauth_live_does_not_inherit_other_credentials(self):
        with patch.dict(os.environ, {'GRV_HTTPFS_BEARER_TOKEN': 'synthetic', 'GRV_HTTPFS_ACCOUNT': 'selected', 'GRV_HTTPFS_PROJECT': 'chosen', 'AWS_SECRET_ACCESS_KEY': 'unselected', 'GOOGLE_APPLICATION_CREDENTIALS': 'unselected', 'HTTPS_PROXY': 'unselected'}, clear=True):
            env = probe.sanitized_env(True, True)
        self.assertEqual(env['GRV_HTTPFS_AUTH'], 'oauth')
        self.assertEqual(env['GRV_HTTPFS_BEARER_TOKEN'], 'synthetic')
        self.assertNotIn('AWS_SECRET_ACCESS_KEY', env)
        self.assertNotIn('GOOGLE_APPLICATION_CREDENTIALS', env)
        self.assertNotIn('HTTPS_PROXY', env)
        self.assertNotIn('GRV_HTTPFS_HMAC_SECRET', env)

    def test_encoded_exact_live_url(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / 'oracle.parquet'
            file.write_bytes(b'synthetic')
            manifest = Path(directory) / 'manifest.tsv'
            uri = 'https://storage.googleapis.com/owned/space%20%2B%25%23%C3%BC.parquet'
            manifest.write_text(f'{uri}\t{file}\t1\n')
            self.assertEqual(probe.validate_manifest(manifest, True, True)[0][0], uri)

    def test_live_rejects_untrusted_urls(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / 'oracle.parquet'
            file.write_bytes(b'synthetic')
            manifest = Path(directory) / 'manifest.tsv'
            for uri in ('http://storage.googleapis.com/owned/x', 'https://evil.example/owned/x', 'https://storage.googleapis.com:443/owned/x', 'https://user@storage.googleapis.com/owned/x', 'https://storage.googleapis.com/owned/x?token=x', 'https://storage.googleapis.com/owned/x#fragment', 'https://storage.googleapis.com/owned/*'):
                with self.subTest(uri=uri):
                    manifest.write_text(f'{uri}\t{file}\t1\n')
                    with self.assertRaises(ValueError):
                        probe.validate_manifest(manifest, True, True)


class OwnershipTests(unittest.TestCase):
    def journal(self, directory, bucket='grv-httpfs-oauth-' + 'a' * 24):
        import json
        path = Path(directory) / 'state.json'
        path.write_text(json.dumps({'account': 'selected', 'project': 'chosen', 'bucket': bucket, 'bucket_created': True, 'cleaned_up': True}))
        path.chmod(0o600)
        return path

    def test_completed_cleanup_is_offline_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            with patch.object(resource, 'request', side_effect=AssertionError('unexpected cloud action')):
                resource.cleanup()

    def test_ambiguous_creation_is_journaled_before_request(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            resource.state.pop('cleaned_up')
            resource.state.pop('bucket_created')
            def request(url, method='GET', data=None, headers=None, allowed=(200,)):
                if method == 'GET':
                    return 404, {}
                persisted = json.loads(resource.path.read_text())
                self.assertTrue(persisted['creation_attempted'])
                self.assertEqual(json.loads(data)['labels']['grv-probe-owner'], persisted['ownership_marker'])
                raise TimeoutError('simulated ambiguous success')
            with patch.object(resource, 'request', side_effect=request):
                with self.assertRaises(TimeoutError):
                    resource.create()
            self.assertTrue(json.loads(resource.path.read_text())['creation_attempted'])

    def test_cleanup_deletes_only_generation_qualified_owned_objects(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            resource.state.update(cleaned_up=False, ownership_marker='expected')
            responses = [(200, {'labels': {'grv-probe-owner': 'expected'}}),
                         (200, {'items': [{'name': 'space +.parquet', 'generation': '7'}]}),
                         (204, {}), (200, {}), (204, {}), (404, {})]
            with patch.object(resource, 'request', side_effect=responses) as request:
                resource.cleanup()
            calls = request.call_args_list
            self.assertIn('generation=7&ifGenerationMatch=7', calls[2].args[0])
            self.assertEqual(calls[2].args[1], 'DELETE')
            self.assertEqual(calls[4].args, (resource.bucket_url(), 'DELETE'))
            evidence = json.loads((Path(directory) / 'cleanup.json').read_text())
            self.assertTrue(evidence['all_versions_inventory_empty'])
            self.assertEqual(evidence['deleted_object_generations'], 1)

    def test_cleanup_refuses_mismatched_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            resource.state.update(cleaned_up=False, ownership_marker='expected')
            with patch.object(resource, 'request', return_value=(200, {'labels': {'grv-probe-owner': 'other'}})) as request:
                with self.assertRaises(RuntimeError):
                    resource.cleanup()
                self.assertEqual(request.call_count, 1)

    def test_cleanup_reconciles_absence_after_delete(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            resource.state.update(cleaned_up=False, all_versions_inventory_empty=True, deleted_object_generations=7)
            with patch.object(resource, 'request', return_value=(404, {})):
                resource.cleanup()
            evidence = json.loads((Path(directory) / 'cleanup.json').read_text())
            self.assertTrue(resource.state['cleaned_up'])
            self.assertTrue(evidence['all_versions_inventory_empty'])
            self.assertEqual(evidence['deleted_object_generations'], 7)

    def test_cleanup_evidence_failure_can_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            resource.state.update(cleaned_up=False)
            with patch.object(resource, 'request', return_value=(404, {})), patch.object(live, 'private_json', side_effect=OSError('simulated')):
                with self.assertRaises(OSError):
                    resource.cleanup()
            self.assertFalse(resource.state['cleaned_up'])
            with patch.object(resource, 'request', return_value=(404, {})):
                resource.cleanup()
            self.assertTrue(resource.state['cleaned_up'])

    def test_atomic_json_refuses_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.journal(directory)
            link = Path(directory) / 'link.json'
            link.symlink_to(path)
            with self.assertRaises(RuntimeError):
                live.private_json(link, {})
            live.private_json(path, {'atomic': True})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_auth_overrides_rejected(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            resource = live.Probe(self.journal(directory))
            with patch.dict(os.environ, {'CLOUDSDK_AUTH_ACCESS_TOKEN': 'synthetic'}, clear=True):
                with self.assertRaises(RuntimeError):
                    resource.check_auth()
            with patch.dict(os.environ, {}, clear=True), patch.object(resource, 'gcloud', return_value=subprocess.CompletedProcess([], 0, b'{"auth":{"impersonate_service_account":"other"}}')):
                with self.assertRaises(RuntimeError):
                    resource.check_auth()

    def test_unowned_bucket_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                live.Probe(self.journal(directory, 'unowned-production'))

    def test_public_or_symlink_journal_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.journal(directory)
            path.chmod(0o644)
            with self.assertRaises(RuntimeError):
                live.Probe(path)
            path.chmod(0o600)
            link = Path(directory) / 'link.json'
            link.symlink_to(path)
            with self.assertRaises(RuntimeError):
                live.Probe(link)


if __name__ == '__main__':
    unittest.main()
