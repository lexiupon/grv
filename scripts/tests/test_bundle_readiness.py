"""Offline distribution gate checks; no builds/native loading/service access."""
import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
from unittest.mock import patch
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PACKAGE = load('bundle_package', 'package.py')
VERIFY = load('bundle_verify', 'verify-bundle.py')


class ReadinessTests(unittest.TestCase):
    def ready(self):
        # Synthetic unit input, NOT actual native inventory evidence.
        return {'dependency_notice_gaps': [],
                'native_dependency_notice_inventory_complete': False,
                'native_notice_policy_review': {'policy': PACKAGE.NOTICE_POLICY,
                                               'native_engine': True, 'signed_extensions': True},
                'native_extensions': {'dependency_notice_inventory_complete': False},
                'profile': 'release', 'source_dirty': False,
                'source_commit': 'test', 'toolchain': 'test'}

    def test_missing_or_false_native_inventory_refuses(self):
        for value in (None, False):
            manifest = self.ready()
            manifest['native_notice_policy_review']['native_engine'] = value
            with self.assertRaisesRegex(SystemExit, 'DuckDB'):
                PACKAGE.require_distribution_ready(manifest)

    def test_missing_or_false_extension_inventory_refuses(self):
        for value in (None, False):
            manifest = self.ready()
            manifest['native_notice_policy_review']['signed_extensions'] = value
            with self.assertRaisesRegex(SystemExit, 'extension'):
                PACKAGE.require_distribution_ready(manifest)

    def test_rust_gaps_refuse(self):
        manifest = self.ready()
        manifest['dependency_notice_gaps'] = ['unknown@1']
        with self.assertRaisesRegex(SystemExit, 'Rust'):
            PACKAGE.require_distribution_ready(manifest)

    def test_dev_dirty_and_unknown_provenance_refuse(self):
        for key, value in (('profile', 'dev'), ('source_dirty', True),
                           ('source_dirty', None), ('source_commit', None),
                           ('toolchain', None)):
            manifest = self.ready()
            manifest[key] = value
            with self.assertRaises(SystemExit):
                VERIFY.require_distribution_ready(manifest)

    def test_actual_macos_minimum_inventory_required(self):
        manifest = dict(self.ready(), platform='Darwin')
        with self.assertRaisesRegex(SystemExit, 'Mach-O'):
            VERIFY.require_distribution_ready(manifest)
        manifest.update(macos_minimum_versions={'bin/grv': '11.0', 'native': '26.0'},
                        macos_minimum_version='26.0')
        VERIFY.require_distribution_ready(manifest)
        manifest['macos_minimum_version'] = '11.0'
        with self.assertRaisesRegex(SystemExit, 'Mach-O'):
            VERIFY.require_distribution_ready(manifest)

    def test_review_payload_hashes_paths_and_unresolved_refuse(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            notices = root / 'notices'
            notices.mkdir()
            (notices / 'LICENSE').write_bytes(b'synthetic-license')
            review = {'version': 1, 'policy': PACKAGE.NOTICE_POLICY,
                      'scope': 'native-engine', 'reviewed': True, 'unresolved': [],
                      'notice_files': [{'path': 'LICENSE', 'sha256': hashlib.sha256(b'synthetic-license').hexdigest()}]}
            path = notices / 'review.json'
            with patch.object(PACKAGE, 'ROOT', root):
                path.write_text(json.dumps(review))
                PACKAGE.notice_review(path, 'native-engine')
                for key, value in [('unresolved', ['missing notice']), ('policy', 'other')]:
                    changed = dict(review, **{key: value})
                    path.write_text(json.dumps(changed))
                    with self.assertRaises(SystemExit):
                        PACKAGE.notice_review(path, 'native-engine')
                review['notice_files'][0]['path'] = '../escape'
                path.write_text(json.dumps(review))
                with self.assertRaisesRegex(SystemExit, 'unsafe'):
                    PACKAGE.notice_review(path, 'native-engine')
                review['notice_files'][0]['path'] = 'LICENSE'
                path.write_text(json.dumps(review))
                (notices / 'LICENSE').write_bytes(b'changed')
                with self.assertRaisesRegex(SystemExit, 'checksum'):
                    PACKAGE.notice_review(path, 'native-engine')

    def test_complete_synthetic_input_passes_only_structural_gate(self):
        VERIFY.require_distribution_ready(self.ready())


if __name__ == '__main__':
    unittest.main()
