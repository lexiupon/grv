import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('audit_release', Path(__file__).resolve().parents[1] / 'audit-release.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class AuditTests(unittest.TestCase):
    def fixture(self, root, content=b'clean', name='bin/grv'):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        (root / 'bundle.json').write_text(json.dumps({'sha256': {name: hashlib.sha256(content).hexdigest()}}))

    def test_clean_and_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            self.assertEqual(MOD.audit(root, [])['file_hashes_verified'], 1)
            (root / 'bin/grv').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'hash'):
                MOD.audit(root, [])

    def test_canary_across_chunk_boundary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root, b'x' * 65534 + b'synthetic-canary')
            with self.assertRaisesRegex(ValueError, 'suppressed'):
                MOD.audit(root, [b'synthetic-canary'])

    def test_pem_parser_marker_is_not_a_key_but_payload_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root, b'-----BEGIN PRIVATE KEY-----\x00parser-marker')
            MOD.audit(root, [])
            self.fixture(root, b'-----BEGIN PRIVATE KEY-----\n' + b'A' * 64)
            with self.assertRaisesRegex(ValueError, 'credential'):
                MOD.audit(root, [])

    def test_unlisted_and_fixture_refuse(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root, name='fixture')
            with self.assertRaisesRegex(ValueError, 'fixture'):
                MOD.audit(root, [])
            self.fixture(root)
            with self.assertRaisesRegex(ValueError, 'unlisted'):
                MOD.audit(root, [])


if __name__ == '__main__':
    unittest.main()
