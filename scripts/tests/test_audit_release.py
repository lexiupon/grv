import hashlib
import importlib.util
import json
from pathlib import Path
import re
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

    def test_each_registered_canary_is_detected_on_its_own(self):
        register = json.loads((Path(__file__).resolve().parents[2] / 'spec/fixtures/release-validation/canaries.json').read_text())
        self.assertEqual(len(register), len(MOD.PATTERNS))
        for index, canary in enumerate(register):
            with self.subTest(canary=canary):
                self.assertEqual([i for i, p in enumerate(MOD.PATTERNS) if re.search(p, canary.encode())], [index])
                with tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    self.fixture(root, b'leading bytes\n' + canary.encode() + b'\ntrailing bytes\n')
                    with self.assertRaisesRegex(ValueError, 'suppressed'):
                        MOD.audit(root, [])
                    with self.assertRaisesRegex(ValueError, 'suppressed'):
                        MOD.audit(root, [canary.encode()])

    def test_clean_fixture_passes_the_full_register_and_reports_the_count(self):
        register = json.loads((Path(__file__).resolve().parents[2] / 'spec/fixtures/release-validation/canaries.json').read_text())
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            report = MOD.audit(root, [c.encode() for c in register])
            self.assertEqual(report['canaries_checked'], len(register))
            self.assertEqual(report['credential_patterns_found'], 0)

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
