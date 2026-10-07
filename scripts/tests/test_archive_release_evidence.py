import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'archive-release-evidence.py'


class ArchiveTests(unittest.TestCase):
    def test_private_archive_hash_inventory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            inputs = root / 'inputs'
            inputs.mkdir()
            (inputs / 'log.txt').write_text('synthetic')
            output = root / 'proof.tar.gz'
            subprocess.run([sys.executable, str(SCRIPT), '--root', str(root), '--include', str(inputs), '--output', str(output)], check=True, capture_output=True)
            receipt = json.loads(Path(str(output) + '.json').read_text())
            self.assertFalse(receipt['durable_backup'])
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertEqual(set(receipt['files']), {'inputs/log.txt'})
            with tarfile.open(output) as archive:
                self.assertEqual(archive.getnames(), ['inputs/log.txt'])

    def test_symlink_refused_before_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            inputs = root / 'inputs'
            inputs.mkdir()
            (root / 'private').write_text('synthetic')
            (inputs / 'link').symlink_to(root / 'private')
            output = root / 'proof.tar.gz'
            process = subprocess.run([sys.executable, str(SCRIPT), '--root', str(root), '--include', str(inputs), '--output', str(output)], capture_output=True)
            self.assertNotEqual(process.returncode, 0)
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
