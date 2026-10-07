import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('capabilities', Path(__file__).resolve().parents[1] / 'validate-s3-capabilities.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class CapabilityTests(unittest.TestCase):
    def test_only_exact_private_uuid_children_collected(self):
        parent = 's3://synthetic-bucket/validation'
        child = '12345678-1234-4123-8123-123456789abc'
        log = '\n'.join([parent, parent + '/sibling', parent + '/' + child + '/space%20and%25',
                         parent + '/' + child, 's3://other-bucket/' + child])
        self.assertEqual(MOD.owned_roots(log, parent), [parent + '/' + child])

    def test_each_live_gate_has_one_exact_selector(self):
        for command in MOD.GATES.values():
            self.assertIn('--exact', command)
            self.assertEqual(command.count('--ignored'), 1)
            self.assertNotIn('--workspace', command)


if __name__ == '__main__':
    unittest.main()
