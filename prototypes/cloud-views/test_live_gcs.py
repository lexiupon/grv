"""Offline scope/cleanup tests: never authenticate or contact Google Cloud."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gcs_range import Refused
from live_gcs import Experiment


class ScopeTests(unittest.TestCase):
    def test_unowned_state_refused_before_auth(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            state = {"created": False, "project": "entit-finance-prod", "project_number": "828148855924",
                     "bucket": "grv-view-probe-" + "a" * 24, "directory": temp, "account": "test@example.com"}
            path.write_text(json.dumps(state)); path.chmod(0o600)
            with patch.object(Experiment, "command", side_effect=AssertionError("auth unexpectedly invoked")):
                for field, value in [("created", False), ("project", "other-project"),
                                     ("bucket", "production-bucket"), ("project_number", "1")]:
                    bad = dict(state, created=True)
                    bad[field] = value
                    path.write_text(json.dumps(bad))
                    with self.subTest(field=field), self.assertRaises(Refused):
                        Experiment(path)

    def test_cleanup_refuses_unknown_objects_before_delete(self):
        exp = object.__new__(Experiment)
        exp.state = {"bucket": "grv-view-probe-" + "a" * 24}
        exp.registry = [{"key": "fixture/owned.parquet"}]
        seen = []

        def request(method, path, query=None, *args):
            seen.append(method)
            return {"items": [{"name": "unknown/object", "bucket": exp.state["bucket"], "generation": "101"}]}

        exp.request = request
        with self.assertRaises(Refused):
            exp.cleanup()
        self.assertEqual(seen, ["GET"])

    def test_outside_upload_refused_before_read_or_effect(self):
        exp = object.__new__(Experiment)
        with self.assertRaises(Refused):
            exp.upload("other-prefix/escape", Path("nonexistent"))


if __name__ == "__main__":
    unittest.main()
