"""Offline safety/contract tests; no sf, HTTP or live org calls."""
import os
import unittest
from unittest.mock import patch

import generate
import mutate


class FakeSalesforce:
    base = "https://example.invalid"
    api = "66.0"
    token = "SECRET-NEVER-PRINT"

    def __init__(self, org):
        self.rows = []
        for i in range(2):
            row = generate.grvfix_stored_values(i)
            self.rows.append({"Name": row["Name"], "Id": "a" * 17 + str(i),
                              "GrvStatus__c": row["GrvStatus__c"],
                              "GrvPartitionDate__c": row["GrvPartitionDate__c"]})

    def query(self, query):
        assert query == mutate.QUERY
        return [dict(row) for row in self.rows]


class Response:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MutationTests(unittest.TestCase):
    def run_mutation(self):
        mutate.mutate(mutate.AUTHORIZED_ORG, mutate.AUTHORIZED_ID)

    @patch.dict(os.environ, {"GRV_SALESFORCE_TEST_MUTATION": "allow"})
    def test_identity_and_authorization_fail_before_token_or_effects(self):
        with patch.object(mutate.provision, "Salesforce") as sf, \
             patch.object(mutate.provision, "sf_json", return_value={
                 "id": "wrong", "username": mutate.AUTHORIZED_ORG}) as display:
            with self.assertRaises(SystemExit):
                self.run_mutation()
            display.assert_called_once()
            sf.assert_not_called()
            with self.assertRaises(SystemExit):
                mutate.mutate("production", mutate.AUTHORIZED_ID)
            display.assert_called_once()

    @patch.dict(os.environ, {"GRV_SALESFORCE_TEST_MUTATION": "deny"})
    def test_missing_opt_in_has_no_auth_calls(self):
        with patch.object(mutate.provision, "sf_json") as display:
            with self.assertRaises(SystemExit):
                self.run_mutation()
            display.assert_not_called()

    @patch.dict(os.environ, {"GRV_SALESFORCE_TEST_MUTATION": "allow"})
    def test_exact_two_rest_patches_and_readback(self):
        sf = FakeSalesforce(mutate.AUTHORIZED_ORG)
        calls = []

        def request(req, timeout):
            import json
            self.assertEqual(timeout, 60)
            self.assertEqual(req.method, "PATCH")
            self.assertIn("/sobjects/GrvFix__c/", req.full_url)
            record = next(r for r in sf.rows if req.full_url.endswith(r["Id"]))
            changes = json.loads(req.data)
            self.assertEqual(changes, mutate.CHANGES[record["Name"]])
            calls.append(record["Name"])
            record.update(changes)
            return Response()

        with patch.object(mutate.provision, "sf_json", return_value={
                "id": mutate.AUTHORIZED_ID, "username": mutate.AUTHORIZED_ORG}), \
             patch.object(mutate.provision, "Salesforce", return_value=sf), \
             patch.object(mutate.urllib.request, "urlopen", side_effect=request):
            self.run_mutation()
        self.assertEqual(calls, list(mutate.CHANGES))

    @patch.dict(os.environ, {"GRV_SALESFORCE_TEST_MUTATION": "allow"})
    def test_http_failure_is_not_tolerated(self):
        import urllib.error
        sf = FakeSalesforce(mutate.AUTHORIZED_ORG)
        with patch.object(mutate.provision, "sf_json", return_value={
                "id": mutate.AUTHORIZED_ID, "username": mutate.AUTHORIZED_ORG}), \
             patch.object(mutate.provision, "Salesforce", return_value=sf), \
             patch.object(mutate.urllib.request, "urlopen", side_effect=
                          urllib.error.HTTPError("hidden", 403, "hidden", {}, None)) as request:
            with self.assertRaisesRegex(SystemExit, "HTTP 403"):
                self.run_mutation()
            self.assertEqual(request.call_count, 1)

    @patch.dict(os.environ, {"GRV_SALESFORCE_TEST_MUTATION": "allow"})
    def test_non_pristine_refused_before_write(self):
        sf = FakeSalesforce(mutate.AUTHORIZED_ORG)
        sf.rows[1]["GrvPartitionDate__c"] = "2026-09-01"
        with patch.object(mutate.provision, "sf_json", return_value={
                "id": mutate.AUTHORIZED_ID, "username": mutate.AUTHORIZED_ORG}), \
             patch.object(mutate.provision, "Salesforce", return_value=sf), \
             patch.object(mutate.urllib.request, "urlopen") as request:
            with self.assertRaises(SystemExit):
                self.run_mutation()
            request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
