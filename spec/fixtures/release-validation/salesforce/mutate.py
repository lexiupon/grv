#!/usr/bin/env python3
"""Fixed, opt-in REST mutations for the pinned disposable release fixture only.

No arbitrary object, name, field, value or query inputs. Reset is deliberately
not implemented here: the live gate uses provision.py's clean reload and strict
full readback, including related rows.
"""
import argparse
import os
import re
import urllib.error
import urllib.request
import json

import generate
import provision

from private_scope import salesforce_scope
QUERY = ("SELECT Id, Name, GrvStatus__c, GrvPartitionDate__c FROM GrvFix__c "
         "WHERE Name IN ('GRVFIX-00000', 'GRVFIX-00001') ORDER BY Name")
CHANGES = {
    "GRVFIX-00000": {"GrvStatus__c": "Inactive"},
    "GRVFIX-00001": {"GrvPartitionDate__c": "2026-09-01"},
}


def fixed_rows(sf):
    rows = sf.query(QUERY)
    by_name = {row["Name"]: row for row in rows}
    if len(rows) != 2 or set(by_name) != set(CHANGES):
        raise SystemExit("Fixed fixture names missing or duplicated; refusing mutation")
    for row in rows:
        if not re.fullmatch(r"[A-Za-z0-9]{18}", row["Id"]):
            raise SystemExit("Invalid fixture record Id; refusing mutation")
    return by_name


def mutate(org, expected_org_id):
    AUTHORIZED_ORG, AUTHORIZED_ID = salesforce_scope()
    if os.environ.get("GRV_SALESFORCE_TEST_MUTATION") != "allow":
        raise SystemExit("Requires GRV_SALESFORCE_TEST_MUTATION=allow")
    if org != AUTHORIZED_ORG or expected_org_id != AUTHORIZED_ID:
        raise SystemExit("Only the pinned disposable fixture org is authorized")
    # Identity is checked BEFORE constructing Salesforce (which retrieves a
    # token), even if someone changes their local CLI alias/auth metadata.
    display = provision.sf_json(["org", "display", "-o", org])
    if display["id"] != AUTHORIZED_ID or display["username"] != AUTHORIZED_ORG:
        raise SystemExit("ORG ID/USERNAME MISMATCH; no token retrieval or mutation")
    sf = provision.Salesforce(org)
    rows = fixed_rows(sf)
    for i, name in enumerate(CHANGES):
        pristine = generate.grvfix_stored_values(i)
        for field in ("GrvStatus__c", "GrvPartitionDate__c"):
            if rows[name][field] != pristine[field]:
                raise SystemExit("Mutation requires pristine fixed rows; reset first")
    for name, changes in CHANGES.items():
        # The only write endpoint is the fixed fixture object, using Ids
        # resolved from the two fixed Names. PATCH preserves all other fields.
        url = f"{sf.base}/services/data/v{sf.api}/sobjects/GrvFix__c/{rows[name]['Id']}"
        request = urllib.request.Request(
            url, data=json.dumps(changes).encode(), method="PATCH",
            headers={"Authorization": f"Bearer {sf.token}",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                if response.status != 204:
                    raise SystemExit("Fixture PATCH did not return HTTP 204")
        except urllib.error.HTTPError as error:
            # Never log headers, token, response body or the auth result.
            raise SystemExit(f"Fixture PATCH failed: HTTP {error.code}") from None
        except urllib.error.URLError:
            raise SystemExit("Fixture PATCH transport failed") from None
    actual = fixed_rows(sf)
    for name, changes in CHANGES.items():
        if actual[name]["Id"] != rows[name]["Id"]:
            raise SystemExit("Fixture Id changed during mutation")
        for field in ("GrvStatus__c", "GrvPartitionDate__c"):
            expected = changes.get(field, rows[name][field])
            if actual[name][field] != expected:
                raise SystemExit("Strict fixed mutation readback failed")
    print("fixed two-row REST fixture mutation verified")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--org", required=True)
    parser.add_argument("--expected-org-id", required=True)
    args = parser.parse_args()
    mutate(args.org, args.expected_org_id)


if __name__ == "__main__":
    main()
