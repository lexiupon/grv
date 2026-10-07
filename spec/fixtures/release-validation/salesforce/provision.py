#!/usr/bin/env python3
"""Deploy, grant access, load and strictly verify disposable Salesforce fixtures.

Requires sf and an explicitly pinned org Id. --verify-only performs readback
without deploying, assigning permissions or loading/deleting records. Re-runs
of normal provisioning delete ONLY records in the two fixture objects.
Metadata canonicalization is a separate reviewed step documented in README.md.
"""
import argparse
import csv
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import generate

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
MANIFEST = os.path.join(HERE, "manifest", "value-manifest.json")
PERMSET = "GrvFixAccess"
OBJECTS = ("GrvFix__c", "GrvFixRel__c")


def sh(*args):
    print("$ " + " ".join(args), file=sys.stderr)
    return subprocess.run(args, cwd=HERE, capture_output=True, text=True, timeout=720)


def sf_json(args):
    r = sh("sf", *args, "--json")
    try:
        d = json.loads(r.stdout[r.stdout.index("{"):], parse_float=Decimal)
    except (ValueError, json.JSONDecodeError):
        raise SystemExit(f"sf command failed (exit {r.returncode}); JSON unavailable")
    if r.returncode or d.get("status", 0):
        # Do not dump org display/auth responses: they can contain secrets.
        raise SystemExit(f"sf {' '.join(args)} failed: {d.get('message', 'see CLI diagnostics')}")
    return d["result"]


def deploy_schema(org, check_only=False):
    args = ["project", "deploy", "start", "-o", org, "--source-dir",
            os.path.join(HERE, "force-app", "main", "default"), "--wait", "10"]
    if check_only:
        args.append("--dry-run")
    result = sf_json(args)
    if not result.get("success"):
        raise SystemExit("Schema/permission-set deploy failed; no data loaded")
    print("schema and permission set deployed", file=sys.stderr)


def assign_access(org):
    """CLI assigns PermissionSetAssignment (not PermissionSetAssign).

    Test the assignment first so repeat provisioning is idempotent. A real
    assignment failure is NOT evidence of a broken org.
    """
    display = sf_json(["org", "display", "-o", org])
    username = display["username"].replace("\\", "\\\\").replace("'", "\\'")
    query = ("SELECT Id FROM PermissionSetAssignment "
             f"WHERE PermissionSet.Name = '{PERMSET}' AND Assignee.Username = '{username}'")
    if sf_json(["data", "query", "-o", org, "--query", query])["records"]:
        print("fixture permission set already assigned", file=sys.stderr)
        return
    result = sf_json(["org", "assign", "permset", "--name", PERMSET, "-o", org])
    if result.get("failures") or not result.get("successes"):
        raise SystemExit("ACTION REQUIRED: assign GrvFixAccess to the authenticated user; "
                         "Setup -> Permission Sets -> GRV Fix Access -> Manage Assignments")
    print("fixture permission set assigned", file=sys.stderr)


class Salesforce:
    def __init__(self, org):
        d = sf_json(["org", "display", "-o", org])
        self.base = d["instanceUrl"].rstrip("/")
        self.api = d.get("apiVersion", "67.0")
        self.token = sf_json(["org", "auth", "show-access-token", "-o", org])["accessToken"]
        self.pages = []

    def get(self, path):
        url = urllib.parse.urljoin(self.base + "/", path)
        if urllib.parse.urlsplit(url).netloc != urllib.parse.urlsplit(self.base).netloc:
            raise SystemExit("Refusing cross-host Salesforce page URL")
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}",
                                                   "Sforce-Query-Options": "batchSize=200"})
        with urllib.request.urlopen(req, timeout=60) as fh:
            return json.load(fh, parse_float=Decimal)

    def resource(self, path):
        return self.get(f"/services/data/v{self.api}/{path}")

    def query(self, soql):
        rows = []
        path = f"/services/data/v{self.api}/query/?q={urllib.parse.quote(soql)}"
        self.pages = []
        while path:
            page = self.get(path)
            records = page.get("records", [])
            self.pages.append(len(records))
            rows.extend(records)
            path = page.get("nextRecordsUrl")
        return rows

    def verify_access(self):
        expected = {"GrvFix__c": generate.GRVFIX_FIELDS[1:] + ["GrvFormula__c"],
                    "GrvFixRel__c": generate.GRVFIXREL_FIELDS[1:]}
        for obj, fields in expected.items():
            desc = self.resource(f"sobjects/{obj}/describe")
            have = {f["name"]: f for f in desc["fields"]}
            missing = [f for f in fields if f not in have]
            if missing:
                raise SystemExit(f"ACCESS CHECK FAILED: {obj}: {missing}. Check object access, "
                                 "field-level security and GrvFixAccess assignment. "
                                 "Missing fields do not prove an activation defect.")
            if not all(desc.get(k) for k in ("queryable", "createable", "deletable")):
                raise SystemExit(f"Fixture object CRUD access missing: {obj}")
            unwritable = [f for f in fields if f != "GrvFormula__c" and not have[f]["createable"]]
            if unwritable:
                raise SystemExit(f"Fixture insert field access missing: {obj}: {unwritable}")
        print("fixture object and field access verified", file=sys.stderr)

    def check_capacity(self):
        limit = self.resource("limits")["DataStorageMB"]
        # Salesforce custom records generally charge 2 KiB each, regardless
        # of shortening the long-text CSV. Include modest headroom.
        required_mb = (generate.ROWS + generate.REL_ROWS) * 2 / 1024 + 1
        reclaimed_mb = (sum(len(self.query(f"SELECT Id FROM {obj}")) for obj in OBJECTS) * 2 / 1024
                        if limit["Max"] >= required_mb else 0)
        if limit["Max"] < required_mb or limit["Remaining"] + reclaimed_mb < required_mb:
            raise SystemExit(f"CAPACITY CHECK FAILED: DataStorageMB={limit}; need about "
                             f"{required_mb:.1f} MB free before loading. Use a disposable org "
                             "with enough capacity; existing data has not been deleted.")


def run_bulk(org, operation, obj, path):
    command = ["data", "import", "bulk"] if operation == "insert" else ["data", "delete", "bulk"]
    result = sf_json(command + ["-o", org, "-s", obj, "-f", path, "--wait", "10"])
    if result.get("state") == "Failed" or result.get("numberRecordsFailed", 0):
        raise SystemExit(f"Bulk {operation} failed for {obj}")
    print(f"bulk {operation} complete: {obj}", file=sys.stderr)


def clean_records(org, sf):
    for obj in OBJECTS:  # referencing side before referenced side
        ids = [r["Id"] for r in sf.query(f"SELECT Id FROM {obj}")]
        if not ids:
            continue
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ids.csv")
            with open(path, "w", newline="") as f:
                f.write("Id\n" + "\n".join(ids) + "\n")
            run_bulk(org, "delete", obj, path)
        if sf.query(f"SELECT Id FROM {obj} LIMIT 1"):
            raise SystemExit(f"Cleanup incomplete for {obj}; refusing to append duplicates")


def resolve_lookups(sf):
    rows = sf.query("SELECT Id, Name FROM GrvFixRel__c")
    by_name = {r["Name"]: r["Id"] for r in rows}
    if len(rows) != generate.REL_ROWS or len(by_name) != generate.REL_ROWS:
        raise SystemExit("Related fixture count/uniqueness mismatch")
    path = os.path.join(DATA, "grvfix.csv")
    with open(path, newline="", encoding="utf-8") as f:
        data = list(csv.DictReader(f))
    for i, row in enumerate(data):
        if i < generate.REL_ROWS:
            row["GrvRel__c"] = by_name[f"GRVFIXREL-{i:03d}"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, generate.GRVFIX_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def comparable(field, value):
    if value is None:
        return None
    if field in ("GrvDecimal__c", "GrvRelDecimal__c", "GrvInt__c"):
        return Decimal(str(value))
    if field == "GrvDateTime__c":
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def row_errors(expected, actual):
    """Strict comparison. No rounding, empty-to-null or boolean coercions here."""
    return [f"{field}: expected {value!r}, got {actual.get(field)!r}"
            for field, value in expected.items()
            if field not in actual or comparable(field, value) != comparable(field, actual[field])]


def readback_verify(sf):
    fields = generate.GRVFIX_FIELDS + ["GrvFormula__c"]
    rows = sf.query("SELECT Id, " + ", ".join(fields) + " FROM GrvFix__c ORDER BY Name")
    all_pages = list(sf.pages)
    rels = sf.query("SELECT Id, " + ", ".join(generate.GRVFIXREL_FIELDS) + " FROM GrvFixRel__c ORDER BY Name")
    rel_ids = {r["Name"]: r["Id"] for r in rels}
    errors = []
    if len(rows) != generate.ROWS or len({r["Name"] for r in rows}) != generate.ROWS:
        errors.append("Main fixture count/unique names mismatch")
    if len(rels) != generate.REL_ROWS or len(rel_ids) != generate.REL_ROWS:
        errors.append("Related fixture count/unique names mismatch")
    if len({r['Id'] for r in rows}) != len(rows):
        errors.append("Duplicate main record Id")
    for actual in rows:
        match = re.fullmatch(r"GRVFIX-(\d{5})", actual["Name"])
        if not match or int(match[1]) >= generate.ROWS:
            errors.append(f"Unexpected record {actual['Name']}")
            continue
        i = int(match[1])
        expected = generate.grvfix_stored_values(i)
        expected["GrvRel__c"] = rel_ids.get(f"GRVFIXREL-{i:03d}") if i < generate.REL_ROWS else None
        errors.extend(f"{actual['Name']} {e}" for e in row_errors(expected, actual))
    for actual in rels:
        match = re.fullmatch(r"GRVFIXREL-(\d{3})", actual["Name"])
        if not match or int(match[1]) >= generate.REL_ROWS:
            errors.append(f"Unexpected related record {actual['Name']}")
        else:
            errors.extend(f"{actual['Name']} {e}" for e in row_errors(generate.grvfixrel_values(int(match[1])), actual))
    active = sf.query("SELECT Id FROM GrvFix__c WHERE " + generate.BASE_FILTER)
    base_pages = list(sf.pages)
    manifest = json.load(open(MANIFEST))
    if len(active) != manifest["selections"]["base"]["row_count"]:
        errors.append("Base filter count mismatch")
    if sf.query("SELECT Id FROM GrvFix__c WHERE " + generate.EMPTY_FILTER):
        errors.append("Empty selection returned records")
    for e in errors[:20]:
        print("MISMATCH: " + e, file=sys.stderr)
    print(f"readback: {len(rows)} + {len(rels)} rows, {len(errors)} mismatches", file=sys.stderr)
    return {"readback_ok": not errors, "mismatch_count": len(errors),
            "counts": dict(zip(OBJECTS, (len(rows), len(rels)))),
            "rest_all_page_sizes": all_pages, "rest_base_page_sizes": base_pages,
            "base_count": len(active)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--org", required=True)
    ap.add_argument("--expected-org-id", required=True, help="pin the authorized disposable org's 18-char Id")
    ap.add_argument("--skip-load", action="store_true", help="deploy, assign access and check schema only")
    ap.add_argument("--check-only", action="store_true", help="dry-run metadata deploy only")
    ap.add_argument("--verify-only", action="store_true", help="verify existing records, no mutation")
    ap.add_argument("--ignore-storage-limit", action="store_true",
                    help="explicitly bypass the conservative capacity preflight in an authorized disposable org; actual load failures still fail")
    args = ap.parse_args()
    d = sf_json(["org", "display", "-o", args.org])
    if d["id"] != args.expected_org_id:
        raise SystemExit("ORG ID MISMATCH; no deployment, permission changes or data mutation performed")
    if not args.verify_only:
        deploy_schema(args.org, args.check_only)
        if args.check_only:
            return
        assign_access(args.org)
    sf = Salesforce(args.org)
    sf.verify_access()
    if args.skip_load:
        return
    resolved_sha = None
    if not args.verify_only:
        if args.ignore_storage_limit:
            print("Storage preflight bypass explicitly selected for the pinned disposable org; "
                  "server load/reset failures remain failures", file=sys.stderr)
        else:
            sf.check_capacity()  # before deleting records, fail closed on insufficient storage
        sf_json_result = sh(sys.executable, os.path.join(HERE, "generate.py"))
        if sf_json_result.returncode:
            raise SystemExit("Fixture generation failed")
        clean_records(args.org, sf)
        run_bulk(args.org, "insert", "GrvFixRel__c", os.path.join(DATA, "grvfixrel.csv"))
        resolved_sha = resolve_lookups(sf)
        run_bulk(args.org, "insert", "GrvFix__c", os.path.join(DATA, "grvfix.csv"))
    evidence = readback_verify(sf)
    evidence.update(org_id=d["id"], org_url=d["instanceUrl"], api_version=sf.api,
                    permission_set=PERMSET, resolved_grvfix_csv_sha256=resolved_sha,
                    mode="verify-only" if args.verify_only else "provision",
                    utc=datetime.now(timezone.utc).isoformat(),
                    coverage_note="Scaled correctness fixture; REST batchSize=200; Bulk paging needs separate live gate; "
                                  "no non-null empty text, null checkbox or sub-second stored timestamps",
                    data_storage=sf.resource("limits")["DataStorageMB"])
    with open(os.path.join(HERE, "manifest", "provision-evidence.json"), "w") as f:
        json.dump(evidence, f, indent=2)
        f.write("\n")
    if not evidence["readback_ok"]:
        raise SystemExit("Verification failed; evidence records failure")
    print("fixture verification complete", file=sys.stderr)


if __name__ == "__main__":
    main()
