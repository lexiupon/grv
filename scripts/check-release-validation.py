#!/usr/bin/env python3
"""Check release-plan traceability without building, authenticating or running tests.

The default checks a review draft. --require-mapped additionally refuses
unreviewed applicability, missing exact selectors/assertions and unmapped cases.
Neither mode produces test-pass evidence or grants live execution authority.
The register records the reviewed scope; incomplete mapping does not prohibit
an explicitly authorized incremental first pass.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent
REGISTER = ROOT / "spec/fixtures/release-validation/scenarios.json"
PLAN = "spec/grv-v1-release-validation.md"
SOURCES = {
    "spec/grv-client-v1-execution.md": "## Required conformance scenarios",
    "spec/grv-adapter-protocol-v1.md": "## 9. Conformance",
}
FROZEN_FILES = set(SOURCES).union({
    PLAN,
    "spec/grv-client-v1.md",
    "spec/grv-storage-v2.md",
    "spec/grv-client-v1-declaration.schema.json",
    "spec/grv-client-v1-command-output.schema.json",
    "spec/grv-client-v1-build-completion.schema.json",
    "spec/adapters/duckdb.schema.json",
    "spec/adapters/salesforce.schema.json",
    "spec/fixtures/identity-v1.json",
})


def local_file(relative):
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("register paths must be repository-relative")
    result = ROOT / path
    if not result.is_file():
        raise ValueError(f"register file is missing: {relative}")
    return result


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table_rows(text):
    section = ""
    for number, line in enumerate(text.splitlines(), 1):
        if line.startswith("### "):
            section = line[4:]
        if not line.startswith("|"):
            continue
        columns = [part.strip() for part in re.split(r"(?<!\\)\|", line)[1:-1]]
        if not columns or all(re.fullmatch(r"[-: ]+", part) for part in columns):
            continue
        yield section, number, columns


def required_rows(relative, marker):
    text = local_file(relative).read_text()
    begin = text.index(marker)
    selected = text[begin:]
    selected = selected.split("\n## ", 1)[0]
    offset = text[:begin].count("\n")
    for section, line, columns in table_rows(selected):
        if columns[0].lower() == "scenario":
            continue
        if len(columns) != 2:
            raise ValueError(f"unexpected required-scenario table in {relative}")
        yield (relative, section, line + offset, *columns)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-mapped", action="store_true")
    args = parser.parse_args()
    register = json.loads(REGISTER.read_text())
    if register["register_version"] != 1 or register["status"] != "draft":
        raise ValueError("unsupported register version/status")
    for relative, expected in register["source_sha256"].items():
        if digest(local_file(relative)) != expected:
            raise ValueError(f"source changed; review and refresh the register: {relative}")
    if not FROZEN_FILES.issubset(register["source_sha256"]):
        raise ValueError("missing required source hashes")
    expected = [row for relative, marker in SOURCES.items()
                for row in required_rows(relative, marker)]
    actual = [(row["source"], row["section"], row["line"], row["scenario"], row["required_result"])
              for row in register["normative_scenarios"]]
    if expected != actual:
        raise ValueError("required scenarios differ: missing, reordered or changed rows")
    plan_rows = [(columns[0], columns[1], columns[2])
                 for section, line, columns in table_rows(local_file(PLAN).read_text())
                 if columns[0].startswith("REL-") and len(columns) == 3]
    cases = register["release_cases"]
    if plan_rows != [(case["id"], case["procedure"], case["required_result"]) for case in cases]:
        raise ValueError("release cases differ from the plan")
    ids = [row["id"] for row in register["normative_scenarios"]] + [case["id"] for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate scenario/case ID")
    case_ids = {case["id"] for case in cases}
    unmapped = 0
    for row in register["normative_scenarios"]:
        for relative in row["candidate_test_files"]:
            local_file(relative)
        mapping = row["mapping"]
        if not set(mapping["release_cases"]).issubset(case_ids):
            raise ValueError(f"unknown release-case reference: {row['id']}")
        applicability = row["applicability"]
        state = applicability["status"]
        if state not in ("review_required", "applicable", "not_applicable"):
            raise ValueError(f"unknown applicability: {row['id']}")
        reviewed_na = state == "not_applicable" and applicability["reviewed"] and applicability["rationale"]
        complete = reviewed_na or (state == "applicable" and applicability["reviewed"]
                                  and mapping["reviewed"] and mapping["test_selectors"]
                                  and mapping["assertions"] and mapping["release_cases"])
        if not complete:
            unmapped += 1
    print(json.dumps({"normative_scenarios": len(expected), "release_cases": len(cases),
                      "unreviewed_or_unmapped": unmapped, "live_execution_authorized": register.get("live_execution_authorized", False),
                      "test_results": "not assessed"}, indent=2))
    if args.require_mapped and unmapped:
        raise ValueError("review mappings are incomplete; full coverage gate remains open (incremental first-pass execution may be separately authorized)")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, json.JSONDecodeError) as error:
        raise SystemExit(str(error))
