#!/usr/bin/env python3
"""Deterministic generator for the GRV Salesforce release fixture.

Produces (paths relative to this file's directory):

  data/grvfixrel.csv                  10 related records
  data/grvfix.csv                     1,000 main records
                                      (GrvRel__c left empty; provision.py
                                       fills the GrvFixRel__c Ids for i < 10)
  manifest/value-manifest.json        the independent value oracle

The functions below are the oracle: the expected value of any row is
computed here, never derived from the loaded org or from GRV code. The
manifest snapshots the enumerated boundary rows plus the exact aggregates
over the full and filtered populations.

Usage:
  python3 generate.py            # write CSVs + manifest
  python3 generate.py --check    # verify committed CSVs/manifest match

Stdlib only. No randomness: output is a pure function of this file.
"""

import csv
import hashlib
import io
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
MANIFEST_DIR = os.path.join(HERE, "manifest")

ROWS = 1_000
REL_ROWS = 10

GRVFIX_FIELDS = [
    "Name",
    "GrvText__c",
    "GrvLongText__c",
    "GrvInt__c",
    "GrvDecimal__c",
    "GrvDate__c",
    "GrvDateTime__c",
    "GrvPicklist__c",
    "GrvBool__c",
    # GrvFormula__c is deliberately absent: it is a formula field (read-only
    # in the org). Bulk inserts that carry a value for it fail with
    # INVALID_FIELD_FOR_INSERT_UPDATE. The expected formula value stays in
    # grvfix_values() and the manifest, where the readback verification
    # compares it against the org's own computation.
    "GrvPartitionDate__c",
    "GrvStatus__c",
    "GrvRel__c",
]

GRVFIXREL_FIELDS = [
    "Name",
    "GrvRelText__c",
    "GrvRelDecimal__c",
    "GrvRelDate__c",
]

# The canonical base push filter (decl/push-base.yml). Exactly the rows with
# i % 10 == 7 are excluded.
BASE_FILTER = "GrvStatus__c = 'Active'"
# A valid absent-name filter; avoid out-of-range numeric literals, which
# this org rejects at query parsing rather than returning an empty result.
EMPTY_FILTER = "GrvStatus__c = 'Active' AND Name = 'GRVFIX-EMPTY-NONEXISTENT'"


def _iso_ms(dt: datetime) -> str:
    """ISO 8601 UTC with millisecond precision, e.g. 1970-01-01T00:00:00.000Z."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def grvfix_values(i: int) -> dict:
    """Exact Salesforce field values for main-table row i (0-based)."""
    v = {}
    v["Name"] = f"GRVFIX-{i:05d}"

    # Partition source: months 2026-07/08/09, days 01..28, cycling by i % 3.
    v["GrvPartitionDate__c"] = f"2026-{7 + (i % 3):02d}-{(i % 28) + 1:02d}"

    # Status: the one mutable dimension the live mutation cases use.
    v["GrvStatus__c"] = "Inactive" if i % 10 == 7 else "Active"

    # Text classes: null / empty / ascii / unicode / emoji / long / duplicates.
    t = i % 8
    if t == 0:
        v["GrvText__c"] = None
    elif t == 1:
        v["GrvText__c"] = ""
    elif t == 2:
        v["GrvText__c"] = f"ascii-value-{i:05d}"
    elif t == 3:
        v["GrvText__c"] = f"unicode-\u00e9-\u4e2d\u6587-{i:05d}"
    elif t == 4:
        v["GrvText__c"] = f"emoji-\U0001F680-{i:05d}"
    elif t == 5:
        v["GrvText__c"] = "long-" + "x" * 200
    elif t == 6:
        v["GrvText__c"] = "dup-a"
    else:
        v["GrvText__c"] = "dup-b"

    # Long text: deterministic ~150 B payload per row. Kept small on purpose:
    # the fixture must fit the (small) data-storage allocation of the
    # disposable orgs; the page-boundary coverage comes from the row count,
    # not the byte volume.
    target = 150 + (i % 7)
    prefix = f"GRVLONG-{i:05d}::"
    block = "0123456789abcdef"
    v["GrvLongText__c"] = prefix + block * ((target - len(prefix)) // len(block))

    # Integer: bounds and null on the first five rows, otherwise i % 1000.
    if i == 0:
        v["GrvInt__c"] = 0
    elif i == 1:
        v["GrvInt__c"] = 1
    elif i == 2:
        v["GrvInt__c"] = -1
    elif i == 3:
        v["GrvInt__c"] = 9999999999  # max for precision 10, scale 0
    elif i == 4:
        v["GrvInt__c"] = None
    else:
        v["GrvInt__c"] = i % 1000

    # Decimal(18,10): large exact value and null on rows 5-6.
    # Large exact value with 8 integer digits; .125 is exactly representable
    # even on Salesforce's floating-point numeric API. This is NOT the
    # decimal(18,10) maximum. Small rows still exercise scale 9/10.
    if i == 5:
        v["GrvDecimal__c"] = "99999999.1250000000"
    elif i == 6:
        v["GrvDecimal__c"] = None
    else:
        num = (i * 7) % 10**10
        v["GrvDecimal__c"] = f"{num // 10**9}.{num % 10**9:09d}"

    # Date: null, epoch, far future on rows 7-9, else pre-epoch + offset.
    # Far-future test value, NOT a claimed maximum. Salesforce rejected year
    # 9999 in live ingestion; no exact maximum has been established here.
    if i == 7:
        v["GrvDate__c"] = None
    elif i == 8:
        v["GrvDate__c"] = "1970-01-01"
    elif i == 9:
        v["GrvDate__c"] = "4000-07-31"
    else:
        v["GrvDate__c"] = (date(1969, 12, 31) + timedelta(days=i % 400)).isoformat()

    # DateTime input (UTC, ms): null, epoch, far future on rows 10-12,
    # else 1969-12-31T23:59:59Z + (i * 137) ms (sub-second, crosses epoch).
    if i == 10:
        v["GrvDateTime__c"] = None
    elif i == 11:
        v["GrvDateTime__c"] = "1970-01-01T00:00:00.000Z"
    elif i == 12:
        v["GrvDateTime__c"] = "4000-07-31T23:59:59.999Z"
    else:
        v["GrvDateTime__c"] = _iso_ms(
            datetime(1969, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
            + timedelta(milliseconds=i * 137)
        )

    # Picklist with null class.
    v["GrvPicklist__c"] = [None, "Alpha", "Beta", "Gamma"][i % 4]

    # Checkbox with null class.
    v["GrvBool__c"] = {0: "TRUE", 1: "FALSE", 2: None}[i % 3]

    # Formula (computed by the org; expected value recorded for the oracle).
    # Note: Salesforce formulas cannot reference picklist fields, so the
    # second component is the checkbox class instead.
    b = v["GrvBool__c"]
    v["GrvFormula__c"] = (
        "null" if v["GrvInt__c"] is None else str(v["GrvInt__c"])
    ) + ":" + ({"TRUE": "T", "FALSE": "F", None: "?"}[b])

    # Lookup filled by provision.py for i < REL_ROWS.
    v["GrvRel__c"] = None
    return v


def grvfixrel_values(i: int) -> dict:
    """Exact Salesforce field values for related-table row i (0-based)."""
    num = (i * 13) % 1_000_000
    return {
        "Name": f"GRVFIXREL-{i:03d}",
        "GrvRelText__c": f"rel-{i:03d}-\u00e9-\u4e2d",
        "GrvRelDecimal__c": f"{num // 100_000}.{num % 100_000:05d}",
        "GrvRelDate__c": (date(2026, 1, 1) + timedelta(days=i)).isoformat(),
    }


def grvfix_stored_values(i: int) -> dict:
    """Expected stored Salesforce values, separately from CSV inputs.

    Empty text is stored as null; omitted checkbox values use the metadata's
    false default; the Bulk ingestion used by this fixture stores DateTime
    values at whole-second precision. These are explicit source expectations,
    not tolerances in the verifier. Acquisition of non-null empty strings,
    nullable booleans and fractional timestamps needs another source fixture.
    """
    v = grvfix_values(i)
    if v["GrvText__c"] == "":
        v["GrvText__c"] = None
    v["GrvBool__c"] = v["GrvBool__c"] == "TRUE"
    if v["GrvDateTime__c"] is not None:
        dt = datetime.fromisoformat(v["GrvDateTime__c"].replace("Z", "+00:00"))
        v["GrvDateTime__c"] = _iso_ms(dt.replace(microsecond=0))
    v["GrvFormula__c"] = (
        "null" if v["GrvInt__c"] is None else str(v["GrvInt__c"])
    ) + (":T" if v["GrvBool__c"] else ":F")
    return v


def grvfix_grv_values(i: int) -> dict:
    """Expected GRV logical values for main row i under decl/push-base.yml."""
    v = grvfix_stored_values(i)
    g = {}
    g["id"] = "<org-assigned 18-char record Id>"
    g["name"] = v["Name"]
    g["text"] = v["GrvText__c"]
    g["long_text"] = v["GrvLongText__c"]
    g["int_value"] = v["GrvInt__c"]
    g["decimal_value"] = v["GrvDecimal__c"]
    g["date_value"] = v["GrvDate__c"]
    g["datetime_value"] = v["GrvDateTime__c"]
    g["picklist"] = v["GrvPicklist__c"]
    g["bool_value"] = v["GrvBool__c"]
    g["formula"] = v["GrvFormula__c"]
    g["partition_date"] = v["GrvPartitionDate__c"]
    g["status"] = v["GrvStatus__c"]
    g["rel_id"] = (
        "<org-assigned GrvFixRel__c Id>" if i < REL_ROWS else None
    )
    g["_month_"] = v["GrvPartitionDate__c"][:7]
    return g


def _csv_bytes(fields, rows) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    w.writerow(fields)
    for r in rows:
        w.writerow(["" if r[f] is None else r[f] for f in fields])
    return buf.getvalue().encode("utf-8")


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _counts(rows, key) -> dict:
    out = {}
    for r in rows:
        k = r[key]
        k = "null" if k is None else str(k)
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def _null_counts(rows, fields) -> dict:
    return {f: sum(1 for r in rows if r[f] is None) for f in fields}


def build_manifest() -> tuple[dict, bytes, bytes]:
    inputs = [grvfix_values(i) for i in range(ROWS)]
    main = [grvfix_stored_values(i) for i in range(ROWS)]
    rel = [grvfixrel_values(i) for i in range(REL_ROWS)]
    main_csv = _csv_bytes(GRVFIX_FIELDS, inputs)
    rel_csv = _csv_bytes(GRVFIXREL_FIELDS, rel)

    filtered = [r for r in main if r["GrvStatus__c"] == "Active"]

    def partition_counts(rows):
        return _counts(rows, "GrvPartitionDate__c")

    def month_counts(rows):
        out = {}
        for r in rows:
            m = r["GrvPartitionDate__c"][:7]
            out[m] = out.get(m, 0) + 1
        return dict(sorted(out.items()))

    long_lens = [len(r["GrvLongText__c"].encode("utf-8")) for r in main]

    enumerated = sorted(
        set(range(14))
        | {199, 200, 399, 400, 599, 600, 799, 800, 997, 998, 999}
    )

    gen_path = os.path.join(HERE, "generate.py")
    with open(gen_path, "rb") as fh:
        gen_sha = _sha256(fh.read())

    manifest = {
        "manifest_version": 3,
        "validation_scale": "Correctness fixture: 1000 main + 10 related; not a performance benchmark.",
        "source_semantics": {
            "text": "Empty CSV text becomes null; no non-null empty-string acquisition coverage.",
            "checkbox": "Omitted CSV checkbox uses false default; no nullable-boolean acquisition coverage.",
            "datetime": "This Bulk load path stores whole seconds; input milliseconds are explicit in enumerated input rows, not promised as stored values.",
            "decimal": "Exact numeric comparison; large 8-integer-digit .125 is representable; arbitrary decimal(18,10) maxima are not claimed to survive Salesforce numeric APIs.",
            "paging": "Scaled fixture requires test-requested REST batchSize=200 and Bulk maxRecords=100 to exercise real locators; default-page CLI acquisition is separately checked. Paging is evidence only after live execution.",
        },
        "fixture": "salesforce",
        "note": (
            "Independent value oracle for the disposable-org Salesforce "
            "fixture. grvfix_values(i)/grvfixrel_values(i) in generate.py are "
            "the single source of truth for every value; enumerated_rows and "
            "aggregates are snapshots of that function. The org is disposable; "
            "provisioning a replacement org is described in README.md."
        ),
        "generator": {
            "path": "generate.py",
            "sha256": gen_sha,
        },
        "org": {
            "requirements": (
                "disposable org, Developer Edition or above (custom objects "
                "required); authorized in the local sf store as alias "
                "grv-fixture; the concrete identity (org id, url, api "
                "version) is recorded in manifest/provision-evidence.json at "
                "provisioning time"
            ),
            "disposable": True,
        },
        "csv": {
            "grvfixrel": {"sha256": _sha256(rel_csv), "rows": REL_ROWS},
            "grvfix": {
                "sha256": _sha256(main_csv),
                "rows": ROWS,
                "note": (
                    "GrvRel__c is empty in the generated file; provision.py "
                    f"resolves it to the loaded GrvFixRel__c Ids for i < {REL_ROWS} "
                    "and records the resolved file hash in the run evidence."
                ),
            },
        },
        "objects": {
            "GrvFix__c": {
                "row_count": ROWS,
                "fields": {
                    "Name": "text(80)",
                    "GrvText__c": "text(255), nullable",
                    "GrvLongText__c": "longtextarea(32000), nullable",
                    "GrvInt__c": "number(10,0), nullable",
                    "GrvDecimal__c": "number(18,10), nullable",
                    "GrvDate__c": "date, nullable",
                    "GrvDateTime__c": "datetime(UTC), nullable; CSV milliseconds explicitly expected to truncate to seconds",
                    "GrvPicklist__c": "picklist[Alpha,Beta,Gamma], nullable",
                    "GrvBool__c": "checkbox, non-null; omitted CSV value defaults to false",
                    "GrvFormula__c": "formula text = TEXT(GrvInt__c or 'null') & ':' & IF(GrvBool__c = NULL, '?', IF(GrvBool__c, 'T', 'F'))",
                    "GrvPartitionDate__c": "date, nullable",
                    "GrvStatus__c": "picklist[Active,Inactive], required",
                    "GrvRel__c": "lookup GrvFixRel__c, nullable",
                },
                "special_rows": {
                    "GrvInt__c": {0: 0, 1: 1, 2: -1, 3: 9999999999, 4: None},
                    "GrvDecimal__c": {5: "99999999.1250000000", 6: None},
                    "GrvDate__c": {7: None, 8: "1970-01-01", 9: "4000-07-31"},
                    "GrvDateTime__c": {
                        10: None,
                        11: "1970-01-01T00:00:00.000Z",
                        12: "4000-07-31T23:59:59.000Z",
                    },
                    "GrvText__c_classes": {
                        "i%8==0": "null",
                        "i%8==1": "empty CSV text, stored as null",
                        "i%8==2": "ascii-value-{i:05d}",
                        "i%8==3": "unicode \\u00e9/\\u4e2d\\u6587",
                        "i%8==4": "emoji \\U0001F680",
                        "i%8==5": "long- + 200 x",
                        "i%8==6": "dup-a",
                        "i%8==7": "dup-b",
                    },
                    "GrvPicklist__c": "null/Alpha/Beta/Gamma by i%4",
                    "GrvBool__c": "TRUE/FALSE/omitted by i%3; stored true/false/false",
                    "GrvStatus__c": "Inactive iff i%10==7",
                    "GrvPartitionDate__c": "2026-{07+i%3}-{01+i%28}",
                    "GrvRel__c": f"set for i<{REL_ROWS}, else null",
                },
                "enumerated_rows": [
                    {"i": i, "input": inputs[i], "sf": main[i], "grv": grvfix_grv_values(i)}
                    for i in enumerated
                ],
                "aggregates": {
                    "status": _counts(main, "GrvStatus__c"),
                    "status_filtered": _counts(filtered, "GrvStatus__c"),
                    "partition_month_all": month_counts(main),
                    "partition_month_filtered": month_counts(filtered),
                    "picklist_all": _counts(main, "GrvPicklist__c"),
                    "picklist_filtered": _counts(filtered, "GrvPicklist__c"),
                    "bool_all": _counts(main, "GrvBool__c"),
                    "bool_filtered": _counts(filtered, "GrvBool__c"),
                    "nulls_all": _null_counts(
                        main,
                        [
                            "GrvText__c",
                            "GrvLongText__c",
                            "GrvInt__c",
                            "GrvDecimal__c",
                            "GrvDate__c",
                            "GrvDateTime__c",
                            "GrvPicklist__c",
                            "GrvBool__c",
                            "GrvRel__c",
                        ],
                    ),
                    "duplicates": {
                        "dup-a": _counts(main, "GrvText__c").get("dup-a", 0),
                        "dup-b": _counts(main, "GrvText__c").get("dup-b", 0),
                    },
                    "long_text_utf8_bytes": {
                        "min": min(long_lens),
                        "max": max(long_lens),
                        "total": sum(long_lens),
                    },
                },
            },
            "GrvFixRel__c": {
                "row_count": REL_ROWS,
                "fields": {
                    "Name": "text(80)",
                    "GrvRelText__c": "text(255), nullable",
                    "GrvRelDecimal__c": "number(18,6), nullable",
                    "GrvRelDate__c": "date, nullable",
                },
                "enumerated_rows": [
                    {"i": i, "sf": rel[i]} for i in range(REL_ROWS)
                ],
            },
        },
        "selections": {
            "base": {
                "declaration": "decl/push-base.yml",
                "filter": BASE_FILTER,
                "row_count": len(filtered),
                "partition_month": month_counts(filtered),
                "rest_test_batch_size": 200,
                "rest_page_sizes_example": [200, 200, 200, 200, 100],
                "rest_page_note": "Actual page sizes depend on selected fields/payload; traverse all nextRecordsUrl pages and verify count and unique Ids.",
                "bulk": "all rows across one or more result files; total must equal row_count with unique Ids",
            },
            "all": {
                "declaration": "decl/push-all.yml",
                "filter": None,
                "row_count": ROWS,
                "partition_month": month_counts(main),
                "rest_test_batch_size": 200,
                "rest_page_sizes_example": [200, 200, 200, 200, 200],
            },
            "empty": {
                "declaration": "decl/push-empty.yml",
                "filter": EMPTY_FILTER,
                "row_count": 0,
            },
            "rel": {
                "declaration": "decl/push-rel.yml",
                "filter": "GrvStatus__c = 'Active'",
                "row_count": len(filtered),
                "note": f"dot-notation relationship sources GrvRel__r.*; rows with i >= {REL_ROWS} have null relationship values",
            },
        },
    }
    return manifest, main_csv, rel_csv


def main() -> None:
    check_only = "--check" in sys.argv
    manifest, main_csv, rel_csv = build_manifest()

    if check_only:
        ok = True
        for name, content in (("grvfix.csv", main_csv), ("grvfixrel.csv", rel_csv)):
            path = os.path.join(DATA, name)
            if not os.path.exists(path) or open(path, "rb").read() != content:
                print(f"MISMATCH: data/{name}")
                ok = False
        mpath = os.path.join(MANIFEST_DIR, "value-manifest.json")
        if os.path.exists(mpath):
            old = json.load(open(mpath))
            if json.dumps(old, sort_keys=True) != json.dumps(
                manifest, sort_keys=True
            ):
                print("MISMATCH: manifest/value-manifest.json")
                ok = False
        else:
            print("MISSING: manifest/value-manifest.json")
            ok = False
        print("OK" if ok else "FAILED")
        sys.exit(0 if ok else 1)

    os.makedirs(DATA, exist_ok=True)
    os.makedirs(MANIFEST_DIR, exist_ok=True)
    with open(os.path.join(DATA, "grvfix.csv"), "wb") as fh:
        fh.write(main_csv)
    with open(os.path.join(DATA, "grvfixrel.csv"), "wb") as fh:
        fh.write(rel_csv)
    with open(os.path.join(MANIFEST_DIR, "value-manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(
        f"wrote data/grvfix.csv ({len(main_csv)} bytes), "
        f"data/grvfixrel.csv ({len(rel_csv)} bytes), "
        f"manifest/value-manifest.json"
    )


if __name__ == "__main__":
    main()
