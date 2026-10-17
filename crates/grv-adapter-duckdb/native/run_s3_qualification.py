#!/usr/bin/env python3
"""Approved, bounded, owned S3 shipping-scanner slice (not release qualification).

Uses only a selected static named-file AWS profile, never a credential chain.
No native build, IAM, ACL, bucket-policy, versioning or existing-data changes.
Unknown child output is discarded. Journals are atomic, private and secret-free.
Run --self-test for offline safety tests. Interrupted/failed runs retain a private
journal; cleanup fails closed on missing ownership or unexpected inventory.
"""
import argparse
import configparser
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
import uuid
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[3]
NATIVE = pathlib.Path(__file__).resolve().parent
EVIDENCE = ROOT / "spec/release-validation/s3-shipping-scanner-checkpoint.json"
ACCOUNT = "590184134474"
ARN = "arn:aws:iam::590184134474:user/lexiupon-aws-cli-s3"
EVIDENCE_DIR = pathlib.Path.home() / "grv-evidence"
MARKER = "grv-qualification-owner"
TRIALS = ("wide", "narrow", "selective_wide", "selective_narrow", "and_range",
          "or_range", "null", "string", "wrapper", "nonidentity_cast")
SCHEMA = {"id": "BIGINT", **{f"payload{column}": "VARCHAR" for column in range(8)}}
# The fixed projection, in the order the fixtures are written and the closed trials read it.
PROJECTION_ORDER = ["id"] + ["payload%d" % column for column in range(8)]
# The positive shape stores one real object whose key carries one space, one literal `+`, one
# non-ASCII letter, one apostrophe and one literal `-`. The stored key is that decoded form; the
# coordinate handed to the proof is the same key in GRV canonical encoding, and the two must
# round-trip. `NONCANONICAL_COORDINATE` is the same key with an over-encoded `-`, which the native
# parser refuses before any remote read.
SPECIAL_KEY = "data/shipping q+a-bé'c.parquet"
NONCANONICAL_COORDINATE = "data/shipping q+a%2Dbé'c.parquet"
GRV_ENCODE_ESCAPE = set('"#%<>?`{}[]*\\')


def grv_encode(data):
    """Python mirror of the native GRV encoder, byte-exact on decoded bytes."""
    out = []
    for byte in data:
        char = chr(byte)
        if byte == 0x2F or (32 < byte < 127 and char not in GRV_ENCODE_ESCAPE):
            out.append(char)
        else:
            out.append("%%%02X" % byte)
    return "".join(out)


def s3_key_canonical(uri):
    """Decode one exact S3 coordinate and require the native canonical round-trip."""
    require(uri.startswith("s3://") and len(uri) <= 8192, "coordinate-not-canonical")
    require(not any(char in uri for char in "?#\\\r\n\t"), "coordinate-not-canonical")
    encoded = uri[5:].split("/", 1)[1]
    decoded = []
    index = 0
    while index < len(encoded):
        char = encoded[index]
        if char == "%":
            require(index + 2 < len(encoded), "coordinate-escape")
            digits = encoded[index + 1:index + 3]
            require(all(digit in "0123456789ABCDEF" for digit in digits), "coordinate-escape")
            byte = int(digits, 16)
            require(byte != 0x2F, "coordinate-encoded-separator")
            index += 3
        else:
            byte = ord(char)
            index += 1
        require(byte >= 32 and byte != 127 and byte != 0x5C, "coordinate-key")
        decoded.append(byte)
    key = bytes(decoded)
    require(grv_encode(key) == encoded, "coordinate-not-canonical")
    require(all(segment not in (b"", b".", b"..") for segment in key.split(b"/")),
            "coordinate-segment")
    return key


# Approved single-file shapes: (name, compression, dictionary, statistics, key, rows).
# A null key means data/<name>.parquet and null rows means the --rows selection.
SHAPES = (
    ("uncompressed-statistics", "NONE", False, True, None, None),
    ("zstd-dictionary-no-statistics", "ZSTD", True, False, None, None),
    ("special-coordinate", "ZSTD", True, False, SPECIAL_KEY, None),
    ("empty", "NONE", False, True, None, 0),
    # The one typed shape reachable without a design change: identical settings to the first
    # shape, same column names and declared types, but every payload value is NULL. Nine of
    # the ten trials then carry typed NULLs alongside a real sum(id) -- mixed validity inside
    # one bounded record -- on fully native local-oracle ground truth. See
    # spec/s3-shipping-evolved-typed-live-plan.md.
    ("all-null-typed", "NONE", False, True, None, None),
)
NULL_PAYLOAD_SHAPES = frozenset({"all-null-typed"})


def shape_plan(shape, rows, bucket):
    """Resolve one approved single-file shape to settings, coordinate and expectation."""
    name, compression, dictionary, statistics, key, shape_rows = shape
    resolved = rows if shape_rows is None else shape_rows
    require(resolved in (0, 8192, 65536), "shape-row-expectation")
    key = "data/" + name + ".parquet" if key is None else key
    coordinate = "s3://" + bucket + "/" + grv_encode(key.encode())
    require(s3_key_canonical(coordinate) == key.encode(), "coordinate-round-trip")
    return {"name": name, "compression": compression, "dictionary": dictionary,
            "statistics": statistics, "key": key, "rows": resolved,
            "local_fixture": name + ".parquet", "columns": dict(SCHEMA),
            "coordinate": coordinate}


# The one multi-file shape this round adds: three single-row-group files (written at
# row_group_size=2048), one shared schema and one aggregated expectation. The harness local
# oracle seam grv_native_verified_file_query admits only ONE descriptor per call, so the
# multi-file ground truth is computed independently in Python from the local fixtures and
# handed to the harness inside a closed private manifest; the guarded cloud path is unchanged.
MULTI_SHAPE_NAME = "multi-file-aggregate"
MULTI_SHAPE_ROWS = (2048, 2048, 2048)
MULTI_SHAPE_TRIAL = "wide"

# The one bounded refusal shape: two files, the second missing a projected column. It
# asserts a REFUSAL at the first guarded EXECUTION rather than a value. A CREATE VIEW binds
# lazily, so on the pinned build the refusal surfaces at the trial query: an observed round
# recorded 212 with HTTP status 206, meaning a bounded read had already succeeded, and that
# observation is why 212 is the expected code here. This shape does not qualify cross-file
# evolution in general: it converts one observed fail-closed behaviour into a qualification.
# See spec/s3-shipping-evolved-typed-live-plan.md.
REFUSAL_SHAPE_NAME = "evolved-missing-column-refusal"
REFUSAL_SHAPE_ROWS = (2048, 2048)
REFUSAL_DROPPED_COLUMN = "payload7"
REFUSAL_EXPECTED_CODE = 212
# The one cold/warm observation shape: ONE file entry, TWO trials against the SAME object, in the
# fixed order that defines the observation. Trial 1 `wide` is the COLD read (first guarded
# execution against that object); trial 2 `narrow` is the WARM read (second guarded execution on
# the same retained connection-session). Order IS the observation, so it is fixed here in code and
# is never caller-supplied. Two different trial names are required because the manifest grammar
# refuses duplicate trial names, which is fine: the observation needs two sequential guarded reads
# of the same object, not the same SQL twice, and both names are already in the closed trial set.
# Warm only exists inside one retained connection-session: no durable cache exists on this path, so
# a second run is cold again. See spec/s3-shipping-cold-warm-live-plan.md.
COLD_WARM_SHAPE_NAME = "cold-warm-same-object"
COLD_WARM_ROWS = 2048
COLD_WARM_TRIALS = ("wide", "narrow")
# Verbatim mirror of the closed ManifestTrialSql in cloud_qualification.cpp for the two trials this
# shape uses. That source is the authority; these strings are quoted here so an offline test pins
# the mirror and any drift in either direction is caught.
COLD_WARM_SQL = {
    "wide": "SELECT sum(id)::BIGINT"
            + "".join(",sum(length(payload%d))::BIGINT" % column for column in range(8))
            + " FROM data",
    "narrow": "SELECT sum(id)::BIGINT FROM data",
}

# --- cross-file type contradiction -------------------------------------------------
# The one cross-file type-contradiction shape: TWO files that carry the SAME nine column
# names, where ONE column (payload4) has TWO different physical types -- int64 in the first
# file, string in the second. The plan is the whole claim: the specified behaviour is a
# REFUSAL, and no value is compared. The exact decision code is deliberately NOT asserted
# here. spec/s3-shipping-evolved-typed-live-plan.md states the numeric codes are per
# call-site, not per mechanism, so offline work cannot say WHICH mechanism refuses on this
# shape: the expected code is per spec, to be confirmed at the round gate. What this slice
# does assert is that a refusal happens, that its observed numbers are recorded whatever they
# are, and that an unexpected success or an unparsable diagnostic stops the run before delete.
CONTRADICTION_SHAPE_NAME = "cross-file-contradiction-refusal"
CONTRADICTION_SHAPE_ROWS = (2048, 2048)
CONTRADICTION_TRIAL = "wide"
CONTRADICTION_COLUMN = "payload4"
CONTRADICTION_EXPECTED_CODE = None  # per spec; to be confirmed at round gate
CONTRADICTION_EXPECTED_CODE_STATUS = "per spec; to be confirmed at round gate"
# Physical types as a local parquet read-back reports them, stated per file so the
# contradiction is planned data and never something inferred from a run. Order is the order the
# offline builder wrote the columns in, which is the standard projection order.
CONTRADICTION_PHYSICAL = ({"id": "int64", "payload0": "string", "payload1": "string",
                           "payload2": "string", "payload3": "string", "payload4": "int64",
                           "payload5": "string", "payload6": "string", "payload7": "string"},
                          {"id": "int64", "payload0": "string", "payload1": "string",
                           "payload2": "string", "payload3": "string", "payload4": "string",
                           "payload5": "string", "payload6": "string", "payload7": "string"})

# --- cross-file evolution ----------------------------------------------------------
# The one cross-file evolution shape: the SECOND file ADDS a projected column (payload3) that
# the FIRST lacks; every other column, by name and physical type, is the standard one. This is
# the multi-file case the single-file all-null shape (fixture sha256 d5dcff4a...) could not
# reach: the file that lacks the projected column carries typed NULLs for it, sum() and
# sum(length()) ignore NULLs, so the union value is what the surviving file alone gives on
# that column and the sum over both files on every other column.
#
# The expectation below was authored BEFORE any live round, computed independently in Python
# from local fixture read-backs (~/grv-evidence/grv-task28-work/build_fixtures.py, system
# python with pyarrow 21.0.0) and re-derived in closed form: ids 0..4095 sum to 8,386,560;
# every payload value is 256 characters; payload3 exists in the second file only, so it
# contributes 2048 x 256 = 524,288, while every other payload is present in both files and
# contributes 4096 x 256 = 1,048,576. It was not fitted to an observed run and no live round
# has run it. The offline indicator for THIS direction (the column missing from the FIRST file)
# comes from the DuckDB 1.4.2 probe recorded in spec/s3-shipping-evolved-typed-live-plan.md,
# not from the pinned 1.5.6 build, so it is an indicator of the rule and not an attestation.
EVOLUTION_SHAPE_NAME = "cross-file-evolution-typed-null-union"
EVOLUTION_SHAPE_ROWS = (2048, 2048)
EVOLUTION_TRIAL = "wide"
EVOLUTION_ADDED_COLUMN = "payload3"
EVOLUTION_EXPECTED_ROW = [8386560, 1048576, 1048576, 1048576, 524288, 1048576, 1048576,
                          1048576, 1048576]
EVOLUTION_PHYSICAL = ({"id": "int64", "payload0": "string", "payload1": "string",
                       "payload2": "string", "payload4": "string", "payload5": "string",
                       "payload6": "string", "payload7": "string"},
                      {"id": "int64", **{"payload%d" % column: "string"
                                         for column in range(8)}})
# Fixture SHA-256 recorded from the offline build (two independent builds agreed). Recorded
# for traceability, NOT gated: byte-identical reproducibility is not an acceptance item, and
# the same content written through a different code path hashes differently.
OFFLINE_FIXTURE_SHA256 = {"contradiction-0.parquet":
                          "1cbfaf00343dab554219ff175ebfa6da8064ec44d13d72e6a8e768a36262639e",
                          "contradiction-1.parquet":
                          "439d7673fb7a49966002387a09d5d76e53653e221c773259fb2686d8558aceea",
                          "evolution-0.parquet":
                          "ca4262e7e07280a2c608397340982490b6c695cd8fad415e6fd3b0a28703ec34",
                          "evolution-1.parquet":
                          "439d7673fb7a49966002387a09d5d76e53653e221c773259fb2686d8558aceea"}

MANIFEST_DOCUMENT_FIELDS = frozenset({"files", "trials"})
MANIFEST_FILE_FIELDS = frozenset({"local", "coordinate", "size", "sha256", "validator"})
MANIFEST_TRIAL_FIELDS = frozenset({"trial", "expected"})
MANIFEST_MAX_FILES = 4096
MANIFEST_MAX_TRIALS = 10


def encode_result(row):
    """Mirror of the harness fixed-width proof encoding: u64 rows, u64 columns, then one
    validity byte plus a u64 byte length plus those bytes for each value. Values are int64
    decimal text or null, exactly as the bounded metadata encoder emits them."""
    body = bytearray()
    body += (1).to_bytes(8, "little")
    body += len(row).to_bytes(8, "little")
    for value in row:
        if value is None:
            body += b"\x00" + (0).to_bytes(8, "little")
            continue
        require(type(value) is int and -2**63 <= value < 2**63, "expectation-value")
        text = str(value).encode()
        body += b"\x01" + len(text).to_bytes(8, "little") + text
    return body.hex()


def aggregate_expectation(columns_by_file, trial):
    """Compute the one aggregated expectation independently in Python from local fixture columns."""
    require(trial == MULTI_SHAPE_TRIAL, "multi-shape-trial")
    require(len(columns_by_file) == len(MULTI_SHAPE_ROWS) and bool(columns_by_file),
            "multi-shape-files")
    for columns in columns_by_file:
        require(sorted(columns) == sorted(SCHEMA), "fixture-schema-drift")
    ids = [value for columns in columns_by_file for value in columns["id"]]
    require(all(value is None or type(value) is int for value in ids), "expectation-value")
    row = [sum(value for value in ids if value is not None)]
    for column in range(8):
        name = f"payload{column}"
        row.append(sum(len(value) for columns in columns_by_file
                       for value in columns[name] if value is not None))
    return encode_result(row)


def manifest_document(entries, trials):
    """Build the closed manifest: exact fields, bounded values, no duplicates, no empty lists."""
    require(type(entries) is list and type(trials) is list, "manifest-shape")
    require(entries, "manifest-files-empty")
    require(trials, "manifest-trials-empty")
    require(len(entries) <= MANIFEST_MAX_FILES and len(trials) <= MANIFEST_MAX_TRIALS,
            "manifest-list-bound")
    files = []
    coordinates, identities = set(), set()
    for entry in entries:
        require(type(entry) is dict and frozenset(entry) == MANIFEST_FILE_FIELDS,
                "manifest-file-fields")
        local, coordinate = entry["local"], entry["coordinate"]
        size, sha, validator = entry["size"], entry["sha256"], entry["validator"]
        require(type(local) is str and 0 < len(local) <= 4096 and local.startswith("/")
                and not any(char in local for char in "\r\n\t"), "manifest-field-value")
        require(type(coordinate) is str and "/" in coordinate[5:] and len(coordinate) <= 8192
                and s3_key_canonical(coordinate) is not None, "manifest-field-value")
        require(type(size) is int and 0 < size < 2**63, "manifest-field-value")
        require(type(sha) is str and re.fullmatch(r"[0-9a-f]{64}", sha), "manifest-field-value")
        require(type(validator) is str and 1 <= len(validator) <= 1024
                and not any(char in validator for char in "\r\n"), "manifest-field-value")
        require(coordinate not in coordinates, "manifest-duplicate-coordinate")
        coordinates.add(coordinate)
        require((size, sha, validator) not in identities, "manifest-duplicate-identity")
        identities.add((size, sha, validator))
        files.append({"local": local, "coordinate": coordinate, "size": size,
                      "sha256": sha, "validator": validator})
    listed = []
    names = set()
    for trial in trials:
        require(type(trial) is dict and frozenset(trial) == MANIFEST_TRIAL_FIELDS,
                "manifest-trial-fields")
        require(trial["trial"] in TRIALS, "manifest-trial-name")
        require(trial["trial"] not in names, "manifest-duplicate-trial")
        names.add(trial["trial"])
        require(type(trial["expected"]) is str
                and re.fullmatch(r"(?:[0-9a-f]{2})+", trial["expected"]), "manifest-trial-expected")
        listed.append({"trial": trial["trial"], "expected": trial["expected"]})
    return {"files": files, "trials": listed}


def safe_manifest_native(raw):
    """Reconstruct the manifest-mode report with a strict schema; never publish arbitrary text."""
    def counts(value):
        require(type(value) is dict, "native-schema")
        result = {}
        for key in ("body_bytes", "head", "get"):
            n = value.get(key)
            require(type(n) is int and 0 <= n <= 10**12, "native-schema")
            result[key] = n
        return result
    def digest_text(value):
        require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "native-schema")
        return value
    try:
        data = json.loads(raw)
        require(type(data) is dict
                and frozenset(data) == {"manifest_mode", "shipping_scanner_source", "files",
                                       "phases", "trials", "denials"}, "native-schema")
        require(data["manifest_mode"] is True and data["shipping_scanner_source"] is True,
                "native-schema")
        require(type(data["files"]) is list and bool(data["files"]), "native-schema")
        files = []
        for index, value in enumerate(data["files"]):
            require(type(value) is dict
                    and frozenset(value) == {"index", "size", "sha256", "validator_sha256"},
                    "native-schema")
            require(value["index"] == index and type(value["size"]) is int
                    and 0 <= value["size"] <= 10**12, "native-schema")
            files.append({"index": index, "size": value["size"], "sha256": digest_text(value["sha256"]),
                          "validator_sha256": digest_text(value["validator_sha256"])})
        require(type(data["phases"]) is dict and frozenset(data["phases"]) == {"view_binding"},
                "native-schema")
        binding = counts(data["phases"]["view_binding"])
        require(type(data["trials"]) is list and bool(data["trials"]), "native-schema")
        trials = []
        for index, value in enumerate(data["trials"]):
            require(type(value) is dict
                    and frozenset(value) == {"trial_index", "trial", "correct", "query",
                                            "explain", "actual"}, "native-schema")
            require(value["trial_index"] == index and value["trial"] in TRIALS
                    and value["correct"] is True, "native-oracle")
            require(type(value["actual"]) is str and re.fullmatch(r"(?:[0-9a-f]{2})+", value["actual"]),
                    "native-schema")
            trials.append({"trial_index": index, "trial": value["trial"], "correct": True,
                           "query": counts(value["query"]), "explain": counts(value["explain"]),
                           "actual": value["actual"]})
        denials = {}
        for key in ("direct", "unlisted", "nested_glob", "secrets"):
            require(data["denials"][key] is True, "native-denial")
            denials[key] = True
        return {"manifest_mode": True, "shipping_scanner_source": True, "files": files,
                "phases": {"view_binding": binding}, "trials": trials, "denials": denials}
    except (ValueError, KeyError, TypeError, OverflowError):
        raise SafeFailure("native-schema") from None


def verify_reported(entries, report, expectations):
    """Refuse to act when the harness reports back anything other than what was recorded.

    Takes the expectations as a LIST and compares them in order against the reported trials, so a
    shape with more than one trial cannot pass on a shuffled report. The three-file aggregate and
    the refusal shape pass a one-element list, which is their unchanged behaviour.
    """
    require(type(expectations) is list and len(report["files"]) == len(entries)
            and len(report["trials"]) == len(expectations), "manifest-report-shape")
    for entry, reported in zip(entries, report["files"]):
        require(reported["size"] == entry["size"] and reported["sha256"] == entry["sha256"]
                and reported["validator_sha256"] ==
                hashlib.sha256(entry["validator"].encode()).hexdigest(),
                "reported-identity-mismatch")
    for expectation, trial in zip(expectations, report["trials"]):
        require(trial["trial"] == expectation["trial"]
                and trial["actual"] == expectation["expected"], "reported-expectation-mismatch")


def multi_shape_plan(rows, bucket):
    """Resolve the one approved multi-file shape: three single-row-group files, one shared schema."""
    require(rows in (8192, 65536) and rows >= sum(MULTI_SHAPE_ROWS), "shape-row-expectation")
    files = []
    for index, count in enumerate(MULTI_SHAPE_ROWS):
        name = f"multi-{index}.parquet"
        key = "data/" + name
        coordinate = "s3://" + bucket + "/" + grv_encode(key.encode())
        require(s3_key_canonical(coordinate) == key.encode(), "coordinate-round-trip")
        files.append({"name": name, "key": key, "local_fixture": name, "rows": count,
                      "coordinate": coordinate, "columns": dict(SCHEMA)})
    require(len({item["coordinate"] for item in files}) == len(files),
            "multi-shape-duplicate-coordinate")
    return {"name": MULTI_SHAPE_NAME, "files": files, "trial": MULTI_SHAPE_TRIAL,
            "columns": dict(SCHEMA), "compression": "NONE", "dictionary": False,
            "statistics": True, "row_group_rows": 2048}


def refusal_plan(rows, bucket):
    """Resolve the one bounded refusal shape: two files, second missing a projected column."""
    require(rows in (8192, 65536) and rows >= sum(REFUSAL_SHAPE_ROWS), "shape-row-expectation")
    columns = [dict(SCHEMA), {key: value for key, value in SCHEMA.items()
                              if key != REFUSAL_DROPPED_COLUMN}]
    files = []
    for index, count in enumerate(REFUSAL_SHAPE_ROWS):
        name = "refusal-%d.parquet" % index
        key = "data/" + name
        coordinate = "s3://" + bucket + "/" + grv_encode(key.encode())
        require(s3_key_canonical(coordinate) == key.encode(), "coordinate-round-trip")
        files.append({"name": name, "key": key, "local_fixture": name, "rows": count,
                      "coordinate": coordinate, "columns": columns[index]})
    require(len({item["coordinate"] for item in files}) == len(files),
            "refusal-duplicate-coordinate")
    require(sorted(files[1]["columns"]) == sorted(k for k in SCHEMA
                                                  if k != REFUSAL_DROPPED_COLUMN),
            "refusal-shape-shape")
    return {"name": REFUSAL_SHAPE_NAME, "files": files, "outcome": "refused",
            "expected_code": REFUSAL_EXPECTED_CODE, "dropped_column": REFUSAL_DROPPED_COLUMN,
            "trial": MULTI_SHAPE_TRIAL, "compression": "NONE", "dictionary": False,
            "statistics": True, "row_group_rows": 2048}


def cold_warm_plan(rows, bucket):
    """Resolve the one cold/warm shape: ONE file, standard data, two closed trials in fixed order."""
    require(rows in (8192, 65536) and rows >= COLD_WARM_ROWS, "shape-row-expectation")
    name = "cold-warm.parquet"
    key = "data/" + name
    coordinate = "s3://" + bucket + "/" + grv_encode(key.encode())
    require(s3_key_canonical(coordinate) == key.encode(), "coordinate-round-trip")
    return {"name": COLD_WARM_SHAPE_NAME,
            "files": [{"name": name, "key": key, "local_fixture": name,
                       "rows": COLD_WARM_ROWS, "coordinate": coordinate, "columns": dict(SCHEMA)}],
            "trials": list(COLD_WARM_TRIALS), "columns": dict(SCHEMA),
            "compression": "NONE", "dictionary": False, "statistics": True, "row_group_rows": 2048}


def cold_warm_expectation(columns, trial):
    """Compute one of exactly two closed-trial expectations from the local fixture columns.

    The value is derived from COLD_WARM_SQL, the quoted mirror of the harness closed SQL, not from
    the trial name: `wide` projects sum(id) plus the eight payload lengths, `narrow` projects
    sum(id) alone, and the only aggregate either one uses is sum() cast to BIGINT. Standard
    generated data only (real payload values, no nulls), so both expectations are unambiguous.
    """
    require(trial in COLD_WARM_TRIALS, "cold-warm-trial-name")
    require(sorted(columns) == sorted(SCHEMA), "fixture-schema-drift")
    require(bool(columns["id"]), "cold-warm-fixture-empty")
    require(all(type(value) is int for value in columns["id"])
            and all(type(value) is str for column in range(8)
                    for value in columns[f"payload{column}"]), "cold-warm-data-not-standard")
    sql = COLD_WARM_SQL[trial]
    require(sql.startswith("SELECT ") and sql.endswith(" FROM data"), "cold-warm-sql-mirror")
    row = []
    for item in sql[len("SELECT "):sql.index(" FROM data")].split(","):
        if item == "sum(id)::BIGINT":
            row.append(sum(columns["id"]))
            continue
        match = re.fullmatch(r"sum\(length\(payload([0-7])\)\)::BIGINT", item)
        require(match is not None, "cold-warm-sql-mirror")
        row.append(sum(len(value) for value in columns["payload" + match.group(1)]))
    require(len(row) == (9 if trial == "wide" else 1), "cold-warm-sql-mirror")
    return encode_result(row)


def recipe_column(column, physical, offset, count):
    """Standard fixture values for one column under its planned physical type.

    string -> the standard 256-character value; int64 -> the row's own id, the only integral
    recipe, so a physical-type difference stays the only variable and the values stay
    reproducible offline. Pure Python and pyarrow-free, so a cross-file shape's pre-authored
    expectation can be recomputed from the recipe alone and never from an observed run. run()
    builds the two new cross-file fixtures from this recipe, so the offline build and the
    run-time build carry the same values even when their bytes differ.
    """
    require(column in SCHEMA, "recipe-column")
    require(physical in ("int64", "string"), "recipe-physical")
    if physical == "int64":
        return list(range(offset, offset + count))
    return [hashlib.sha256(f"{column}-{row}".encode()).hexdigest() * 4
            for row in range(offset, offset + count)]


def recipe_array(column, physical, offset, count):
    """One standard fixture column as an Arrow array, for the run-time build (pyarrow caller)."""
    import pyarrow as pa
    return pa.array(recipe_column(column, physical, offset, count),
                    type=(pa.int64() if physical == "int64" else pa.string()))


def contradiction_plan(rows, bucket):
    """Resolve the one cross-file type-contradiction shape: same names, one column, two types.

    The outcome is a refusal and no value is compared, so the plan carries no expectation value.
    The decision code is None on purpose: it is per spec, to be confirmed at the round gate, and
    this runner will not invent a number for a mechanism it cannot attribute offline.
    """
    require(rows in (8192, 65536) and rows >= sum(CONTRADICTION_SHAPE_ROWS),
            "shape-row-expectation")
    require(len(CONTRADICTION_PHYSICAL) == len(CONTRADICTION_SHAPE_ROWS) == 2,
            "contradiction-shape-files")
    for physical in CONTRADICTION_PHYSICAL:
        require(sorted(physical) == sorted(SCHEMA), "contradiction-name-closure")
    differ = sorted(column for column in PROJECTION_ORDER
                    if CONTRADICTION_PHYSICAL[0][column] != CONTRADICTION_PHYSICAL[1][column])
    require(differ == [CONTRADICTION_COLUMN], "contradiction-single-column")
    require({CONTRADICTION_PHYSICAL[0][CONTRADICTION_COLUMN],
             CONTRADICTION_PHYSICAL[1][CONTRADICTION_COLUMN]} == {"int64", "string"},
            "contradiction-physical-types")
    require(CONTRADICTION_EXPECTED_CODE is None
            and CONTRADICTION_EXPECTED_CODE_STATUS == "per spec; to be confirmed at round gate",
            "contradiction-code-not-asserted")
    files = []
    for index, count in enumerate(CONTRADICTION_SHAPE_ROWS):
        name = "contradiction-%d.parquet" % index
        key = "data/" + name
        coordinate = "s3://" + bucket + "/" + grv_encode(key.encode())
        require(s3_key_canonical(coordinate) == key.encode(), "coordinate-round-trip")
        files.append({"name": name, "key": key, "local_fixture": name, "rows": count,
                      "offset": sum(CONTRADICTION_SHAPE_ROWS[:index]),
                      "coordinate": coordinate, "columns": list(CONTRADICTION_PHYSICAL[index]),
                      "physical": dict(CONTRADICTION_PHYSICAL[index]),
                      "offline_fixture_sha256": OFFLINE_FIXTURE_SHA256[name]})
    require(len({item["coordinate"] for item in files}) == len(files),
            "contradiction-duplicate-coordinate")
    require(files[0]["physical"][CONTRADICTION_COLUMN] != files[1]["physical"][CONTRADICTION_COLUMN],
            "contradiction-single-column")
    return {"name": CONTRADICTION_SHAPE_NAME, "files": files, "outcome": "refused",
            "value_compared": False, "expected_code": CONTRADICTION_EXPECTED_CODE,
            "expected_code_status": CONTRADICTION_EXPECTED_CODE_STATUS,
            "contradicted_column": CONTRADICTION_COLUMN, "trial": CONTRADICTION_TRIAL,
            "columns": dict(SCHEMA), "compression": "NONE", "dictionary": False,
            "statistics": True, "row_group_rows": 2048}


def evolution_plan(rows, bucket):
    """Resolve the one cross-file evolution shape: the second file adds one projected column."""
    require(rows in (8192, 65536) and rows >= sum(EVOLUTION_SHAPE_ROWS), "shape-row-expectation")
    require(len(EVOLUTION_PHYSICAL) == len(EVOLUTION_SHAPE_ROWS) == 2, "evolution-shape-files")
    require(set(EVOLUTION_PHYSICAL[1]) == set(SCHEMA), "evolution-second-file-closure")
    require(set(EVOLUTION_PHYSICAL[0]) == set(SCHEMA) - {EVOLUTION_ADDED_COLUMN},
            "evolution-first-file-closure")
    require(len(EVOLUTION_EXPECTED_ROW) == len(SCHEMA), "evolution-expectation-shape")
    files = []
    for index, count in enumerate(EVOLUTION_SHAPE_ROWS):
        name = "evolution-%d.parquet" % index
        key = "data/" + name
        coordinate = "s3://" + bucket + "/" + grv_encode(key.encode())
        require(s3_key_canonical(coordinate) == key.encode(), "coordinate-round-trip")
        files.append({"name": name, "key": key, "local_fixture": name, "rows": count,
                      "offset": sum(EVOLUTION_SHAPE_ROWS[:index]),
                      "coordinate": coordinate,
                      "columns": [column for column in PROJECTION_ORDER
                                  if column in EVOLUTION_PHYSICAL[index]],
                      "physical": dict(EVOLUTION_PHYSICAL[index]),
                      "offline_fixture_sha256": OFFLINE_FIXTURE_SHA256[name]})
    require(len({item["coordinate"] for item in files}) == len(files),
            "evolution-duplicate-coordinate")
    return {"name": EVOLUTION_SHAPE_NAME, "files": files, "outcome": "typed-null-union",
            "added_column": EVOLUTION_ADDED_COLUMN, "expected_row": list(EVOLUTION_EXPECTED_ROW),
            "trial": EVOLUTION_TRIAL, "columns": dict(SCHEMA), "compression": "NONE",
            "dictionary": False, "statistics": True, "row_group_rows": 2048}


def evolution_expectation(columns_by_file, plan):
    """Recompute the pre-authored typed-NULL union from local fixture read-backs and require agreement.

    A projected column a file does not carry is a typed NULL on that file's rows, and sum() and
    sum(length()) ignore NULLs, so the file that lacks the column contributes nothing to it and
    every other column sums across both files. EVOLUTION_EXPECTED_ROW was authored offline before
    any live round; a fixture that is not the fixture the expectation was written for stops the
    run here, before any upload, so a drift can never be absorbed as a pass.
    """
    require(len(columns_by_file) == len(plan["files"]) == 2, "evolution-shape-files")
    require(plan["trial"] == EVOLUTION_TRIAL, "evolution-trial")
    union = set()
    for entry, columns in zip(plan["files"], columns_by_file):
        require(sorted(columns) == sorted(entry["columns"]), "evolution-fixture-drift")
        union.update(columns)
    require(union == set(SCHEMA), "evolution-union-closure")
    first, second = (set(entry["columns"]) for entry in plan["files"])
    require(second - first == {EVOLUTION_ADDED_COLUMN} and not first - second,
            "evolution-added-column")
    row = []
    for column in PROJECTION_ORDER:
        total = 0
        for columns in columns_by_file:
            values = columns.get(column, [])
            if column == "id":
                require(all(type(value) is int for value in values), "evolution-id-recipe-drift")
                total += sum(values)
            else:
                require(all(value is None or type(value) is str for value in values),
                        "evolution-payload-recipe-drift")
                total += sum(len(value) for value in values if value is not None)
        row.append(total)
    require(row == EVOLUTION_EXPECTED_ROW, "evolution-expectation-drift")
    return encode_result(row)


def require_contradiction_types(types_by_file, plan):
    """Refuse to act when the two fixtures do not carry the planned contradiction.

    Both files must carry every projected column by name, exactly one column may differ in
    physical type, and that column must be the planned one with the two planned types. Pure
    Python and pyarrow-free so the gate is testable offline.
    """
    require(len(types_by_file) == len(plan["files"]) == 2, "contradiction-shape-files")
    for entry, types in zip(plan["files"], types_by_file):
        require(sorted(types) == sorted(entry["columns"]), "contradiction-name-closure")
        for column in entry["columns"]:
            require(types[column] == entry["physical"][column], "contradiction-physical-drift")
    differ = sorted(column for column in PROJECTION_ORDER
                    if types_by_file[0][column] != types_by_file[1][column])
    require(differ == [CONTRADICTION_COLUMN], "contradiction-column-drift")


class SafeFailure(Exception):
    """Only programmer-defined stages, never remote error text."""


def require(condition, stage):
    if not condition:
        raise SafeFailure(stage)


def selected_slices(only):
    """The slices a run will execute. Empty selection means every slice, in the fixed order."""
    universe = ([entry[0] for entry in SHAPES] + [MULTI_SHAPE_NAME, REFUSAL_SHAPE_NAME,
                                                 COLD_WARM_SHAPE_NAME, CONTRADICTION_SHAPE_NAME,
                                                 EVOLUTION_SHAPE_NAME])
    if not only:
        return universe
    unknown = [name for name in only if name not in universe]
    require(not unknown, "unknown-slice-selector")
    return [name for name in universe if name in set(only)]


def require_null_payload(columns):
    """Refuse to act when a shape planned as all-null actually contains a non-null payload."""
    require(sorted(columns) == sorted(SCHEMA), "fixture-schema-drift")
    require(bool(columns["id"]) and all(value is not None for value in columns["id"]),
            "null-shape-id-must-be-real")
    for column in range(8):
        values = columns[f"payload{column}"]
        require(bool(values) and all(value is None for value in values),
                "null-shape-payload-not-all-null")


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, value):
    """Create at 0600 from the start; atomic rename and fsync parent directory."""
    fd, temporary = tempfile.mkstemp(prefix=".journal-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def selected_environment(profile):
    """Refuse indirect providers; strip all ambient AWS/proxy/logging settings.

    Both CLI and native read the same existing named-profile files. Checking the
    selected sections prevents AWS CLI from invoking SSO/process/role providers.
    No credential values are returned, copied, printed or journaled.
    """
    home = pathlib.Path.home()
    credentials = home / ".aws/credentials"
    configuration = home / ".aws/config"
    allowed = {"aws_access_key_id", "aws_secret_access_key", "aws_session_token",
               "region", "output"}
    found = {}
    for path, section in ((credentials, profile),
                          (configuration, "profile " + profile)):
        if not path.exists():
            continue
        parsed = configparser.RawConfigParser()
        parsed.read(path)
        require(not parsed.defaults(), "profile-defaults-refused")
        if parsed.has_section(section):
            values = dict(parsed.items(section))
            require(set(values) <= allowed, "indirect-profile-provider-refused")
            for key, value in values.items():
                require(key not in found or found[key] == value, "ambiguous-profile")
                found[key] = value
    require(bool(found.get("aws_access_key_id")) and
            bool(found.get("aws_secret_access_key")), "selected-static-profile-unavailable")
    env = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR", "LANG")
           if key in os.environ}
    env.update(AWS_CONFIG_FILE=str(configuration),
               AWS_SHARED_CREDENTIALS_FILE=str(credentials),
               AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1",
               AWS_RETRY_MODE="standard", AWS_CLI_AUTO_PROMPT="off", AWS_PAGER="")
    return env


def bounded_process(command, env, timeout):
    """Reap the whole process group before returning, including on interruption.

    Temporary streams start private and are unlinked immediately; unknown
    stdout/stderr are never logged or retained as evidence.
    """
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as error:
        child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                 stdout=output, stderr=error, start_new_session=True)
        try:
            child.wait(timeout=timeout)
        except BaseException:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
            raise
        require(output.tell() <= 1024 * 1024 and error.tell() <= 1024 * 1024,
                "child-output-limit")
        output.seek(0)
        error.seek(0)
        return child.returncode, output.read(), error.read()


def safe_native(raw):
    """Reconstruct evidence using a strict schema; never publish arbitrary JSON."""
    try:
        data = json.loads(raw)
        require(type(data) is dict and data.get("provider") == "protected_s3" and
                data.get("shipping_scanner_source") is True, "native-schema")
        def counts(value):
            require(type(value) is dict, "native-schema")
            result = {}
            for key in ("body_bytes", "head", "get"):
                n = value.get(key)
                require(type(n) is int and 0 <= n <= 10**12, "native-schema")
                result[key] = n
            return result
        binding = counts(data["phases"]["view_binding"])
        require(type(data["trials"]) is list and len(data["trials"]) == len(TRIALS),
                "native-schema")
        trials = []
        for name, value in zip(TRIALS, data["trials"]):
            require(value["trial"] == name and value["correct"] is True, "native-oracle")
            trials.append({"trial": name, "correct": True,
                           "query": counts(value["query"]),
                           "explain": counts(value["explain"])})
        denials = {}
        for key in ("direct", "unlisted", "nested_glob", "secrets"):
            require(data["denials"][key] is True, "native-denial")
            denials[key] = True
        savings = {}
        for key in ("projection", "selective_wide"):
            n = data["qualifying_savings"][key]
            require(type(n) in (int, float) and math.isfinite(n) and n <= 1,
                    "native-schema")
            savings[key] = n
        require(trials[0]["query"]["body_bytes"] > 0, "native-empty-baseline")
        return {"provider": "protected_s3", "shipping_scanner_source": True,
                "phases": {"view_binding": binding}, "trials": trials,
                "denials": denials, "qualifying_savings": savings}
    except (ValueError, KeyError, TypeError, OverflowError):
        raise SafeFailure("native-schema") from None


class Probe:
    def __init__(self, directory, args, env):
        self.directory, self.args, self.env = directory, args, env
        self.aws = shutil.which("aws", path=env.get("PATH"))
        require(bool(self.aws), "aws-cli-unavailable")
        self.state = {"bucket": "grv-s3-shipping-probe-" + uuid.uuid4().hex,
                      "owner_marker": uuid.uuid4().hex, "profile": args.profile,
                      "region": args.region, "account": ACCOUNT,
                      "objects": {}, "events": [], "creation_intent": False}
        self.journal("initialized")

    def journal(self, event):
        self.state["events"].append({"event": event, "time_unix": int(time.time())})
        atomic_json(self.directory / "state.json", self.state)

    def call(self, service, operation, **parameters):
        command = [self.aws, "--profile", self.args.profile, "--region", self.args.region,
                   "--no-cli-pager", "--no-cli-auto-prompt", "--cli-connect-timeout", "10",
                   "--cli-read-timeout", "30", "--output", "json", service, operation]
        body = parameters.pop("Body", None)
        if parameters:
            # Only non-secret coordinates, validators and fixture metadata.
            command += ["--cli-input-json", json.dumps(parameters)]
        if body is not None:
            # Streaming blobs must use --body, not a literal JSON string.
            command += ["--body", body]
        try:
            code, output, error = bounded_process(command, self.env, 120)
        except subprocess.TimeoutExpired:
            raise SafeFailure("aws-timeout") from None
        if code:
            # Accept only exact structured AWS error code, suppress everything else.
            match = re.search(rb"An error occurred \(([^)\r\n]{1,80})\) when calling", error)
            known = match.group(1).decode("ascii", errors="ignore") if match else "unknown"
            return known if known in ("404", "NotFound", "NoSuchBucket") else "unknown", None
        try:
            result = json.loads(output) if output.strip() else {}
            require(type(result) is dict, "aws-response-schema")
            return None, result
        except (ValueError, TypeError):
            raise SafeFailure("aws-response-schema") from None

    def ok(self, service, operation, **parameters):
        error, result = self.call(service, operation, **parameters)
        require(error is None, "aws-" + operation + "-failed")
        return result

    def bucket_args(self):
        return {"Bucket": self.state["bucket"], "ExpectedBucketOwner": ACCOUNT}

    def absence(self):
        error, _ = self.call("s3api", "head-bucket", **self.bucket_args())
        require(error in ("404", "NotFound", "NoSuchBucket"), "bucket-absence-unproved")

    def owner(self):
        tags = self.ok("s3api", "get-bucket-tagging", **self.bucket_args()).get("TagSet")
        require(type(tags) is list and {"Key": MARKER, "Value": self.state["owner_marker"]}
                in tags, "bucket-owner-marker-mismatch")

    def create(self):
        identity = self.ok("sts", "get-caller-identity")
        require(identity.get("Account") == ACCOUNT and identity.get("Arn") == ARN,
                "unexpected-selected-profile-identity")
        self.state["identity"] = {"account": ACCOUNT, "arn": ARN, "fallback": False}
        self.absence()
        self.state["absence_before_creation"] = True
        self.state["creation_intent"] = True
        self.journal("absence-proved-creation-intent")
        parameters = {"Bucket": self.state["bucket"]}
        if self.args.region != "us-east-1":
            parameters["CreateBucketConfiguration"] = {"LocationConstraint": self.args.region}
        self.ok("s3api", "create-bucket", **parameters)
        self.journal("bucket-created")
        self.ok("s3api", "put-bucket-tagging", **self.bucket_args(),
                Tagging={"TagSet": [{"Key": MARKER, "Value": self.state["owner_marker"]}]})
        self.owner()
        self.state["marker_verified"] = True
        self.journal("owner-marker-verified")
        versioning = self.ok("s3api", "get-bucket-versioning", **self.bucket_args())
        require(not versioning.get("Status"), "unexpected-bucket-versioning")
        self.state["versioning_at_creation"] = "unversioned"
        self.journal("versioning-checked-no-change")

    def upload(self, key, fixture):
        self.owner()
        entry = {"sha256": digest(fixture), "bytes": fixture.stat().st_size,
                 "upload_intent": True}
        self.state["objects"][key] = entry
        self.journal("upload-intent")
        result = self.ok("s3api", "put-object", **self.bucket_args(), Key=key,
                         Body=str(fixture), Metadata={"grv-owner": self.state["owner_marker"],
                                                     "grv-sha256": entry["sha256"]})
        # Persist PUT identity before HEAD, so partial HEAD failures remain recoverable.
        entry["put_etag"] = self.etag(result.get("ETag"))
        entry["put_version_id"] = self.version(result.get("VersionId"))
        self.journal("put-identity-recorded")
        head = self.inspect_object(key, entry["put_version_id"])
        require(head["etag"] == entry["put_etag"] and
                head["version_id"] == entry["put_version_id"], "put-head-identity-mismatch")
        entry.update(head)
        self.journal("head-identity-verified")
        return entry

    @staticmethod
    def etag(value):
        require(type(value) is str and re.fullmatch(r'"[a-fA-F0-9]{32}(?:-[0-9]+)?"', value),
                "unexpected-etag")
        return value

    @staticmethod
    def version(value):
        require(value is None or (type(value) is str and 0 < len(value) <= 1024 and
                                 re.fullmatch(r"[A-Za-z0-9+/=._-]+", value)),
                "unexpected-version")
        return value

    @classmethod
    def canonical_version(cls, value):
        # S3 lists unversioned objects as VersionId="null", but PUT/HEAD may
        # omit the header. Real version identifiers must never collapse to null.
        value = cls.version(value)
        return None if value in (None, "null") else value

    def inspect_object(self, key, version):
        require(key in self.state["objects"], "unexpected-object-key")
        parameters = dict(self.bucket_args(), Key=key)
        if version is not None:
            parameters["VersionId"] = version
        head = self.ok("s3api", "head-object", **parameters)
        expected = self.state["objects"][key]
        require(head.get("Metadata") == {"grv-owner": self.state["owner_marker"],
                                         "grv-sha256": expected["sha256"]} and
                head.get("ContentLength") == expected["bytes"], "object-owner-identity-mismatch")
        return {"etag": self.etag(head.get("ETag")),
                "version_id": self.version(head.get("VersionId"))}

    def inventory(self):
        objects = self.ok("s3api", "list-objects-v2", **self.bucket_args(), MaxKeys=100)
        versions = self.ok("s3api", "list-object-versions", **self.bucket_args(), MaxKeys=100)
        uploads = self.ok("s3api", "list-multipart-uploads", **self.bucket_args(), MaxUploads=100)
        for value in (objects, versions, uploads):
            require(not value.get("IsTruncated"), "inventory-truncated")
        require(not uploads.get("Uploads"), "unexpected-multipart-upload")
        return objects.get("Contents", []), versions.get("Versions", []), versions.get("DeleteMarkers", [])

    def delete_owned(self, key, version, etag):
        """Delete exactly one tracked owned generation, conditionally and journaled."""
        self.owner()
        require(not self.ok("s3api", "get-bucket-versioning", **self.bucket_args()).get("Status"),
                "cleanup-versioning-changed")
        parameters = dict(self.bucket_args(), Key=key, IfMatch=etag)
        if version is not None:
            parameters["VersionId"] = version
        self.ok("s3api", "delete-object", **parameters)
        self.journal("owned-generation-deleted")

    def harness(self, command, timeout):
        """Run the reviewed harness binary; unknown child output is discarded, not journaled."""
        try:
            code, stdout, _error = bounded_process(command, self.env, timeout)
        except subprocess.TimeoutExpired:
            self.journal("harness-timeout-killed-reaped")
            raise SafeFailure("harness-timeout") from None
        require(code == 0, "harness-failed")
        return stdout

    def harness_code(self, command, timeout):
        """Run the reviewed harness binary and keep its exit status for the caller."""
        try:
            code, stdout, error = bounded_process(command, self.env, timeout)
        except subprocess.TimeoutExpired:
            self.journal("harness-timeout-killed-reaped")
            raise SafeFailure("harness-timeout") from None
        return code, stdout, error

    def multi_file_slice(self, columns_by_file, plan, evidence=None):
        """Run the one multi-file shape: three files, one shared schema, one aggregated expectation.

        The harness local oracle admits only ONE descriptor per call, so the multi-file ground
        truth is computed independently here in Python from the local fixtures and handed to the
        harness inside a closed private manifest written at 0600 by atomic rename. The guarded
        cloud path itself is unchanged. Every object is tracked and cleaned individually, and a
        disagreement between a recorded identity and the reported result stops the slice before
        any delete.
        """
        require(len(columns_by_file) == len(plan["files"]) == len(MULTI_SHAPE_ROWS),
                "multi-shape-files")
        require(frozenset(plan["columns"]) == frozenset(SCHEMA), "fixture-schema-drift")
        entries = []
        for item in plan["files"]:
            fixture = self.directory / item["local_fixture"]
            entry = self.upload(item["key"], fixture)
            entries.append({"local": str(fixture), "coordinate": item["coordinate"],
                            "size": entry["bytes"], "sha256": entry["sha256"],
                            "validator": entry["etag"]})
        expectation = {"trial": plan["trial"],
                       "expected": aggregate_expectation(columns_by_file, plan["trial"])}
        manifest = self.directory / "manifest.json"
        atomic_json(manifest, manifest_document(entries, [expectation]))
        self.journal("manifest-written-private")
        command = [str(self.args.binary), "s3", str(self.args.httpfs), str(self.args.aws),
                   "s3://" + self.state["bucket"] + "/data", self.args.profile, self.args.region,
                   str(manifest)]
        self.journal("manifest-harness-start")
        started = time.monotonic()
        code, stdout, error = self.harness_code(command, self.args.native_timeout)
        self.journal("manifest-harness-reaped")
        if code != 0:
            # Fixed numeric decision codes only; no manifest content or provider text is kept.
            diagnostic = re.fullmatch(
                rb"shipping qualification failed \(private details suppressed; protected read "
                rb"phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", error)
            if evidence is not None:
                evidence.setdefault("failed_slices", []).append({
                    "name": plan["name"], "multi_file": True,
                    "decision_code": int(diagnostic.group(1)) if diagnostic else None,
                    "http_status": int(diagnostic.group(2)) if diagnostic else None,
                    "native_exit": code, "stage": "manifest-harness-failed",
                    "native_reaped": True, "elapsed_seconds": round(time.monotonic() - started, 3)})
            raise SafeFailure("manifest-harness-failed")
        report = safe_manifest_native(stdout)
        verify_reported(entries, report, [expectation])
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require(self.inspect_object(item["key"], entry["version_id"]) ==
                    {"etag": entry["etag"], "version_id": entry["version_id"]},
                    "post-native-identity-mismatch")
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require("put_etag" in entry and "put_version_id" in entry,
                    "cleanup-generation-mismatch")
            self.delete_owned(item["key"], self.canonical_version(entry["put_version_id"]),
                              entry["put_etag"])
        return {"name": plan["name"], "multi_file": True,
                "shared_schema": "id BIGINT plus eight VARCHAR payloads",
                "ground_truth": "computed independently in Python from the local fixtures; the "
                                "harness local oracle admits only one descriptor per call",
                "files": [{"local_fixture": item["local_fixture"],
                           "canonical_coordinate": item["coordinate"], "rows": item["rows"],
                           "bytes": self.state["objects"][item["key"]]["bytes"],
                           "sha256": self.state["objects"][item["key"]]["sha256"],
                           "etag": self.state["objects"][item["key"]]["etag"],
                           "version_id": self.state["objects"][item["key"]]["version_id"],
                           "cleaned_individually": True} for item in plan["files"]],
                "trial": {"trial": plan["trial"],
                          "expected": "sum(id) plus eight sum(length(payloadN)) over all three files",
                          "expected_sha256": hashlib.sha256(bytes.fromhex(expectation["expected"])).hexdigest(),
                          "reported": report["trials"][0]},
                "phases": report["phases"], "denials": report["denials"],
                "native_timeout_seconds": self.args.native_timeout,
                "elapsed_seconds": round(time.monotonic() - started, 3)}

    def cold_warm_slice(self, columns_by_file, plan, evidence=None):
        """Run the one cold/warm shape: ONE object, TWO trials, in the order that defines the read.

        Trial 1 (`wide`) is the cold read: the first guarded execution against that object. Trial 2
        (`narrow`) is the warm read: the second guarded execution on the same retained
        connection-session, the only sense of warm available on this path. The order is fixed in
        COLD_WARM_TRIALS and is not taken from the caller. Ground truth is computed independently in
        Python from the local fixture, the same way the three-file aggregate does it, and the
        expectation mirror follows the closed harness SQL rather than the trial name.

        ASSERTED: both trials correct; all four standing denials refused, which is checked after the
        warm read because that is where a warm-path authority leak would show; per-trial counters
        recorded for BOTH trials; the warm read gains no new authority (same owner, same guard, same
        denials refusing). NOT ASSERTED: which way the counters fall. Reduced warm fetching and full
        warm re-fetch are both consistent with the documented design, so either is accepted and only
        recorded. Counters cover accepted protected-reader GET bodies and HEAD/GET attempts only, so
        any statement this shape makes is bounded to that.
        """
        require(len(columns_by_file) == len(plan["files"]) == 1, "cold-warm-files")
        require(plan["trials"] == list(COLD_WARM_TRIALS), "cold-warm-trial-order")
        require(frozenset(plan["columns"]) == frozenset(SCHEMA), "fixture-schema-drift")
        require(frozenset(columns_by_file[0]) == frozenset(SCHEMA), "fixture-schema-drift")
        entries = []
        for item in plan["files"]:
            fixture = self.directory / item["local_fixture"]
            entry = self.upload(item["key"], fixture)
            entries.append({"local": str(fixture), "coordinate": item["coordinate"],
                            "size": entry["bytes"], "sha256": entry["sha256"],
                            "validator": entry["etag"]})
        expectations = [{"trial": name,
                         "expected": cold_warm_expectation(columns_by_file[0], name)}
                        for name in COLD_WARM_TRIALS]
        manifest = self.directory / "manifest-cold-warm.json"
        atomic_json(manifest, manifest_document(entries, expectations))
        self.journal("manifest-written-private")
        command = [str(self.args.binary), "s3", str(self.args.httpfs), str(self.args.aws),
                   "s3://" + self.state["bucket"] + "/data", self.args.profile, self.args.region,
                   str(manifest)]
        self.journal("manifest-harness-start")
        started = time.monotonic()
        code, stdout, error = self.harness_code(command, self.args.native_timeout)
        self.journal("manifest-harness-reaped")
        if code != 0:
            # Fixed numeric decision codes only; no manifest content or provider text is kept.
            diagnostic = re.fullmatch(
                rb"shipping qualification failed \(private details suppressed; protected read "
                rb"phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", error)
            if evidence is not None:
                evidence.setdefault("failed_slices", []).append({
                    "name": plan["name"], "cold_warm": True,
                    "decision_code": int(diagnostic.group(1)) if diagnostic else None,
                    "http_status": int(diagnostic.group(2)) if diagnostic else None,
                    "native_exit": code, "stage": "manifest-harness-failed",
                    "native_reaped": True, "elapsed_seconds": round(time.monotonic() - started, 3)})
            raise SafeFailure("manifest-harness-failed")
        report = safe_manifest_native(stdout)
        verify_reported(entries, report, expectations)
        # Asserted for BOTH trials: correct, and counters recorded. Only presence is asserted here,
        # never direction, so a reduced warm fetch and a full warm re-fetch both pass and whatever
        # happened is what gets recorded.
        require(len(report["trials"]) == len(COLD_WARM_TRIALS), "cold-warm-trial-count")
        for index, name in enumerate(COLD_WARM_TRIALS):
            trial = report["trials"][index]
            require(trial["trial_index"] == index and trial["trial"] == name,
                    "cold-warm-trial-order")
            require(trial["correct"] is True, "cold-warm-trial-correct")
            for phase in ("query", "explain"):
                require(type(trial[phase]) is dict and len(trial[phase]) == 3
                        and all(type(trial[phase][key]) is int
                                for key in ("body_bytes", "head", "get")),
                        "cold-warm-counter-missing")
        # Asserted: the four standing denials still refuse. They run after both reads, so this is
        # the check that the warm read gained no new authority.
        for key in ("direct", "unlisted", "nested_glob", "secrets"):
            require(report["denials"][key] is True, "cold-warm-denial-refused")
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require(self.inspect_object(item["key"], entry["version_id"]) ==
                    {"etag": entry["etag"], "version_id": entry["version_id"]},
                    "post-native-identity-mismatch")
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require("put_etag" in entry and "put_version_id" in entry,
                    "cleanup-generation-mismatch")
            self.delete_owned(item["key"], self.canonical_version(entry["put_version_id"]),
                              entry["put_etag"])
        return {"name": plan["name"], "manifest_mode": True, "one_object_two_trials": True,
                "observation": [{"trial_index": 0, "trial": COLD_WARM_TRIALS[0], "kind": "cold"},
                                {"trial_index": 1, "trial": COLD_WARM_TRIALS[1], "kind": "warm"}],
                "order_fixed_in_code": list(COLD_WARM_TRIALS),
                "counter_direction_not_asserted": True,
                "warm_read_gains_no_new_authority": True,
                "ground_truth": "computed independently in Python from the local fixture; the "
                                "harness local oracle admits only one descriptor per call",
                "files": [{"local_fixture": item["local_fixture"],
                           "canonical_coordinate": item["coordinate"], "rows": item["rows"],
                           "row_group_rows": plan["row_group_rows"],
                           "compression": plan["compression"],
                           "dictionary": plan["dictionary"], "statistics": plan["statistics"],
                           "bytes": self.state["objects"][item["key"]]["bytes"],
                           "sha256": self.state["objects"][item["key"]]["sha256"],
                           "etag": self.state["objects"][item["key"]]["etag"],
                           "version_id": self.state["objects"][item["key"]]["version_id"],
                           "cleaned_individually": True} for item in plan["files"]],
                "trials": [{"trial_index": index, "trial": name, "correct": True,
                            "expected_sha256": hashlib.sha256(
                                bytes.fromhex(expectation["expected"])).hexdigest(),
                            "query": trial["query"], "explain": trial["explain"]}
                           for index, (name, expectation, trial) in enumerate(
                               zip(COLD_WARM_TRIALS, expectations, report["trials"]))],
                "denials": report["denials"],
                "native_timeout_seconds": self.args.native_timeout,
                "elapsed_seconds": round(time.monotonic() - started, 3)}

    def refusal_slice(self, readbacks, plan, evidence):
        """Run the one bounded refusal shape and require the harness to refuse it.

        The second file omits a projected column, so this shape asserts refusal rather than
        a value. The manifest still carries a placeholder expectation because its grammar
        requires one for every trial; nothing here weakens a refusal into a pass, and any
        other outcome stops the run before any delete. The guarded cloud path is unchanged.
        """
        require(len(readbacks) == len(plan["files"]) == len(REFUSAL_SHAPE_ROWS),
                "refusal-shape-files")
        require(sorted(readbacks[0]) == sorted(SCHEMA), "refusal-first-file-schema")
        require(sorted(readbacks[1]) == sorted(k for k in SCHEMA
                                               if k != REFUSAL_DROPPED_COLUMN),
                "refusal-second-file-schema")
        entries = []
        for item in plan["files"]:
            fixture = self.directory / item["local_fixture"]
            entry = self.upload(item["key"], fixture)
            entries.append({"local": str(fixture), "coordinate": item["coordinate"],
                            "size": entry["bytes"], "sha256": entry["sha256"],
                            "validator": entry["etag"]})
        placeholder = hashlib.sha256(b"refusal shape: value not compared").hexdigest()
        manifest = self.directory / "manifest-refusal.json"
        atomic_json(manifest, manifest_document(entries, [{"trial": plan["trial"],
                                                           "expected": placeholder}]))
        manifest.chmod(0o600)
        self.journal("manifest-written-private")
        command = [str(self.args.binary), "s3", str(self.args.httpfs), str(self.args.aws),
                   "s3://" + self.state["bucket"] + "/data", self.args.profile,
                   self.args.region, str(manifest)]
        self.journal("manifest-harness-start")
        started = time.monotonic()
        code, _stdout, error = self.harness_code(command, self.args.native_timeout)
        self.journal("manifest-harness-reaped")
        diagnostic = re.fullmatch(
            rb"shipping qualification failed \(private details suppressed; protected read "
            rb"phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", error)
        if code == 0:
            observed = "unexpected-success"
        elif diagnostic is None:
            observed = "unparsable-diagnostic"
        elif int(diagnostic.group(1)) != plan["expected_code"]:
            observed = "code-mismatch"
        else:
            observed = None
        if evidence is not None and observed is not None:
            # Never lose the observed numbers on any path that is not the planned outcome.
            evidence.setdefault("failed_slices", []).append({
                "name": plan["name"], "multi_file": True,
                "decision_code": int(diagnostic.group(1)) if diagnostic else None,
                "http_status": int(diagnostic.group(2)) if diagnostic else None,
                "native_exit": code, "expected_code": plan["expected_code"],
                "stage": "refusal-shape-" + observed, "native_reaped": True,
                "elapsed_seconds": round(time.monotonic() - started, 3)})
        if observed is not None:
            raise SafeFailure("refusal-shape-" + observed)
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require(self.inspect_object(item["key"], entry["version_id"]) ==
                    {"etag": entry["etag"], "version_id": entry["version_id"]},
                    "post-native-identity-mismatch")
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require("put_etag" in entry and "put_version_id" in entry,
                    "cleanup-generation-mismatch")
            self.delete_owned(item["key"], self.canonical_version(entry["put_version_id"]),
                              entry["put_etag"])
        return {"name": plan["name"], "multi_file": True, "outcome": "refused",
                "assertion": "refusal required at the harness first guarded call; no value "
                             "comparison performed",
                "decision_code": int(diagnostic.group(1)),
                "http_status": int(diagnostic.group(2)), "native_exit": code,
                "native_reaped": True, "dropped_column": plan["dropped_column"],
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "native_timeout_seconds": self.args.native_timeout,
                "manifest_placeholder_expected_sha256": placeholder,
                "files": [{"local_fixture": item["local_fixture"],
                           "canonical_coordinate": item["coordinate"], "rows": item["rows"],
                           "columns": sorted(item["columns"]),
                           "bytes": self.state["objects"][item["key"]]["bytes"],
                           "sha256": self.state["objects"][item["key"]]["sha256"],
                           "etag": self.state["objects"][item["key"]]["etag"],
                           "version_id": self.state["objects"][item["key"]]["version_id"],
                           "cleaned_individually": True} for item in plan["files"]]}

    def contradiction_slice(self, types_by_file, plan, evidence):
        """Run the one cross-file type-contradiction shape and require the harness to refuse it.

        Two files, the same nine column names, one column (payload4) carrying two different
        physical types. This shape asserts a REFUSAL and compares no value. The decision code is
        recorded, not asserted: the plan states the code as per spec, to be confirmed at the round
        gate, because the closed codes are per call-site and offline work cannot attribute a
        mechanism. An unexpected success or an unparsable diagnostic stops the run before any
        delete, and whatever numbers were observed are kept on every path that is not the plan.
        The guarded cloud path and the manifest grammar are unchanged.
        """
        require_contradiction_types(types_by_file, plan)
        entries = []
        for item in plan["files"]:
            fixture = self.directory / item["local_fixture"]
            entry = self.upload(item["key"], fixture)
            entries.append({"local": str(fixture), "coordinate": item["coordinate"],
                            "size": entry["bytes"], "sha256": entry["sha256"],
                            "validator": entry["etag"]})
        placeholder = hashlib.sha256(b"contradiction shape: value not compared").hexdigest()
        manifest = self.directory / "manifest-contradiction.json"
        atomic_json(manifest, manifest_document(entries, [{"trial": plan["trial"],
                                                          "expected": placeholder}]))
        manifest.chmod(0o600)
        self.journal("manifest-written-private")
        command = [str(self.args.binary), "s3", str(self.args.httpfs), str(self.args.aws),
                   "s3://" + self.state["bucket"] + "/data", self.args.profile,
                   self.args.region, str(manifest)]
        self.journal("manifest-harness-start")
        started = time.monotonic()
        code, _stdout, error = self.harness_code(command, self.args.native_timeout)
        self.journal("manifest-harness-reaped")
        diagnostic = re.fullmatch(
            rb"shipping qualification failed \(private details suppressed; protected read "
            rb"phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", error)
        if code == 0:
            observed = "unexpected-success"
        elif diagnostic is None:
            observed = "unparsable-diagnostic"
        else:
            # Any refusal is the planned outcome here; the number is recorded, never asserted.
            observed = None
        if observed is not None:
            # Never lose the observed numbers on any path that is not the planned outcome.
            if evidence is not None:
                evidence.setdefault("failed_slices", []).append({
                    "name": plan["name"], "multi_file": True,
                    "decision_code": int(diagnostic.group(1)) if diagnostic else None,
                    "http_status": int(diagnostic.group(2)) if diagnostic else None,
                    "native_exit": code, "expected_code": plan["expected_code"],
                    "expected_code_status": plan["expected_code_status"],
                    "stage": "contradiction-shape-" + observed, "native_reaped": True,
                    "elapsed_seconds": round(time.monotonic() - started, 3)})
            raise SafeFailure("contradiction-shape-" + observed)
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require(self.inspect_object(item["key"], entry["version_id"]) ==
                    {"etag": entry["etag"], "version_id": entry["version_id"]},
                    "post-native-identity-mismatch")
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require("put_etag" in entry and "put_version_id" in entry,
                    "cleanup-generation-mismatch")
            self.delete_owned(item["key"], self.canonical_version(entry["put_version_id"]),
                              entry["put_etag"])
        return {"name": plan["name"], "multi_file": True, "outcome": "refused",
                "assertion": "refusal required; no value comparison performed and no decision "
                             "code asserted (per spec, to be confirmed at the round gate)",
                "expected_code": plan["expected_code"],
                "expected_code_status": plan["expected_code_status"],
                "decision_code": int(diagnostic.group(1)),
                "http_status": int(diagnostic.group(2)), "native_exit": code,
                "native_reaped": True, "contradicted_column": plan["contradicted_column"],
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "native_timeout_seconds": self.args.native_timeout,
                "manifest_placeholder_expected_sha256": placeholder,
                "files": [{"local_fixture": item["local_fixture"],
                           "canonical_coordinate": item["coordinate"], "rows": item["rows"],
                           "columns": sorted(item["columns"]),
                           "physical_types": {column: item["physical"][column]
                                              for column in sorted(item["columns"])},
                           "offline_fixture_sha256": item["offline_fixture_sha256"],
                           "bytes": self.state["objects"][item["key"]]["bytes"],
                           "sha256": self.state["objects"][item["key"]]["sha256"],
                           "etag": self.state["objects"][item["key"]]["etag"],
                           "version_id": self.state["objects"][item["key"]]["version_id"],
                           "cleaned_individually": True} for item in plan["files"]]}

    def evolution_slice(self, columns_by_file, plan, evidence):
        """Run the one cross-file evolution shape: two files, the second adds one column.

        Ground truth is computed independently in Python from the local fixtures, the same way the
        three-file aggregate does it, because the harness local oracle seam admits only ONE
        descriptor per call. The expectation is the pre-authored typed-NULL union and it is
        recomputed here from the fixture read-backs before anything is uploaded: if the two do not
        agree, the run stops before any cloud write. A disagreement between a recorded identity and
        the reported result stops the slice before any delete.
        """
        expectation = {"trial": plan["trial"],
                       "expected": evolution_expectation(columns_by_file, plan)}
        entries = []
        for item in plan["files"]:
            fixture = self.directory / item["local_fixture"]
            entry = self.upload(item["key"], fixture)
            entries.append({"local": str(fixture), "coordinate": item["coordinate"],
                            "size": entry["bytes"], "sha256": entry["sha256"],
                            "validator": entry["etag"]})
        manifest = self.directory / "manifest-evolution.json"
        atomic_json(manifest, manifest_document(entries, [expectation]))
        manifest.chmod(0o600)
        self.journal("manifest-written-private")
        command = [str(self.args.binary), "s3", str(self.args.httpfs), str(self.args.aws),
                   "s3://" + self.state["bucket"] + "/data", self.args.profile, self.args.region,
                   str(manifest)]
        self.journal("manifest-harness-start")
        started = time.monotonic()
        code, stdout, error = self.harness_code(command, self.args.native_timeout)
        self.journal("manifest-harness-reaped")
        if code != 0:
            # Fixed numeric decision codes only; no manifest content or provider text is kept.
            diagnostic = re.fullmatch(
                rb"shipping qualification failed \(private details suppressed; protected read "
                rb"phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", error)
            if evidence is not None:
                evidence.setdefault("failed_slices", []).append({
                    "name": plan["name"], "multi_file": True,
                    "decision_code": int(diagnostic.group(1)) if diagnostic else None,
                    "http_status": int(diagnostic.group(2)) if diagnostic else None,
                    "native_exit": code, "stage": "manifest-harness-failed",
                    "native_reaped": True, "elapsed_seconds": round(time.monotonic() - started, 3)})
            raise SafeFailure("manifest-harness-failed")
        report = safe_manifest_native(stdout)
        verify_reported(entries, report, [expectation])
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require(self.inspect_object(item["key"], entry["version_id"]) ==
                    {"etag": entry["etag"], "version_id": entry["version_id"]},
                    "post-native-identity-mismatch")
        for item in plan["files"]:
            entry = self.state["objects"][item["key"]]
            require("put_etag" in entry and "put_version_id" in entry,
                    "cleanup-generation-mismatch")
            self.delete_owned(item["key"], self.canonical_version(entry["put_version_id"]),
                              entry["put_etag"])
        return {"name": plan["name"], "manifest_mode": True, "multi_file": True,
                "outcome": "typed-null-union", "added_column": plan["added_column"],
                "ground_truth": "computed independently in Python from the local fixtures; the "
                                "harness local oracle admits only one descriptor per call; the "
                                "pre-authored value was authored before the round and recomputed "
                                "here from the fixture read-backs",
                "preauthored_row": plan["expected_row"],
                "files": [{"local_fixture": item["local_fixture"],
                           "canonical_coordinate": item["coordinate"], "rows": item["rows"],
                           "columns": sorted(item["columns"]),
                           "physical_types": {column: item["physical"][column]
                                              for column in sorted(item["columns"])},
                           "offline_fixture_sha256": item["offline_fixture_sha256"],
                           "row_group_rows": plan["row_group_rows"],
                           "compression": plan["compression"],
                           "dictionary": plan["dictionary"], "statistics": plan["statistics"],
                           "bytes": self.state["objects"][item["key"]]["bytes"],
                           "sha256": self.state["objects"][item["key"]]["sha256"],
                           "etag": self.state["objects"][item["key"]]["etag"],
                           "version_id": self.state["objects"][item["key"]]["version_id"],
                           "cleaned_individually": True} for item in plan["files"]],
                "trial": {"trial": plan["trial"],
                          "expected": "sum(id) plus eight sum(length(payloadN)) over the union of "
                                      "the two files, where the column only one file carries is a "
                                      "typed NULL on the other file's rows and contributes nothing",
                          "expected_sha256": hashlib.sha256(
                              bytes.fromhex(expectation["expected"])).hexdigest(),
                          "reported": report["trials"][0]},
                "phases": report["phases"], "denials": report["denials"],
                "native_timeout_seconds": self.args.native_timeout,
                "elapsed_seconds": round(time.monotonic() - started, 3)}

    def cleanup(self):
        if not self.state["creation_intent"]:
            return {"not_created": True}
        require(self.state.get("absence_before_creation") is True and
                self.state.get("account") == ACCOUNT and
                self.state.get("profile") == self.args.profile and
                self.state.get("region") == self.args.region, "cleanup-creation-fence")
        identity = self.ok("sts", "get-caller-identity")
        require(identity.get("Account") == ACCOUNT and identity.get("Arn") == ARN,
                "cleanup-identity-mismatch")
        # An ambiguous create is not ownership. Never delete an unmarked bucket.
        error, _ = self.call("s3api", "head-bucket", **self.bucket_args())
        if error in ("404", "NotFound", "NoSuchBucket"):
            self.journal("cleanup-absence-proved")
            return {"bucket_absent": True, "bucket_absence_code": "404"}
        require(error is None, "cleanup-bucket-access-failed")
        self.owner()
        versioning = self.ok("s3api", "get-bucket-versioning", **self.bucket_args())
        require(self.state.get("versioning_at_creation") == "unversioned" and
                not versioning.get("Status"), "cleanup-versioning-changed")
        objects, versions, markers = self.inventory()
        require(not markers, "unexpected-delete-marker")
        self.state["version_identity_normalization"] = {
            "rule": "absent-json-null-and-string-null-are-unversioned-only",
            "versioning_unchanged_unversioned": True}
        self.journal("unversioned-validator-normalization-verified")
        # Validate the ENTIRE inventory before any deletion. Null version IDs are
        # unversioned generations, guarded by IfMatch; real IDs are deleted exactly.
        candidates = []
        for value in versions or objects:
            key = value.get("Key")
            version = self.version(value.get("VersionId"))
            head = self.inspect_object(key, version)
            entry = self.state["objects"][key]
            require(value.get("ETag") == head["etag"] and
                    value.get("Size") == entry["bytes"] and
                    self.canonical_version(version) == self.canonical_version(head["version_id"]),
                    "inventory-head-mismatch")
            require("put_etag" in entry and "put_version_id" in entry and
                    head["etag"] == entry["put_etag"] and
                    self.canonical_version(head["version_id"]) ==
                    self.canonical_version(entry["put_version_id"]),
                    "cleanup-generation-mismatch")
            # Conditional DELETE with VersionId="null" is not implemented by
            # S3. In an unchanged never-versioned bucket, omit VersionId and
            # require IfMatch; real version IDs remain exact and never collapse.
            candidates.append((key, self.canonical_version(version), head["etag"]))
        require(len(candidates) == len({key for key, _, _ in candidates}),
                "unexpected-multiple-generations")
        require({value["Key"] for value in objects} == {key for key, _, _ in candidates},
                "unexpected-live-object")
        for value in objects:
            entry = self.state["objects"][value["Key"]]
            require(value.get("Size") == entry["bytes"] and
                    value.get("ETag") == entry["put_etag"], "live-inventory-identity-mismatch")
        self.state["cleanup_candidates"] = [dict(key=k, version_id=v, etag=e)
                                             for k, v, e in candidates]
        self.journal("cleanup-inventory-validated")
        for key, version, etag in candidates:
            self.delete_owned(key, version, etag)
        self.owner()
        remaining = self.inventory()
        require(not any(remaining), "cleanup-inventory-not-empty")
        self.journal("all-inventories-empty")
        self.owner()
        self.ok("s3api", "delete-bucket", **self.bucket_args())
        self.journal("bucket-deleted")
        self.absence()
        result = {"bucket": self.state["bucket"], "owner_marker_matched": True,
                  "deleted_owned_generations": len(candidates),
                  "live_inventory_empty": True, "versions_inventory_empty": True,
                  "delete_markers_inventory_empty": True, "multipart_inventory_empty": True,
                  "bucket_absence_code": "404", "iam_changes": False,
                  "credential_creation": False, "existing_data_changes": False}
        self.state["cleanup"] = result
        self.journal("cleanup-absence-proved")
        atomic_json(self.directory / "cleanup.json", result)
        return result


def artifact_hashes(args):
    files = {"native/" + path.name: path for path in NATIVE.iterdir()
             if path.suffix in (".cpp", ".inc", ".hpp", ".h")}
    files.update({"native/run_s3_qualification.py": pathlib.Path(__file__).resolve(),
                  "grv_cloud_qualification": args.binary,
                  "libgrv_duckdb_guard.dylib": args.binary.parent / "libgrv_duckdb_guard.dylib",
                  "httpfs.duckdb_extension": args.httpfs,
                  "aws.duckdb_extension": args.aws})
    return {name: digest(path) for name, path in sorted(files.items())}


def run(args):
    import pyarrow as pa
    import pyarrow.parquet as pq
    env = selected_environment(args.profile)
    before = artifact_hashes(args)
    run_set = set(selected_slices(getattr(args, "only", [])))
    directory = pathlib.Path(tempfile.mkdtemp(prefix="grv-s3-shipping-", dir=EVIDENCE_DIR))
    directory.chmod(0o700)
    probe = Probe(directory, args, env)
    evidence = {"status": "incomplete", "qualification": "isolated-s3-shipping-scanner-slice",
                "identity": {"profile": args.profile, "region": args.region,
                             "account": ACCOUNT, "arn": ARN, "fallback": False},
                "artifact_sha256_before": before, "slices": [],
                "private_evidence_directory": "~/grv-evidence/" + directory.name,
                "limitations": [
                    "Not release qualification, a frozen release candidate or the full shipping matrix.",
                    "Recorded hashes identify attempted artifacts and sources, not source-to-binary build attestation or the current installed package.",
                    "Counters cover accepted protected-reader GET bodies and HEAD/GET attempts only; not headers, rejected bodies, TLS or total wire bytes.",
                    "Full preparation traffic and parent end-to-end operation traffic are not measured.",
                    "EXPLAIN counters do not publish or verify plan contents.",
                    "Four fixed single-file slices (the two settings pairs, the approved special-name coordinate and one empty fixture) plus one three-file aggregate slice omit typed/evolved and broad fault/cancellation/resource tests.",
                    "Multi-file ground truth is computed independently in Python from the local fixtures because the harness local oracle admits only one descriptor per call; the guarded cloud path is unchanged.",
                    "Cold versus warm on this path means two sequential guarded executions inside one retained connection-session: no durable cache exists across processes, so a second run is cold again.",
                    "The cross-file type-contradiction shape asserts a REFUSAL only and asserts no decision code: the closed codes are per call-site, not per mechanism, so the mechanism cannot be attributed offline and the expected code is per spec, to be confirmed at the round gate.",
                    "The cross-file evolution shape's typed-NULL union expectation was pre-authored in Python from local fixture read-backs before any live round; ground truth stays Python-computed because the harness local oracle admits one descriptor per call, and the offline indicator for this direction came from DuckDB 1.4.2, not the pinned build.",
                    "Fixture SHA-256s recorded for the two new cross-file shapes come from the offline build and are recorded, not gated: byte-identical reproducibility is not an acceptance item, and the same content written through another code path hashes differently.",
                    "The cold/warm shape records per-trial counters but asserts no direction: reduced warm fetching and full warm re-fetch are both accepted outcomes.",
                    "No universal savings claim; no-statistics savings are not evidence of statistics pruning.",
                    "Native verification trusts an exact matching ETag; the supplied local SHA256 is used for the mismatch fallback, not independently rehashed remotely on this path.",
                    "Outer process timeout is not qualification of native absolute in-flight deadlines.",
                    "Independent security review and remaining package/release gates remain open."],
                "capability_advertised": False, "release_published": False}
    if EVIDENCE.exists():
        previous = json.loads(EVIDENCE.read_text())
        # Preserve sanitized prior attempt and cleanup recovery without mutating
        # its original 0400 failure archive.
        atomic_json(directory / "prior-sanitized-checkpoint.json", previous)
        (directory / "prior-sanitized-checkpoint.json").chmod(0o400)
        evidence["prior_attempt"] = {
            "status": previous.get("status"), "failure": previous.get("failure"),
            "cleanup": previous.get("cleanup"),
            "cleanup_recovery": previous.get("cleanup_recovery"),
            "initial_failure_archive": previous.get("initial_failure_archive") or
                                       previous.get("prior_attempt", {}).get("initial_failure_archive"),
            "archive": "~/grv-evidence/" + directory.name + "/prior-sanitized-checkpoint.json",
            "archive_sha256": digest(directory / "prior-sanitized-checkpoint.json")}
    failure = None
    try:
        probe.create()
        data = {"id": pa.array(range(args.rows), type=pa.int64())}
        for column in range(8):
            data[f"payload{column}"] = pa.array([
                hashlib.sha256(f"{column}-{row}".encode()).hexdigest() * 4
                for row in range(args.rows)])
        table = pa.table(data)
        null_data = {"id": data["id"]}
        for column in range(8):
            null_data[f"payload{column}"] = pa.array([None] * args.rows, type=pa.string())
        null_table = pa.table(null_data)
        require(sorted(data) == sorted(SCHEMA), "fixture-schema-drift")
        for shape in SHAPES:
            plan = shape_plan(shape, args.rows, probe.state["bucket"])
            name, compression, dictionary, statistics = (plan["name"], plan["compression"],
                                                         plan["dictionary"], plan["statistics"])
            if name not in run_set:
                continue
            probe.journal("fixture-generation-" + name)
            fixture = directory / plan["local_fixture"]
            pq.write_table(table.slice(0, 0) if plan["rows"] == 0 else
                           (null_table if name in NULL_PAYLOAD_SHAPES else table), fixture,
                           row_group_size=2048, compression=compression,
                           use_dictionary=dictionary, write_statistics=statistics)
            fixture.chmod(0o600)
            if name in NULL_PAYLOAD_SHAPES:
                # Gate the fixture on disk before uploading it, not just the in-memory table.
                readback = pq.read_table(fixture)
                require_null_payload({column: readback[column].to_pylist()
                                      for column in SCHEMA})
            key = plan["key"]
            entry = probe.upload(key, fixture)
            # This checkout has no S3ParseValidator: VerifiedS3State::Verify in
            # verified_s3.inc directly compares the exact quoted HTTP ETag.
            validator = entry["etag"]
            command = [str(args.binary), "s3", str(args.httpfs), str(args.aws),
                       "s3://" + probe.state["bucket"] + "/data", args.profile, args.region,
                       str(fixture), plan["coordinate"],
                       str(entry["bytes"]), entry["sha256"], validator]
            probe.journal("native-start-" + name)
            started = time.monotonic()
            try:
                code, stdout, stderr = bounded_process(command, env, args.native_timeout)
            except subprocess.TimeoutExpired:
                probe.journal("native-timeout-killed-reaped-" + name)
                raise SafeFailure("native-timeout") from None
            probe.journal("native-reaped-" + name)
            if code != 0:
                diagnostic = re.fullmatch(rb"shipping qualification failed \(private details suppressed; protected read phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", stderr)
                evidence.setdefault("failed_slices", []).append({
                    "protected_read_phase": int(diagnostic.group(1)) if diagnostic else None,
                    "http_status": int(diagnostic.group(2)) if diagnostic else None,
                    "name": name, "canonical_coordinate": plan["coordinate"], "native_exit": code,
                    "stage": "native-qualification-failed", "native_reaped": True,
                    "fixture_bytes": entry["bytes"], "fixture_sha256": entry["sha256"],
                    "compression": compression, "dictionary": dictionary,
                    "statistics": statistics, "etag": entry["etag"],
                    "version_id": entry["version_id"],
                    "elapsed_seconds": round(time.monotonic() - started, 3)})
                failure = "native-qualification-failed"
                continue
            result = safe_native(stdout)
            # Recheck the same identity after native consumption.
            require(probe.inspect_object(key, entry["version_id"]) ==
                    {"etag": entry["etag"], "version_id": entry["version_id"]},
                    "post-native-identity-mismatch")
            item = {"name": name, "local_fixture": plan["local_fixture"],
                    "canonical_coordinate": plan["coordinate"],
                    "fixture": {"rows": plan["rows"], "row_group_rows": 2048,
                     "columns": "id BIGINT plus eight VARCHAR payloads", "bytes": entry["bytes"],
                     "sha256": entry["sha256"], "compression": compression,
                     "dictionary": dictionary, "statistics": statistics},
                    "remote_identity": {"etag": entry["etag"], "version_id": entry["version_id"],
                                        "validator_kind": "exact-quoted-head-etag"},
                    "native_timeout_seconds": args.native_timeout,
                    "elapsed_seconds": round(time.monotonic() - started, 3), "result": result}
            atomic_json(directory / (name + "-results.json"), item)
            evidence["slices"].append(item)
        plan = multi_shape_plan(args.rows, probe.state["bucket"])
        columns_by_file = []
        offset = 0
        for item in plan["files"]:
            probe.journal("fixture-generation-" + item["name"])
            fixture = directory / item["local_fixture"]
            pq.write_table(table.slice(offset, item["rows"]), fixture, row_group_size=2048,
                           compression=plan["compression"], use_dictionary=plan["dictionary"],
                           write_statistics=plan["statistics"])
            fixture.chmod(0o600)
            offset += item["rows"]
            read = pq.read_table(fixture)
            columns_by_file.append({name: read[name].to_pylist() for name in SCHEMA})
        if MULTI_SHAPE_NAME in run_set:
            evidence["slices"].append(probe.multi_file_slice(columns_by_file, plan, evidence))
        plan = refusal_plan(args.rows, probe.state["bucket"])
        readbacks = []
        offset = 0
        for item in plan["files"]:
            probe.journal("fixture-generation-" + item["name"])
            fixture = directory / item["local_fixture"]
            pq.write_table(table.slice(offset, item["rows"]).select(sorted(item["columns"])),
                           fixture, row_group_size=2048, compression=plan["compression"],
                           use_dictionary=plan["dictionary"],
                           write_statistics=plan["statistics"])
            fixture.chmod(0o600)
            offset += item["rows"]
            read = pq.read_table(fixture)
            readbacks.append({name: read[name].to_pylist() for name in sorted(item["columns"])})
        if REFUSAL_SHAPE_NAME in run_set:
            evidence["slices"].append(probe.refusal_slice(readbacks, plan, evidence))
        plan = cold_warm_plan(args.rows, probe.state["bucket"])
        cold_warm_columns = []
        for item in plan["files"]:
            probe.journal("fixture-generation-" + item["name"])
            fixture = directory / item["local_fixture"]
            # Standard generated data (real payload values, no nulls), one row group.
            pq.write_table(table.slice(0, item["rows"]), fixture,
                           row_group_size=plan["row_group_rows"], compression=plan["compression"],
                           use_dictionary=plan["dictionary"], write_statistics=plan["statistics"])
            fixture.chmod(0o600)
            read = pq.read_table(fixture)
            cold_warm_columns.append({name: read[name].to_pylist() for name in SCHEMA})
        if COLD_WARM_SHAPE_NAME in run_set:
            evidence["slices"].append(probe.cold_warm_slice(cold_warm_columns, plan, evidence))
        # The two cross-file shapes need per-file column sets and per-file physical types, so
        # their fixtures are written from the standard recipe column by column, in the planned
        # order, one row group per file. Both fixtures of a shape are read back locally and the
        # read-back is what the shape's gate checks before anything is uploaded.
        plan = contradiction_plan(args.rows, probe.state["bucket"])
        contradiction_types = []
        for item in plan["files"]:
            probe.journal("fixture-generation-" + item["name"])
            fixture = directory / item["local_fixture"]
            data = {column: recipe_array(column, item["physical"][column], item["offset"],
                                        item["rows"])
                    for column in item["columns"]}
            pq.write_table(pa.table(data), fixture, row_group_size=plan["row_group_rows"],
                           compression=plan["compression"], use_dictionary=plan["dictionary"],
                           write_statistics=plan["statistics"])
            fixture.chmod(0o600)
            read = pq.read_table(fixture)
            contradiction_types.append({column: str(read.schema.field(column).type)
                                        for column in read.schema.names})
        if CONTRADICTION_SHAPE_NAME in run_set:
            evidence["slices"].append(probe.contradiction_slice(contradiction_types, plan,
                                                                 evidence))
        plan = evolution_plan(args.rows, probe.state["bucket"])
        evolution_columns = []
        for item in plan["files"]:
            probe.journal("fixture-generation-" + item["name"])
            fixture = directory / item["local_fixture"]
            data = {}
            for column in item["columns"]:
                data[column] = recipe_array(column, item["physical"][column], item["offset"],
                                            item["rows"])
            pq.write_table(pa.table(data), fixture, row_group_size=plan["row_group_rows"],
                           compression=plan["compression"], use_dictionary=plan["dictionary"],
                           write_statistics=plan["statistics"])
            fixture.chmod(0o600)
            read = pq.read_table(fixture)
            evolution_columns.append({column: read[column].to_pylist()
                                      for column in read.schema.names})
        if EVOLUTION_SHAPE_NAME in run_set:
            evidence["slices"].append(probe.evolution_slice(evolution_columns, plan, evidence))
    except BaseException as exc:
        failure = exc.args[0] if isinstance(exc, SafeFailure) else "unknown-failure-suppressed"
    finally:
        # bounded_process has killed/reaped any child before cleanup can execute.
        try:
            evidence["cleanup"] = probe.cleanup()
        except BaseException as exc:
            evidence["cleanup"] = {"complete": False, "failure": exc.args[0]
                                   if isinstance(exc, SafeFailure) else "unknown-failure-suppressed"}
            failure = failure or "cleanup-incomplete"
        try:
            evidence["artifact_sha256_after"] = artifact_hashes(args)
            evidence["artifacts_unchanged"] = before == evidence["artifact_sha256_after"]
            if not evidence["artifacts_unchanged"]:
                failure = failure or "artifact-changed"
        except BaseException:
            failure = failure or "artifact-verification-failed"
        evidence["failure"] = failure
        evidence["status"] = "failed-closed" if failure else "qualification-slice-not-release-qualified"
        evidence["private_evidence_sha256"] = {path.name: digest(path) for path in directory.iterdir()
                                               if path.suffix == ".json"}
        atomic_json(directory / "summary.json", evidence)
        atomic_json(EVIDENCE, evidence)
    print(json.dumps({"status": evidence["status"], "completed_slices": len(evidence["slices"]),
                      "cleanup": evidence["cleanup"], "failure": failure,
                      "evidence": str(EVIDENCE)}))
    return 1 if failure else 0


class SafetyTests(unittest.TestCase):
    def test_null_typed_shape_is_planned_and_gated(self):
        shape = {entry[0]: entry for entry in SHAPES}["all-null-typed"]
        self.assertIn("all-null-typed", NULL_PAYLOAD_SHAPES)
        self.assertEqual(shape, ("all-null-typed", "NONE", False, True, None, None))
        # Identical settings to the first single-file shape, so the nulls are the only variable.
        self.assertEqual(shape[1:4], SHAPES[0][1:4])
        base = {"id": [1, 2], **{f"payload{column}": [None, None] for column in range(8)}}
        require_null_payload(base)
        for case, columns, stage in (
                ("non-null payload", {**base, "payload3": [None, "x"]},
                 "null-shape-payload-not-all-null"),
                ("null id", {**base, "id": [1, None]}, "null-shape-id-must-be-real"),
                ("schema drift", {key: value for key, value in base.items()
                                  if key != "payload7"}, "fixture-schema-drift")):
            with self.subTest(case=case):
                with self.assertRaisesRegex(SafeFailure, re.escape(stage)):
                    require_null_payload(columns)

    def test_slice_selector_cannot_widen_or_narrow_the_universe_silently(self):
        universe = ([entry[0] for entry in SHAPES] + [MULTI_SHAPE_NAME, REFUSAL_SHAPE_NAME,
                                                     COLD_WARM_SHAPE_NAME, CONTRADICTION_SHAPE_NAME,
                                                     EVOLUTION_SHAPE_NAME])
        self.assertEqual(selected_slices([]), universe)
        self.assertEqual(len(universe), 10)
        self.assertEqual(selected_slices([MULTI_SHAPE_NAME]), [MULTI_SHAPE_NAME])
        self.assertEqual(selected_slices(["empty", "all-null-typed"]),
                         ["empty", "all-null-typed"])  # fixed order, not call order
        # The new slice is selectable on its own, and the fixed order still wins over call order.
        self.assertEqual(selected_slices([COLD_WARM_SHAPE_NAME]), [COLD_WARM_SHAPE_NAME])
        # The two cross-file slices are selectable on their own, for a round gate that wants them
        # alone, and the fixed order still wins over call order.
        self.assertEqual(selected_slices([CONTRADICTION_SHAPE_NAME]), [CONTRADICTION_SHAPE_NAME])
        self.assertEqual(selected_slices([EVOLUTION_SHAPE_NAME]), [EVOLUTION_SHAPE_NAME])
        self.assertEqual(selected_slices([EVOLUTION_SHAPE_NAME, CONTRADICTION_SHAPE_NAME]),
                         [CONTRADICTION_SHAPE_NAME, EVOLUTION_SHAPE_NAME])
        self.assertEqual(selected_slices([COLD_WARM_SHAPE_NAME, MULTI_SHAPE_NAME,
                                         REFUSAL_SHAPE_NAME]),
                         [MULTI_SHAPE_NAME, REFUSAL_SHAPE_NAME, COLD_WARM_SHAPE_NAME])
        with self.assertRaisesRegex(SafeFailure, re.escape("unknown-slice-selector")):
            selected_slices(["nonexistent-slice"])

    def test_selector_guards_cannot_skip_cleanup(self):
        # The selector guards live inside the try; cleanup must stay in the finally so a
        # single-slice run still owns and removes exactly what it created.
        text = pathlib.Path(__file__).read_text()
        body = text[text.index("def run(args):"):]
        finally_at = body.index("    finally:")
        self.assertIn("probe.cleanup()", body[finally_at:finally_at + 200])
        for guard in ("if name not in run_set:", "if MULTI_SHAPE_NAME in run_set:",
                      "if REFUSAL_SHAPE_NAME in run_set:",
                      "if COLD_WARM_SHAPE_NAME in run_set:",
                      "if CONTRADICTION_SHAPE_NAME in run_set:",
                      "if EVOLUTION_SHAPE_NAME in run_set:"):
            self.assertLess(body.index(guard), finally_at)

    def test_evidence_root_is_the_durable_location(self):
        # ~/grv-* was consolidated into ~/grv-evidence/; /tmp is volatile and already lost
        # working files once, so evidence must not be written there or back to the home root.
        self.assertEqual(EVIDENCE_DIR, pathlib.Path.home() / "grv-evidence")
        text = pathlib.Path(__file__).read_text()
        self.assertIn("dir=EVIDENCE_DIR", text)
        self.assertIn('"private_evidence_directory": "~/grv-evidence/" + directory.name,', text)
        self.assertIn('"archive": "~/grv-evidence/" + directory.name +', text)
        needle = "dir=pathlib.Path.ho" + "me()" + ")"
        self.assertNotIn(needle, text)

    def test_atomic_permissions(self):
        with tempfile.TemporaryDirectory() as name:
            path = pathlib.Path(name) / "state.json"
            atomic_json(path, {"intent": True})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            atomic_json(path, {"intent": False})
            self.assertEqual(json.loads(path.read_text()), {"intent": False})
            self.assertEqual(len(list(path.parent.iterdir())), 1)

    def test_native_schema_does_not_accept_arbitrary_text(self):
        for raw in (b"secret material", b'{"provider":"protected_s3","token":"x"}', b"[]"):
            with self.assertRaises(SafeFailure):
                safe_native(raw)

    def test_child_timeout_reaped(self):
        import sys
        started = []
        real = subprocess.Popen
        def capture(*args, **kwargs):
            child = real(*args, **kwargs)
            started.append(child)
            return child
        with mock.patch.object(subprocess, "Popen", side_effect=capture):
            with self.assertRaises(subprocess.TimeoutExpired):
                bounded_process([sys.executable, "-c", "import time; time.sleep(30)"],
                                dict(os.environ), 0.05)
        self.assertEqual(started[0].returncode, -signal.SIGKILL)

    def test_selected_profile_no_fallback(self):
        with tempfile.TemporaryDirectory() as name:
            home = pathlib.Path(name)
            (home / ".aws").mkdir()
            path = home / ".aws/credentials"
            # Deliberately dummy noncredentials for offline testing.
            path.write_text("[chosen]\naws_access_key_id=dummy\naws_secret_access_key=dummy\n")
            with mock.patch.object(pathlib.Path, "home", return_value=home), \
                    mock.patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "ambient",
                                                  "AWS_PROFILE": "default", "HTTP_PROXY": "ambient"}):
                env = selected_environment("chosen")
                self.assertNotIn("AWS_ACCESS_KEY_ID", env)
                self.assertNotIn("AWS_PROFILE", env)
                self.assertNotIn("HTTP_PROXY", env)
                with self.assertRaises(SafeFailure):
                    selected_environment("missing")
                path.write_text("[chosen]\nrole_arn=dummy\nsource_profile=default\n")
                with self.assertRaises(SafeFailure):
                    selected_environment("chosen")

    def probe(self, directory):
        args = argparse.Namespace(profile="dummy", region="eu-west-1",
                                  binary=pathlib.Path("/dummy/grv_cloud_qualification"),
                                  httpfs=pathlib.Path("/dummy/httpfs.duckdb_extension"),
                                  aws=pathlib.Path("/dummy/aws.duckdb_extension"),
                                  native_timeout=1)
        with mock.patch.object(shutil, "which", return_value="/dummy/aws"):
            probe = Probe(pathlib.Path(directory), args, {})
        probe.state.update(creation_intent=True, absence_before_creation=True,
                           versioning_at_creation="unversioned")
        return probe

    @staticmethod
    def cleanup_ok(_service, operation, **_parameters):
        return {"Account": ACCOUNT, "Arn": ARN} if operation == "get-caller-identity" else {}

    def test_no_cleanup_without_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            probe.call = mock.Mock(return_value=(None, {}))
            probe.ok = mock.Mock(side_effect=lambda service, operation, **kwargs:
                                 {"TagSet": []} if operation == "get-bucket-tagging" else
                                 self.cleanup_ok(service, operation, **kwargs))
            with self.assertRaisesRegex(SafeFailure, "bucket-owner-marker-mismatch"):
                probe.cleanup()
            self.assertEqual([call.args[1] for call in probe.ok.call_args_list],
                             ["get-caller-identity", "get-bucket-tagging"])

    def test_absence_must_be_404(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            probe.call = mock.Mock(return_value=("unknown", None))
            with self.assertRaises(SafeFailure):
                probe.absence()

    def test_exact_generation_cleanup(self):
        for version in (None, "null", "owned-generation"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                probe = self.probe(directory)
                etag = '"' + "a" * 32 + '"'
                probe.state["objects"]["data/f.parquet"] = {"put_etag": etag,
                                                           "put_version_id": version, "bytes": 12}
                probe.call = mock.Mock(return_value=(None, {}))
                probe.owner = mock.Mock()
                item = {"Key": "data/f.parquet", "ETag": etag, "Size": 12}
                if version is not None:
                    item["VersionId"] = version
                probe.inventory = mock.Mock(side_effect=[([item], [], []), ([], [], [])])
                probe.inspect_object = mock.Mock(return_value={
                    "etag": etag, "version_id": Probe.canonical_version(version)})
                probe.ok = mock.Mock(side_effect=self.cleanup_ok)
                probe.absence = mock.Mock()
                result = probe.cleanup()
                delete = next(call for call in probe.ok.call_args_list
                              if call.args[1] == "delete-object")
                self.assertEqual(delete.args, ("s3api", "delete-object"))
                self.assertEqual(delete.kwargs["IfMatch"], etag)
                self.assertEqual(delete.kwargs.get("VersionId"), Probe.canonical_version(version))
                self.assertTrue(result["versions_inventory_empty"])

    def test_version_normalization_exact(self):
        self.assertIsNone(Probe.canonical_version(None))
        self.assertIsNone(Probe.canonical_version("null"))
        self.assertEqual(Probe.canonical_version("real-version"), "real-version")
        with self.assertRaises(SafeFailure):
            Probe.canonical_version("")

    def test_cleanup_versioning_change_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            probe.call = mock.Mock(return_value=(None, {}))
            probe.owner = mock.Mock()
            probe.ok = mock.Mock(side_effect=lambda service, operation, **kwargs:
                                 {"Status": "Enabled"} if operation == "get-bucket-versioning" else
                                 self.cleanup_ok(service, operation, **kwargs))
            with self.assertRaisesRegex(SafeFailure, "cleanup-versioning-changed"):
                probe.cleanup()
            self.assertFalse(any(call.args[1] == "delete-object" for call in probe.ok.call_args_list))

    def test_safe_native_reconstructs_only_known_fields(self):
        counters = {"body_bytes": 10, "head": 1, "get": 1, "unknown": "private"}
        payload = {"provider": "protected_s3", "shipping_scanner_source": True,
                   "unknown": "private", "phases": {"view_binding": counters},
                   "trials": [{"trial": name, "correct": True, "query": counters,
                               "explain": counters} for name in TRIALS],
                   "denials": {name: True for name in ("direct", "unlisted", "nested_glob", "secrets")},
                   "qualifying_savings": {"projection": 0.5, "selective_wide": 0.1}}
        safe = safe_native(json.dumps(payload).encode())
        self.assertNotIn("private", json.dumps(safe))
        payload["denials"]["direct"] = False
        with self.assertRaises(SafeFailure):
            safe_native(json.dumps(payload).encode())

    def test_generation_mismatch_stops_before_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            etag = '"' + "a" * 32 + '"'
            probe.state["objects"]["data/f"] = {"put_etag": etag, "put_version_id": "original", "bytes": 12}
            probe.call = mock.Mock(return_value=(None, {}))
            probe.owner = mock.Mock()
            probe.inventory = mock.Mock(return_value=([], [{"Key": "data/f", "ETag": etag,
                                                           "VersionId": "replacement", "Size": 12}], []))
            probe.inspect_object = mock.Mock(return_value={"etag": etag, "version_id": "replacement"})
            probe.ok = mock.Mock(side_effect=self.cleanup_ok)
            with self.assertRaisesRegex(SafeFailure, "cleanup-generation-mismatch"):
                probe.cleanup()
            self.assertFalse(any(call.args[1] == "delete-object" for call in probe.ok.call_args_list))

    @staticmethod
    def shape(name):
        return next(value for value in SHAPES if value[0] == name)

    def test_special_coordinate_accepted_and_canonicalised(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            plan = shape_plan(self.shape("special-coordinate"), 8192, probe.state["bucket"])
            key = plan["key"]
            decoded = s3_key_canonical(plan["coordinate"])
            for character in (b" ", b"+", b"'", b"-"):
                self.assertEqual(decoded.count(character), 1)
            self.assertEqual(decoded.count(b"\xc3\xa9"), 1)
            self.assertEqual(key, "data/shipping q+a-bé'c.parquet")
            self.assertEqual(decoded, key.encode())
            self.assertEqual(plan["coordinate"],
                             "s3://" + probe.state["bucket"] +
                             "/data/shipping%20q+a-b%C3%A9'c.parquet")
            # The spelling a live run refused is refused by this rule too: a literal space,
            # a literal non-ASCII letter and an over-encoded `-` are not canonical. Refused
            # before any remote read, so no live slice may claim a read for this spelling.
            with self.assertRaisesRegex(SafeFailure, "coordinate-not-canonical"):
                s3_key_canonical("s3://" + probe.state["bucket"] + "/" + NONCANONICAL_COORDINATE)
            fixture = pathlib.Path(directory) / plan["local_fixture"]
            fixture.write_bytes(b"offline special-coordinate fixture")
            etag = '"' + "a" * 32 + '"'
            seen = []

            def ok(_service, operation, **parameters):
                seen.append((operation, parameters.get("Key")))
                if operation == "get-bucket-tagging":
                    return {"TagSet": [{"Key": MARKER, "Value": probe.state["owner_marker"]}]}
                if operation == "put-object":
                    return {"ETag": etag}
                if operation == "head-object":
                    return {"ETag": etag,
                            "Metadata": {"grv-owner": probe.state["owner_marker"],
                                         "grv-sha256": digest(fixture)},
                            "ContentLength": fixture.stat().st_size}
                return {}

            probe.ok = mock.Mock(side_effect=ok)
            entry = probe.upload(key, fixture)
            self.assertEqual([operation for operation, _ in seen],
                             ["get-bucket-tagging", "put-object", "head-object"])
            self.assertEqual([value for _, value in seen], [None, key, key])
            self.assertEqual(entry["sha256"], digest(fixture))
            self.assertEqual(Probe.canonical_version(entry["version_id"]), None)
            self.assertEqual(probe.state["objects"][key]["sha256"], digest(fixture))
            self.assertEqual(probe.state["objects"][key]["upload_intent"], True)

    def test_unrecorded_coordinate_still_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            with self.assertRaisesRegex(SafeFailure, "unexpected-object-key"):
                probe.inspect_object("data/unrecorded.parquet", None)
            probe.call = mock.Mock(return_value=(None, {}))
            probe.owner = mock.Mock()
            probe.inventory = mock.Mock(return_value=([{"Key": "data/unrecorded.parquet"}], [], []))
            probe.ok = mock.Mock(side_effect=self.cleanup_ok)
            with self.assertRaisesRegex(SafeFailure, "unexpected-object-key"):
                probe.cleanup()
            self.assertFalse(any(call.args[1] == "delete-object" for call in probe.ok.call_args_list))

    def test_empty_shape_keeps_schema_and_zero_row_expectation(self):
        reference = shape_plan(self.shape("uncompressed-statistics"), 65536, "bucket")
        empty = shape_plan(self.shape("empty"), 65536, "bucket")
        self.assertEqual(empty["rows"], 0)
        self.assertEqual(empty["columns"], SCHEMA)
        self.assertEqual(empty["columns"], reference["columns"])
        self.assertEqual((empty["compression"], empty["dictionary"], empty["statistics"]),
                         (reference["compression"], reference["dictionary"],
                          reference["statistics"]))
        self.assertEqual(empty["local_fixture"], "empty.parquet")
        self.assertEqual(empty["coordinate"], "s3://bucket/data/empty.parquet")
        self.assertEqual(shape_plan(self.shape("special-coordinate"), 65536, "bucket")["columns"],
                         SCHEMA)

    def test_generation_mismatch_stops_before_delete_for_new_shapes(self):
        for name in ("special-coordinate", "empty"):
            with self.subTest(shape=name), tempfile.TemporaryDirectory() as directory:
                probe = self.probe(directory)
                plan = shape_plan(self.shape(name), 8192, probe.state["bucket"])
                etag = '"' + "a" * 32 + '"'
                probe.state["objects"][plan["key"]] = {"put_etag": etag, "put_version_id": "original",
                                                       "bytes": 12}
                probe.call = mock.Mock(return_value=(None, {}))
                probe.owner = mock.Mock()
                probe.inventory = mock.Mock(return_value=([], [{"Key": plan["key"], "ETag": etag,
                                                                "VersionId": "replacement",
                                                                "Size": 12}], []))
                probe.inspect_object = mock.Mock(return_value={"etag": etag,
                                                               "version_id": "replacement"})
                probe.ok = mock.Mock(side_effect=self.cleanup_ok)
                with self.assertRaisesRegex(SafeFailure, "cleanup-generation-mismatch"):
                    probe.cleanup()
                self.assertFalse(any(call.args[1] == "delete-object"
                                     for call in probe.ok.call_args_list))

    def test_manifest_field_closure_and_unknown_field_refusal(self):
        entry = {"local": "/tmp/multi-0.parquet", "coordinate": "s3://bucket/data/multi-0.parquet",
                 "size": 12, "sha256": "a" * 64, "validator": '"' + "d" * 32 + '"'}
        trials = [{"trial": "wide", "expected": encode_result([1, 2, 3, 4, 5, 6, 7, 8, 9])}]
        document = manifest_document([entry], trials)
        self.assertEqual(frozenset(document), MANIFEST_DOCUMENT_FIELDS)
        self.assertEqual(frozenset(document["files"][0]), MANIFEST_FILE_FIELDS)
        self.assertEqual(frozenset(document["trials"][0]), MANIFEST_TRIAL_FIELDS)
        accepted = manifest_document([entry], trials + [{"trial": "nonidentity_cast",
                                                         "expected": encode_result([1])}])
        self.assertEqual([trial["trial"] for trial in accepted["trials"]],
                         ["wide", "nonidentity_cast"])
        for bad in (dict(entry, extra=1),
                    {k: v for k, v in entry.items() if k != "sha256"},
                    dict(entry, size="12"), dict(entry, sha256="A" * 64),
                    dict(entry, validator=""), dict(entry, local="relative/path")):
            with self.subTest(fields=sorted(bad)), self.assertRaises(SafeFailure):
                manifest_document([bad], trials)
        for bad in ([{"trial": "wide", "expected": encode_result([1]), "extra": 1}],
                    [{"trial": "wide"}],
                    [{"trial": "nonidentity_cast2", "expected": encode_result([1])}],
                    [{"trial": "wide", "expected": "zz"}]):
            with self.assertRaises(SafeFailure):
                manifest_document([entry], bad)

    def test_manifest_duplicate_coordinate_refused(self):
        trials = [{"trial": "wide", "expected": encode_result([3])}]
        first = {"local": "/tmp/a.parquet", "coordinate": "s3://bucket/data/a.parquet",
                 "size": 12, "sha256": "a" * 64, "validator": '"a"'}
        second = dict(first, local="/tmp/b.parquet", coordinate="s3://bucket/data/b.parquet",
                      size=13, sha256="b" * 64, validator='"b"')
        self.assertEqual(len(manifest_document([first, second], trials)["files"]), 2)
        with self.assertRaisesRegex(SafeFailure, "manifest-duplicate-coordinate"):
            manifest_document([first, dict(first, size=14, sha256="c" * 64, validator='"c"')], trials)
        with self.assertRaisesRegex(SafeFailure, "manifest-duplicate-identity"):
            manifest_document([first, dict(first, local="/tmp/c.parquet",
                                           coordinate="s3://bucket/data/c.parquet")], trials)

    def test_manifest_empty_lists_refused(self):
        entry = {"local": "/tmp/a.parquet", "coordinate": "s3://bucket/data/a.parquet",
                 "size": 12, "sha256": "a" * 64, "validator": '"a"'}
        trials = [{"trial": "wide", "expected": encode_result([1, 2])}]
        with self.assertRaisesRegex(SafeFailure, "manifest-files-empty"):
            manifest_document([], trials)
        with self.assertRaisesRegex(SafeFailure, "manifest-trials-empty"):
            manifest_document([entry], [])

    def test_refusal_shape_requires_the_exact_refusal(self):
        def etag_for(key):
            return '"' + hashlib.sha256(key.encode()).hexdigest()[:32] + '"'
        full = {"id": [0], **{"payload%d" % column: ["x" * 3] for column in range(8)}}
        dropped = {key: value for key, value in full.items() if key != "payload7"}
        planned = (b"shipping qualification failed (private details suppressed; protected "
                   b"read phase 212, HTTP status 206)\n")
        mismatched = (b"shipping qualification failed (private details suppressed; protected "
                      b"read phase 211, HTTP status 0)\n")
        for case, code, stdout, error, stage, recorded in (
                ("refusal-as-planned", 1, b"", planned, None, None),
                ("other-code", 1, b"", mismatched, "refusal-shape-code-mismatch", 211),
                ("unexpected-success", 0, b"{}", b"", "refusal-shape-unexpected-success", None),
                ("unparsable", 1, b"", b"some other diagnostic\n",
                 "refusal-shape-unparsable-diagnostic", None)):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                probe = self.probe(directory)
                plan = refusal_plan(8192, probe.state["bucket"])
                self.assertEqual(len(plan["files"]), 2)
                self.assertEqual(plan["expected_code"], 212)
                self.assertEqual(plan["dropped_column"], "payload7")
                fixtures = {}
                for index, item in enumerate(plan["files"]):
                    fixture = pathlib.Path(directory) / item["local_fixture"]
                    fixture.write_bytes(("fixture-%d" % index).encode())
                    fixtures[item["key"]] = fixture

                def ok(_service, operation, **parameters):
                    key = parameters.get("Key")
                    if operation == "get-bucket-tagging":
                        return {"TagSet": [{"Key": MARKER, "Value": probe.state["owner_marker"]}]}
                    if operation == "put-object":
                        return {"ETag": etag_for(key)}
                    if operation == "head-object":
                        fixture = fixtures[key]
                        return {"ETag": etag_for(key),
                                "Metadata": {"grv-owner": probe.state["owner_marker"],
                                             "grv-sha256": digest(fixture)},
                                "ContentLength": fixture.stat().st_size}
                    return {}

                probe.ok = mock.Mock(side_effect=ok)
                probe.harness_code = mock.Mock(return_value=(code, stdout, error))
                readbacks = [full, dropped]
                evidence = {"failed_slices": []}
                if stage is None:
                    item = probe.refusal_slice(readbacks, plan, evidence)
                    self.assertEqual(item["outcome"], "refused")
                    self.assertEqual(item["decision_code"], 212)
                    self.assertEqual(item["http_status"], 206)
                    self.assertEqual([call.args[1] for call in probe.ok.call_args_list
                                      if call.args[1] == "delete-object"].count("delete-object"), 2)
                    continue
                with self.assertRaisesRegex(SafeFailure, re.escape(stage)):
                    probe.refusal_slice(readbacks, plan, evidence)
                # The observed numbers must survive the refusal, whatever they were.
                self.assertEqual(evidence["failed_slices"][-1]["decision_code"], recorded)
                self.assertEqual(evidence["failed_slices"][-1]["stage"], stage)
                self.assertFalse(any(call.args[1] == "delete-object"
                                     for call in probe.ok.call_args_list))

    def test_reported_identity_disagreement_stops_before_delete(self):
        def etag_for(key):
            return '"' + hashlib.sha256(key.encode()).hexdigest()[:32] + '"'
        for field in (None, "size", "sha256", "validator_sha256"):
            with self.subTest(disagreement=field), tempfile.TemporaryDirectory() as directory:
                probe = self.probe(directory)
                plan = multi_shape_plan(8192, probe.state["bucket"])
                columns_by_file, fixtures = [], {}
                for index, item in enumerate(plan["files"]):
                    fixture = pathlib.Path(directory) / item["local_fixture"]
                    fixture.write_bytes((item["name"] + "-fixture").encode())
                    fixtures[item["key"]] = fixture
                    columns_by_file.append({"id": [index],
                                           **{f"payload{column}": ["x" * 3] for column in range(8)}})
                seen = []

                def ok(_service, operation, **parameters):
                    seen.append(operation)
                    key = parameters.get("Key")
                    if operation == "get-bucket-tagging":
                        return {"TagSet": [{"Key": MARKER, "Value": probe.state["owner_marker"]}]}
                    if operation == "put-object":
                        return {"ETag": etag_for(key)}
                    if operation == "head-object":
                        fixture = fixtures[key]
                        return {"ETag": etag_for(key),
                                "Metadata": {"grv-owner": probe.state["owner_marker"],
                                             "grv-sha256": digest(fixture)},
                                "ContentLength": fixture.stat().st_size}
                    return {}

                probe.ok = mock.Mock(side_effect=ok)
                report = {"manifest_mode": True, "shipping_scanner_source": True, "phases":
                          {"view_binding": {"body_bytes": 0, "head": 0, "get": 0}},
                          "files": [{"index": index, "size": fixtures[item["key"]].stat().st_size,
                                     "sha256": digest(fixtures[item["key"]]),
                                     "validator_sha256": hashlib.sha256(
                                         etag_for(item["key"]).encode()).hexdigest()}
                                    for index, item in enumerate(plan["files"])],
                          "trials": [{"trial_index": 0, "trial": "wide", "correct": True,
                                      "query": {"body_bytes": 0, "head": 0, "get": 0},
                                      "explain": {"body_bytes": 0, "head": 0, "get": 0},
                                      "actual": aggregate_expectation(columns_by_file, "wide")}],
                          "denials": {name: True for name in
                                      ("direct", "unlisted", "nested_glob", "secrets")}}
                probe.harness_code = mock.Mock(return_value=(0, json.dumps(report), b""))
                if field is None:
                    item = probe.multi_file_slice(columns_by_file, plan)
                    self.assertEqual(item["multi_file"], True)
                    self.assertEqual(seen.count("delete-object"), len(plan["files"]))
                    self.assertTrue(all(call.kwargs["IfMatch"] == etag_for(call.kwargs["Key"])
                                        for call in probe.ok.call_args_list
                                        if call.args[1] == "delete-object"))
                    continue
                report["files"][1][field] = 0 if field == "size" else "f" * 64
                probe.harness_code = mock.Mock(return_value=(0, json.dumps(report), b""))
                with self.assertRaisesRegex(SafeFailure, "reported-identity-mismatch"):
                    probe.multi_file_slice(columns_by_file, plan)
                self.assertFalse(any(call.args[1] == "delete-object" for call in probe.ok.call_args_list))

    def test_harness_accepts_written_manifest_spacing(self):
        # The manifest is written with json.dump spacing; the harness must parse that form.
        # An empty files list is refused before any network use, so this stays offline.
        binary = pathlib.Path("/Users/jiacao/.grv-native-work/cmake/grv_cloud_qualification")
        if not binary.exists():
            self.skipTest("harness binary not built")
        with tempfile.TemporaryDirectory() as directory:
            manifest = pathlib.Path(directory) / "manifest.json"
            with open(manifest, "w") as handle:
                json.dump({"files": [], "trials": []}, handle)
            completed = subprocess.run(
                [str(binary), "s3", "/nonexistent/httpfs.duckdb_extension",
                 "/nonexistent/aws.duckdb_extension",
                 "s3://grv-s3-shipping-probe-000000000000000000000000/data", "profile",
                 "region", str(manifest)],
                capture_output=True, timeout=60, check=False, env=os.environ.copy())
            code, error = completed.returncode, completed.stderr
            self.assertEqual(code, 1)
            diagnostic = re.fullmatch(
                rb"shipping qualification failed \(private details suppressed; protected read "
                rb"phase ([0-9]{1,3}), HTTP status ([0-9]{1,3})\)\n", error)
            self.assertIsNotNone(diagnostic)
            self.assertEqual(int(diagnostic.group(1)), 202)

    def test_manifest_decision_code_recorded_without_text(self):
        def etag_for(key):
            return '"' + hashlib.sha256(key.encode()).hexdigest()[:32] + '"'
        for code, message, expected in (
                (205, b"shipping qualification failed (private details suppressed; protected "
                       b"read phase 205, HTTP status 0)\n", 205),
                (211, b"shipping qualification failed (private details suppressed; protected "
                       b"read phase 211, HTTP status 0)\n", 211),
                (212, b"shipping qualification failed (private details suppressed; protected "
                       b"read phase 212, HTTP status 0)\n", 212),
                (213, b"shipping qualification failed (private details suppressed; protected "
                       b"read phase 213, HTTP status 0)\n", 213),
                (None, b"unparsable child diagnostic\n", None)):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                probe = self.probe(directory)
                plan = multi_shape_plan(8192, probe.state["bucket"])
                columns_by_file, fixtures = [], {}
                for index, item in enumerate(plan["files"]):
                    fixture = pathlib.Path(directory) / item["local_fixture"]
                    fixture.write_bytes((item["name"] + "-fixture").encode())
                    fixtures[item["key"]] = fixture
                    columns_by_file.append({"id": [index],
                                           **{f"payload{column}": ["x" * 3] for column in range(8)}})

                def ok(_service, operation, **parameters):
                    key = parameters.get("Key")
                    if operation == "get-bucket-tagging":
                        return {"TagSet": [{"Key": MARKER, "Value": probe.state["owner_marker"]}]}
                    if operation == "put-object":
                        return {"ETag": etag_for(key)}
                    if operation == "head-object":
                        fixture = fixtures[key]
                        return {"ETag": etag_for(key),
                                "Metadata": {"grv-owner": probe.state["owner_marker"],
                                             "grv-sha256": digest(fixture)},
                                "ContentLength": fixture.stat().st_size}
                    return {}

                probe.ok = mock.Mock(side_effect=ok)
                probe.harness_code = mock.Mock(return_value=(1, b"", message))
                evidence = {"failed_slices": []}
                with self.assertRaisesRegex(SafeFailure, "manifest-harness-failed"):
                    probe.multi_file_slice(columns_by_file, plan, evidence)
                recorded = evidence["failed_slices"][0]
                self.assertEqual(recorded["decision_code"], expected)
                self.assertEqual(recorded["native_exit"], 1)
                self.assertFalse(any(call.args[1] == "delete-object"
                                     for call in probe.ok.call_args_list))

    def test_unexpected_inventory_stops_before_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = self.probe(directory)
            probe.call = mock.Mock(return_value=(None, {}))
            probe.owner = mock.Mock()
            probe.inventory = mock.Mock(return_value=([{"Key": "foreign"}], [], []))
            probe.ok = mock.Mock(side_effect=self.cleanup_ok)
            with self.assertRaises(SafeFailure):
                probe.cleanup()
            self.assertFalse(any(call.args[1] == "delete-object" for call in probe.ok.call_args_list))

    @staticmethod
    def etag_for(key):
        return '"' + hashlib.sha256(key.encode()).hexdigest()[:32] + '"'

    @staticmethod
    def cold_warm_columns(rows=4):
        # The standard generated data, exactly as run() writes it: real payload values, no nulls.
        return {"id": list(range(rows)),
                **{f"payload{column}":
                   [hashlib.sha256(f"{column}-{row}".encode()).hexdigest() * 4
                    for row in range(rows)] for column in range(8)}}

    def cold_warm_setup(self, directory, plan):
        """Offline scaffolding: placeholder fixtures, mocked identity, mocked harness, no network."""
        fixtures = {}
        for item in plan["files"]:
            fixture = pathlib.Path(directory) / item["local_fixture"]
            fixture.write_bytes(b"cold-warm-offline-fixture")
            fixtures[item["key"]] = fixture
        probe = self.probe(directory)
        seen = []

        def ok(_service, operation, **parameters):
            key = parameters.get("Key")
            seen.append(operation)
            if operation == "get-bucket-tagging":
                return {"TagSet": [{"Key": MARKER, "Value": probe.state["owner_marker"]}]}
            if operation == "put-object":
                return {"ETag": self.etag_for(key)}
            if operation == "head-object":
                return {"ETag": self.etag_for(key),
                        "Metadata": {"grv-owner": probe.state["owner_marker"],
                                     "grv-sha256": digest(fixtures[key])},
                        "ContentLength": fixtures[key].stat().st_size}
            return {}

        probe.ok = mock.Mock(side_effect=ok)
        return probe, fixtures[plan["files"][0]["key"]], seen

    @staticmethod
    def cold_warm_report(plan, fixture, columns, counters, denials=None, correct=None):
        etag = '"' + hashlib.sha256(plan["files"][0]["key"].encode()).hexdigest()[:32] + '"'
        return {"manifest_mode": True, "shipping_scanner_source": True,
                "files": [{"index": 0, "size": fixture.stat().st_size, "sha256": digest(fixture),
                           "validator_sha256": hashlib.sha256(etag.encode()).hexdigest()}],
                "phases": {"view_binding": {"body_bytes": 0, "head": 0, "get": 0}},
                "trials": [{"trial_index": index, "trial": name,
                             "correct": (correct or {}).get(name, True),
                             "query": counters[name]["query"], "explain": counters[name]["explain"],
                             "actual": cold_warm_expectation(columns, name)}
                            for index, name in enumerate(COLD_WARM_TRIALS)],
                "denials": dict({name: True for name in
                                 ("direct", "unlisted", "nested_glob", "secrets")},
                                **(denials or {}))}

    def test_cold_warm_plan_is_one_object_two_trials_in_fixed_order(self):
        # The order IS the observation: wide first (cold, first guarded execution), narrow second
        # (warm, second guarded execution on the same retained connection-session). It is fixed in
        # code and never caller-supplied.
        self.assertEqual(COLD_WARM_TRIALS, ("wide", "narrow"))
        self.assertEqual(COLD_WARM_ROWS, 2048)
        for rows in (8192, 65536):
            plan = cold_warm_plan(rows, "bucket")
            self.assertEqual(plan["name"], COLD_WARM_SHAPE_NAME)
            self.assertEqual(len(plan["files"]), 1)
            self.assertEqual(plan["trials"], ["wide", "narrow"])
            self.assertEqual((plan["compression"], plan["dictionary"], plan["statistics"]),
                             ("NONE", False, True))
            # 2048 rows at row_group_rows 2048 is exactly one row group.
            self.assertEqual(plan["row_group_rows"], 2048)
            self.assertEqual(plan["files"][0]["rows"], 2048)
            self.assertEqual(plan["columns"], SCHEMA)
            self.assertEqual(plan["files"][0]["columns"], SCHEMA)
            self.assertEqual(plan["files"][0]["key"], "data/cold-warm.parquet")
            self.assertEqual(plan["files"][0]["coordinate"], "s3://bucket/data/cold-warm.parquet")
        with self.assertRaisesRegex(SafeFailure, "shape-row-expectation"):
            cold_warm_plan(2048, "bucket")
        # Standard generated data only, never the all-null table, so both expectations are exact.
        text = pathlib.Path(__file__).read_text()
        block = text[text.index("plan = cold_warm_plan(args.rows"):]
        block = block[:block.index("if COLD_WARM_SHAPE_NAME in run_set:")]
        self.assertIn('table.slice(0, item["rows"])', block)
        self.assertNotIn("null_table", block)
        self.assertIn("row_group_size=plan[\"row_group_rows\"]", block)

    def test_cold_warm_expectations_mirror_the_closed_sql_for_both_trials(self):
        # Quoted from ManifestTrialSql in cloud_qualification.cpp, which is the authority; the
        # mirror is pinned here so drift in either direction is caught offline.
        self.assertEqual(COLD_WARM_SQL["wide"],
                         "SELECT sum(id)::BIGINT,sum(length(payload0))::BIGINT,"
                         "sum(length(payload1))::BIGINT,sum(length(payload2))::BIGINT,"
                         "sum(length(payload3))::BIGINT,sum(length(payload4))::BIGINT,"
                         "sum(length(payload5))::BIGINT,sum(length(payload6))::BIGINT,"
                         "sum(length(payload7))::BIGINT FROM data")
        self.assertEqual(COLD_WARM_SQL["narrow"], "SELECT sum(id)::BIGINT FROM data")
        self.assertEqual(set(COLD_WARM_SQL), set(COLD_WARM_TRIALS))
        self.assertEqual(len(COLD_WARM_SQL), 2)
        # Independent arithmetic the parent re-derives on its own, not a call to the mirror.
        columns = {"id": [10, 32, 7],
                   **{f"payload{column}": ["x" * (column + 1)] * 2 for column in range(8)}}
        self.assertEqual(cold_warm_expectation(columns, "narrow"), encode_result([49]))
        self.assertEqual(cold_warm_expectation(columns, "wide"),
                         encode_result([49] + [2 * (column + 1) for column in range(8)]))
        standard = self.cold_warm_columns(4)
        self.assertEqual(cold_warm_expectation(standard, "narrow"), encode_result([6]))
        self.assertEqual(cold_warm_expectation(standard, "wide"), encode_result([6] + [1024] * 8))
        self.assertNotEqual(cold_warm_expectation(standard, "wide"),
                            cold_warm_expectation(standard, "narrow"))
        # The pair is closed: each trial must be its own SQL, so neither can borrow the other's.
        # This is what pins the mirror to the harness source in both directions.
        with mock.patch.dict(COLD_WARM_SQL, {"narrow": COLD_WARM_SQL["wide"]}):
            with self.assertRaisesRegex(SafeFailure, "cold-warm-sql-mirror"):
                cold_warm_expectation(standard, "narrow")
        with mock.patch.dict(COLD_WARM_SQL, {"wide": COLD_WARM_SQL["narrow"]}):
            with self.assertRaisesRegex(SafeFailure, "cold-warm-sql-mirror"):
                cold_warm_expectation(standard, "wide")
        # And a projected item the closed SQL does not contain is refused rather than guessed.
        with mock.patch.dict(COLD_WARM_SQL, {"narrow": "SELECT sum(id)::BIGINT," + " FROM data"}):
            with self.assertRaisesRegex(SafeFailure, "cold-warm-sql-mirror"):
                cold_warm_expectation(standard, "narrow")
        for case, trial, stage in (("null payload", "wide", "cold-warm-data-not-standard"),
                                   ("null payload", "narrow", "cold-warm-data-not-standard")):
            with self.subTest(case=case, trial=trial):
                with self.assertRaisesRegex(SafeFailure, re.escape(stage)):
                    cold_warm_expectation({**standard, "payload3": ["x" * 256, None]}, trial)
        with self.assertRaisesRegex(SafeFailure, "cold-warm-trial-name"):
            cold_warm_expectation(standard, "and_range")
        with self.assertRaisesRegex(SafeFailure, "fixture-schema-drift"):
            cold_warm_expectation({key: value for key, value in standard.items()
                                   if key != "payload7"}, "narrow")
        with self.assertRaisesRegex(SafeFailure, "cold-warm-fixture-empty"):
            cold_warm_expectation({**standard, "id": []}, "narrow")

    def test_cold_warm_manifest_accepts_one_file_with_two_trials(self):
        # The closed parser bounds stay exactly as they are; only the shape uses them.
        self.assertEqual((MANIFEST_MAX_FILES, MANIFEST_MAX_TRIALS), (4096, 10))
        entry = {"local": "/tmp/cold-warm.parquet",
                 "coordinate": "s3://bucket/data/cold-warm.parquet", "size": 12,
                 "sha256": "a" * 64, "validator": '"' + "d" * 32 + '"'}
        columns = self.cold_warm_columns()
        trials = [{"trial": name, "expected": cold_warm_expectation(columns, name)}
                  for name in COLD_WARM_TRIALS]
        document = manifest_document([entry], trials)
        self.assertEqual(frozenset(document), MANIFEST_DOCUMENT_FIELDS)
        self.assertEqual(len(document["files"]), 1)
        self.assertEqual([trial["trial"] for trial in document["trials"]], ["wide", "narrow"])
        # The grammar still refuses a duplicate trial name, which is why the shape uses two names.
        with self.assertRaisesRegex(SafeFailure, "manifest-duplicate-trial"):
            manifest_document([entry], trials + [dict(trials[0])])
        with self.assertRaisesRegex(SafeFailure, "manifest-duplicate-trial"):
            manifest_document([entry], [dict(trials[1]), dict(trials[1])])
        with self.assertRaisesRegex(SafeFailure, "manifest-trials-empty"):
            manifest_document([entry], [])

    def test_cold_warm_records_per_trial_counters_for_both_trials(self):
        # Reduced warm fetching and full warm re-fetch are BOTH acceptable: only recording is
        # asserted, never which way the counters fell.
        cold = {"query": {"body_bytes": 4096, "head": 4, "get": 4},
                "explain": {"body_bytes": 0, "head": 0, "get": 0}}
        for case, warm in (("warm-fetched-less", {"query": {"body_bytes": 0, "head": 0, "get": 0},
                                                  "explain": {"body_bytes": 0, "head": 0,
                                                             "get": 0}}),
                           ("warm-refetched-everything",
                            {"query": {"body_bytes": 900000, "head": 60, "get": 60},
                             "explain": {"body_bytes": 128, "head": 2, "get": 2}})):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                plan = cold_warm_plan(8192, "bucket")
                columns = self.cold_warm_columns()
                probe, fixture, seen = self.cold_warm_setup(directory, plan)
                counters = dict(zip(COLD_WARM_TRIALS, (cold, warm)))
                probe.harness_code = mock.Mock(return_value=(
                    0, json.dumps(self.cold_warm_report(plan, fixture, columns, counters)), b""))
                evidence = {"failed_slices": []}
                item = probe.cold_warm_slice([columns], plan, evidence)
                self.assertEqual([trial["trial"] for trial in item["trials"]],
                                 list(COLD_WARM_TRIALS))
                self.assertTrue(all(trial["correct"] for trial in item["trials"]))
                self.assertEqual(item["trials"][0]["query"], cold["query"])
                self.assertEqual(item["trials"][0]["explain"], cold["explain"])
                self.assertEqual(item["trials"][1]["query"], warm["query"])
                self.assertEqual(item["trials"][1]["explain"], warm["explain"])
                self.assertTrue(item["counter_direction_not_asserted"])
                self.assertEqual(evidence["failed_slices"], [])
                # Evidence stays numbers-only: no provider text, no manifest content.
                self.assertNotIn("actual", json.dumps(item["trials"]))
                for trial in item["trials"]:
                    self.assertEqual(frozenset(trial),
                                     {"trial_index", "trial", "correct", "expected_sha256",
                                      "query", "explain"})
                    self.assertTrue(all(type(value) is int
                                        for phase in ("query", "explain")
                                        for value in trial[phase].values()))
                self.assertTrue(item["files"][0]["cleaned_individually"])
                self.assertEqual(seen.count("delete-object"), 1)

    def test_cold_warm_mismatch_on_either_trial_stops_before_delete(self):
        counters = {name: {"query": {"body_bytes": 10, "head": 1, "get": 1},
                           "explain": {"body_bytes": 0, "head": 0, "get": 0}}
                    for name in COLD_WARM_TRIALS}
        columns = self.cold_warm_columns()
        for case, index in (("cold-trial-mismatch", 0), ("warm-trial-mismatch", 1)):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                plan = cold_warm_plan(8192, "bucket")
                probe, fixture, _seen = self.cold_warm_setup(directory, plan)
                report = self.cold_warm_report(plan, fixture, columns, counters)
                report["trials"][index]["actual"] = hashlib.sha256(b"other").hexdigest()
                probe.harness_code = mock.Mock(return_value=(0, json.dumps(report), b""))
                with self.assertRaisesRegex(SafeFailure, "reported-expectation-mismatch"):
                    probe.cold_warm_slice([columns], plan, {"failed_slices": []})
                self.assertFalse(any(call.args[1] == "delete-object"
                                     for call in probe.ok.call_args_list))
        # A report that does not carry both trials is refused: this shape needs two observations.
        with tempfile.TemporaryDirectory() as directory:
            plan = cold_warm_plan(8192, "bucket")
            probe, fixture, _seen = self.cold_warm_setup(directory, plan)
            report = self.cold_warm_report(plan, fixture, columns, counters)
            report["trials"] = [report["trials"][0]]
            probe.harness_code = mock.Mock(return_value=(0, json.dumps(report), b""))
            with self.assertRaisesRegex(SafeFailure, "manifest-report-shape"):
                probe.cold_warm_slice([columns], plan, {"failed_slices": []})
            self.assertFalse(any(call.args[1] == "delete-object"
                                 for call in probe.ok.call_args_list))

    def test_cold_warm_warm_read_gains_no_new_authority(self):
        # Both trials must be correct and all four standing denials must still refuse, checked after
        # the warm read. Nothing about counter direction is asserted here.
        counters = {name: {"query": {"body_bytes": 10, "head": 1, "get": 1},
                           "explain": {"body_bytes": 0, "head": 0, "get": 0}}
                    for name in COLD_WARM_TRIALS}
        columns = self.cold_warm_columns()
        for case, correct, denials, stage in (
                ("cold-not-correct", {"wide": False}, None, "native-oracle"),
                ("warm-not-correct", {"narrow": False}, None, "native-oracle"),
                ("direct-accepted", None, {"direct": False}, "native-denial"),
                ("unlisted-accepted", None, {"unlisted": False}, "native-denial"),
                ("nested-glob-accepted", None, {"nested_glob": False}, "native-denial"),
                ("secrets-accepted", None, {"secrets": False}, "native-denial")):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                plan = cold_warm_plan(8192, "bucket")
                probe, fixture, _seen = self.cold_warm_setup(directory, plan)
                report = self.cold_warm_report(plan, fixture, columns, counters,
                                               denials=denials, correct=correct)
                probe.harness_code = mock.Mock(return_value=(0, json.dumps(report), b""))
                with self.assertRaisesRegex(SafeFailure, re.escape(stage)):
                    probe.cold_warm_slice([columns], plan, {"failed_slices": []})
                self.assertFalse(any(call.args[1] == "delete-object"
                                     for call in probe.ok.call_args_list))

    def test_verify_reported_compares_expectations_in_order(self):
        columns = self.cold_warm_columns()
        validator = '"' + "d" * 32 + '"'
        entries = [{"local": "/tmp/cold-warm.parquet",
                    "coordinate": "s3://bucket/data/cold-warm.parquet", "size": 12,
                    "sha256": "a" * 64, "validator": validator}]
        files = [{"index": 0, "size": 12, "sha256": "a" * 64,
                  "validator_sha256": hashlib.sha256(validator.encode()).hexdigest()}]

        def expectation(name):
            return {"trial": name, "expected": cold_warm_expectation(columns, name)}

        def reported(name):
            return {"trial": name, "actual": cold_warm_expectation(columns, name)}

        ordered = [reported("wide"), reported("narrow")]
        verify_reported(entries, {"files": files, "trials": ordered},
                        [expectation("wide"), expectation("narrow")])
        # A shuffled report is refused: the comparison is positional, not lookup-by-name.
        with self.assertRaisesRegex(SafeFailure, "reported-expectation-mismatch"):
            verify_reported(entries, {"files": files, "trials": list(reversed(ordered))},
                            [expectation("wide"), expectation("narrow")])
        # The one-element list is the unchanged behaviour the aggregate and refusal shapes use.
        verify_reported(entries, {"files": files, "trials": [ordered[1]]}, [expectation("narrow")])
        with self.assertRaisesRegex(SafeFailure, "manifest-report-shape"):
            verify_reported(entries, {"files": files, "trials": ordered}, [expectation("narrow")])
        with self.assertRaisesRegex(SafeFailure, "manifest-report-shape"):
            verify_reported(entries, {"files": files, "trials": [ordered[0]]}, expectation("wide"))
        with self.assertRaisesRegex(SafeFailure, "reported-identity-mismatch"):
            verify_reported(entries, {"files": [dict(files[0], size=13)],
                                      "trials": [ordered[1]]}, [expectation("narrow")])

    def test_existing_multi_file_and_refusal_shapes_are_unchanged(self):
        # The aggregate shape is still three single-row-group files with one trial, and the refusal
        # shape still two files with a dropped column; only verify_reported's signature generalised.
        plan = multi_shape_plan(8192, "bucket")
        self.assertEqual(len(plan["files"]), 3)
        self.assertEqual([item["rows"] for item in plan["files"]], [2048, 2048, 2048])
        self.assertEqual(plan["trial"], "wide")
        self.assertEqual((plan["compression"], plan["dictionary"], plan["statistics"],
                          plan["row_group_rows"]), ("NONE", False, True, 2048))
        columns = [{"id": [index], **{f"payload{column}": ["x" * 3] for column in range(8)}}
                   for index in range(3)]
        self.assertEqual(aggregate_expectation(columns, "wide"),
                         encode_result([3] + [9] * 8))
        refusal = refusal_plan(8192, "bucket")
        self.assertEqual(len(refusal["files"]), 2)
        self.assertEqual(refusal["outcome"], "refused")
        self.assertEqual(refusal["expected_code"], 212)
        self.assertEqual(refusal["dropped_column"], "payload7")
        text = pathlib.Path(__file__).read_text()
        self.assertIn("verify_reported(entries, report, [expectation])", text)
        self.assertIn("verify_reported(entries, report, expectations)", text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--approved", action="store_true")
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--binary", type=pathlib.Path,
                        default=pathlib.Path.home() / ".grv-native-work/cmake/grv_cloud_qualification")
    parser.add_argument("--httpfs", type=pathlib.Path, default=pathlib.Path.home() /
                        ".grv-httpfs-prototype-extensions/osx_arm64/httpfs.duckdb_extension")
    parser.add_argument("--aws", type=pathlib.Path, default=pathlib.Path.home() /
                        ".grv-httpfs-prototype-extensions/osx_arm64/aws.duckdb_extension")
    parser.add_argument("--native-timeout", type=int, default=300)
    parser.add_argument("--only", action="append", default=[], metavar="SLICE",
                        help="restrict the run to named slices; default is every slice")
    parser.add_argument("--rows", type=int, choices=(8192, 65536), default=65536,
                        help="8192 is an explicitly smaller diagnostic slice, not the full fixture")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SafetyTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not args.approved or args.profile != "lexiupon-aws-cli-s3" or args.region != "eu-west-1":
        parser.error("explicit --approved --profile lexiupon-aws-cli-s3 --region eu-west-1 required")
    if not 1 <= args.native_timeout <= 600:
        parser.error("native timeout must be 1..600 seconds")
    os.umask(0o077)
    def interrupted(_signum, _frame):
        raise SafeFailure("interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        args.binary = args.binary.resolve(strict=True)
        args.httpfs = args.httpfs.resolve(strict=True)
        args.aws = args.aws.resolve(strict=True)
        return run(args)
    except BaseException:
        # Do not print exception strings, child commands, tracebacks or arbitrary output.
        print("S3 qualification failed; unknown details suppressed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
