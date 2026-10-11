#!/usr/bin/env python3
"""Explicitly approved owned GCS shipping-scanner slice; no IAM/key changes.

Requires pyarrow. This is not release qualification or full preparation evidence.
OAuth token stays in memory/stdin; native failure output remains private.
"""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--approved", action="store_true")
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--httpfs", type=pathlib.Path, required=True)
    parser.add_argument("--compression", choices=("NONE", "ZSTD"), default="NONE")
    parser.add_argument("--dictionary", action="store_true")
    parser.add_argument("--no-statistics", action="store_true")
    args = parser.parse_args()
    if not args.approved or not args.account or not args.project:
        parser.error("explicit owned-fixture approval and account/project required")
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = pathlib.Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location(
        "owned_probe", root / "prototypes/cloud-views/httpfs/live_oauth.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    directory = pathlib.Path(tempfile.mkdtemp(prefix="grv-shipping-matrix-", dir=pathlib.Path.home()))
    state = directory / "state.json"
    module.private_json(state, {"bucket": "grv-httpfs-oauth-" + uuid.uuid4().hex[:24],
                               "account": args.account, "project": args.project})
    probe = module.Probe(state)
    binary, httpfs = args.binary.resolve(strict=True), args.httpfs.resolve(strict=True)
    try:
        probe.check_auth()
        probe.create()
        data = {"id": pa.array(range(65536), type=pa.int64())}
        for column in range(8):
            data[f"payload{column}"] = pa.array([
                hashlib.sha256(f"{column}-{row}".encode()).hexdigest() * 4 for row in range(65536)])
        fixture = directory / "wide.parquet"
        pq.write_table(pa.table(data), fixture, row_group_size=2048,
                       use_dictionary=args.dictionary, compression=args.compression,
                       write_statistics=not args.no_statistics)
        probe.upload("data/wide.parquet", fixture)
        command = [str(binary), "gcs", str(httpfs), "-",
                   f"https://storage.googleapis.com/{probe.state['bucket']}/data",
                   args.project, "-", str(fixture),
                   f"gs://{probe.state['bucket']}/data/wide.parquet", str(fixture.stat().st_size),
                   hashlib.sha256(fixture.read_bytes()).hexdigest(), "owned-generation"]
        binary_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
        result = subprocess.run(command, input=(probe.token() + "\n").encode(),
                                capture_output=True, timeout=300)
        module.private_json(directory / "summary.json", {
            "exit": result.returncode, "account": args.account, "project": args.project,
            "fixture_bytes": fixture.stat().st_size, "compression": args.compression,
            "dictionary": args.dictionary, "statistics": not args.no_statistics,
            "binary_sha256_before": binary_hash,
            "binary_sha256_after": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "source": "actual-shipping-native-scanners-test-only-counters",
            "limitations": ["preparation traffic not measured", "not a release candidate",
                            "one-file scanner slice, not full shipping pull matrix"]})
        if result.returncode:
            path = directory / "private-result.txt"
            path.write_bytes(result.stdout + result.stderr)
            path.chmod(0o600)
            raise RuntimeError("shipping scanner failed; private evidence: " + str(directory))
        evidence = json.loads(result.stdout)
        module.private_json(directory / "results.json", evidence)
        print("Shipping GCS slice:", directory, "savings:", evidence["qualifying_savings"])
    finally:
        probe.cleanup()


if __name__ == "__main__":
    main()
