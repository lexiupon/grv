#!/usr/bin/env python3
"""Verify a relocated development/candidate bundle with an isolated local lifecycle.

Uses no service credentials and publishes no release. Creates temporary local
data beside the bundle, verifies build/pull/replay, and removes only that data.
"""
import argparse
import ctypes
from contextlib import nullcontext
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import uuid


def verify_extensions(bundle, inventory, library):
    if inventory is None:
        return False
    # Anchor signature and byte identities to the pinned source registry rather
    # than trusting a relocated bundle's self-reported verification status.
    spec = importlib.util.spec_from_file_location(
        "grv_extension_verifier", Path(__file__).with_name("fetch-duckdb-extensions.py"))
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    registry = verifier.load_manifest()
    selected = [entry for entry in registry["artifacts"]
                if entry["platform"] == inventory["platform"]]
    if len(selected) != 2 or inventory["artifacts"] != selected:
        raise SystemExit("packaged extension inventory differs from pinned registry")
    directory = bundle / "adapters/duckdb/bin/extensions"
    public_key = verifier.read_notice(registry, registry["signature"]["public_key"])
    with tempfile.TemporaryDirectory(prefix=".grv-extension-proof-", dir=bundle.parent) as scratch:
        for entry in selected:
            verifier.verify_binary(directory / f'{entry["name"]}.duckdb_extension', entry,
                                   public_key,
                                   Path(scratch), private=True)
    # Load both extensions through the relocated native library without creating
    # credentials, consulting profiles, reading source rows or contacting S3.
    native = ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
    pointer = ctypes.c_void_p
    native.duckdb_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(pointer)]
    native.duckdb_open.restype = ctypes.c_uint32
    native.duckdb_connect.argtypes = [pointer, ctypes.POINTER(pointer)]
    native.duckdb_connect.restype = ctypes.c_uint32
    native.grv_native_load_static_extensions.argtypes = [pointer, ctypes.c_char_p, ctypes.c_size_t]
    native.grv_native_load_static_extensions.restype = ctypes.c_int
    class Result(ctypes.Structure):
        _fields_ = [("columns", ctypes.c_uint64), ("rows", ctypes.c_uint64),
                    ("changed", ctypes.c_uint64), ("data", pointer),
                    ("error", ctypes.c_char_p), ("internal", pointer)]
    native.duckdb_query.argtypes = [pointer, ctypes.c_char_p, ctypes.POINTER(Result)]
    native.duckdb_query.restype = ctypes.c_uint32
    native.duckdb_destroy_result.argtypes = [ctypes.POINTER(Result)]
    native.duckdb_disconnect.argtypes = [ctypes.POINTER(pointer)]
    native.duckdb_close.argtypes = [ctypes.POINTER(pointer)]
    database, connection = pointer(), pointer()
    try:
        if native.duckdb_open(None, ctypes.byref(database)):
            raise SystemExit("packaged native probe could not open memory database")
        error = ctypes.create_string_buffer(256)
        if native.grv_native_load_static_extensions(database, error, len(error)):
            raise SystemExit("packaged native static extensions failed")
        if native.duckdb_connect(database, ctypes.byref(connection)):
            raise SystemExit("packaged native probe could not connect")
        statements = ["SET autoinstall_known_extensions=false", "SET autoload_known_extensions=false",
                      "SET enable_logging=false"]
        statements.extend("LOAD '" + str(directory / f'{name}.duckdb_extension').replace("'", "''") + "'"
                          for name in ("httpfs", "aws"))
        for statement in statements:
            result = Result()
            status = native.duckdb_query(connection, statement.encode(), ctypes.byref(result))
            native.duckdb_destroy_result(ctypes.byref(result))
            if status:
                raise SystemExit("packaged signed extension loading failed")
    finally:
        if connection:
            native.duckdb_disconnect(ctypes.byref(connection))
        if database:
            native.duckdb_close(ctypes.byref(database))
    return True


def require_distribution_ready(manifest):
    spec = importlib.util.spec_from_file_location(
        "grv_bundle_packager", Path(__file__).with_name("package.py"))
    packager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(packager)
    packager.require_distribution_ready(manifest)
    if manifest.get("profile") != "release" or manifest.get("source_dirty") is not False:
        raise SystemExit("distribution requires release profile and clean source provenance")
    if not manifest.get("source_commit") or not manifest.get("toolchain"):
        raise SystemExit("distribution requires source/toolchain provenance")
    if manifest.get("platform") == "Darwin":
        versions = manifest.get("macos_minimum_versions") or {}
        if not versions or manifest.get("macos_minimum_version") != max(
                versions.values(), key=lambda v: tuple(map(int, v.split(".")))):
            raise SystemExit("distribution requires actual Mach-O minimum OS inventory")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--compatibility-root", type=Path,
                        help="fresh protected persistent workspace for later candidate replay; never auto-deleted")
    parser.add_argument("--require-distribution-ready", action="store_true",
                        help="fail closed on notices/profile/provenance; not release approval")
    args = parser.parse_args()
    bundle = args.bundle.resolve(strict=True)
    manifest = json.loads((bundle / "bundle.json").read_text())
    if args.require_distribution_ready:
        require_distribution_ready(manifest)
        for relative, scope in [("notices/native-source-review.json", "native-engine"),
                                ("notices/extension-third-party-review.json", "signed-extensions")]:
            review = json.loads((bundle / relative).read_text())
            if (review.get("policy") != "pinned-upstream-plus-conservative-third-party-notices"
                    or review.get("scope") != scope or review.get("reviewed") is not True
                    or review.get("unresolved") or not review.get("notice_files")):
                raise SystemExit("packaged notice policy review invalid")
            if relative not in manifest["sha256"]:
                raise SystemExit("notice review not anchored in artifact inventory")
            if scope == "native-engine":
                if review.get("source_archive_sha256") != "1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b":
                    raise SystemExit("notice source release identity differs")
            else:
                extensions = manifest.get("native_extensions") or {}
                expected = {entry["sha256"] for entry in extensions.get("artifacts", [])}
                reviewed = {entry["sha256"] for entry in review.get("artifacts", [])
                            if entry["platform"] == extensions.get("platform")}
                if not expected or expected != reviewed:
                    raise SystemExit("notice review is for another extension artifact set")
            for notice in review["notice_files"]:
                path = "notices/" + notice["path"]
                if manifest["sha256"].get(path) != notice["sha256"]:
                    raise SystemExit("notice payload not anchored in artifact inventory")
    for relative, expected in manifest["sha256"].items():
        path = bundle / relative
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"bundle file missing or unsafe: {relative}")
        path.resolve().relative_to(bundle)
        with path.open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        if actual != expected:
            raise SystemExit(f"bundle file digest differs: {relative}")
    if manifest["dependency_notice_gaps"]:
        raise SystemExit("bundle has dependency notice gaps")
    environment = {key: value for key, value in os.environ.items()
                   if key in ("HOME", "PATH", "TMPDIR", "LANG", "LC_ALL")}
    environment["GRV_ADAPTERS_DIR"] = str(bundle / "adapters")
    cli = str(bundle / "bin/grv")

    def call(*arguments):
        process = subprocess.run([cli, "--json", *map(str, arguments)],
                                 env=environment, capture_output=True, timeout=90)
        value = json.loads(process.stdout)
        if process.returncode or value.get("ok") is not True:
            raise SystemExit(f"packaged CLI operation failed: {value}")
        return value

    listing = call("adapter", "list")
    for name in ("duckdb", "salesforce"):
        observed = call("adapter", name, "capabilities")["result"]["adapter"]
        if observed != manifest["adapters"][name]:
            raise SystemExit("relocated adapter capabilities differ")
    # Loader tracing happens before the protocol starts; no credentials or
    # helper output are retained. The selected guard must reside in this bundle.
    library = bundle / "adapters/duckdb/bin/lib" / (
        "libgrv_duckdb_guard.dylib" if platform.system() == "Darwin"
        else "libgrv_duckdb_guard.so")
    loader_env = dict(environment)
    loader_env["DYLD_PRINT_LIBRARIES" if platform.system() == "Darwin" else "LD_DEBUG"] = (
        "1" if platform.system() == "Darwin" else "libs")
    trace = subprocess.run([str(bundle / "adapters/duckdb/bin/grv-adapter-duckdb")],
                           input=b"", env=loader_env, capture_output=True, timeout=15)
    selected = [line for line in trace.stderr.decode(errors="replace").splitlines()
                if "grv_duckdb_guard" in line]
    if not any(str(library) in line for line in selected):
        raise SystemExit("adapter did not load the relocated guard library")
    extensions_verified = verify_extensions(bundle, manifest.get("native_extensions"), library)
    if args.compatibility_root:
        temp = args.compatibility_root.absolute()
        if temp.exists() or temp.is_symlink():
            raise SystemExit("compatibility workspace must be fresh")
        packager_spec = importlib.util.spec_from_file_location(
            "grv_compat_packager", Path(__file__).with_name("package.py"))
        packager = importlib.util.module_from_spec(packager_spec)
        packager_spec.loader.exec_module(packager)
        packager.protected_parent(temp.parent)
        temp.mkdir(mode=0o700)
        workspace = nullcontext(str(temp))
    else:
        workspace = tempfile.TemporaryDirectory(prefix=".grv-bundle-proof-", dir=bundle.parent)
    with workspace as directory:
        temp = Path(directory)
        store, state = temp / "store", temp / "state"
        engine, destination = temp / "build.duckdb", temp / "pull.duckdb"
        declaration = temp / "push.yml"
        attempt = str(uuid.uuid4())
        declaration.write_text(
            "declaration_version: 1\nkind: push\ndataset: proof\nadapter: duckdb\n"
            f"connection: {{database: {json.dumps(str(engine))}}}\n"
            "build: {execution: managed}\ntables:\n"
            "  - name: rows\n    source: {sql: 'SELECT i::BIGINT AS value FROM range(3) AS t(i)'}\n"
            "    columns: [{name: value, type: int64}]\n"
            "  - name: empty\n    source: {sql: 'SELECT 1::BIGINT AS value WHERE false'}\n"
            "    columns: [{name: value, type: int64}]\n")
        call("init", "--grv", store)
        pushed = call("push", "--grv", store, "--decl", declaration,
                      "--state", state, "--attempt", attempt)
        call("verify", "proof", "--grv", store, "--full")
        pull_decl = temp / "pull.yml"
        pull_decl.write_text(
            "declaration_version: 1\nkind: pull\ndataset: proof\nadapter: duckdb\n"
            f"connection: {{database: {json.dumps(str(destination))}}}\n"
            "target: {schema: proof}\ntables: [{name: rows}, {name: empty}]\n")
        pull_attempt = str(uuid.uuid4())
        pulled = call("pull", "--grv", store, "--decl", pull_decl, "--state", state,
                      "--attempt", pull_attempt)
        engine.unlink()
        declaration.unlink()
        replay = call("push", "--grv", store, "--decl", declaration,
                      "--state", state, "--attempt", attempt)
        if replay["result"]["replayed"] is not True:
            raise SystemExit("packaged terminal outcome was not replayed")
        proof = {"format_version": 1, "relocation_verified": True,
                 "distribution_readiness_checked": args.require_distribution_ready,
                 "signed_extensions_loaded": extensions_verified,
                 "guard_library": str(library.relative_to(bundle)),
                 "manifest_digests_verified": len(manifest["sha256"]),
                 "adapter_listing": listing["result"],
                 "build_revision": pushed["result"]["outcome"]["revision"],
                 "pull_result": pulled["result"], "terminal_replayed": True}
        if args.compatibility_root:
            baseline = {"version": 1, "scope": "same-path-persisted-compatibility-baseline",
                        "limitations": ["Paths are identity-bearing; retain workspace in place.",
                                        "Terminal build and committed pull receipts only; accepted nonterminal fixtures not covered."],
                        "workspace": str(temp), "store": str(store), "state": str(state),
                        "push_declaration": str(declaration), "push_attempt": attempt,
                        "pull_declaration": str(pull_decl), "pull_attempt": pull_attempt,
                        "push_result": replay["result"], "pull_result": pulled["result"],
                        "producer_bundle_manifest_sha256": hashlib.sha256((bundle / "bundle.json").read_bytes()).hexdigest()}
            baseline_path = temp / "compatibility.json"
            baseline_path.write_text(json.dumps(baseline, indent=2) + "\n")
            baseline_path.chmod(0o600)
    path = bundle / "verification.json"
    path.write_text(json.dumps(proof, indent=2) + "\n")
    path.chmod(0o600)
    print(path)


if __name__ == "__main__":
    main()
