#!/usr/bin/env python3
"""Build a local development or unqualified candidate bundle; never publish."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def protected_parent(path):
    # An explicitly configured artifacts archive alias is allowed; arbitrary
    # writable/symlink installation ancestors remain refused.
    if (ROOT / "artifacts").is_symlink() and path.absolute().is_relative_to(ROOT / "artifacts"):
        import importlib.util
        spec = importlib.util.spec_from_file_location("artifact_evidence", ROOT / "scripts/validation-evidence.py")
        evidence = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(evidence)
        approved = evidence.artifact_root()
        if not path.resolve().is_relative_to(approved):
            raise SystemExit("bundle path escapes approved artifact archive")
        path = path.resolve()
    for ancestor in (path, *path.parents):
        metadata = ancestor.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
            raise SystemExit("bundle parent must have protected directory ancestors")
        if metadata.st_uid not in (0, os.geteuid()):
            raise SystemExit("bundle ancestor has an untrusted owner")


def copy(source, destination, executable=False):
    if not source.is_file() or source.is_symlink():
        raise SystemExit(f"required regular build artifact missing: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    shutil.copyfile(source, destination)
    destination.chmod(0o700 if executable else 0o600)


def dependency_notices(metadata, packages, staging):
    """Copy upstream notices and report missing files without inferring licenses."""
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    selected = set()
    pending = [packages[name]["id"] for name in
               ("grv", "grv-adapter-duckdb", "grv-adapter-salesforce")]
    while pending:
        package_id = pending.pop()
        if package_id in selected:
            continue
        selected.add(package_id)
        pending.extend(dependency["pkg"] for dependency in nodes[package_id]["deps"]
                       if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]))
    inventory = []
    upstream = {(entry["name"], entry["version"]): entry
                for entry in json.loads((ROOT / "notices/upstream.json").read_text())}
    for package in sorted(metadata["packages"], key=lambda p: (p["name"], p["version"])):
        if package["id"] not in selected or package["source"] is None:
            continue
        source = Path(package["manifest_path"]).parent
        files = {path for path in source.iterdir()
                 if path.is_file() and (path.name.upper().startswith(
                     ("LICENSE", "COPYING", "NOTICE", "UNLICENSE"))
                     or path.name.upper().endswith("-LICENSE"))}
        if package.get("license_file"):
            path = Path(package["license_file"])
            files.add(path if path.is_absolute() else source / path)
        notices = []
        upstream_sources = []
        for path in sorted(files):
            if not path.is_file() or path.is_symlink():
                raise SystemExit(f"dependency notice is not a regular file: {path}")
            relative = path.resolve().relative_to(source.resolve())
            destination = (Path("notices/dependencies") /
                           f'{package["name"]}-{package["version"]}' / relative)
            copy(path, staging / destination)
            notices.append(str(destination))
        if not notices and (package["name"], package["version"]) in upstream:
            entry = upstream[(package["name"], package["version"])]
            vcs = json.loads((source / ".cargo_vcs_info.json").read_text())
            if vcs["git"]["sha1"] != entry["vcs_commit"]:
                raise SystemExit("upstream dependency notices do not match the crate commit")
            for notice in entry["files"]:
                path = (ROOT / "notices" / notice["path"]).resolve(strict=True)
                path.relative_to((ROOT / "notices/upstream").resolve())
                if digest(path) != notice["sha256"]:
                    raise SystemExit("upstream dependency notice checksum differs")
                destination = (Path("notices/dependencies") /
                               f'{package["name"]}-{package["version"]}' / path.name)
                copy(path, staging / destination)
                notices.append(str(destination))
                upstream_sources.append({"url": notice["url"], "sha256": notice["sha256"]})
        inventory.append({"name": package["name"], "version": package["version"],
                          "license_expression": package["license"],
                          "source": package["source"], "files": notices,
                          "upstream_sources": upstream_sources,
                          "notice_files_found": bool(notices)})
    destination = staging / "notices/dependencies.json"
    destination.write_text(json.dumps(inventory, indent=2) + "\n")
    destination.chmod(0o600)
    return [package["name"] + "@" + package["version"]
            for package in inventory if not package["notice_files_found"]]


NOTICE_POLICY = "pinned-upstream-plus-conservative-third-party-notices"


def notice_review(path, scope, staging=None):
    """Bind reviewed conservative notice payloads, not an exact linked SBOM."""
    review = json.loads(path.read_text())
    if (review.get("version") != 1 or review.get("policy") != NOTICE_POLICY
            or review.get("scope") != scope or review.get("reviewed") is not True
            or review.get("unresolved") or not review.get("notice_files")):
        raise SystemExit(f"{scope} notice policy review is incomplete")
    for notice in review["notice_files"]:
        relative = Path(notice["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise SystemExit("unsafe reviewed notice path")
        source = ROOT / "notices" / relative
        if not source.resolve().is_relative_to((ROOT / "notices").resolve()):
            raise SystemExit("notice path escapes inventory")
        if source.is_symlink() or digest(source) != notice["sha256"]:
            raise SystemExit("reviewed notice checksum differs")
        if staging:
            copy(source, staging / "notices" / relative)
    if staging:
        copy(path, staging / "notices" / path.name)
    return review


def require_distribution_ready(status):
    """Fail closed on user-reviewed notice policy, not an exact upstream SBOM."""
    if status.get("dependency_notice_gaps"):
        raise SystemExit("bundle has Rust dependency notice gaps")
    review = status.get("native_notice_policy_review") or {}
    if (review.get("policy") != NOTICE_POLICY or review.get("native_engine") is not True):
        raise SystemExit("native DuckDB notice policy review is incomplete")
    if review.get("signed_extensions") is not True:
        raise SystemExit("native extension notice policy review is incomplete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-dir", type=Path, required=True)
    parser.add_argument("--extensions-dir", type=Path, required=True,
                        help="verified signed httpfs/aws root containing platform subdirectories")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-notices", type=Path,
                        help="optional conservative source-notice inventory bound to the guard digest; not completeness proof")
    parser.add_argument("--profile", choices=("dev", "release"), default="release")
    parser.add_argument("--skip-build", action="store_true", help="verify and package existing host artifacts")
    parser.add_argument("--require-distribution-ready", action="store_true",
                        help="refuse missing Rust notices or unreviewed pinned/conservative native notice payloads; not release approval")
    args = parser.parse_args()
    native = args.native_dir.resolve(strict=True)
    output = args.output.absolute()
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    protected_parent(output.parent)
    if output.exists() or output.is_symlink():
        raise SystemExit("bundle output already exists; choose a fresh path")
    host = platform.system()
    machine = platform.machine()
    if host not in ("Darwin", "Linux") or machine not in ("arm64", "aarch64", "x86_64"):
        raise SystemExit("bundle supports only Linux/macOS on x86-64/ARM64")
    library = "libgrv_duckdb_guard.dylib" if host == "Darwin" else "libgrv_duckdb_guard.so"
    if not (native / library).is_file():
        raise SystemExit("pinned native guard library is missing")
    env = dict(os.environ, GRV_DUCKDB_NATIVE_LIB_DIR=str(native),
               GRV_DUCKDB_BUNDLE_ONLY_RPATH="1")
    if not args.skip_build:
        # Compile all three artifacts in one invocation so a later default build
        # cannot silently replace the native adapter executable.
        subprocess.run(["cargo", "build", "--locked", "--offline", "--profile", args.profile,
                        "-p", "grv", "-p", "grv-adapter-duckdb", "-p", "grv-adapter-salesforce",
                        "--features", "grv-adapter-duckdb/native"], cwd=ROOT, env=env, check=True)
    target = next(line.split(": ", 1)[1] for line in
                  subprocess.check_output(["rustc", "-vV"], text=True).splitlines()
                  if line.startswith("host: "))
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--offline", "--format-version", "1",
         "--filter-platform", target, "--features", "grv-adapter-duckdb/native"], cwd=ROOT))
    packages = {package["name"]: package for package in metadata["packages"]}
    artifacts = Path(metadata["target_directory"]) / ("debug" if args.profile == "dev" else "release")
    staging = Path(tempfile.mkdtemp(prefix=".grv-bundle-", dir=output.parent))
    try:
        copy(artifacts / "grv", staging / "bin/grv", True)
        for name in ("duckdb", "salesforce"):
            package = staging / "adapters" / name
            entrypoint = f"bin/grv-adapter-{name}"
            copy(artifacts / f"grv-adapter-{name}", package / entrypoint, True)
            version = packages[f"grv-adapter-{name}"]["version"]
            manifest = (f'name = "{name}"\nversion = "{version}"\ninterface_versions = [1]\n'
                        f'binding_schema_version = 1\nentrypoint = "{entrypoint}"\n'
                        f'entrypoint_sha256 = "{digest(package / entrypoint)}"\n')
            (package / "adapter.toml").write_text(manifest)
            (package / "adapter.toml").chmod(0o600)
        native_build = json.loads((ROOT / "notices/native-build.json").read_text())
        native_build_bound = native_build.get("native_library_sha256") == digest(native / library)
        if args.require_distribution_ready and not native_build_bound:
            raise SystemExit("native build record bound to another guard library")
        for relative, expected in native_build["guard_sources_sha256"].items():
            path = ROOT / relative
            if not path.resolve().is_relative_to(ROOT) or digest(path) != expected:
                raise SystemExit("guard sources differ from recorded native build")
        copy(native / library, staging / "adapters/duckdb/bin/lib" / library)
        if native_build_bound:
            copy(ROOT / "notices/native-build.json", staging / "notices/native-build.json")
        extension_inventory = None
        if args.extensions_dir:
            extension_platform = (("osx_" if host == "Darwin" else "linux_")
                                  + ("arm64" if machine in ("arm64", "aarch64") else "amd64"))
            extension_input = args.extensions_dir.resolve(strict=True)
            subprocess.run(["python3", str(ROOT / "scripts/fetch-duckdb-extensions.py"),
                            "--output", str(extension_input), "--platform", extension_platform,
                            "--verify-only"], cwd=ROOT, check=True)
            extension_inventory = json.loads((ROOT / "notices/native-extensions.json").read_text())
            for name in ("httpfs", "aws"):
                copy(extension_input / extension_platform / f"{name}.duckdb_extension",
                     staging / "adapters/duckdb/bin/extensions" / f"{name}.duckdb_extension")
            copy(ROOT / "notices/native-extensions.json", staging / "notices/native-extensions.json")
            notices = [extension_inventory["signature"]["public_key"],
                       extension_inventory["signature"]["signing_source_license"]]
            for extension in extension_inventory["extensions"]:
                notices.extend(extension["files"])
            for notice in notices:
                source = ROOT / "notices" / notice["path"]
                if digest(source) != notice["sha256"]:
                    raise SystemExit("native extension notice checksum differs")
                copy(source, staging / "notices" / notice["path"])
        # The native shim includes the pinned DuckDB implementation.
        copy(native.parent / "duckdb-1.5.6/LICENSE", staging / "notices/DuckDB-LICENSE.txt")
        copy(ROOT / "LICENSE", staging / "notices/GRV-LICENSE.txt")
        native_notice_inventory = None
        if args.native_notices:
            notice_root = args.native_notices.resolve(strict=True)
            native_notice_inventory = json.loads((notice_root / "inventory.json").read_text())
            if native_notice_inventory.get("native_library_sha256") != digest(native / library):
                raise SystemExit("native notice inventory bound to another library")
            if native_notice_inventory.get("scope") != "conservative-vendored-source-notice-superset":
                raise SystemExit("unsupported native notice inventory")
            for notice in native_notice_inventory["notice_files"]:
                relative = Path(notice["path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise SystemExit("unsafe native notice path")
                source = notice_root / relative
                if source.resolve().is_relative_to(notice_root) is False or digest(source) != notice["sha256"]:
                    raise SystemExit("native notice identity differs")
                copy(source, staging / "notices/native-source" / relative)
            copy(notice_root / "inventory.json", staging / "notices/native-source/inventory.json")
        engine_review = notice_review(ROOT / "notices/native-source-review.json", "native-engine", staging)
        if engine_review.get("source_archive_sha256") != "1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b":
            raise SystemExit("native notice source release identity differs")
        extension_review = notice_review(ROOT / "notices/extension-third-party-review.json", "signed-extensions", staging)
        required_extensions = {entry["sha256"] for entry in extension_inventory["artifacts"]
                               if entry["platform"] == extension_platform}
        reviewed_extensions = {entry["sha256"] for entry in extension_review["artifacts"]
                               if entry["platform"] == extension_platform}
        extensions_reviewed = required_extensions == reviewed_extensions
        if args.require_distribution_ready and not extensions_reviewed:
            raise SystemExit("extension notice review is for another artifact set")
        missing_notices = dependency_notices(metadata, packages, staging)
        # Do not let ambient native loader overrides hide a broken bundle.
        environment = {key: value for key, value in os.environ.items()
                       if key not in ("DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH",
                                      "LD_LIBRARY_PATH", "GRV_DUCKDB_NATIVE_LIB_DIR")}
        environment["GRV_ADAPTERS_DIR"] = str(staging / "adapters")
        capabilities = {}
        for name in ("duckdb", "salesforce"):
            result = json.loads(subprocess.check_output([str(staging / "bin/grv"), "--json", "adapter", name, "capabilities"], env=environment, cwd=ROOT))
            if not result["ok"]:
                raise SystemExit("packaged adapter handshake failed")
            capabilities[name] = result["result"]["adapter"]
        if not capabilities["duckdb"]["capabilities"]["pull"]:
            raise SystemExit("DuckDB artifact is not the native local-pull build")
        files = {str(path.relative_to(staging)): digest(path)
                 for path in sorted(staging.rglob("*")) if path.is_file()}
        macos_minimums = {}
        if host == "Darwin":
            import re
            for artifact in [staging / "bin/grv", staging / "adapters/duckdb/bin/grv-adapter-duckdb",
                             staging / "adapters/salesforce/bin/grv-adapter-salesforce",
                             staging / "adapters/duckdb/bin/lib" / library,
                             *sorted((staging / "adapters/duckdb/bin/extensions").glob("*.duckdb_extension"))]:
                commands = subprocess.check_output(["otool", "-l", str(artifact)], text=True)
                # LC_BUILD_VERSION reports "minos"; older x86-64 objects (for
                # example prebuilt extensions) use LC_VERSION_MIN_MACOSX instead.
                match = (re.search(r"\bminos\s+([0-9.]+)", commands)
                         or re.search(r"cmd LC_VERSION_MIN_MACOSX\b[^\n]*\n(?:[^\n]*\n)*?\s*version\s+([0-9.]+)",
                                      commands))
                if not match:
                    raise SystemExit("Mach-O minimum OS version missing")
                macos_minimums[str(artifact.relative_to(staging))] = match.group(1)
        status = {"format_version": 1,
                  "delivery": "development" if args.profile == "dev" else "candidate-unqualified",
                  "complete_client_v1": False,
                  "native_dependency_notice_inventory_complete": False,
                  "native_notice_policy_review": {"policy": NOTICE_POLICY,
                      "native_engine": native_build_bound, "signed_extensions": extensions_reviewed,
                      "exact_linked_dependency_graph_claimed": False,
                      "review_files": ["notices/native-source-review.json", "notices/extension-third-party-review.json"]},
                  "native_source_notice_inventory_included": native_notice_inventory is not None,
                  "source_commit": subprocess.check_output(
                      ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  "source_dirty": bool(subprocess.check_output(
                      ["git", "status", "--porcelain"], cwd=ROOT)),
                  "toolchain": subprocess.check_output(["rustc", "-vV"], text=True),
                  "platform": host, "architecture": machine, "profile": args.profile,
                  "build_host_version": platform.mac_ver()[0] if host == "Darwin" else platform.release(),
                  "macos_minimum_versions": macos_minimums,
                  "macos_minimum_version": max(macos_minimums.values(), key=lambda v: tuple(map(int, v.split(".")))) if macos_minimums else None,
                  "duckdb_version": "1.5.6", "parquet_version": "58.3.0",
                  "dependency_notice_gaps": missing_notices,
                  "native_extensions": None if extension_inventory is None else {
                      "platform": extension_platform,
                      "artifacts": [entry for entry in extension_inventory["artifacts"]
                                    if entry["platform"] == extension_platform],
                      "signature_verification": "verified",
                      "dependency_notice_inventory_complete": False},
                  "adapters": capabilities, "sha256": files}
        if args.require_distribution_ready:
            require_distribution_ready(status)
        (staging / "bundle.json").write_text(json.dumps(status, indent=2) + "\n")
        (staging / "bundle.json").chmod(0o600)
        staging.rename(output)
        print(output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


if __name__ == "__main__":
    main()
