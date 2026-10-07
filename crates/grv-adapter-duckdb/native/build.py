#!/usr/bin/env python3
"""Build the exact DuckDB 1.5.6 archive with GRV pre-callback guard sites.

No network request or dependency resolution occurs here. A different engine archive
or changed upstream binding site is an error, never a silently unguarded fallback.
"""
import argparse
import hashlib
import os
import pathlib
import subprocess
import tarfile
import json

ARCHIVE_HASH = "1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b"
SITES = {
    "src/function/cast/cast_function_set.cpp": (
        "fbf6a2c58ea9e5c19d081a3b68207c0be948089096f27eab0443fc4daeabea55",
        [("\tif (entry) {\n\t\tif (entry->bind_function) {", "\tif (entry) {\n\t\tGRVGuard::CustomCast(input.context);\n\t\tif (entry->bind_function) {")]),
    "src/catalog/catalog_entry_retriever.cpp": (
        "728fc8a3d04d77fd119b8ffcf5892e2ac9ff70b2f606226634969209fda4d51b",
        [("\tif (callback) {\n\t\t// Call the callback if it's set\n\t\tcallback(*result);\n\t}\n\treturn result;\n}\n\nvoid CatalogEntryRetriever::Inherit", "\tGRVGuard::Entry(context, *result);\n\tif (callback) {\n\t\t// Call the callback if it's set\n\t\tcallback(*result);\n\t}\n\treturn result;\n}\n\nvoid CatalogEntryRetriever::Inherit")]),
    "src/function/function_binder.cpp": (
        "34df3dad0f439dcb02147ba08c01bed4a0255b942a6a08037b60dac1eaa6588e",
        [("\t// Attempt to resolve template types, before we call", "\tGRVGuard::Scalar(context, bound_function);\n\t// Attempt to resolve template types, before we call"),
         ("\tResolveTemplateTypes(bound_function, children);\n\n\tunique_ptr<FunctionData> bind_info;\n\tif (bound_function.HasBindCallback())", "\tGRVGuard::Aggregate(context, bound_function);\n\tResolveTemplateTypes(bound_function, children);\n\n\tunique_ptr<FunctionData> bind_info;\n\tif (bound_function.HasBindCallback())")]),
    "src/planner/binder/tableref/bind_table_function.cpp": (
        "8da30dba60d794bbdf72ea9811f8dac4f26719166659ef28fc23ea22d5f8deaa",
        [("\tauto function_name = GetAlias(ref);", "\tGRVGuard::Table(context, table_function);\n\tauto function_name = GetAlias(ref);")]),
    "src/planner/binder/tableref/bind_basetableref.cpp": (
        "c17ef16e35ae299c1b2d857105555459c6bf8a65da66463f5da183f19521c1f6",
        [("BoundStatement Binder::BindWithReplacementScan(ClientContext &context, BaseTableRef &ref) {", "BoundStatement Binder::BindWithReplacementScan(ClientContext &context, BaseTableRef &ref) {\n\tGRVGuard::Replacement(context);")]),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=pathlib.Path, required=True)
    parser.add_argument("--build", type=pathlib.Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    if hashlib.sha256(args.archive.read_bytes()).hexdigest() != ARCHIVE_HASH:
        raise SystemExit("DuckDB archive hash mismatch: require official v1.5.6 source archive")
    args.build.mkdir(parents=True, exist_ok=True)
    source = args.build / "duckdb-1.5.6"
    if not source.exists():
        with tarfile.open(args.archive) as archive:
            archive.extractall(args.build, filter="data")
    # A reused build tree must still contain the exact pinned upstream sources.
    # Only the explicitly patched sites may differ from the archive.
    with tarfile.open(args.archive) as archive:
        for member in archive:
            if not member.isfile():
                continue
            relative = pathlib.PurePosixPath(member.name).relative_to("duckdb-1.5.6")
            if str(relative) in SITES:
                continue
            actual = source / str(relative)
            expected = archive.extractfile(member).read()
            if not actual.is_file() or hashlib.sha256(actual.read_bytes()).digest() != hashlib.sha256(expected).digest():
                raise SystemExit(f"pinned upstream source changed: {relative}")
    native = pathlib.Path(__file__).resolve().parent
    for relative, (expected, replacements) in SITES.items():
        path = source / relative
        backup = path.with_suffix(path.suffix + ".grv-original")
        original = backup.read_bytes() if backup.exists() else path.read_bytes()
        if hashlib.sha256(original).hexdigest() != expected:
            raise SystemExit(f"pinned binding source mismatch: {relative}")
        patched = original.decode()
        for old, new in replacements:
            if patched.count(old) != 1:
                raise SystemExit(f"native patch does not match exactly one binding site: {relative}")
            patched = patched.replace(old, new)
        patched = '#include "duckdb/main/grv_guard.hpp"\n' + patched
        if path.read_bytes() not in (original, patched.encode()):
            raise SystemExit(f"unexpected modification in patched source: {relative}")
        if not backup.exists():
            backup.write_bytes(original)
        if path.read_bytes() != patched.encode():
            path.write_text(patched)
    header = source / "src/include/duckdb/main/grv_guard.hpp"
    wanted_header = (native / "grv_guard.hpp").read_bytes()
    if not header.exists() or header.read_bytes() != wanted_header:
        header.write_bytes(wanted_header)
    # Pin data-expression builtins from reviewed source modules, including their
    # documented aliases. Catalog/secret/sequence/logging/debug helpers are
    # outside the v1 user query boundary; this is not a per-field or SQL-parser ban.
    scalar_names = {"constant_or_null", "error", "create_sort_key", "hash", "least", "greatest", "typeof", "get_type", "make_type", "can_cast_implicitly", "cast_to_type", "replace_type", "equi_width_bins", "is_histogram_other_bin", "current_date", "current_time", "current_localtimestamp", "current_localtime"}
    for base in (source / "src/function/scalar", source / "extension/core_functions/scalar"):
        for path in base.rglob("functions.json"):
            if path.parent.name in {"system", "sequence", "debug", "generic"}:
                continue
            for entry in json.loads(path.read_text()):
                if entry["name"] == "setseed":
                    continue
                scalar_names.add(entry["name"])
                scalar_names.update(entry.get("aliases", []))
    profile = source / "src/include/duckdb/main/grv_scalar_profile.hpp"
    profile_content = '#pragma once\n#include <set>\n#include <string>\nstatic const std::set<std::string> GRV_DATA_SCALARS = {' + ','.join(json.dumps(name) for name in sorted(scalar_names)) + '};\n'
    if not profile.exists() or profile.read_text() != profile_content:
        profile.write_text(profile_content)
    cmake_build = args.build / "cmake"
    # DuckDB's configure step rewrites unity files even when content is unchanged.
    # Avoid recompiling the full engine for an unchanged reproducibility check.
    configure_stamp = cmake_build / "grv-configure.sha256"
    configuration_hash = hashlib.sha256((native / "CMakeLists.txt").read_bytes() + str(source.resolve()).encode()).hexdigest()
    if not configure_stamp.exists() or configure_stamp.read_text() != configuration_hash:
        unity_files = {path: (path.read_bytes(), path.stat()) for path in cmake_build.rglob("ub_*.cpp")}
        subprocess.run(["cmake", "-S", str(native), "-B", str(cmake_build),
                        f"-DGRV_DUCKDB_SOURCE={source.resolve()}", "-DCMAKE_BUILD_TYPE=Release",
                        "-DCMAKE_CXX_FLAGS_RELEASE=-O0 -DNDEBUG"], check=True)
        for path, (old_content, old_stat) in unity_files.items():
            if path.exists() and path.read_bytes() == old_content:
                os.utime(path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
        configure_stamp.write_text(configuration_hash)
    subprocess.run(["cmake", "--build", str(cmake_build), "--target", "grv_duckdb_guard", "grv_native_proof", "grv_native_fixture", "grv_s3_limits_proof", "-j", str(args.jobs)], check=True)
    subprocess.run(["ctest", "--test-dir", str(cmake_build), "--output-on-failure"], check=True)


if __name__ == "__main__":
    main()
