#!/usr/bin/env python3
"""Offline JSON-to-TSV manifest adapter. Never discovers credentials or accesses cloud."""
import hashlib
import json
import pathlib
import sys


def main():
    source, target = map(pathlib.Path, sys.argv[1:])
    if source.stat().st_size > 65536:
        raise ValueError("manifest exceeds 64KiB")
    raw = source.read_text(encoding="utf-8")
    if not raw.lstrip().startswith("{"):
        target.write_text(raw, encoding="utf-8")
        return
    manifest = json.loads(raw)
    if manifest.get("project") != "entit-finance-prod":
        raise ValueError("manifest project must be entit-finance-prod")
    objects = manifest["objects"]
    if not isinstance(objects, list) or not 5 <= len(objects) <= 6:
        raise ValueError("five fixture roles required; optional sixth special object")
    lines = []
    for obj in objects:
        name = obj["name"]
        if name not in {"old.parquet", "new.parquet", "empty.parquet", "old-no-stats.parquet", "new-no-stats.parquet", "special.parquet"}:
            raise ValueError("unsupported fixture name")
        path = pathlib.Path(obj["local_path"])
        if not path.is_absolute() or not path.is_file() or path.stat().st_size > 20000000:
            raise ValueError("invalid local fixture")
        size = obj["size"]
        if isinstance(size, bool) or not isinstance(size, (int, str)) or str(size) != str(path.stat().st_size):
            raise ValueError("size mismatch")
        generation = obj["generation"]
        if not isinstance(generation, str) or not generation.isascii() or not generation.isdecimal():
            raise ValueError("generation must be a decimal string")
        if "sha256" in obj:
            digest = hashlib.sha256()
            with path.open("rb") as fixture:
                for block in iter(lambda: fixture.read(65536), b""):
                    digest.update(block)
            if digest.hexdigest() != obj["sha256"]:
                raise ValueError("local SHA256 mismatch")
        fields = [name.removesuffix(".parquet"), obj["uri"], str(path), str(size), generation]
        if any(not isinstance(field, str) or any(c in field for c in "\t\r\n\0") for field in fields):
            raise ValueError("invalid manifest field")
        lines.append("\t".join(fields))
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Do not echo manifest contents, URIs or file paths.
        sys.exit("Invalid local manifest; expected JSON project/objects or five-field TSV")
