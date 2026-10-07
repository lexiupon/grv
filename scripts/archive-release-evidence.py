#!/usr/bin/env python3
"""Create a private local evidence archive and hashed inventory, never upload.

Exact input files are copied into a tar without dereferencing symlinks. Raw
logs must be reviewed for secrets before external retention. Local archive is
NOT durable backup. Identity-bearing compatibility paths are not rewritten.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tarfile


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--include', type=Path, action='append', required=True,
                        help='exact tree/file under root; repeat, no symlinks')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    output = args.output.absolute()
    if not output.parent.resolve().is_relative_to(root):
        raise SystemExit('archive output escapes root')
    if output.exists() or output.is_symlink():
        raise SystemExit('fresh archive required')
    files = {}
    for tree in args.include:
        tree = tree.resolve(strict=True) if not tree.is_symlink() else tree.absolute()
        if tree.is_symlink() or not tree.resolve(strict=True).is_relative_to(root):
            raise SystemExit('include escapes root or is a symlink')
        entries = tree.rglob('*') if tree.is_dir() else [tree]
        for path in entries:
            if path.is_symlink():
                raise SystemExit('archive symlink refused')
            if path.is_file():
                if path == output or output.is_relative_to(tree):
                    raise SystemExit('archive output must be outside included trees')
                files[str(path.relative_to(root))] = digest(path)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as target, tarfile.open(fileobj=target, mode='w:gz') as archive:
        for relative, expected in sorted(files.items()):
            path = root / relative
            if digest(path) != expected:
                raise SystemExit('file changed before archiving')
            archive.add(path, arcname=relative, recursive=False)
    report = {'version': 1, 'archive': str(output), 'sha256': digest(output), 'files': files,
              'bytes': output.stat().st_size, 'durable_backup': False,
              'limitations': ['Local private archive only; external destination/retention not approved.',
                              'Raw input logs require secret review before external upload.',
                              'Compatibility fixtures retain original identity-bearing paths; copying does not rebind identities.']}
    receipt = Path(str(output) + '.json')
    with receipt.open('x') as target:
        json.dump(report, target, indent=2)
        target.write('\n')
    receipt.chmod(0o600)
    print(receipt)


if __name__ == '__main__':
    main()
