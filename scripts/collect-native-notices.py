#!/usr/bin/env python3
"""Collect conservative DuckDB source notice superset; never certifies completeness.

Requires the pinned source archive plus built native library and compile database.
Official downloaded httpfs/aws dependency closure is a separate unresolved gate.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

ARCHIVE_SHA = '1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b'


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('source', 'archive', 'native-library', 'compile-commands', 'output'):
        parser.add_argument('--' + flag, required=True, type=Path)
    args = parser.parse_args()
    if digest(args.archive) != ARCHIVE_SHA:
        raise SystemExit('source archive differs from pinned DuckDB1.5.6')
    source = args.source.resolve(strict=True)
    output = args.output.absolute()
    if output.exists() or output.is_symlink():
        raise SystemExit('fresh output required')
    output.mkdir(mode=0o700)
    paths = [source / 'LICENSE']
    paths += [p for p in (source / 'third_party').rglob('*') if p.is_file() and
              (any(term in p.name.upper() for term in ('LICENSE', 'COPYING', 'NOTICE', 'COPYRIGHT'))
               or p.name == 'AUTHORS')]
    notices = []
    for path in sorted(set(paths)):
        if path.is_symlink():
            raise SystemExit('source notice symlink refused')
        relative = path.relative_to(source)
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copyfile(path, destination)
        destination.chmod(0o600)
        notices.append({'path': str(relative), 'sha256': digest(destination)})
    # Capture exact source-file identities for compilation and indicate header
    # attribution/link analysis still required; never infer complete by count.
    commands = json.loads(args.compile_commands.read_text())
    inputs = sorted({entry['file'] for entry in commands})
    compiled = {str(Path(p).relative_to(source)): digest(Path(p)) for p in inputs
                if Path(p).is_relative_to(source) and Path(p).is_file()}
    inventory = {
        'version': 1, 'scope': 'conservative-vendored-source-notice-superset',
        'complete': False, 'source_archive_sha256': ARCHIVE_SHA,
        'native_library_sha256': digest(args.native_library),
        'compile_commands_sha256': digest(args.compile_commands),
        'compiled_source_sha256': compiled, 'notice_files': notices,
        'gaps': ['Header-embedded notices/attribution and required upstream NOTICE recovery.',
                 'Actual linked/header-only component closure not certified by compile filenames.',
                 'Catch/IMDB relevance and absent notices must be reviewed.',
                 'Official signed httpfs/aws bundled dependency inventory remains separate.'],
    }
    (output / 'inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')
    (output / 'inventory.json').chmod(0o600)
    print(json.dumps({'notice_files': len(notices), 'compiled_sources': len(compiled),
                      'complete': False, 'output': str(output)}))


if __name__ == '__main__':
    main()
