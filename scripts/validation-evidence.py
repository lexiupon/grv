#!/usr/bin/env python3
"""Index/check validation evidence. Never runs tests, authenticates or approves release."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import uuid

ROOT = Path(__file__).resolve().parent.parent
CATALOG = ROOT / 'spec/release-validation/catalog.json'
HISTORY = ROOT / 'spec/release-validation/evidence/first-pass.json'
GATES = {f'G{i}' for i in range(1, 7)}
DETAILS = {'command', 'assertions', 'limitations', 'cleanup', 'environment',
           'binary_sha256', 'fault'}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def relative_file(name):
    path = Path(name)
    if path.is_absolute() or '..' in path.parts:
        raise ValueError('evidence paths must be repository-relative')
    resolved = (ROOT / path).resolve()
    if not resolved.is_relative_to(ROOT) or not resolved.is_file():
        raise ValueError(f'missing or escaped file: {name}')
    return resolved


def load_catalog():
    catalog = json.loads(CATALOG.read_text())
    if catalog['version'] != 1:
        raise ValueError('unsupported catalog version')
    entries = catalog['tests']
    ids = [entry['id'] for entry in entries]
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate catalog ID')
    for entry in entries:
        if not entry['command'] or not entry['limits'] or not entry['triggers']:
            raise ValueError(f'incomplete catalog entry: {entry["id"]}')
        if not set(entry['gates']).issubset(GATES):
            raise ValueError('unknown gate')
        for path in entry['test_files']:
            relative_file(path)
    return {entry['id']: entry for entry in entries}


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError('timestamps must use UTC timezone')
    return parsed


def check_record(record, catalog):
    if record['version'] != 1 or record['catalog_id'] not in catalog:
        raise ValueError('unknown record version/catalog ID')
    if record['result'] not in ('passed', 'failed'):
        raise ValueError('execution records must be passed or failed')
    code = record['exit_code']
    if code is not None and (not isinstance(code, int) or
                            (record['result'] == 'passed') != (code == 0)):
        raise ValueError('result/exit code disagree')
    if record['provenance'] not in ('historical-import', 'recorded-after-execution'):
        raise ValueError('unknown provenance')
    if record['provenance'] != 'historical-import':
        if code is None or not record['command']:
            raise ValueError('new records need command and exit code')
        if timestamp(record['finished_at']) < timestamp(record['started_at']):
            raise ValueError('reversed run interval')
        if not DETAILS.issubset(record):
            raise ValueError('missing run details')
    if not record['logs']:
        raise ValueError('execution record needs a retained log')
    for blob in record['logs']:
        if digest(relative_file(blob['path'])) != blob['sha256']:
            raise ValueError(f'changed log: {blob["path"]}')
    # Source/binary hashes refer to a past snapshot, not today's checkout.
    # Validate syntax without incorrectly requiring equality to current files.
    for field in ('source_sha256', 'binary_sha256'):
        for value in (record.get(field) or {}).values():
            if len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
                raise ValueError(f'invalid {field} digest')


def snapshot():
    # Include versioned and untracked nonignored files, exclude .git/target/
    # artifacts/auth stores by gitignore. Store names/hashes, never contents.
    paths = subprocess.check_output(
        ['git', 'ls-files', '-co', '--exclude-standard', '-z'], cwd=ROOT
    ).decode().split('\0')
    hashes = {}
    for name in sorted(set(paths) - {''}):
        path = ROOT / name
        if path.is_file() and not path.is_symlink():
            hashes[name] = digest(path)
    return hashes


def record_run(args, catalog):
    if args.catalog_id not in catalog:
        raise ValueError('unknown catalog ID')
    details = json.loads(Path(args.details).read_text())
    if set(details) != DETAILS:
        raise ValueError(f'details keys must be exactly: {sorted(DETAILS)}')
    path = Path(args.log).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError('log must be in repository artifact workspace')
    record = {
        'version': 1, 'run_id': str(uuid.uuid4()), 'catalog_id': args.catalog_id,
        'catalog_sha256': digest(CATALOG), 'provenance': 'recorded-after-execution',
        'result': args.result, 'exit_code': args.exit_code,
        'started_at': args.started_at, 'finished_at': args.finished_at,
        'recorded_at': datetime.now(timezone.utc).isoformat(),
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        'source_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
        'source_sha256': snapshot(),
        'recording_host': {'system': platform.system(), 'machine': platform.machine()},
        'logs': [{'path': path.relative_to(ROOT).as_posix(), 'sha256': digest(path)}],
        **details,
    }
    check_record(record, catalog)
    directory = Path(args.out_dir).resolve()
    artifacts = ROOT / 'artifacts'
    if not directory.is_relative_to(artifacts.resolve()):
        raise ValueError('new raw run records must stay under ignored artifacts/')
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination = directory / f'{record["run_id"]}.json'
    with destination.open('x') as output:
        destination.chmod(0o600)
        output.write(json.dumps(record, indent=2) + '\n')
    print(destination.relative_to(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    check = sub.add_parser('check')
    check.add_argument('--runs', type=Path)
    run = sub.add_parser('record')
    for name in ('catalog-id', 'log', 'started-at', 'finished-at', 'details', 'out-dir'):
        run.add_argument('--' + name, required=True)
    run.add_argument('--result', choices=('passed', 'failed'), required=True)
    run.add_argument('--exit-code', type=int, required=True)
    args = parser.parse_args()
    catalog = load_catalog()
    if args.action == 'record':
        record_run(args, catalog)
        return
    history = json.loads(HISTORY.read_text())
    if history['version'] != 1:
        raise ValueError('unsupported historical index version')
    histories = [history]
    for index in sorted(HISTORY.parent.glob('*.json')):
        if index != HISTORY:
            additional = json.loads(index.read_text())
            if additional.get('version') != 1:
                raise ValueError('unsupported evidence index version')
            histories.append(additional)
    records = []
    for imported in histories:
        for blob in imported.get('supporting_files', []):
            if digest(relative_file(blob['path'])) != blob['sha256']:
                raise ValueError(f'changed supporting evidence: {blob["path"]}')
        records.extend(imported['runs'])
    if args.runs:
        records.extend(json.loads(path.read_text()) for path in sorted(args.runs.glob('*.json')))
    ids = [record['run_id'] for record in records]
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate run ID')
    for record in records:
        check_record(record, catalog)
    print(json.dumps({'catalog_tests': len(catalog), 'records_checked': len(records),
                      'log_digests': 'matched', 'release_readiness': 'not assessed'}, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error))
