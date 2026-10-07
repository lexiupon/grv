#!/usr/bin/env python3
"""Execute one explicit catalog command and retain pre/post provenance privately.

No shell interpretation or automatic --ignored expansion. Live tests require
--allow-live; this flag does not expand provider authorization. Never pass
secrets in argv/environment entries. Inspect diagnostic logs before archiving.
"""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import signal
import uuid

SPEC = importlib.util.spec_from_file_location('validation_evidence', Path(__file__).with_name('validation-evidence.py'))
EVIDENCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EVIDENCE)
ROOT = EVIDENCE.ROOT


def now():
    return datetime.now(timezone.utc).isoformat()


def execute(command, output, timeout):
    """Own one process group; stop inherited descendants before sealing a log.

    Detached/escaped workers and remote effects still need separate review.
    This is harness cleanup, never proof of a GRV/cloud writer-stop contract.
    """
    process = subprocess.Popen(command, cwd=ROOT, stdout=output,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        return process.wait(timeout=timeout), False
    except subprocess.TimeoutExpired:
        # Kill the exact owned group, even if the leader exited meanwhile.
        for kind in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, kind)
            except ProcessLookupError:
                pass
        process.wait()
        return 124, True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog-id', required=True)
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--details', type=Path, required=True)
    parser.add_argument('--allow-live', action='store_true')
    parser.add_argument('--timeout', type=int, default=1800)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    catalog = EVIDENCE.load_catalog()
    entry = catalog[args.catalog_id]
    if entry['live'] and not args.allow_live:
        raise SystemExit('live catalog execution requires --allow-live and existing scoped authorization')
    command = args.command
    if command and command[0] == '--':
        command = command[1:]
    if not command:
        raise SystemExit('explicit command argv required; catalog recipes are not executed implicitly')
    details = json.loads(args.details.read_text())
    if set(details) != EVIDENCE.DETAILS - {'command'}:
        raise SystemExit('details need assertions/limitations/cleanup/environment/binary_sha256/fault only')
    destination = args.out_dir.absolute()
    if not destination.resolve().is_relative_to((ROOT / 'artifacts').resolve()):
        raise SystemExit('private logs must be under ignored artifacts/')
    # Reject aliases before creating even the evidence directory.
    for parent in (destination, *destination.parents):
        if parent.is_symlink():
            raise SystemExit('evidence directory symlink refused')
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_id = str(uuid.uuid4())
    log = destination / f'{args.catalog_id}-{run_id}.log'
    pre = EVIDENCE.snapshot()
    started = now()
    timed_out = False
    with log.open('x') as output:
        log.chmod(0o600)
        code, timed_out = execute(command, output, args.timeout)
    finished = now()
    post = EVIDENCE.snapshot()
    changed = pre != post
    limitations = list(details['limitations'])
    if changed:
        limitations.append('Repository snapshot changed during execution; not frozen-candidate evidence.')
    if timed_out:
        limitations.append('Command timed out; owned process group terminated before log sealing. Escaped descendants/remote effects require separate cleanup review.')
    record = {
        'version': 1, 'run_id': run_id, 'catalog_id': args.catalog_id,
        'catalog_sha256': EVIDENCE.digest(EVIDENCE.CATALOG),
        'provenance': 'recorded-after-execution', 'result': 'passed' if code == 0 else 'failed',
        'exit_code': code, 'started_at': started, 'finished_at': finished,
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        'source_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
        'source_sha256': pre, 'source_changed_during_execution': changed,
        'post_source_sha256': post if changed else None,
        'command': command, **details, 'limitations': limitations,
        'logs': [{'path': log.relative_to(ROOT).as_posix(), 'sha256': EVIDENCE.digest(log)}],
    }
    EVIDENCE.check_record(record, catalog)
    with (destination / f'{run_id}.json').open('x') as output:
        os.chmod(output.name, 0o600)
        output.write(json.dumps(record, indent=2) + '\n')
    print(json.dumps({'result': record['result'], 'exit_code': code,
                      'record': str(destination / f'{run_id}.json'),
                      'source_changed_during_execution': changed}))
    raise SystemExit(code)


if __name__ == '__main__':
    main()
