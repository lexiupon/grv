#!/usr/bin/env python3
"""Run ONE named live S3 gate, then prove exact UUID-owned prefix cleanup.

Private config only. No blanket ignored expansion, parent deletion, IAM changes
or version-history cleanup. Core/adapter conformance binaries are not production
CLI packaging proof; qualify the production bundle separately.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import uuid

SPEC = importlib.util.spec_from_file_location('s3_proof', Path(__file__).with_name('validate-s3-production.py'))
PROOF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROOF)
GATES = {
    'primitives': ['cargo', 'test', '--locked', '-p', 'grv-storage', '--features', 'cloud',
                   '--lib', 'cloud::tests::live_s3_conditional_multipart_contract', '--', '--ignored', '--exact', '--nocapture', '--test-threads=1'],
    'views': ['cargo', 'test', '--locked', '-p', 'grv-conformance', '--features', 'native-duckdb',
              '--test', 's3_views_cli', 'live_s3_views_multifile_refresh_atomic_checks_and_source_free_receipts',
              '--', '--ignored', '--exact', '--nocapture', '--test-threads=1'],
    'reader': ['cargo', 'test', '--locked', '-p', 'grv-conformance', '--features', 'native-duckdb',
               '--test', 's3_views_cli', 'live_s3_native_reader_exact_uri_validator_hash_fallback_and_changed_hash_refusal',
               '--', '--ignored', '--exact', '--nocapture', '--test-threads=1'],
    'external-build': ['cargo', 'test', '--locked', '-p', 'grv-conformance', '--features', 'native-duckdb',
                      '--test', 's3_build_cli', 'live_external_s3_build_fixed_view_inputs_self_base_accepted_export_restart_and_terminal_replay',
                      '--', '--ignored', '--exact', '--nocapture', '--test-threads=1'],
}


def owned_roots(log, parent):
    roots = set(re.findall(r's3://[^\s\x1b]+', log))
    result = set()
    for root in roots:
        if not root.startswith(parent + '/'):
            continue
        child = root[len(parent) + 1:].split('/')[0]
        try:
            identity = uuid.UUID(child)
        except ValueError:
            continue
        if str(identity) == child and identity.version == 4:
            result.add(parent + '/' + child)
    return sorted(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gate', choices=GATES, required=True)
    parser.add_argument('--upload-mode', choices=('multipart', 'single-put'), default='multipart')
    parser.add_argument('--object-bytes', type=int)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=1800)
    args = parser.parse_args()
    config = PROOF.PROOF.private_config()
    scope = config['s3']
    if args.object_bytes is not None and (args.gate != 'primitives' or not 1 <= args.object_bytes <= 5_000_000_000):
        raise SystemExit('large object size requires primitives and 1..5000000000 bytes')
    output = args.output.resolve()
    output.mkdir(mode=0o700, exist_ok=False)
    output.chmod(0o700)
    environment = dict(os.environ)
    environment.update(GRV_S3_TEST_ROOT=scope['root'], AWS_PROFILE=scope['profile'],
                       AWS_REGION=scope['region'], AWS_DEFAULT_REGION=scope['region'],
                       AWS_EC2_METADATA_DISABLED='true', AWS_PAGER='',
                       GRV_DUCKDB_S3_READ_PROFILE=scope['profile'],
                       GRV_S3_UPLOAD_MODE=args.upload_mode)
    # Exercise the actual default limit unless the caller explicitly overrides it.
    environment.pop('GRV_S3_TEST_OBJECT_BYTES', None)
    if args.object_bytes is not None:
        environment['GRV_S3_TEST_OBJECT_BYTES'] = str(args.object_bytes)
    log = output / 'gate.log'
    with log.open('x') as stream:
        log.chmod(0o600)
        process = subprocess.Popen(GATES[args.gate], env=environment, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            for kind in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(process.pid, kind)
                except ProcessLookupError:
                    pass
            process.wait()
            code = 124
    roots = owned_roots(log.read_text(errors='replace'), scope['root'])
    runner = PROOF.PROOF.Runner(output, environment)
    result = {'version': 1, 'gate': args.gate, 'upload_mode': args.upload_mode,
              'object_bytes': args.object_bytes, 'command': GATES[args.gate],
              'exit_code': code, 'ok': code == 0 and len(roots) == 1, 'cleanup': [],
              'limitations': ['Same explicitly configured writer/reader profile, not independent least privilege.',
                              'Conformance harness, not packaged production CLI proof.',
                              'No version-history reclamation or remote writer-stop claim.']}
    for root in roots:
        cloud_args = argparse.Namespace(write_root=root, profile=scope['profile'], region=scope['region'],
                                        upload_mode='multipart')
        try:
            proof = PROOF.Cloud(cloud_args, runner).cleanup()
            result['cleanup'].append({'owned_root': root, **proof})
        except Exception:
            result['ok'] = False
            result['cleanup'].append({'owned_root': root, 'absence_proved': False})
    PROOF.PROOF.private_json(output / 'result.json', result)
    print(output / 'result.json')
    raise SystemExit(0 if result['ok'] else 1)


if __name__ == '__main__':
    main()
