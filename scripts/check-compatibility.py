#!/usr/bin/env python3
"""Replay a retained same-path persisted baseline with an explicit candidate bundle.

Local-only; no auth/service effect. Leaves compatibility data for later releases.
Does not relocate identity-bearing paths or claim accepted-incomplete coverage.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text())
    bundle = args.bundle.resolve(strict=True)
    env = {k: v for k, v in os.environ.items() if k in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL')}
    env['GRV_ADAPTERS_DIR'] = str(bundle / 'adapters')
    cli = str(bundle / 'bin/grv')
    results = {}
    for mode in ('push', 'pull'):
        command = [cli, '--json', mode, '--grv', baseline['store'], '--decl',
                   baseline[f'{mode}_declaration'], '--state', baseline['state'],
                   '--attempt', baseline[f'{mode}_attempt']]
        child = subprocess.run(command, env=env, capture_output=True, timeout=120)
        value = json.loads(child.stdout)
        if child.returncode or value.get('ok') is not True:
            raise SystemExit('compatibility replay refused: ' + mode)
        expected = dict(baseline[f'{mode}_result'], replayed=True)
        if value['result'] != expected:
            raise SystemExit('compatibility immutable result mismatch: ' + mode)
        results[mode] = {'immutable_replayed': True}
    proof = {'version': 1, 'baseline_sha256': hashlib.sha256(args.baseline.read_bytes()).hexdigest(),
             'candidate_bundle_sha256': hashlib.sha256((bundle / 'bundle.json').read_bytes()).hexdigest(),
             'results': results, 'limitations': baseline['limitations']}
    with args.output.open('x') as output:
        os.chmod(output.name, 0o600)
        output.write(json.dumps(proof, indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    main()
