#!/usr/bin/env python3
"""Audit exact release files for hashes, unintended fixtures and secret canaries.

Heuristic credential detection is not universal. Does not scan private auth
stores or dataset contents. Never prints matched secret values. No publishing.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

# PEM parsers legitimately embed marker literals. Require an actual encoded
# payload after the marker, not the standalone format string in native code.
PATTERNS = [rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----\r?\n[A-Za-z0-9+/=]{32,}',
            rb'\bAKIA[0-9A-Z]{16}\b', rb'\bASIA[0-9A-Z]{16}\b',
            rb'\bya29\.[A-Za-z0-9_-]{30,}', rb'\bgh[pousr]_[A-Za-z0-9]{30,}']


def audit(bundle, canaries):
    bundle = bundle.resolve(strict=True)
    manifest = json.loads((bundle / 'bundle.json').read_text())
    listed = manifest['sha256']
    actual = {str(p.relative_to(bundle)) for p in bundle.rglob('*') if p.is_file()}
    allowed = set(listed) | {'bundle.json', 'verification.json'}
    if actual - allowed:
        raise ValueError('unlisted files in bundle')
    if set(listed) - actual:
        raise ValueError('missing files in bundle')
    for name, expected in listed.items():
        path = bundle / name
        if path.is_symlink() or not path.resolve().is_relative_to(bundle):
            raise ValueError('unsafe bundle path')
        if any(x in name.lower() for x in ('fixture', 'adversary', '.credentials', 'access-token')):
            raise ValueError('unintended fixture/auth artifact')
        with path.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != expected:
                raise ValueError('artifact hash mismatch')
    for path in bundle.rglob('*'):
        if path.is_symlink():
            raise ValueError('bundle symlink refused')
        if not path.is_file():
            continue
        tail = b''
        with path.open('rb') as source:
            while chunk := source.read(65536):
                data = tail + chunk
                if any(re.search(pattern, data) for pattern in PATTERNS) or any(c in data for c in canaries):
                    raise ValueError('credential pattern/canary found; matched value suppressed')
                tail = data[-4096:]
    return {'version': 1, 'file_hashes_verified': len(listed), 'unlisted_files': 0,
            'fixture_auth_artifacts': 0, 'credential_patterns_found': 0,
            'canaries_checked': len(canaries),
            'limitations': ['Heuristic known-token/private-key patterns plus explicit synthetic canaries only.',
                            'No claim of universal secret detection; private credential stores not scanned.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--canary-file', type=Path, help='private JSON array of synthetic strings only')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    canaries = [v.encode() for v in json.loads(args.canary_file.read_text())] if args.canary_file else []
    if any(not c or len(c) > 4096 for c in canaries):
        raise SystemExit('canary length must be 1..4096')
    report = audit(args.bundle.resolve(strict=True), canaries)
    with args.output.open('x') as output:
        output.write(json.dumps(report, indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError) as error:
        raise SystemExit(str(error))
