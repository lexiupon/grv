#!/usr/bin/env python3
"""Offline production directory/tar installation proof with fresh HOME.

Run on the frozen bundle. Clean environment on the current host, not a claim
of independently provisioned host/notarization. Does not fetch/login/publish.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--allow-unqualified', action='store_true',
                        help='exercise development baseline only; never candidate evidence')
    args = parser.parse_args()
    bundle = args.bundle.resolve(strict=True)
    manifest = json.loads((bundle / 'bundle.json').read_text())
    qualified_source = manifest.get('profile') == 'release' and manifest.get('source_dirty') is False
    if not qualified_source and not args.allow_unqualified:
        raise SystemExit('frozen release-profile bundle required')
    cli = str(bundle / 'bin/grv')
    for path, expected in manifest['sha256'].items():
        artifact = bundle / path
        if artifact.is_symlink() or not artifact.resolve().is_relative_to(bundle):
            raise SystemExit('unsafe artifact')
        with artifact.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != expected:
                raise SystemExit('bundle hash differs')
    results = []
    with tempfile.TemporaryDirectory(prefix='.grv-install-proof-', dir=bundle.parent) as temporary:
        root = Path(temporary)
        for method in ('directory', 'tarball'):
            home = root / method / 'home'
            adapters = root / method / 'installed'
            home.mkdir(parents=True, mode=0o700)
            adapters.mkdir(mode=0o700)
            env = {'HOME': str(home), 'PATH': os.defpath,
                   'GRV_ADAPTERS_DIR': str(adapters), 'LANG': 'C'}

            def call(*arguments, success=True):
                completed = subprocess.run([cli, '--json', *map(str, arguments)], env=env,
                                           capture_output=True, timeout=90)
                value = json.loads(completed.stdout)
                if (completed.returncode == 0 and value.get('ok') is True) != success:
                    raise SystemExit('installation operation differed; helper stderr suppressed')
                return value

            for name in ('duckdb', 'salesforce'):
                source = bundle / 'adapters' / name
                if method == 'tarball':
                    archive = root / method / (name + '.tar.gz')
                    with tarfile.open(archive, 'w:gz') as tar:
                        tar.add(source, arcname=name)
                    source = archive
                installed = call('adapter', 'install', source)
                if installed['result']['replaced'] is not False:
                    raise SystemExit('fresh install replaced unexpected package')
                capabilities = call('adapter', name, 'capabilities')['result']['adapter']
                if capabilities != manifest['adapters'][name]:
                    raise SystemExit('installed adapter capabilities differ from bundle')
                call('adapter', 'install', source, success=False)
                replaced = call('adapter', 'install', source, '--replace')
                if replaced['result']['replaced'] is not True:
                    raise SystemExit('explicit replacement not reported')
                call('adapter', name, 'capabilities')
            listing = call('adapter', 'list')['result']
            # Traversal rejection must preserve both previous installations.
            attack = root / method / 'traversal.tar'
            import io
            with tarfile.open(attack, 'w') as tar:
                entry = tarfile.TarInfo('../escape')
                entry.size = 1
                tar.addfile(entry, io.BytesIO(b'x'))
            call('adapter', 'install', attack, '--replace', success=False)
            for name in ('duckdb', 'salesforce'):
                call('adapter', name, 'capabilities')
            if any(root.rglob('escape')):
                raise SystemExit('tar traversal created unexpected file')
            results.append({'method': method, 'fresh_install': True,
                            'duplicate_refused': True, 'replacement': True,
                            'traversal_refused_prior_install_usable': True,
                            'adapter_listing': listing})
    proof = {'version': 1, 'bundle_manifest_sha256': hashlib.sha256((bundle / 'bundle.json').read_bytes()).hexdigest(),
             'methods': results, 'network_fetch': False, 'fresh_home': True,
             'clean_release_source': qualified_source,
             'allow_unqualified': args.allow_unqualified,
             'limitations': ['Fresh HOME/env on existing host, not independent clean host.',
                             'Capabilities/load and protected install only; production lifecycle verified separately.']}
    with args.output.open('x') as output:
        output.write(json.dumps(proof, indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    main()
