#!/usr/bin/env python3
"""Assemble pinned conservative native notices from previously fetched sources.

No service effects, execution or downloads. Retains extra test/platform
attributions intentionally; review manifests distinguish superset from SBOM.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil

ROOT = Path(__file__).resolve().parent.parent
POLICY = 'pinned-upstream-plus-conservative-third-party-notices'
ARCHIVE_SHA = '1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def embedded(base):
    blocks = {}
    for path in sorted(base.rglob('*')):
        if not path.is_file() or path.suffix not in ('.c', '.h', '.hpp', '.cpp', '.cc', '.inc'):
            continue
        text = path.read_text(errors='replace')
        # Include leading comments only; do not distribute implementation bodies.
        leading = re.match(r'\s*(?:(?://[^\n]*\n|/\*.*?\*/|\s)+)', text, re.S)
        if not leading:
            continue
        block = leading.group(0).strip()
        if any(term in block.lower() for term in ('copyright', 'permission', 'licensed', 'license', 'redistribution')):
            blocks.setdefault(block, []).append(str(path.relative_to(base)))
    return '\n\n'.join('SOURCE FILES: ' + ', '.join(paths) + '\n' + text for text, paths in blocks.items()) + '\n'


def collect(base, output, source_url):
    rows = []
    for path in sorted(base.rglob('*')):
        if path.is_file() and (any(path.name.upper().startswith(term) for term in
                                 ('LICENSE', 'COPYING', 'NOTICE', 'COPYRIGHT'))
                              or path.name in ('AUTHORS', 'attribution')):
            if path.suffix in ('.c', '.cpp', '.h', '.hpp', '.pm', '.pem'):
                continue
            if path.is_symlink():
                raise SystemExit('notice symlink refused')
            dest = output / path.relative_to(base)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, dest)
            rows.append({'path': str(dest.relative_to(ROOT / 'notices')),
                         'sha256': digest(dest), 'source': source_url + '/' + str(path.relative_to(base))})
    text = embedded(base)
    if text.strip():
        dest = output / 'EMBEDDED-ATTRIBUTIONS.txt'
        dest.write_text(text)
        rows.append({'path': str(dest.relative_to(ROOT / 'notices')),
                     'sha256': digest(dest), 'source': source_url,
                     'method': 'Deduplicated leading source/header comment blocks with origin paths; conservative extras included.'})
    return rows


def extra(source, destination, url):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    return {'path': str(destination.relative_to(ROOT / 'notices')), 'sha256': digest(destination), 'source': url}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb-source', type=Path, required=True)
    parser.add_argument('--duckdb-archive', type=Path, required=True)
    parser.add_argument('--research', type=Path, required=True)
    args = parser.parse_args()
    if digest(args.duckdb_archive) != ARCHIVE_SHA:
        raise SystemExit('DuckDB archive pin differs')
    source, research = args.duckdb_source.resolve(), args.research.resolve()
    engine = ROOT / 'notices/native-source'
    extensions = ROOT / 'notices/extension-third-party'
    if engine.exists() or extensions.exists():
        raise SystemExit('fresh notice trees required')
    # Entire release tree contributes attribution superset, including optional
    # extensions/tests. No test datasets or their data are copied.
    rows = collect(source, engine, 'https://github.com/duckdb/duckdb/blob/v1.5.6')
    for name, dest, url in [
        ('thrift-0.22.0-NOTICE', 'upstream/thrift-0.22.0-NOTICE', 'https://raw.githubusercontent.com/apache/thrift/v0.22.0/NOTICE'),
        ('parquet-format-2.11.0-NOTICE', 'upstream/parquet-format-2.11.0-NOTICE', 'https://raw.githubusercontent.com/apache/parquet-format/apache-parquet-format-2.11.0/NOTICE'),
        ('catch-2.13.7-LICENSE', 'upstream/catch-2.13.7-LICENSE', 'https://raw.githubusercontent.com/catchorg/Catch2/v2.13.7/LICENSE.txt')]:
        rows.append(extra(research / name, engine / dest, url))
    # Prior Thrift notice is also retained because runtime origin version is
    # not pinned by generated compiler version. Generic ASF attribution same.
    rows.append(extra(ROOT / 'notices/upstream/thrift-0.17.0/NOTICE', engine / 'upstream/thrift-0.17.0-NOTICE',
                      'https://raw.githubusercontent.com/apache/thrift/v0.17.0/NOTICE'))
    review = {'version': 1, 'policy': POLICY, 'scope': 'native-engine',
              'source_archive_sha256': ARCHIVE_SHA, 'reviewed': False,
              'unresolved': ['Human review of assembled source/header attribution payload pending.'],
              'notice_files': rows, 'limitations': [
                  'Conservative source/header/license superset, not exact linked graph.',
                  'Thrift runtime origin version uncertain; generic ASF notices from 0.17.0 and 0.22.0 both retained.',
                  'Catch/test/platform notices retained as extras; no test datasets or IMDb data redistributed.']}
    (ROOT / 'notices/native-source-review.json').write_text(json.dumps(review, indent=2) + '\n')
    rows = []
    components = []
    for manifest in sorted(research.glob('*-vcpkg.json')):
        name = manifest.name.removesuffix('-vcpkg.json')
        value = json.loads(manifest.read_text())
        version = value.get('version') or value.get('version-string')
        if not version:
            continue
        # SDK checkout is deliberately not required (very large source); its
        # pinned root license/NOTICE/attribution files are copied below.
        if name == 'aws-sdk-cpp':
            for filename in ('LICENSE', 'LICENSE.txt', 'NOTICE.txt', 'attribution'):
                rows.append(extra(research / ('aws-sdk-' + filename), extensions / f'{name}-{version}' / filename,
                                  f'https://raw.githubusercontent.com/aws/aws-sdk-cpp/{version}/{filename}'))
            components.append({'name': name, 'version': version})
            continue
        folder = research / name
        bases = [p for p in folder.iterdir() if p.is_dir()] if folder.exists() else []
        if len(bases) != 1:
            raise SystemExit('missing pinned component source: ' + name)
        port = (research / (name + '-portfile.cmake')).read_text()
        repo = re.search(r'\bREPO\s+(\S+)', port).group(1)
        tag = ('curl-' + version.replace('.', '_') if name == 'curl' else
               'openssl-' + version if name == 'openssl' else 'v' + version)
        url = f'https://github.com/{repo}/blob/{tag}'
        rows += collect(bases[0], extensions / f'{name}-{version}', url)
        components.append({'name': name, 'version': version,
                           'source_archive_sha256': digest(research / (name + '.tar.gz'))})
    for name in ('httplib', 'mbedtls'):
        rows.append(extra(research / (name + '-LICENSE'), extensions / f'duckdb-1.5.6-{name}/LICENSE',
                          f'https://github.com/duckdb/duckdb/blob/v1.5.6/third_party/{name}/LICENSE'))
    registry = json.loads((ROOT / 'notices/native-extensions.json').read_text())
    review = {'version': 1, 'policy': POLICY, 'scope': 'signed-extensions',
              'reviewed': False, 'unresolved': ['Human review of conservative component attribution payload pending.'],
              'artifacts': [a for a in registry['artifacts'] if a['platform'] == 'osx_arm64'],
              'vcpkg_baseline': '84bab45d415d22042bd0b9081aea57f362da3f35',
              'components': components, 'notice_files': rows, 'limitations': [
                  'Observed pinned manifest/baseline conservative dependency superset; exact upstream CI/linked graph not reconstructed.',
                  'Platform/test extras retained; not all listed components necessarily occur in signed binaries.',
                  'AWS C Common third-party licenses retained (ittapi/cJSON/libcbor).',
                  'AWS-LC submodule not inferred linked: inspected manifests use OpenSSL.',
                  'Only osx_arm64 1.5.6 signed artifacts reviewed by this payload.']}
    (ROOT / 'notices/extension-third-party-review.json').write_text(json.dumps(review, indent=2) + '\n')
    print(json.dumps({'engine_notice_files': len(json.loads((ROOT / 'notices/native-source-review.json').read_text())['notice_files']),
                      'extension_notice_files': len(rows), 'reviewed': False}))


if __name__ == '__main__':
    main()
