#!/usr/bin/env python3
"""Stock HTTPFS loopback experiment. No credential discovery or extension fetch."""
import argparse
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote, urlsplit

ROOT = Path(__file__).resolve().parents[3]
ID = 'offline-fixture-id'
KEY = 'offline-fixture-secret-not-a-cloud-key'


def verify_extension(extension, scratch):
    spec = importlib.util.spec_from_file_location('pinned_fetch', ROOT / 'scripts/fetch-duckdb-extensions.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = module.load_manifest()
    entry = next(e for e in manifest['artifacts'] if e['name'] == 'httpfs' and e['platform'] == 'osx_arm64')
    key = module.read_notice(manifest, manifest['signature']['public_key'])
    module.verify_binary(extension, entry, key, scratch, private=True)


def valid_signature(handler):
    """Verify AWS4 HMAC without persisting Authorization, key ID, URL, or headers."""
    auth = handler.headers.get('Authorization', '')
    m = re.fullmatch(r'AWS4-HMAC-SHA256 Credential=([^/]+)/([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]+)', auth)
    if not m or m[1] != ID:
        return False
    scope, names, signature = m[2], m[3], m[4]
    date, region, service, terminator = scope.split('/')
    headers = ''.join(n + ':' + handler.headers.get(n, '').strip() + '\n' for n in names.split(';'))
    url = urlsplit(handler.path)
    canonical = handler.command + '\n' + url.path + '\n' + url.query + '\n' + headers + '\n' + names + '\n' + handler.headers.get('x-amz-content-sha256', '')
    string_to_sign = 'AWS4-HMAC-SHA256\n' + handler.headers.get('x-amz-date', '') + '\n' + scope + '\n' + hashlib.sha256(canonical.encode()).hexdigest()
    signing = ('AWS4' + KEY).encode()
    for part in (date, region, service, terminator):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    return hmac.compare_digest(hmac.new(signing, string_to_sign.encode(), hashlib.sha256).hexdigest(), signature)


class Mock(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    objects = {}
    records = []
    oauth = False
    def log_message(self, *args):
        pass
    def do_HEAD(self):
        self.handle_read()
    def do_GET(self):
        self.handle_read()
    def do_PUT(self):
        self.deny()
    do_POST = do_DELETE = do_PUT
    def deny(self):
        self.send_response(403)
        self.send_header('Content-Length', '0')
        self.end_headers()
    def handle_read(self):
        url = urlsplit(self.path)
        path = unquote(url.path)
        signed = hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + KEY) if self.oauth else valid_signature(self)
        record = {'method': self.command, 'signature_valid': signed, 'requester_pays': self.headers.get('x-goog-user-project') == 'offline-only' if self.oauth else self.headers.get('x-amz-request-payer') == 'requester', 'range': bool(self.headers.get('Range')), 'bytes': 0, 'recognized': path in self.objects}
        self.records.append(record)
        if not signed or path not in self.objects or url.query:
            self.deny()
            return
        data = self.objects[path].read_bytes()
        lo, hi, status = 0, len(data) - 1, 200
        if self.headers.get('Range'):
            m = re.fullmatch(r'bytes=(\d+)-(\d+)', self.headers['Range'])
            if not m:
                self.deny()
                return
            lo, hi = int(m[1]), min(int(m[2]), len(data) - 1)
            status = 206
        self.send_response(status)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('ETag', '"offline-static-1"')
        self.send_header('Last-Modified', 'Wed, 01 Jan 2020 00:00:00 GMT')
        self.send_header('Content-Length', str(hi - lo + 1))
        if status == 206:
            self.send_header('Content-Range', f'bytes {lo}-{hi}/{len(data)}')
        self.end_headers()
        if self.command == 'GET':
            body = data[lo:hi + 1]
            self.wfile.write(body)
            record['bytes'] = len(body)


def sanitized_env(live=False, oauth=False):
    # No inherited AWS/GCP, endpoint, proxy or DuckDB credential variables.
    env = {k: v for k, v in os.environ.items() if not k.upper().startswith(('AWS', 'GOOGLE', 'GCP', 'DUCKDB', 'S3_', 'GRV_HTTPFS_', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY'))}
    env['NO_PROXY'] = '127.0.0.1'
    if live:
        for k in (('BEARER_TOKEN', 'ACCOUNT', 'PROJECT') if oauth else ('HMAC_ID', 'HMAC_SECRET', 'ACCOUNT', 'PROJECT')):
            value = os.environ.get('GRV_HTTPFS_' + k)
            if not value:
                raise ValueError('live requires explicit HMAC and account/project metadata')
            env['GRV_HTTPFS_' + k] = value
    else:
        env.update(GRV_HTTPFS_HMAC_ID=ID, GRV_HTTPFS_HMAC_SECRET=KEY, GRV_HTTPFS_ACCOUNT='offline-only', GRV_HTTPFS_PROJECT='offline-only')
    env['GRV_HTTPFS_AUTH'] = 'oauth' if oauth else 'hmac'
    if oauth and not live:
        env['GRV_HTTPFS_BEARER_TOKEN'] = KEY
    return env


def validate_manifest(path, live, oauth=False):
    rows = []
    for line in path.read_text().splitlines():
        uri, local, extra = line.split('\t')
        parsed = urlsplit(uri)
        if oauth:
            if parsed.scheme not in (('https',) if live else ('http',)) or (live and parsed.netloc != 'storage.googleapis.com') or not parsed.path or parsed.query or parsed.fragment or any(c in uri for c in '*[]\\\n\r\t'):
                raise ValueError('OAuth manifest requires exact permitted HTTP(S) URL')
        else:
            if parsed.scheme not in ('gs', 'gcs') or not parsed.netloc or not parsed.path or '?' in uri or any(c in uri for c in '*[]\\\n\r\t'):
                raise ValueError('manifest requires exact gs/gcs URI (no query/glob)')
            if live and parsed.scheme != 'gs':
                raise ValueError('live requires gs://')
        fixture = Path(local)
        if not fixture.is_absolute() or not fixture.is_file() or fixture.is_symlink() or extra not in ('0', '1'):
            raise ValueError('manifest requires existing absolute local oracle file and extra=0/1')
        rows.append((uri, fixture, extra))
    if not rows:
        raise ValueError('manifest is empty')
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--binary', required=True, type=Path)
    p.add_argument('--extension', type=Path, default=Path.home() / '.grv-httpfs-prototype-extensions/osx_arm64/httpfs.duckdb_extension')
    p.add_argument('--fixtures', type=Path)
    p.add_argument('--manifest', type=Path)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--live', action='store_true', help='requires explicit approval plus env; NEVER invoked by offline suite')
    p.add_argument('--oauth', action='store_true', help='temporary HTTP bearer secret, not HMAC')
    p.add_argument('--trials', help='explicit comma-separated subset of known trials')
    args = p.parse_args()
    env = sanitized_env(args.live, args.oauth)
    if args.live and os.environ.get('GRV_HTTPFS_LIVE_APPROVED') != 'yes':
        raise ValueError('live approval missing')
    with tempfile.TemporaryDirectory(prefix='grv-httpfs-') as tmp:
        scratch = Path(tmp)
        verify_extension(args.extension, scratch)
        server = None
        if args.live:
            if not args.manifest:
                raise ValueError('live requires manifest')
            validate_manifest(args.manifest, True, args.oauth)
            endpoint, trials = 'storage.googleapis.com', ['wide', 'narrow', 'selective', 'selective-wide', 'null-elimination']
        else:
            if not args.fixtures:
                raise ValueError('offline requires --fixtures private existing path')
            Mock.oauth = args.oauth
            server = ThreadingHTTPServer(('127.0.0.1', 0), Mock)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            endpoint = f'127.0.0.1:{server.server_port}'
            trials = ['wide', 'narrow', 'selective', 'selective-wide', 'null-elimination', 'payer', 'empty', 'special', 'gcs', 'no-stats-selective', 'guard']
        if args.trials:
            trials = args.trials.split(',')
            known = {'wide', 'narrow', 'selective', 'selective-wide', 'null-elimination', 'payer', 'empty', 'special', 'gcs', 'no-stats-selective', 'guard'}
            if not trials or any(t not in known for t in trials):
                raise ValueError('unknown trials')
        results = []
        try:
            for trial in trials:
                manifest = args.manifest
                if not args.live:
                    names = [('old.parquet', 'old.parquet', '0'), ('new.parquet', 'new.parquet', '1')]
                    scheme = 'gcs' if trial == 'gcs' else 'gs'
                    if trial == 'empty':
                        names = [('empty.parquet', 'empty.parquet', '1')]
                    if trial == 'special':
                        names = [('space + percent% # ü.parquet', 'old.parquet', '0'), ("quote's.parquet", 'new.parquet', '1')]
                    if trial == 'no-stats-selective':
                        names = [('old-no-stats.parquet', 'old-no-stats.parquet', '0'), ('new-no-stats.parquet', 'new-no-stats.parquet', '1')]
                    manifest = scratch / 'manifest.tsv'
                    def uri_for(key):
                        return f'http://{endpoint}/fixture-bucket/{quote(key, safe="/")}' if args.oauth else f'{scheme}://fixture-bucket/{key}'
                    manifest.write_text(''.join(f'{uri_for(key)}\t{args.fixtures.resolve() / file}\t{extra}\n' for key, file, extra in names))
                    rows = validate_manifest(manifest, False, args.oauth)
                    Mock.objects = {(unquote(urlsplit(uri).path) if args.oauth else '/' + uri.split('://', 1)[1]): file for uri, file, _ in rows}
                    Mock.records = []
                report = scratch / 'report.json'
                command = [str(args.binary), str(manifest), str(args.extension), str(report), 'LIVE' if args.live else 'TEST', endpoint, trial]
                completed = subprocess.run(command, env=env, capture_output=True, timeout=120)
                if completed.returncode:
                    # Do not relay native stdout/stderr: unknown extension errors may contain credentials.
                    stage = re.fullmatch(rb'prototype failed at ([a-z]+) \(SQL, credentials, error details suppressed\)\s*', completed.stderr)
                    raise RuntimeError('native trial failed: ' + trial + (' at ' + stage[1].decode() if stage else ' (details suppressed)'))
                result = json.loads(report.read_text())
                if server:
                    result['mock'] = {'requests': len(Mock.records), 'body_bytes': sum(r['bytes'] for r in Mock.records), 'all_signatures_valid': all(r['signature_valid'] for r in Mock.records), 'requester_pays_requests': sum(r['requester_pays'] for r in Mock.records), 'unrecognized': sum(not r['recognized'] for r in Mock.records), 'methods': {method: sum(r['method'] == method for r in Mock.records) for method in ('HEAD', 'GET')}}
                else:
                    # Native HTTPFS counts body bytes; actual wire/TLS bytes remain unknown.
                    result['live_transport_bytes'] = None
                if trial != 'guard':
                    http = result['native_http']
                    phases = [http[name] for name in ('view_binding', 'query', 'explain')]
                    http['measured_total'] = {key: sum(phase[key] for phase in phases) for key in phases[0]}
                    if server:
                        total, mock = http['measured_total'], result['mock']
                        matches = (total['bytes_received'] == mock['body_bytes'] and
                                   total['requests'] == mock['requests'] and
                                   total['head'] == mock['methods']['HEAD'] and
                                   total['get'] == mock['methods']['GET'] and
                                   total['bytes_sent'] == 0)
                        if not matches:
                            raise RuntimeError('native HTTP counters disagree with loopback body/request counts: ' + trial)
                        result['native_http_mock_validated'] = True
                results.append(result)
        finally:
            if server:
                server.shutdown()
                server.server_close()
        if server and not args.trials:
            by = {r['trial']: r for r in results}
            require = lambda ok: None if ok else (_ for _ in ()).throw(RuntimeError('offline correctness/security/reduction assertion failed'))
            for r in results:
                require(r['mock']['all_signatures_valid'] and r['mock']['unrecognized'] == 0)
                if r['trial'] != 'guard':
                    require(r['correct'] and r['nonallowlisted_rejected'] and r['glob_rejected'] and r['nested_nonallowlisted_rejected'])
            full = by['wide']['native_http']['query']['bytes_received']
            require(full > 0 and by['wide']['native_http']['query']['requests'] > 0)
            require(by['narrow']['native_http']['query']['bytes_received'] * 4 < full)
            require(by['selective-wide']['native_http']['query']['bytes_received'] * 4 < full)
            require(by['guard']['production_owner_direct_rejected'] and by['guard']['production_owner_nested_rejected'] and by['guard']['production_owner_glob_rejected'])
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({'extension_verified': True, 'stock_httpfs': True, 'live': args.live, 'trials': results}, indent=2) + '\n')
        args.output.chmod(0o600)
        for r in results:
            print(r['trial'], r.get('rows', '-'),
                  {'native_http': r.get('native_http'), 'mock': r.get('mock')})


if __name__ == '__main__':
    try:
        main()
    except RuntimeError as error:
        # These messages are authored here, never native/extension output.
        raise SystemExit(str(error)) from None
    except Exception:
        raise SystemExit('HTTPFS prototype failed; details suppressed (check prerequisites/manifest/approval)')
