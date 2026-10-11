#!/usr/bin/env python3
"""Independent two-origin redirect trap; synthetic credentials, no cloud access."""
import argparse
import os
import re
from pathlib import Path
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from offline import verify_extension

TOKEN = 'synthetic-policy-only'


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args):
        pass
    def do_HEAD(self):
        self.read()
    def do_GET(self):
        self.read()
    def read(self):
        self.server.requests += 1
        self.server.bearer_requests += self.headers.get('Authorization') == 'Bearer ' + TOKEN
        if self.server.redirect:
            self.send_response(302)
            self.send_header('Location', self.server.redirect)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        body = self.server.fixture.read_bytes()
        lo, hi = 0, len(body) - 1
        if self.headers.get('Range'):
            lo, hi = map(int, self.headers['Range'].removeprefix('bytes=').split('-'))
            hi = min(hi, len(body) - 1)
        self.send_response(206 if self.headers.get('Range') else 200)
        self.send_header('Content-Length', str(hi - lo + 1))
        self.send_header('Accept-Ranges', 'bytes')
        if self.headers.get('Range'):
            self.send_header('Content-Range', f'bytes {lo}-{hi}/{len(body)}')
        self.end_headers()
        if self.command == 'GET':
            self.wfile.write(body[lo:hi + 1])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--extension', type=Path, default=Path.home() / '.grv-httpfs-prototype-extensions/osx_arm64/httpfs.duckdb_extension')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='grv-policy-fixture-') as temporary:
        scratch = Path(temporary)
        verify_extension(args.extension, scratch)
        env = {k: v for k, v in os.environ.items() if not k.upper().startswith(('GRV_', 'AWS', 'GOOGLE', 'GCP', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY'))}
        env.update(GRV_POLICY_SYNTHETIC_TOKEN=TOKEN, NO_PROXY='127.0.0.1')
        fixture = scratch / 'fixture.parquet'
        def run(url, mode):
            result = subprocess.run([str(args.binary), str(args.extension), url, str(fixture), mode], env=env, capture_output=True, timeout=30)
            if result.returncode:
                stage = re.fullmatch(rb'policy probe failed at ([a-z]+) \(details suppressed\)\s*', result.stderr)
                raise RuntimeError('Offline policy probe failed in ' + mode + (' at ' + stage[1].decode() if stage else '') + '; native details suppressed')
        run('http://127.0.0.1:1/fixture.parquet', 'generate')
        servers = []
        try:
            for _ in range(2):
                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                server.fixture, server.redirect = fixture, None
                server.requests = server.bearer_requests = 0
                servers.append(server)
                threading.Thread(target=server.serve_forever, daemon=True).start()
            origin, target = servers
            url = f'http://127.0.0.1:{origin.server_port}/fixture.parquet'
            run(url, 'direct')
            assert origin.requests > 0 and origin.bearer_requests == origin.requests
            origin.redirect = f'http://127.0.0.1:{target.server_port}/fixture.parquet'
            run(url, 'redirect')
            assert target.requests == 0 and target.bearer_requests == 0
            # Control proves the same signed stock transport follows the redirect
            # when not hardened; sensitive production credentials never participate.
            run(url, 'stock')
            assert target.requests > 0
            print('PASS: stock HTTPFS direct bearer read; all parameter overloads hardened; redirect target 0 requests under policy; stock control followed redirect')
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()


if __name__ == '__main__':
    main()
