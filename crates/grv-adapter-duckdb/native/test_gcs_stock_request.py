#!/usr/bin/env python3
"""Signed stock HTTPFS request delegate proof; loopback synthetic bearer only."""
import argparse
import http.server
import json
import pathlib
import subprocess
import tempfile
import threading
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proof", type=pathlib.Path, required=True)
    parser.add_argument("--httpfs", type=pathlib.Path, required=True)
    parser.add_argument("--skip-known-header-blocker", action="store_true",
                        help="explicit partial proof only; absolute deadline remains unqualified")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="grv-stock-request-") as temporary:
        fixture = pathlib.Path(temporary) / "fixture.parquet"
        subprocess.run([str(args.proof), str(args.httpfs), str(fixture), "generate"],
                       check=True, capture_output=True, timeout=10)
        body = fixture.read_bytes()
        seen = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_HEAD(self):
                seen.append(self.path)
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/target")
                    self.end_headers()
                    return
                if self.path == "/stall":
                    time.sleep(1)
                if self.path == "/header_trickle":
                    try:
                        self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                        for index in range(45):
                            self.wfile.write(f"X-Synthetic-{index}: value\r\n".encode())
                            self.wfile.flush()
                            time.sleep(0.08)
                        self.wfile.write(f"Content-Length: {len(body)}\r\n\r\n".encode())
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()

            def do_GET(self):
                seen.append(self.path)
                request_range = self.headers.get("Range")
                data = body
                if request_range:
                    start, end = request_range.removeprefix("bytes=").split("-")
                    data = body[int(start):int(end) + 1]
                self.send_response(206 if request_range else 200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                try:
                    if self.path == "/trickle":
                        for offset in range(0, len(data), 64):
                            self.wfile.write(data[offset:offset + 64])
                            self.wfile.flush()
                            time.sleep(0.08)
                    else:
                        self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        results = []
        try:
            modes = ("direct", "redirect", "stall", "trickle", "expired", "unlisted", "header_trickle")
            if args.skip_known_header_blocker:
                print("WARNING: header-trickle hard deadline gate skipped; partial proof only")
                modes = modes[:-1]
            for mode in modes:
                result = subprocess.run(
                    [str(args.proof), str(args.httpfs),
                     f"http://127.0.0.1:{server.server_port}/{mode}", mode],
                    capture_output=True, timeout=8)
                if result.returncode:
                    print("FAILED signed-stock gate:", mode,
                          result.stdout.decode("utf-8", errors="replace").strip())
                    raise RuntimeError("signed-stock hard deadline/request gate failed")
                results.append({"mode": mode, **json.loads(result.stdout)})
            assert "/target" not in seen, "redirect destination contacted"
            assert "/expired" not in seen and "/unlisted" not in seen, "inactive authority contacted"
            print(json.dumps({"trials": results, "redirect_target_requests": 0}, indent=2))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    main()
