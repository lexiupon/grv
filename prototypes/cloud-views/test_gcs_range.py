#!/usr/bin/env python3
"""Loopback contract tests; not evidence of live Google Cloud behavior."""
import hashlib
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from gcs_range import Reader, Refused, coordinate, generation


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.data = b"PAR1" + bytes(range(256)) * 1024 + b"\x00\x00\x00\x00PAR1"
        self.current_generation = "101"
        self.objects = {"101": self.data}
        self.requests = []
        self.metadata_body = None
        self.late_fail = False
        self.fault = None
        self.encoding = "identity"
        case = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                uri = urlsplit(self.path)
                query = parse_qs(uri.query)
                case.requests.append((uri.path, query, dict(self.headers)))
                if self.headers.get("Authorization") != "Bearer test-only":
                    self.send_error(401)
                    return
                if uri.path.startswith("/storage/v1/"):
                    body = case.metadata_body if case.metadata_body is not None else json.dumps(
                        {"size": str(len(case.data)), "generation": case.current_generation,
                         "contentEncoding": case.encoding}).encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                pinned = query.get("generation", [None])[0]
                if pinned != query.get("ifGenerationMatch", [None])[0]:
                    self.send_error(412)
                    return
                if case.late_fail and len(case.requests) > 2:
                    self.send_error(412)
                    return
                if pinned not in case.objects:
                    self.send_error(404)
                    return
                bounds = self.headers.get("Range", "").removeprefix("bytes=").split("-")
                start, end = map(int, bounds)
                body = case.objects[pinned][start:end + 1]
                if case.fault == "redirect":
                    self.send_response(302)
                    self.send_header("Location", "https://example.invalid/credential-leak")
                    self.end_headers()
                    return
                self.send_response(200 if case.fault == "status" else 206)
                self.send_header("Content-Length", str(len(body) + (1 if case.fault == "length" else 0)))
                self.send_header("Content-Range", "bytes 0-0/1" if case.fault == "range"
                                 else f"bytes {start}-{end}/{len(case.objects[pinned])}")
                if case.fault != "no-generation-header":
                    self.send_header("x-goog-generation", "999" if case.fault == "generation" else pinned)
                if case.fault == "duplicate-header":
                    self.send_header("Content-Range", "bytes 0-0/1")
                if case.fault == "encoding" or case.encoding != "identity":
                    self.send_header("Content-Encoding", "gzip")
                self.end_headers()
                self.wfile.write(body[:-1] if case.fault == "truncated" else body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.reader = Reader("gs://fixture-bucket/exact/a%20b/%E9%9B%AA%25.parquet", "test-only",
                             "test-project", _test_endpoint=f"http://127.0.0.1:{self.server.server_port}")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def pin(self):
        return self.reader.pin(len(self.data), "101")

    def test_bounded_ranges_and_exact_key(self):
        self.pin()
        self.assertEqual(self.reader.read(2, 140000), self.data[2:140002])
        ranges = self.requests[1:]
        self.assertEqual(len(ranges), 3)
        self.assertEqual(self.reader.media_bytes, 140000)
        for path, query, headers in ranges:
            self.assertEqual(path, "/download/storage/v1/b/fixture-bucket/o/exact%2Fa%20b%2F%E9%9B%AA%25.parquet")
            self.assertEqual(query["generation"], ["101"])
            self.assertEqual(query["ifGenerationMatch"], ["101"])
            self.assertEqual(query["alt"], ["media"])
            self.assertEqual(query["userProject"], ["test-project"])
            self.assertEqual(headers["Accept-Encoding"], "identity")
            first, last = map(int, headers["Range"][6:].split("-"))
            self.assertLessEqual(last - first + 1, 65536)

    def test_replacement_reads_retained_pinned_generation(self):
        self.pin()
        self.current_generation = "102"
        self.objects["102"] = b"x" * len(self.data)
        self.assertEqual(self.reader.read(0, 8), self.data[:8])
        del self.objects["101"]
        with self.assertRaises(Refused):
            self.reader.read(0, 8)

    def test_mismatch_never_pins(self):
        for size, version in [(len(self.data) + 1, "101"), (len(self.data), "102")]:
            with self.assertRaises(Refused):
                self.reader.pin(size, version)
        self.assertIsNone(self.reader.identity)

    def test_cannot_repin_fixed_reader(self):
        self.pin()
        with self.assertRaises(Refused):
            self.pin()

    def test_rejects_transcoding_metadata(self):
        self.encoding = "gzip"
        with self.assertRaises(Refused):
            self.pin()

    def test_bad_range_responses_refused(self):
        self.pin()
        for fault in ["status", "range", "length", "generation", "encoding", "truncated", "redirect", "duplicate-header"]:
            with self.subTest(fault=fault):
                self.fault = fault
                with self.assertRaises(Refused):
                    self.reader.read(0, 4)
        self.assertEqual(self.reader.media_bytes, 0)

    def test_generation_header_optional_with_request_preconditions(self):
        self.pin()
        self.fault = "no-generation-header"
        self.assertEqual(self.reader.read(0, 4), b"PAR1")

    def test_hash_and_mismatch(self):
        self.pin()
        self.reader.verify_hash(hashlib.sha256(self.data).hexdigest())
        self.assertEqual(self.reader.media_bytes, len(self.data))
        with self.assertRaises(Refused):
            self.reader.verify_hash("0" * 64)

    def test_invalid_bounds_issue_no_requests(self):
        self.pin()
        before = len(self.requests)
        for start, length in [(-1, 1), (0, -1), (len(self.data), 1), (True, 1), (0, 1.5),
                              (0, Reader.MAX_READ + 1)]:
            with self.assertRaises(Refused):
                self.reader.read(start, length)
        self.assertEqual(self.reader.read(len(self.data), 0), b"")
        self.assertEqual(len(self.requests), before)

    def test_ambiguous_or_oversized_metadata(self):
        for body in [b'{"generation":"999","generation":"101","size":"262156"}',
                     b"x" * (Reader.MAX_METADATA + 1), b"not-json-private-canary"]:
            with self.subTest(body=body[:50]):
                self.metadata_body = body
                with self.assertRaises(Refused) as caught:
                    self.pin()
                self.assertIsNone(caught.exception.__context__)
                self.assertIsNone(self.reader.identity)

    def test_late_chunk_and_encoding_mutation_refused(self):
        self.pin()
        self.late_fail = True
        with self.assertRaises(Refused):
            self.reader.read(0, 140000)
        self.assertEqual(self.reader.media_bytes, Reader.MAX_RANGE)
        self.late_fail = False
        self.encoding = "gzip"
        with self.assertRaises(Refused):
            self.reader.read(0, 4)

    def test_auth_failure_redacted_and_no_media_accepted(self):
        self.pin()
        self.reader.token = "wrong-test-token"
        with self.assertRaises(Refused) as caught:
            self.reader.read(0, 4)
        self.assertEqual(str(caught.exception), "GCS request failed")
        self.assertIsNone(caught.exception.__context__)
        self.assertEqual(self.reader.media_bytes, 0)

    def test_explicit_credentials_and_fixed_endpoint(self):
        for token in ["", "token\r\nInjected: value", "a" * 16385, "☃"]:
            with self.assertRaises(Refused):
                Reader("gs://fixture-bucket/exact/a", token)
        with self.assertRaises(Refused):
            Reader("gs://fixture-bucket/exact/a", "token", _test_endpoint="https://example.com")


class IdentityTests(unittest.TestCase):
    def test_canonical_and_no_globbing(self):
        self.assertEqual(coordinate("gs://fixture-bucket/a%20b/x%2A.parquet"),
                         ("fixture-bucket", "a b/x*.parquet"))
        for uri in ["s3://fixture-bucket/a", "gs://fixture-bucket/a?token=x", "gs://fixture-bucket/a#x",
                    "gs://fixture-bucket/a/../b", "gs://fixture-bucket/a%2Fb", "gs://fixture-bucket/a%00",
                    "gs://fixture-bucket/a//b", "gs://fixture-bucket/a%ZZ", "gs://fixture-bucket/a*.parquet",
                    "gs://fixture-bucket/%FF", "gs://fixture-bucket/%61", "gs://[bad/a",
                    "\ngs://fixture-bucket/a", "gs://fixture-bucket/a\tb", "GS://fixture-bucket/a",
                    "gs://fixture-bucket/" + "a" * 8192]:
            with self.subTest(uri=uri), self.assertRaises(Refused):
                coordinate(uri)

    def test_invalid_generations(self):
        self.assertEqual(generation("18446744073709551615"), "18446744073709551615")
        for value in ["0", "01", "-1", "18446744073709551616", "101\n", 101, None]:
            with self.subTest(value=value), self.assertRaises(Refused):
                generation(value)


if __name__ == "__main__":
    unittest.main()
