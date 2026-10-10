#!/usr/bin/env python3
"""Experimental GCS JSON API reader. Not imported by GRV production code."""
import argparse
import hashlib
import http.client
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


class Refused(Exception):
    """No bytes from an unverified response may reach a scanner."""


def unique_headers(headers):
    for name in ("Content-Length", "Content-Range", "Content-Encoding", "x-goog-generation"):
        if len(headers.get_all(name, [])) > 1:
            raise Refused("ambiguous response headers")
    if headers.get_all("Transfer-Encoding", []):
        raise Refused("unsupported response framing")


def unique_json(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Refused("duplicate metadata field")
        result[key] = value
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Refused("redirect refused")


def coordinate(uri):
    """Restricted experimental bucket syntax; GRV-style exact key encoding."""
    if (not isinstance(uri, str) or not uri.startswith("gs://") or len(uri) > 8192
            or any(ord(c) < 32 or ord(c) == 127 for c in uri)):
        raise Refused("invalid exact GCS URI")
    try:
        parsed = urllib.parse.urlsplit(uri)
    except ValueError as exc:
        raise Refused("malformed object URI") from exc
    if (parsed.scheme != "gs" or parsed.query or parsed.fragment
            or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", parsed.netloc)):
        raise Refused("expected exact gs://bucket/key")
    encoded = parsed.path[1:]
    if re.search(r"%2[fF]", encoded):
        raise Refused("encoded path separator refused")
    try:
        key = urllib.parse.unquote(encoded, encoding="utf-8", errors="strict")
    except UnicodeError as exc:
        raise Refused("invalid UTF-8 key") from exc
    if (not key or any(part in ("", ".", "..") for part in key.split("/"))
            or any(ord(c) < 32 or ord(c) == 127 or c == "\\" for c in key)):
        raise Refused("noncanonical key")
    # Match GRV's printable-key escaping rather than treating URI text as a glob.
    canonical = "".join(chr(b) if b == 47 or (33 <= b <= 126 and
                        chr(b) not in '\"#%<>?`{}[]*\\') else f"%{b:02X}"
                        for b in key.encode("utf-8"))
    if canonical != encoded:
        raise Refused("noncanonical URI escaping")
    return parsed.netloc, key


def generation(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", value):
        raise Refused("invalid object generation")
    if int(value) > 2**64 - 1:
        raise Refused("object generation overflow")
    return value


@dataclass(frozen=True)
class Identity:
    uri: str
    size: int
    generation: str


class Reader:
    MAX_RANGE = 65536
    MAX_METADATA = 16384
    MAX_READ = 8 * 1024 * 1024

    def __init__(self, uri, token, project=None, *, _test_endpoint=None):
        self.bucket, self.key = coordinate(uri)
        if (not isinstance(token, str) or len(token) > 16384
                or not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token)):
            raise Refused("valid explicit bearer token required")
        if project is not None and not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", project):
            raise Refused("invalid quota project")
        self.uri, self.token, self.project = uri, token, project
        # CLI never exposes endpoint selection; loopback injection is tests-only.
        self.endpoint = _test_endpoint or "https://storage.googleapis.com"
        if _test_endpoint and not re.fullmatch(r"http://127\.0\.0\.1:[0-9]+", _test_endpoint):
            raise Refused("test endpoint must be loopback")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        self.requests = 0
        self.media_bytes = 0
        self.metadata_bytes = 0
        self.identity = None

    def url(self, media=False, query=None):
        path = "/download/storage/v1/b/" if media else "/storage/v1/b/"
        path += urllib.parse.quote(self.bucket, safe="") + "/o/" + urllib.parse.quote(self.key, safe="")
        params = dict(query or {})
        if media:
            params["alt"] = "media"
        if self.project:
            params["userProject"] = self.project
        return self.endpoint + path + "?" + urllib.parse.urlencode(params)

    def get(self, url, limit, headers=None, check=None):
        request_headers = {"Authorization": "Bearer " + self.token, "Accept-Encoding": "identity"}
        request_headers.update(headers or {})
        self.requests += 1
        try:
            with self.opener.open(urllib.request.Request(url, headers=request_headers), timeout=10) as response:
                unique_headers(response.headers)
                if check:
                    check(response.status, response.headers)
                elif response.status != 200:
                    raise Refused("metadata status refused")
                body = response.read(limit + 1)
                if len(body) > limit:
                    raise Refused("response exceeds bounded allowance")
                return body
        except Refused:
            raise
        except urllib.error.HTTPError as exc:
            exc.close()
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            pass
        # Raise outside exception handling so no private underlying exception
        # remains attached, even for callers that inspect __context__.
        raise Refused("GCS request failed")

    def pin(self, expected_size, expected_generation):
        if self.identity is not None:
            raise Refused("reader identity is fixed; create a new reader to repin")
        generation(expected_generation)
        if (isinstance(expected_size, bool) or not isinstance(expected_size, int)
                or expected_size < 0 or expected_size > 2**63 - 1):
            raise Refused("invalid expected size")
        body = self.get(self.url(query={"fields": "size,generation,contentEncoding"}), self.MAX_METADATA)
        self.metadata_bytes += len(body)
        try:
            meta = json.loads(body, object_pairs_hook=unique_json)
            size = meta["size"]
            if not isinstance(size, str) or not re.fullmatch(r"0|[1-9][0-9]*", size):
                raise Refused("invalid metadata size")
            actual_generation = generation(meta["generation"])
            if int(size) != expected_size or actual_generation != expected_generation:
                raise Refused("metadata differs from manifest; full-hash fallback not implemented")
            if meta.get("contentEncoding", "identity") != "identity":
                raise Refused("transformed object refused")
        except (ValueError, KeyError, TypeError, RecursionError):
            meta = None
        if meta is None:
            raise Refused("invalid metadata")
        self.identity = Identity(self.uri, expected_size, expected_generation)
        return self.identity

    def read(self, start, length):
        identity = self.identity
        if identity is None:
            raise Refused("object must be pinned before reading")
        if (isinstance(start, bool) or isinstance(length, bool)
                or not isinstance(start, int) or not isinstance(length, int)
                or start < 0 or length < 0 or length > self.MAX_READ
                or start > identity.size or length > identity.size - start):
            raise Refused("range outside pinned object")
        chunks = []
        for offset in range(start, start + length, self.MAX_RANGE):
            count = min(self.MAX_RANGE, start + length - offset)
            end = offset + count - 1

            def check(status, headers):
                if (status != 206 or headers.get("Content-Length") != str(count)
                        or headers.get("Content-Range") != f"bytes {offset}-{end}/{identity.size}"
                        or headers.get("Content-Encoding", "identity") != "identity"):
                    raise Refused("conditional range response refused")
                # Preconditions pin identity even if this optional header is absent.
                returned = headers.get("x-goog-generation")
                if returned is not None and returned != identity.generation:
                    raise Refused("response generation differs from pinned object")

            body = self.get(self.url(True, {"generation": identity.generation,
                                           "ifGenerationMatch": identity.generation}), count,
                            {"Range": f"bytes={offset}-{end}"}, check)
            if len(body) != count:
                raise Refused("truncated range response")
            self.media_bytes += len(body)
            chunks.append(body)
        return b"".join(chunks)

    def verify_hash(self, expected):
        if not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise Refused("invalid expected SHA-256")
        if self.identity is None:
            raise Refused("object must be pinned before hashing")
        sha = hashlib.sha256()
        for start in range(0, self.identity.size, self.MAX_RANGE):
            sha.update(self.read(start, min(self.MAX_RANGE, self.identity.size - start)))
        if sha.hexdigest() != expected:
            raise Refused("full hash mismatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--sha256", help="optional full-object hash check (downloads every byte)")
    parser.add_argument("--project")
    args = parser.parse_args()
    try:
        reader = Reader(args.uri, os.environ.get("GRV_PROTOTYPE_GCS_TOKEN", ""), args.project)
        identity = reader.pin(args.size, args.generation)
        if args.sha256:
            reader.verify_hash(args.sha256)
        footer = reader.read(max(0, identity.size - 8), min(8, identity.size))
        header = reader.read(0, min(4, identity.size))
        if len(footer) != 8 or header != b"PAR1" or footer[-4:] != b"PAR1":
            raise Refused("expected Parquet magic")
        print(json.dumps({"status": "passed", "transport": "live-gcs-json-api",
                          "requests": reader.requests, "metadata_bytes": reader.metadata_bytes,
                          "media_bytes": reader.media_bytes, "full_hash_checked": bool(args.sha256),
                          "duckdb_integration": False}, indent=2))
    except Refused as exc:
        parser.exit(1, f"refused: {exc}\n")


if __name__ == "__main__":
    main()
