"""Offline adversarial gates for pinned extension packaging inputs."""
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "fetch-duckdb-extensions.py"
SPEC = importlib.util.spec_from_file_location("fetch_extensions", SCRIPT)
FETCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FETCH)


class Response(io.BytesIO):
    status = 200
    headers = {}

    def geturl(self):
        return "https://extensions.duckdb.org/v1.5.6/osx_arm64/httpfs.duckdb_extension.gz"


class FetchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=FETCH.ROOT, prefix=".extension-test-")
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_registry_exactly_covers_eight_signed_platform_artifacts_and_pinned_notices(self):
        manifest = FETCH.load_manifest()
        self.assertEqual(len(manifest["artifacts"]), 8)
        self.assertEqual({a["footer"]["duckdb_version"] for a in manifest["artifacts"]}, {"v1.5.6"})
        self.assertEqual({a["footer"]["abi"] for a in manifest["artifacts"]}, {"CPP"})
        self.assertEqual({a["source_commit"][:7] for a in manifest["artifacts"]}, {"4bc690d", "28c853c"})
        self.assertIn("not yet complete", manifest["notice_inventory_scope"])

    def test_directory_links_and_writable_ancestors_are_refused_before_artifact_creation(self):
        actual = self.root / "actual"
        actual.mkdir()
        alias = self.root / "alias"
        alias.symlink_to(actual, target_is_directory=True)
        with self.assertRaises(FETCH.VerificationError):
            FETCH.protected_output(alias / "output")
        self.assertFalse((actual / "output").exists())
        actual.chmod(0o777)
        try:
            with self.assertRaises(FETCH.VerificationError):
                FETCH.protected_output(actual / "output")
            self.assertFalse((actual / "output").exists())
        finally:
            actual.chmod(0o700)

    def test_noncanonical_output_and_notice_paths_are_refused(self):
        with self.assertRaises(FETCH.VerificationError):
            FETCH.protected_output(self.root / ".." / "escape")
        for value in ("/absolute", "native/../escape"):
            with self.assertRaises(FETCH.VerificationError):
                FETCH.relative_path(value)

    def test_bounded_get_retries_only_transient_failures_and_never_uses_head(self):
        calls = []
        attempts = [urllib.error.HTTPError("url", 503, "retry", {}, None), Response(b"gzip")]

        class Opener:
            def open(inner, request, timeout):
                calls.append((request.get_method(), request.full_url, timeout))
                response = attempts.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        with patch.object(FETCH.urllib.request, "build_opener", return_value=Opener()), patch.object(FETCH.time, "sleep"):
            FETCH.download(Response().geturl(), self.root / "artifact", 4)
        self.assertEqual((self.root / "artifact").read_bytes(), b"gzip")
        self.assertEqual([c[0] for c in calls], ["GET", "GET"])
        self.assertEqual(len(attempts), 0)

    def test_truncated_oversized_and_wrong_origin_gets_never_install_partial_contents(self):
        for body in (b"abc", b"abcde"):
            target = self.root / "download"
            class Opener:
                def open(inner, request, timeout):
                    return Response(body)
            with patch.object(FETCH.urllib.request, "build_opener", return_value=Opener()):
                with self.assertRaises(FETCH.VerificationError):
                    FETCH.download(Response().geturl(), target, 4)
            self.assertFalse(target.exists())
        with self.assertRaises(FETCH.VerificationError):
            FETCH.download("http://extensions.duckdb.org/v1.5.6/unsafe", self.root / "download", 4)
        with self.assertRaises(FETCH.VerificationError):
            FETCH.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://untrusted.invalid/artifact")

    def test_permanent_get_failure_does_not_retry(self):
        class Opener:
            calls = 0
            def open(inner, request, timeout):
                inner.calls += 1
                raise urllib.error.HTTPError("url", 403, "permanent", {}, None)
        opener = Opener()
        with patch.object(FETCH.urllib.request, "build_opener", return_value=opener), patch.object(FETCH.time, "sleep") as sleep:
            with self.assertRaises(FETCH.VerificationError):
                FETCH.download(Response().geturl(), self.root / "download", 4)
        self.assertEqual(opener.calls, 1)
        sleep.assert_not_called()

    def test_atomic_install_replays_a_crash_without_replacing_conflicting_or_linked_evidence(self):
        first = self.root / "first"
        first.write_bytes(b"immutable")
        first.chmod(0o600)
        final = self.root / "installed"
        FETCH.install(first, final)
        self.assertFalse(first.exists())
        self.assertEqual(final.stat().st_nlink, 1)
        retry = self.root / "retry"
        retry.write_bytes(b"immutable")
        FETCH.install(retry, final)
        retry.write_bytes(b"conflict")
        with self.assertRaises(FETCH.VerificationError):
            FETCH.install(retry, final)
        self.assertEqual(final.read_bytes(), b"immutable")
        link = self.root / "link"
        link.symlink_to(final)
        with self.assertRaises(FETCH.VerificationError):
            FETCH.install(retry, link)
        os.link(final, self.root / "hardlink")
        with self.assertRaises(FETCH.VerificationError):
            FETCH.install(retry, final)

    def test_footer_requires_exact_version_platform_cpp_and_no_unknown_fields(self):
        values = ["4", "osx_arm64", "v1.5.6", "4bc690d", "CPP", "", "", ""]
        path = self.root / "extension"
        def write(fields):
            path.write_bytes(b"binary" + b"".join(v.encode().ljust(32, b"\0") for v in fields[::-1]) + bytes(256))
        write(values)
        self.assertEqual(FETCH.footer(path), {"magic":"4", "platform":"osx_arm64", "duckdb_version":"v1.5.6", "extension_version":"4bc690d", "abi":"CPP"})
        write(values[:5] + ["unknown", "", ""])
        with self.assertRaises(FETCH.VerificationError):
            FETCH.footer(path)

    def test_real_rsa_signature_across_chunk_boundary_rejects_modified_payload_and_signature(self):
        private = self.root / "fixture-private.pem"
        public = self.root / "fixture-public.pem"
        subprocess.run(["openssl", "genrsa", "-out", str(private), "2048"], capture_output=True, check=True)
        subprocess.run(["openssl", "rsa", "-in", str(private), "-pubout", "-out", str(public)], capture_output=True, check=True)
        payload = b"a" * FETCH.MIB + b"tail and metadata"
        expected = hashlib.sha256(hashlib.sha256(b"a" * FETCH.MIB).digest() + hashlib.sha256(b"tail and metadata").digest()).digest()
        digest = self.root / "digest"
        digest.write_bytes(expected)
        signature = self.root / "signed"
        subprocess.run(["openssl", "pkeyutl", "-sign", "-inkey", str(private), "-in", str(digest), "-pkeyopt", "digest:sha256", "-out", str(signature)], capture_output=True, check=True)
        path = self.root / "extension"
        data = payload + signature.read_bytes()
        path.write_bytes(data)
        FETCH.verify_signature(path, public, self.root)
        path.write_bytes(b"b" + data[1:])
        with self.assertRaises(FETCH.VerificationError):
            FETCH.verify_signature(path, public, self.root)
        path.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
        with self.assertRaises(FETCH.VerificationError):
            FETCH.verify_signature(path, public, self.root)


if __name__ == "__main__":
    unittest.main()
