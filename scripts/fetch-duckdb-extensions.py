#!/usr/bin/env python3
"""Fetch signed pinned DuckDB extensions into protected local packaging input.

No INSTALL, credential lookup, native compilation, service writes or release
publication occurs. --verify-only is offline; --cache also avoids all network
calls. Production loading and S3 capabilities are separate guarded lifecycles.
Requires Python 3.11+ and OpenSSL's pkeyutl verifier.
"""
import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "notices/native-extensions.json"
PLATFORMS = ("osx_arm64", "osx_amd64", "linux_amd64", "linux_arm64")
NAMES = ("httpfs", "aws")
MIB = 1024 * 1024


class VerificationError(Exception):
    pass


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def relative_path(value):
    path = Path(value)
    if path.is_absolute() or any(part in (".", "..") for part in path.parts):
        raise VerificationError("manifest path must be canonical and relative")
    return path


def regular(path, private=False):
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise VerificationError("artifact must be a regular, unlinked file")
    if private and (metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077):
        raise VerificationError("installed artifact must be owned and private")
    return metadata


def protected_output(path):
    """Check every ancestor before creating a component; never follow links."""
    path = path.expanduser().absolute()
    if ".." in path.parts:
        raise VerificationError("output path must be canonical")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        check_directory(current)
        current /= component
        try:
            current.lstat()
        except FileNotFoundError:
            current.mkdir(mode=0o700)
        check_directory(current)
    return path


def check_directory(path):
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
        raise VerificationError("output directory ancestors must be protected")
    if metadata.st_uid not in (0, os.geteuid()):
        raise VerificationError("output directory ancestor has an untrusted owner")


def read_notice(manifest, entry):
    path = ROOT / "notices" / relative_path(entry["path"])
    regular(path)
    if digest(path) != entry["sha256"]:
        raise VerificationError("upstream notice or signing key checksum differs")
    return path


def load_manifest():
    manifest = json.loads(MANIFEST.read_text())
    if (manifest["manifest_version"] != 1
            or manifest["duckdb_version"] != "v1.5.6"
            or set(manifest["platforms"]) != set(PLATFORMS)):
        raise VerificationError("unsupported native extension registry")
    artifacts = manifest["artifacts"]
    if len(artifacts) != len(PLATFORMS) * len(NAMES):
        raise VerificationError("registry must cover both extensions on all platforms")
    seen = set()
    for entry in artifacts:
        pair = (entry["platform"], entry["name"])
        if pair in seen or pair[0] not in PLATFORMS or pair[1] not in NAMES:
            raise VerificationError("invalid or repeated artifact identity")
        seen.add(pair)
        expected_url = (f"https://extensions.duckdb.org/v1.5.6/{pair[0]}/"
                        f"{pair[1]}.duckdb_extension.gz")
        if entry["url"] != expected_url:
            raise VerificationError("artifact URL differs from pinned official origin")
        for field in ("sha256", "compressed_sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", entry[field]):
                raise VerificationError("invalid artifact checksum")
        if not (512 < entry["bytes"] <= 32 * MIB
                and 0 < entry["compressed_bytes"] <= 16 * MIB):
            raise VerificationError("artifact exceeds pinned size budget")
    read_notice(manifest, manifest["signature"]["public_key"])
    read_notice(manifest, manifest["signature"]["signing_source_license"])
    for extension in manifest["extensions"]:
        for notice in extension["files"]:
            read_notice(manifest, notice)
    return manifest


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, url):
        if response is not None:
            response.close()
        raise VerificationError("artifact redirects are forbidden")


def download(url, destination, expected_bytes, attempts=3):
    """Only bounded HTTPS GETs to the fixed public registry; no ambient auth."""
    if not url.startswith("https://extensions.duckdb.org/v1.5.6/"):
        raise VerificationError("download origin is not the pinned public repository")
    opener = urllib.request.build_opener(NoRedirect())
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={
                "User-Agent": "grv-pinned-extension-fetch/1", "Accept-Encoding": "identity"})
            deadline = time.monotonic() + 30
            with opener.open(request, timeout=20) as response:
                if response.status != 200 or response.geturl() != url:
                    raise VerificationError("artifact GET did not return the complete pinned URL")
                length = response.headers.get("Content-Length")
                if length is not None and int(length) != expected_bytes:
                    raise VerificationError("artifact Content-Length differs from pinned size")
                with destination.open("xb") as sink:
                    os.chmod(destination, 0o600)
                    remaining = expected_bytes
                    while remaining:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("artifact GET exceeded its bounded deadline")
                        chunk = response.read1(min(64 * 1024, remaining))
                        if not chunk:
                            raise VerificationError("artifact download is truncated")
                        sink.write(chunk)
                        remaining -= len(chunk)
                    if response.read(1):
                        raise VerificationError("artifact download exceeds pinned size")
                    sink.flush()
                    os.fsync(sink.fileno())
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            destination.unlink(missing_ok=True)
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            retryable = not isinstance(error, urllib.error.HTTPError) or error.code in (
                408, 429, 500, 502, 503, 504)
            if not retryable or attempt + 1 == attempts:
                raise VerificationError("bounded public artifact GET failed") from error
            time.sleep(0.25 * (attempt + 1))
        except Exception:
            destination.unlink(missing_ok=True)
            raise


def check_bytes(path, size, sha256, private=False):
    if regular(path, private).st_size != size or digest(path) != sha256:
        raise VerificationError("artifact checksum or size differs from the registry")


def decode(compressed, destination, entry):
    check_bytes(compressed, entry["compressed_bytes"], entry["compressed_sha256"])
    try:
        with gzip.open(compressed, "rb") as source, destination.open("xb") as sink:
            os.chmod(destination, 0o600)
            remaining = entry["bytes"]
            while remaining:
                chunk = source.read(min(MIB, remaining))
                if not chunk:
                    raise VerificationError("decompressed artifact is truncated")
                sink.write(chunk)
                remaining -= len(chunk)
            if source.read(1):
                raise VerificationError("decompressed artifact exceeds pinned size")
            sink.flush()
            os.fsync(sink.fileno())
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    check_bytes(destination, entry["bytes"], entry["sha256"])


def footer(path):
    with path.open("rb") as source:
        source.seek(-512, os.SEEK_END)
        data = source.read(256)
    fields = [data[index:index + 32].rstrip(b"\0").decode("ascii")
              for index in range(0, 256, 32)][::-1]
    if any(fields[5:]):
        raise VerificationError("unknown extension metadata fields")
    return dict(zip(("magic", "platform", "duckdb_version", "extension_version", "abi"), fields[:5]))


def verify_signature(path, public_key, scratch):
    """Pinned DuckDB two-level digest, verified by OpenSSL RSA PKCS#1 v1.5."""
    openssl = shutil.which("openssl")
    if openssl is None:
        raise VerificationError("OpenSSL pkeyutl is required for signature verification")
    chunks = []
    with path.open("rb") as source:
        remaining = path.stat().st_size - 256
        if remaining <= 0:
            raise VerificationError("extension is too short to contain a signature")
        while remaining:
            chunk = source.read(min(MIB, remaining))
            if not chunk:
                raise VerificationError("extension changed while hashing its signature")
            chunks.append(hashlib.sha256(chunk).digest())
            remaining -= len(chunk)
        signature = source.read(256)
        if len(signature) != 256 or source.read(1):
            raise VerificationError("extension signature size is invalid")
    digest_path = scratch / "signature-digest.bin"
    signature_path = scratch / "signature.bin"
    digest_path.write_bytes(hashlib.sha256(b"".join(chunks)).digest())
    signature_path.write_bytes(signature)
    digest_path.chmod(0o600)
    signature_path.chmod(0o600)
    result = subprocess.run([
        openssl, "pkeyutl", "-verify", "-pubin", "-inkey", str(public_key),
        "-sigfile", str(signature_path), "-in", str(digest_path),
        "-pkeyopt", "digest:sha256"], capture_output=True, timeout=10, check=False)
    if result.returncode != 0:
        raise VerificationError("official DuckDB extension signature verification failed")


def verify_binary(path, entry, public_key, scratch, private=False):
    check_bytes(path, entry["bytes"], entry["sha256"], private)
    if footer(path) != entry["footer"]:
        raise VerificationError("extension version/platform/ABI/source footer differs")
    verify_signature(path, public_key, scratch)
    # Reread identity/content after the external verifier before installation.
    check_bytes(path, entry["bytes"], entry["sha256"], private)


def install(source, destination):
    """No replacement: crash retries adopt only identical protected contents."""
    try:
        os.link(source, destination, follow_symlinks=False)
    except FileExistsError:
        regular(destination, private=True)
        if source.stat().st_size != destination.stat().st_size or digest(source) != digest(destination):
            raise VerificationError("existing extension packaging input differs; replacement is explicit")
    else:
        source.unlink()
    directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def fetch(manifest, output, platforms, cache=None, verify_only=False):
    output = protected_output(output)
    lock_path = output / ".fetch.lock"
    lock = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        regular(lock_path, private=True)
        fcntl.flock(lock, fcntl.LOCK_EX)
        public_key = read_notice(manifest, manifest["signature"]["public_key"])
        for platform in platforms:
            directory = output / platform
            if verify_only:
                check_directory(directory)
            else:
                directory = protected_output(directory)
            with tempfile.TemporaryDirectory(prefix=".verify-", dir=output) as temporary:
                scratch = Path(temporary)
                for entry in (a for a in manifest["artifacts"] if a["platform"] == platform):
                    binary = directory / f'{entry["name"]}.duckdb_extension'
                    compressed = directory / (binary.name + ".gz")
                    if verify_only:
                        check_bytes(compressed, entry["compressed_bytes"], entry["compressed_sha256"], private=True)
                        verify_binary(binary, entry, public_key, scratch, private=True)
                        continue
                    encoded = scratch / (binary.name + ".gz")
                    decoded = scratch / binary.name
                    if cache is None:
                        download(entry["url"], encoded, entry["compressed_bytes"])
                    else:
                        cached = cache / platform / encoded.name
                        check_bytes(cached, entry["compressed_bytes"], entry["compressed_sha256"])
                        shutil.copyfile(cached, encoded, follow_symlinks=False)
                        encoded.chmod(0o600)
                    decode(encoded, decoded, entry)
                    verify_binary(decoded, entry, public_key, scratch, private=True)
                    install(encoded, compressed)
                    install(decoded, binary)
        return {"duckdb_version": manifest["duckdb_version"], "verified_platforms": platforms,
                "output": str(output), "capabilities_changed": False}
    finally:
        os.close(lock)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True,
                        help="protected directory, with no symlink or writable ancestors")
    parser.add_argument("--platform", action="append", choices=PLATFORMS, required=True,
                        help="repeat to select platforms; all eight artifacts have pinned signed hashes")
    parser.add_argument("--cache", type=Path,
                        help="offline source containing <platform>/<name>.duckdb_extension.gz")
    parser.add_argument("--verify-only", action="store_true",
                        help="verify existing pinned compressed/binary files without network calls")
    args = parser.parse_args()
    try:
        if args.cache is not None and args.verify_only:
            raise VerificationError("--cache and --verify-only are mutually exclusive")
        result = fetch(load_manifest(), args.output, list(dict.fromkeys(args.platform)),
                       args.cache, args.verify_only)
        print(json.dumps(result, sort_keys=True))
    except (VerificationError, OSError, ValueError, gzip.BadGzipFile,
            subprocess.SubprocessError) as error:
        parser.exit(1, f"extension verification failed: {error}\n")


if __name__ == "__main__":
    main()
