#!/usr/bin/env python3
"""Explicitly authorized, owned-bucket experiments. Never discover other buckets.

Requires a private ownership state written after successful bucket creation.
All mutations/deletions are confined to that exact bucket and recorded objects.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.parse
import urllib.request

from gcs_range import Reader, Refused, unique_json


def require(ok, message):
    if not ok:
        raise Refused(message)


def save(path, value):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, indent=2) + "\n")
    temp.chmod(0o600)
    temp.replace(path)


class Experiment:
    def __init__(self, state_path):
        require(not state_path.is_symlink(), "ownership state must not be a symlink")
        self.state_path = state_path.resolve(strict=True)
        info = self.state_path.stat()
        require(info.st_mode & 0o077 == 0 and info.st_uid == os.getuid() and info.st_nlink == 1,
                "ownership state must be private and owned")
        self.state = json.loads(self.state_path.read_text(), object_pairs_hook=unique_json)
        s = self.state
        require(s.get("created") is True and s["project"] == "entit-finance-prod"
                and s["project_number"] == "828148855924"
                and re.fullmatch(r"grv-view-probe-[0-9a-f]{24}", s["bucket"]), "invalid owned scope")
        require(self.state_path.parent == Path(s["directory"]).resolve(), "ownership journal location differs")
        self.directory = self.state_path.parent
        self.flags = ["--project=" + s["project"], "--account=" + s["account"], "--quiet"]
        self.environment = dict(os.environ, CLOUDSDK_CORE_DISABLE_FILE_LOGGING="true",
                                CLOUDSDK_CORE_LOG_HTTP="false", CLOUDSDK_CORE_DISABLE_PROMPTS="true")
        self.registry_path = self.directory / "objects.json"
        self.registry = json.loads(self.registry_path.read_text()) if self.registry_path.exists() else []
        self.token = self.command(["gcloud", "auth", "print-access-token", *self.flags]).decode().strip()
        require(bool(re.fullmatch(r"[A-Za-z0-9._~+/=-]+", self.token)), "credential helper refused")
        self.api = Reader("gs://" + s["bucket"] + "/probe", self.token, s["project"])
        # Verify immutable project/bucket association before any effect.
        meta = self.request("GET", "/storage/v1/b/" + s["bucket"],
                            {"fields": "name,projectNumber,iamConfiguration,softDeletePolicy"})
        require(meta.get("name") == s["bucket"] and str(meta.get("projectNumber")) == s["project_number"],
                "bucket project identity differs")
        require(meta.get("iamConfiguration", {}).get("publicAccessPrevention") == "enforced",
                "bucket must have public access prevention")
        require(meta.get("softDeletePolicy", {}).get("retentionDurationSeconds") in ("0", None),
                "throwaway bucket must not retain soft-deleted objects")

    def command(self, argv, timeout=60):
        result = subprocess.run(argv, env=self.environment, stdin=subprocess.DEVNULL,
                                capture_output=True, timeout=timeout)
        require(result.returncode == 0 and len(result.stdout) <= 1024 * 1024,
                "explicit cloud command failed (output suppressed)")
        return result.stdout

    def request(self, method, path, query=None, data=None, headers=None):
        params = dict(query or {}, userProject=self.state["project"])
        url = "https://storage.googleapis.com" + path + "?" + urllib.parse.urlencode(params)
        h = {"Authorization": "Bearer " + self.token, "Accept-Encoding": "identity"}
        h.update(headers or {})
        failure = False
        try:
            with self.api.opener.open(urllib.request.Request(url, data=data, headers=h, method=method),
                                      timeout=30) as response:
                body = response.read(65537)
                require(len(body) <= 65536, "API response exceeds budget")
                return json.loads(body, object_pairs_hook=unique_json) if body else {}
        except Refused:
            raise
        except Exception as error:
            if hasattr(error, "close"):
                error.close()
            failure = True
        if failure:
            raise Refused("owned fixture API request failed (details suppressed)")

    def upload(self, key, path, expected="0"):
        require(key.startswith("fixture/") and len(key) < 1024 and ".." not in key.split("/"),
                "fixture upload outside exact scope")
        raw = path.read_bytes()
        require(len(raw) <= 32 * 1024 * 1024, "fixture too large")
        # Journal intent before effect: cleanup will refuse unknown objects.
        intent = {"key": key, "generation": None, "size": len(raw),
                  "sha256": hashlib.sha256(raw).hexdigest()}
        self.registry.append(intent)
        save(self.registry_path, self.registry)
        meta = self.request("POST", "/upload/storage/v1/b/" + self.state["bucket"] + "/o",
                            {"uploadType": "media", "name": key, "ifGenerationMatch": expected}, raw,
                            {"Content-Type": "application/octet-stream"})
        require(meta.get("name") == key and meta.get("bucket") == self.state["bucket"]
                and int(meta["size"]) == len(raw), "upload metadata differs")
        intent["generation"] = meta["generation"]
        save(self.registry_path, self.registry)
        return intent

    def uri(self, key):
        encoded = "".join(chr(b) if b == 47 or (33 <= b <= 126 and chr(b) not in '\"#%<>?`{}[]*\\')
                          else f"%{b:02X}" for b in key.encode())
        return "gs://" + self.state["bucket"] + "/" + encoded

    def reader(self, obj):
        reader = Reader(self.uri(obj["key"]), self.token, self.state["project"])
        reader.pin(obj["size"], obj["generation"])
        return reader

    def object_path(self, key):
        return "/storage/v1/b/" + self.state["bucket"] + "/o/" + urllib.parse.quote(key, safe="")

    def delete(self, obj):
        self.request("DELETE", self.object_path(obj["key"]),
                     {"generation": obj["generation"], "ifGenerationMatch": obj["generation"]})
        obj["deleted"] = True
        save(self.registry_path, self.registry)

    def requester_pays(self):
        bucket = self.request("GET", "/storage/v1/b/" + self.state["bucket"], {"fields": "name,billing"})
        require(bucket.get("billing", {}).get("requesterPays") is True, "Requester Pays is not enabled")
        obj = next((o for o in reversed(self.registry) if not o.get("deleted") and o.get("generation")), None)
        require(obj is not None, "no owned object to probe")
        billed = self.reader(obj)
        require(billed.read(0, 4) == b"PAR1", "billed range differs")
        no_project = Reader(self.uri(obj["key"]), self.token)
        absent_project_refused = False
        try:
            no_project.pin(obj["size"], obj["generation"])
            no_project.read(0, 4)
        except Refused:
            absent_project_refused = True
        evidence = json.loads((self.directory / "transport.json").read_text())
        evidence["checks"]["requester_pays_enabled_explicit_project_passed"] = True
        evidence["checks"]["absent_project_request_refused_for_selected_principal"] = absent_project_refused
        evidence["limitations"] = ["Same available reader/provisioning principal; independent least-privilege identity not tested.",
            "Billing attribution is not established by response behavior."]
        save(self.directory / "transport.json", evidence)
        print("Requester Pays: explicit project passed; absent project refused:", absent_project_refused)

    def transport(self, fixture):
        special = "fixture/space quote'percent%hash#star*[bracket]+unicode-雪.parquet"
        obj = self.upload(special, fixture)
        reader = self.reader(obj)
        reader.verify_hash(obj["sha256"])
        require(reader.read(0, 4) == b"PAR1" and reader.read(obj["size"] - 8, 8)[-4:] == b"PAR1",
                "Parquet magic differs")
        hash_probe_requests, hash_probe_bytes = reader.requests, reader.media_bytes
        observed = {}
        def observe(status, headers):
            observed.update(status=status, content_length=headers.get("Content-Length"),
                content_range=headers.get("Content-Range"), content_encoding=headers.get("Content-Encoding"),
                generation_header_present=headers.get("x-goog-generation") is not None,
                generation_matches=headers.get("x-goog-generation") == obj["generation"])
        raw = reader.get(reader.url(True, {"generation": obj["generation"], "ifGenerationMatch": obj["generation"]}),
                         4, {"Range": "bytes=0-3"}, observe)
        require(raw == b"PAR1" and observed["status"] == 206, "observed range response differs")
        checks = {"special_key_roundtrip": True, "full_sha256": True,
                  "generation_string": True, "range_requests": hash_probe_requests,
                  "accepted_media_bytes": hash_probe_bytes}
        contradictory = reader.url(True, {"generation": obj["generation"],
                                           "ifGenerationMatch": str(int(obj["generation"]) + 1)})
        try:
            reader.get(contradictory, 4, {"Range": "bytes=0-3"})
        except Refused:
            checks["conflicting_generation_precondition_refused"] = True
        else:
            raise Refused("conflicting generation condition unexpectedly accepted")
        # Auth remains explicit and private; invalid token must refuse.
        bad = Reader(self.uri(special), "invalid-test-token", self.state["project"])
        try:
            bad.pin(obj["size"], obj["generation"])
        except Refused:
            checks["invalid_credentials_refused"] = True
        else:
            raise Refused("invalid credentials unexpectedly accepted")
        replacement = self.upload(special, fixture, obj["generation"])
        require(replacement["generation"] != obj["generation"], "overwrite did not change generation")
        require(reader.read(0, 4) == b"PAR1", "retained generation not coherent")
        checks["retained_generation_read_after_overwrite"] = True
        self.delete(obj)
        try:
            reader.read(0, 4)
        except Refused:
            checks["deleted_pinned_generation_refused"] = True
        else:
            raise Refused("deleted generation unexpectedly readable")
        current = self.reader(replacement)
        self.request("PATCH", self.object_path(special),
                     {"ifGenerationMatch": replacement["generation"]},
                     json.dumps({"contentEncoding": "gzip"}).encode(), {"Content-Type": "application/json"})
        try:
            current.read(0, 4)
        except Refused:
            checks["encoding_change_after_pin_refused"] = True
        else:
            raise Refused("changed outer encoding unexpectedly accepted")
        # Restore only this fixture's metadata, same generation.
        self.request("PATCH", self.object_path(special),
                     {"ifGenerationMatch": replacement["generation"]},
                     b'{"contentEncoding":"identity"}', {"Content-Type": "application/json"})
        checks["restored_identity_range"] = current.read(0, 4) == b"PAR1"
        save(self.directory / "transport.json", {"status": "passed", "transport": "real-gcs-json-api",
             "project": self.state["project"], "bucket": self.state["bucket"], "checks": checks,
             "observed_header_range": observed,
             "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        print(json.dumps(checks, indent=2))

    def upload_native(self, directory):
        records = []
        for name in ("old.parquet", "new.parquet", "empty.parquet", "old-no-stats.parquet", "new-no-stats.parquet"):
            path = directory / name
            require(path.is_file() and not path.is_symlink(), "missing exact native fixture")
            obj = self.upload("fixture/native/" + name, path)
            reader = self.reader(obj)
            require(reader.read(0, 4) == b"PAR1", "native fixture header differs")
            records.append({"name": name, "uri": self.uri(obj["key"]), "local_path": str(path.resolve()),
                            "size": obj["size"], "generation": obj["generation"], "sha256": obj["sha256"]})
        save(self.directory / "native-manifest.json", {"project": self.state["project"], "objects": records})
        print("native_manifest", self.directory / "native-manifest.json")

    def cleanup(self):
        # Inventory only this created bucket, including retained versions. Compare
        # every exact object to a pre-journaled upload intent before deletion.
        allowed = {x["key"] for x in self.registry}
        inventory = self.request("GET", "/storage/v1/b/" + self.state["bucket"] + "/o",
                                 {"versions": "true", "maxResults": "100"})
        require(not inventory.get("nextPageToken"), "unexpected inventory size; manual review required")
        deleted = 0
        items = inventory.get("items", [])
        for meta in items:
            require(meta["name"] in allowed and meta["bucket"] == self.state["bucket"]
                    and re.fullmatch(r"[1-9][0-9]{0,19}", meta["generation"]),
                    "unknown object in owned bucket; cleanup refused")
        for meta in items:
            self.request("DELETE", self.object_path(meta["name"]),
                         {"generation": meta["generation"], "ifGenerationMatch": meta["generation"]})
            deleted += 1
        empty = self.request("GET", "/storage/v1/b/" + self.state["bucket"] + "/o", {"versions": "true"})
        require(not empty.get("items") and not empty.get("nextPageToken"), "bucket not empty")
        self.request("DELETE", "/storage/v1/b/" + self.state["bucket"])
        # Preserve exact HTTP status privately, not arbitrary error text.
        absent = False
        try:
            with self.api.opener.open(urllib.request.Request(
                "https://storage.googleapis.com/storage/v1/b/" + self.state["bucket"],
                headers={"Authorization": "Bearer " + self.token}), timeout=10):
                pass
        except urllib.error.HTTPError as error:
            absent = error.code == 404
            error.close()
        require(absent, "bucket absence not proved")
        save(self.directory / "cleanup.json", {"bucket": self.state["bucket"], "deleted_generations": deleted,
             "all_versions_empty_before_bucket_delete": True, "bucket_absence_http_404": True})
        self.state["cleaned_up"] = True
        save(self.state_path, self.state)
        print("cleanup: exact owned bucket deleted; absence verified by HTTP 404")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("transport").add_argument("--fixture", type=Path, required=True)
    sub.add_parser("upload-native").add_argument("--fixtures", type=Path, required=True)
    sub.add_parser("requester-pays")
    sub.add_parser("cleanup")
    args = parser.parse_args()
    try:
        exp = Experiment(args.state)
        if args.action == "transport":
            exp.transport(args.fixture)
        elif args.action == "upload-native":
            exp.upload_native(args.fixtures)
        elif args.action == "requester-pays":
            exp.requester_pays()
        else:
            exp.cleanup()
    except (Refused, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"refused: {error if isinstance(error, Refused) else 'command timeout'}\n")


if __name__ == "__main__":
    main()
