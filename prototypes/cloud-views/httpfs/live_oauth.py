#!/usr/bin/env python3
"""Explicitly authorized owned-fixture stock HTTPFS OAuth experiment.

Creates only an isolated bucket; no IAM, service-account, key or policy changes.
OAuth tokens stay in process memory/child environment, never arguments or output.
Private ownership journal supports cleanup after interruption. No ambient account.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent
ENV = {k: v for k, v in os.environ.items() if not k.upper().startswith(('GRV_HTTPFS_', 'CLOUDSDK_AUTH_'))}
ENV.update(CLOUDSDK_CORE_DISABLE_FILE_LOGGING='true', CLOUDSDK_CORE_LOG_HTTP='false', CLOUDSDK_CORE_DISABLE_PROMPTS='true')


def private_json(path, value):
    """Atomic owner-only replacement within the private journal directory."""
    if path.is_symlink():
        raise RuntimeError('Refuse symlink journal/evidence output')
    fd, temporary = tempfile.mkstemp(prefix='.journal-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Probe:
    def __init__(self, state_path):
        info, parent = state_path.lstat(), state_path.parent.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077
                or not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or parent.st_mode & 0o077):
            raise RuntimeError('Ownership journal must be owner-only regular file in private directory')
        self.path = state_path
        self.state = json.loads(state_path.read_text())
        if not re.fullmatch(r'grv-httpfs-oauth-[0-9a-f]{24}', self.state.get('bucket', '')) or not self.state.get('account') or not self.state.get('project'):
            raise RuntimeError('Invalid owned resource journal')

    def save(self):
        private_json(self.path, self.state)

    def check_auth(self):
        if any(v for k, v in os.environ.items() if k.upper().startswith('CLOUDSDK_AUTH_')):
            raise RuntimeError('Ambient gcloud authentication override is not permitted')
        result = self.gcloud(['config', 'list', '--all', '--format=json(auth.impersonate_service_account,auth.credential_file_override,auth.access_token_file)'])
        auth = json.loads(result.stdout).get('auth', {})
        if any(auth.get(k) for k in ('impersonate_service_account', 'credential_file_override', 'access_token_file')):
            raise RuntimeError('Configured gcloud authentication override is not permitted')

    def gcloud(self, args, allow_failure=False):
        command = ['gcloud', *args, '--project=' + self.state['project'], '--account=' + self.state['account'], '--quiet']
        result = subprocess.run(command, env=ENV, capture_output=True, timeout=90)
        if result.returncode and not allow_failure:
            raise RuntimeError('Explicit gcloud operation failed (details suppressed)')
        return result

    def token(self):
        return self.gcloud(['auth', 'print-access-token']).stdout.decode().strip()

    def request(self, url, method='GET', data=None, headers=None, allowed=(200,)):
        req = urllib.request.Request(url, method=method, data=data, headers={'Authorization': 'Bearer ' + self.token(), **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=90) as response:
                code, body = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, body = error.code, error.read(65536)
            error.close()
        if code not in allowed:
            raise RuntimeError('Explicit GCS operation failed HTTP ' + str(code))
        return code, json.loads(body) if body else {}

    def bucket_url(self):
        return 'https://storage.googleapis.com/storage/v1/b/' + self.state['bucket']

    def upload(self, key, local):
        url = 'https://storage.googleapis.com/upload/storage/v1/b/' + self.state['bucket'] + '/o?' + urllib.parse.urlencode({'uploadType': 'media', 'name': key, 'ifGenerationMatch': '0'})
        _, obj = self.request(url, 'POST', local.read_bytes(), {'Content-Type': 'application/octet-stream'})
        self.state.setdefault('objects', []).append({'key': key, 'generation': obj['generation'], 'size': int(obj['size'])})
        self.save()

    def create(self):
        self.request(self.bucket_url(), allowed=(404,))
        self.state.update(creation_attempted=True, ownership_marker=uuid.uuid4().hex)
        self.save()
        body = {'name': self.state['bucket'], 'location': 'EUROPE-WEST1',
                'labels': {'grv-probe-owner': self.state['ownership_marker']},
                'iamConfiguration': {'uniformBucketLevelAccess': {'enabled': True}, 'publicAccessPrevention': 'enforced'},
                'softDeletePolicy': {'retentionDurationSeconds': '0'},
                'lifecycle': {'rule': [{'action': {'type': 'Delete'}, 'condition': {'age': 1}}]}}
        self.request('https://storage.googleapis.com/storage/v1/b?' + urllib.parse.urlencode({'project': self.state['project']}),
                     'POST', json.dumps(body).encode(), {'Content-Type': 'application/json'})
        self.state['bucket_created'] = True
        self.save()

    def finish_cleanup(self, status, inventory_empty):
        evidence_path = self.path.parent / 'cleanup.json'
        prior = json.loads(evidence_path.read_text()) if evidence_path.exists() else {}
        evidence = {'bucket': self.state['bucket'],
                    'deleted_object_generations': self.state.get('deleted_object_generations', prior.get('deleted_object_generations', 0)),
                    'all_versions_inventory_empty': inventory_empty or prior.get('all_versions_inventory_empty', False),
                    'bucket_absence_http_status': status, 'iam_changes': False, 'key_creation': False, 'organization_policy_changes': False}
        private_json(evidence_path, evidence)
        self.state['cleaned_up'] = True
        self.save()

    def cleanup(self):
        if not (self.state.get('bucket_created') or self.state.get('creation_attempted')) or self.state.get('cleaned_up'):
            return
        status, bucket = self.request(self.bucket_url(), allowed=(200, 404))
        if status == 404:
            self.finish_cleanup(404, self.state.get('all_versions_inventory_empty', False))
            return
        marker = self.state.get('ownership_marker')
        if not marker or bucket.get('labels', {}).get('grv-probe-owner') != marker:
            raise RuntimeError('Bucket ownership marker mismatch; no cleanup mutations performed')
        # Inventory entire uniquely owned bucket, including versions, before deleting.
        inventory = []
        token = None
        while True:
            params = {'versions': 'true'}
            if token:
                params['pageToken'] = token
            _, page = self.request(self.bucket_url() + '/o?' + urllib.parse.urlencode(params))
            inventory.extend(page.get('items', []))
            token = page.get('nextPageToken')
            if not token:
                break
        for obj in inventory:
            url = self.bucket_url() + '/o/' + urllib.parse.quote(obj['name'], safe='') + '?' + urllib.parse.urlencode({'generation': obj['generation'], 'ifGenerationMatch': obj['generation']})
            self.request(url, 'DELETE', allowed=(204, 404))
            self.state['deleted_object_generations'] = self.state.get('deleted_object_generations', 0) + 1
            self.save()
        _, page = self.request(self.bucket_url() + '/o?versions=true')
        if page.get('items'):
            raise RuntimeError('Cleanup inventory not empty')
        self.state['all_versions_inventory_empty'] = True
        self.save()
        self.request(self.bucket_url(), 'DELETE', allowed=(204, 404))
        status, _ = self.request(self.bucket_url(), allowed=(404,))
        self.finish_cleanup(status, True)
        print('Owned fixture generations/bucket deleted; empty inventory and HTTP404 verified')

    def run(self, manifest, output, trials=None):
        env = dict(ENV, GRV_HTTPFS_BEARER_TOKEN=self.token(), GRV_HTTPFS_ACCOUNT=self.state['account'], GRV_HTTPFS_PROJECT=self.state['project'], GRV_HTTPFS_LIVE_APPROVED='yes')
        command = ['bash', str(HERE / 'run.sh'), '--live', '--oauth', '--manifest', str(manifest), '--output', str(output)]
        if trials:
            command += ['--trials', trials]
        result = subprocess.run(command, env=env, capture_output=True, timeout=300)
        # Do not relay unknown native/CLI output or error strings.
        if result.returncode:
            raise RuntimeError('Stock HTTPFS live trial failed (details suppressed)')
        evidence = json.loads(output.read_text())
        for trial in evidence['trials']:
            if trial['trial'] != 'guard' and not all(trial.get(k) for k in ('correct', 'nonallowlisted_rejected', 'glob_rejected', 'nested_nonallowlisted_rejected')):
                raise RuntimeError('Live correctness/negative-boundary assertion failed')
        return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--account')
    parser.add_argument('--project')
    parser.add_argument('--fixtures', type=Path)
    parser.add_argument('--approved', action='store_true')
    parser.add_argument('--cleanup-state', type=Path)
    args = parser.parse_args()
    if args.cleanup_state:
        probe = Probe(args.cleanup_state)
        probe.check_auth()
        probe.cleanup()
        return
    if not (args.approved and args.account and args.project and args.fixtures):
        raise RuntimeError('Explicit approval/account/project/fixtures required')
    fixtures = args.fixtures.resolve()
    names = [('old.parquet', 'old.parquet', '0'), ('new.parquet', 'new.parquet', '1'), ('empty.parquet', 'empty.parquet', '1'), ('old-no-stats.parquet', 'old-no-stats.parquet', '0'), ('new-no-stats.parquet', 'new-no-stats.parquet', '1'), ('space + percent% # ü.parquet', 'old.parquet', '0'), ("quote's.parquet", 'new.parquet', '1')]
    if any(not (fixtures / file).is_file() for _, file, _ in names):
        raise RuntimeError('Missing fixtures')
    directory = Path(tempfile.mkdtemp(prefix='grv-httpfs-oauth-live-'))
    directory.chmod(0o700)
    state = directory / 'state.json'
    private_json(state, {'account': args.account, 'project': args.project, 'bucket': 'grv-httpfs-oauth-' + uuid.uuid4().hex[:24], 'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat()})
    probe = Probe(state)
    print('Private ownership/evidence state:', state, flush=True)
    probe.check_auth()
    try:
        probe.create()
        for key, file, _ in names:
            probe.upload(key, fixtures / file)
        def manifest(name, entries):
            path = directory / (name + '.tsv')
            path.write_text(''.join('https://storage.googleapis.com/' + probe.state['bucket'] + '/' + urllib.parse.quote(key, safe='/') + '\t' + str(fixtures / file) + '\t' + extra + '\n' for key, file, extra in entries))
            return path
        standard = manifest('standard', names[:2])
        results = {'account': args.account, 'project': args.project, 'authentication': 'OAuth bearer TYPE http, stock signed HTTPFS', 'live': True, 'cases': []}
        for label, entries, trials in [('standard', names[:2], None), ('special', names[5:], 'wide'), ('empty', names[2:3], 'empty'), ('no-statistics', names[3:5], 'no-stats-selective'), ('guard', names[:2], 'guard')]:
            results['cases'].append({'case': label, 'evidence': probe.run(manifest(label, entries), directory / (label + '.json'), trials)})
            private_json(directory / 'results.json', results)
            print('Live case passed:', label, flush=True)
        probe.gcloud(['storage', 'buckets', 'update', 'gs://' + probe.state['bucket'], '--requester-pays'])
        try:
            _, bucket = probe.request(probe.bucket_url())
            if bucket.get('billing', {}).get('requesterPays') is not True:
                raise RuntimeError('Requester Pays flag not verified')
            results['requester_pays_bucket_flag_verified'] = True
            # Privileged callers can legitimately succeed without a billing header.
            # This identity cannot prove mandatory-project rejection for lesser roles.
            results['cases'].append({'case': 'requester-pays-no-header', 'evidence': probe.run(standard, directory / 'missing-billing.json', 'narrow')})
            results['cases'].append({'case': 'requester-pays', 'evidence': probe.run(standard, directory / 'requester-pays.json', 'payer')})
        finally:
            probe.gcloud(['storage', 'buckets', 'update', 'gs://' + probe.state['bucket'], '--no-requester-pays'])
        standard_trials = results['cases'][0]['evidence']['trials']
        by = {t['trial']: t for t in standard_trials}
        full = by['wide']['native_http']['query']['bytes_received']
        if not (full > 0 and by['narrow']['native_http']['query']['bytes_received'] * 4 < full and by['selective-wide']['native_http']['query']['bytes_received'] * 4 < full):
            raise RuntimeError('Live pushdown reduction assertion failed')
        results['query_body_reductions_percent'] = {name: round(100 * (1 - by[name]['native_http']['query']['bytes_received'] / full), 2) for name in ('narrow', 'selective-wide')}
        results['limitations'] = ['No mid-query OAuth renewal tested; fresh explicitly selected token supplied before each case.', 'Standalone allowed_paths tests and default production-owner denials are not positive production guard integration.', 'HTTPState counters cover body bytes only, excluding headers/TLS/wire overhead; source layout pinned.', 'Requester Pays flag verified; explicit billing-header scan succeeded, but no-header scan also succeeded for this identity. Mandatory billing-project enforcement/billing attribution not proven.', 'No object generation pinning or out-of-band replacement protection tested.']
        private_json(directory / 'results.json', results)
        print('Live query body reductions:', results['query_body_reductions_percent'], flush=True)
    finally:
        probe.cleanup()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Only authored exception messages; never external/native response bodies.
        if type(error) is RuntimeError:
            raise SystemExit(str(error)) from None
        raise SystemExit('Live OAuth orchestration failed; details suppressed. Review private ownership state for cleanup.') from None
