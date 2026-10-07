#!/usr/bin/env python3
"""Production-bundle S3 lifecycle using the explicit authorized staging scope.

Reuses the independent native/generator/raw-source oracle from the GCS recipe,
not its cloud implementation. Writes/deletes only a fresh UUID child; no IAM,
retention, parent-prefix or bucket changes. No provisioning/source mutation.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile
import uuid

SPEC = importlib.util.spec_from_file_location('production_oracle', Path(__file__).with_name('validate-gcs-production.py'))
PROOF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROOF)
AUTHORIZED = 's3://private-scope-placeholder/private-scope-placeholder'
PROFILE = 'private-scope-placeholder'
REGION = 'eu-west-1'
LIMIT = 512


def validate_args(args):
    PROOF.require(args.write_root.startswith(AUTHORIZED + '/'), 'unauthorized S3 root')
    child = args.write_root[len(AUTHORIZED) + 1:]
    try:
        parsed = uuid.UUID(child)
    except ValueError:
        raise PROOF.ValidationError('S3 root must be an exact UUID child') from None
    PROOF.require(str(parsed) == child and parsed.version == 4, 'canonical UUID4 child required')
    PROOF.require(args.profile == PROFILE and args.region == REGION, 'unauthorized staging profile/region')
    PROOF.require(args.sf_org == PROOF.SF_USER and args.sf_org_id == PROOF.SF_ID, 'unauthorized Salesforce org')


class Cloud:
    def __init__(self, args, runner):
        self.root, self.runner = args.write_root, runner
        self.bucket, self.prefix = args.write_root[5:].split('/', 1)
        self.prefix += '/'
        self.flags = ['--profile', args.profile, '--region', args.region,
                      '--no-cli-pager', '--no-cli-auto-prompt']

    def json(self, *arguments):
        return self.runner.json(['aws', *arguments, *self.flags], 'owned-s3-' + arguments[1])

    def inventory(self):
        value = self.json('s3api', 'list-objects-v2', '--bucket', self.bucket,
                          '--prefix', self.prefix, '--max-keys', str(LIMIT + 1), '--no-paginate')
        PROOF.require(not value.get('IsTruncated'), 'owned S3 object limit exceeded')
        entries = value.get('Contents', [])
        PROOF.require(len(entries) <= LIMIT, 'owned S3 object limit exceeded')
        for entry in entries:
            PROOF.require(entry['Key'].startswith(self.prefix) and entry['Key'] != self.prefix
                          and re.fullmatch(r'[A-Za-z0-9/._=+-]+', entry['Key']) is not None,
                          'unsafe owned S3 object key')
        return entries

    def uploads(self):
        value = self.json('s3api', 'list-multipart-uploads', '--bucket', self.bucket,
                          '--prefix', self.prefix, '--max-uploads', str(LIMIT + 1), '--no-paginate')
        PROOF.require(not value.get('IsTruncated'), 'owned multipart inventory truncated')
        entries = value.get('Uploads', [])
        PROOF.require(len(entries) <= LIMIT, 'owned multipart limit exceeded')
        for entry in entries:
            PROOF.require(entry['Key'].startswith(self.prefix) and bool(entry['UploadId']),
                          'unsafe owned multipart upload')
        return entries

    def cleanup(self):
        objects = self.inventory()
        uploads = self.uploads()
        for entry in uploads:
            code, _, _ = self.runner.run(['aws', 's3api', 'abort-multipart-upload', '--bucket', self.bucket,
                                         '--key', entry['Key'], '--upload-id', entry['UploadId'], *self.flags],
                                        'owned-s3-multipart-abort')
            PROOF.require(code == 0, 'owned S3 multipart abort failed')
        for entry in objects:
            # S3 delete can return empty output, unlike list operations.
            code, _, _ = self.runner.run(['aws', 's3api', 'delete-object', '--bucket', self.bucket,
                                         '--key', entry['Key'], *self.flags], 'owned-s3-object-delete')
            PROOF.require(code == 0, 'owned S3 delete failed')
        PROOF.require(not self.inventory() and not self.uploads(), 'owned S3 prefix not absent')
        return {'deleted_objects': len(objects), 'aborted_uploads': len(uploads),
                'current_objects_absent': True, 'multipart_uploads_absent': True,
                'absence_proved': True,
                'limitations': ['Versioned historical objects/delete markers not inventoried or reclaimed.',
                                'Bucket versioning/retention settings remain unchanged.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('bundle', 'write-root', 'profile', 'region', 'sf-org', 'sf-org-id', 'output'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--only', choices=('all', 'duckdb'), default='all')
    args = parser.parse_args()
    validate_args(args)
    output = Path(args.output).absolute()
    output.mkdir(mode=0o700, exist_ok=False)
    output.chmod(0o700)
    environment = {k: v for k, v in os.environ.items() if k in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL')}
    environment.update(AWS_PROFILE=args.profile, AWS_REGION=args.region, AWS_DEFAULT_REGION=args.region,
                       AWS_EC2_METADATA_DISABLED='true', AWS_PAGER='', AWS_CLI_AUTO_PROMPT='off')
    runner = PROOF.Runner(output, environment)
    cloud = Cloud(args, runner)
    report = {'version': 1, 'scope': 'production-bundle-S3-lifecycle', 'owned_root': args.write_root,
              'profile': args.profile, 'region': args.region, 'ok': False,
              'limitations': ['No least-privilege reader-credential claim; same explicit staging profile.',
                              'Representative managed publication/local pull/replay, not full external build/view fault matrix.',
                              'No versioned historical object reclamation claim.'],
              'cleanup': {'absence_proved': False}}
    PROOF.private_json(output / 'result.json', report)
    owned = False
    try:
        bundle = Path(args.bundle).resolve(strict=True)
        manifest, library = PROOF.check_bundle(bundle)
        report['manifest_sha256'] = hashlib.sha256((bundle / 'bundle.json').read_bytes()).hexdigest()
        report['source_commit'] = manifest['source_commit']
        PROOF.require(not cloud.inventory() and not cloud.uploads(), 'S3 child preexists; ownership refused')
        owned = True
        report['ownership_confirmed'] = True
        PROOF.private_json(output / 'result.json', report)
        with tempfile.TemporaryDirectory(prefix='s3-production-', dir=output) as work:
            PROOF.lifecycle(args, runner, cloud, bundle, library, Path(work), report)
        report['ok'] = True
    except Exception as error:
        report['failure'] = {'type': type(error).__name__, 'detail': str(error)
                             if isinstance(error, PROOF.ValidationError) else 'suppressed; see diagnostic phase/status'}
    finally:
        if owned:
            try:
                report['cleanup'] = cloud.cleanup()
            except Exception:
                report['ok'] = False
                report['cleanup'] = {'absence_proved': False, 'failure': 'owned S3 cleanup failed'}
        PROOF.private_json(output / 'result.json', report)
    print(output / 'result.json')
    raise SystemExit(0 if report['ok'] else 1)


if __name__ == '__main__':
    main()
