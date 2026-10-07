#!/usr/bin/env python3
"""Scoped production-bundle GCS lifecycle proof, not full G5/release approval.

Requires a fresh UUID child of the authorized validation root and explicit
identities. Runs serially, never builds or provisions/mutates Salesforce. The
full SF base fixture is required by default; --only duckdb explicitly records
missing SF coverage. Result and bounded, content-suppressed diagnostics live in
an explicitly selected fresh private output directory. Tokens are obtained only
by the production CLI's credential subprocesses (never written by this script).
"""
import argparse
import ctypes
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import threading
import uuid

ROOT = Path(__file__).resolve().parent.parent
LIMITATIONS = [
    'Not a full build/fault-injection/transport/platform matrix or G5 approval.',
    'Does not prove removal of provider-retained soft-deleted objects.',
    'SF fixture must already be provisioned; no org or source mutation is performed.',
]


class ValidationError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG = load_module('release_validation_config', ROOT / 'scripts/release-validation-config.py')


def private_config():
    try:
        return CONFIG.load_config()
    except CONFIG.ConfigError as error:
        raise ValidationError(str(error)) from None


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    for flag in ('bundle', 'write-root', 'account', 'project', 'sf-org', 'sf-org-id', 'output'):
        p.add_argument('--' + flag, required=True)
    p.add_argument('--only', choices=('all', 'duckdb'), default='all',
                   help='duckdb explicitly skips SF and reports missing coverage')
    return p


def validate_args(args):
    config = private_config()
    prefix = config['gcs']['root'] + '/'
    require(args.write_root.startswith(prefix), 'unauthorized write root')
    child = args.write_root[len(prefix):]
    try:
        parsed = uuid.UUID(child)
    except ValueError:
        raise ValidationError('write root must be an exact UUID child') from None
    require(str(parsed) == child and parsed.version == 4, 'write root must be a canonical UUID4 child')
    require(args.sf_org == config['sf']['org'] and args.sf_org_id == config['sf']['org_id'],
            'Salesforce identity not authorized')
    require(args.account == config['gcs']['account'] and args.project == config['gcs']['project'],
            'GCS account/project not authorized')
    for value in (args.account, args.project):
        require(bool(re.fullmatch(r'[A-Za-z0-9@._:+-]+', value)) and not value.startswith('-'),
                'explicit account/project is invalid')


def private_json(path, value):
    # Output directory is private and created exclusively by this invocation.
    temporary = path.with_suffix('.tmp')
    with temporary.open('w', encoding='utf-8') as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write('\n')
    temporary.chmod(0o600)
    temporary.replace(path)


class Runner:
    """Bound pipes in memory, kill overflow/timeouts; never print child output.

    Retained diagnostics deliberately suppress ALL stream contents: even failed
    credential helpers can leak tokens. Private diagnostic metadata records the
    phase, status, byte counts and overflow; no argv/SQL/org records are logged.
    """
    def __init__(self, directory, environment):
        self.directory, self.environment = directory, environment
        self.count = 0

    def run(self, argv, phase, env=None, timeout=900):
        self.count += 1
        buffers = [bytearray(), bytearray()]
        sizes = [0, 0]
        overflow = threading.Event()
        process = subprocess.Popen(list(map(str, argv)), env=env or self.environment,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        def drain(stream, index, limit):
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    break
                sizes[index] += len(chunk)
                room = max(0, limit - len(buffers[index]))
                buffers[index].extend(chunk[:room])
                if sizes[index] > limit:
                    overflow.set()
                    process.kill()
            stream.close()
        threads = [threading.Thread(target=drain, args=(process.stdout, 0, 2 * 1024 * 1024)),
                   threading.Thread(target=drain, args=(process.stderr, 1, 64 * 1024))]
        for thread in threads:
            thread.start()
        timed_out = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            process.wait()
        finally:
            for thread in threads:
                thread.join()
            private_json(self.directory / f'diagnostic-{self.count:03d}.json',
                         {'phase': phase, 'returncode': process.returncode,
                          'stream_bytes': sizes, 'overflow': overflow.is_set(),
                          'timeout': timed_out, 'stream_contents': 'suppressed (credential safety)'})
        require(not timed_out and not overflow.is_set(), 'subprocess exceeded bounded execution')
        return process.returncode, bytes(buffers[0]), bytes(buffers[1])

    def json(self, argv, phase, env=None):
        code, out, _ = self.run(argv, phase, env)
        require(code == 0, 'subprocess failed: ' + phase)
        try:
            value = json.loads(out, parse_float=Decimal)
        except (ValueError, UnicodeError):
            raise ValidationError('invalid subprocess JSON: ' + phase) from None
        require(isinstance(value, dict), 'JSON output must be an object: ' + phase)
        return value


class Cloud:
    def __init__(self, args, runner):
        self.root, self.runner = args.write_root, runner
        self.flags = ['--account', args.account, '--project', args.project, '--quiet']

    def inventory(self):
        # Offline gcloud help confirms ** is flat objects-only; recursive ls
        # emits directory headers. Never list a bucket or parent prefix.
        code, out, err = self.runner.run(
            ['gcloud', 'storage', 'ls', '--all-versions', self.root + '/**', *self.flags],
            'owned-prefix-inventory')
        if code:
            require(not out.strip() and re.fullmatch(
                rb'ERROR: \(gcloud\.storage\.ls\) One or more URLs matched no objects\.\s*', err.strip()),
                'owned-prefix inventory failed')
            return []
        urls = out.decode('utf-8').splitlines()
        require(len(urls) == len(set(urls)), 'duplicate cloud inventory')
        for url in urls:
            require(url.startswith(self.root + '/') and
                    re.fullmatch(r'gs://[A-Za-z0-9/._=+-]+#[0-9]+', url) is not None,
                    'unsafe or unversioned cloud inventory object')
        return sorted(urls)

    def cleanup(self):
        inventory = self.inventory()
        # Exact generation-addressed object deletion, no recursive bucket or
        # managed-folder operation, no wildcard deletion and no IAM mutation.
        for url in inventory:
            code, _, _ = self.runner.run(['gcloud', 'storage', 'rm', url, *self.flags],
                                         'owned-object-delete')
            require(code == 0, 'owned object deletion failed')
        require(not self.inventory(), 'owned prefix not absent after cleanup')
        return {'deleted_objects': len(inventory), 'absence_proved': True}


class NativeDuckDB:
    """Ordinary independent C API queries via the packaged guard's DuckDB.

    Result ABI mirrors verify-bundle.py. No GRV adapter inspection command,
    Python duckdb package, source DB or cloud access participates in this oracle.
    """
    class Result(ctypes.Structure):
        _fields_ = [('columns', ctypes.c_uint64), ('rows', ctypes.c_uint64),
                    ('changed', ctypes.c_uint64), ('data', ctypes.c_void_p),
                    ('error', ctypes.c_char_p), ('internal', ctypes.c_void_p)]

    def __init__(self, library, database):
        require(database.is_file(), 'pulled DuckDB database missing')
        self.native = n = ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
        pointer = ctypes.c_void_p
        result = ctypes.POINTER(self.Result)
        signatures = {
            'duckdb_open': ([ctypes.c_char_p, ctypes.POINTER(pointer)], ctypes.c_uint32),
            'duckdb_connect': ([pointer, ctypes.POINTER(pointer)], ctypes.c_uint32),
            'grv_native_load_static_extensions': ([pointer, ctypes.c_char_p, ctypes.c_size_t], ctypes.c_int),
            'duckdb_query': ([pointer, ctypes.c_char_p, result], ctypes.c_uint32),
            'duckdb_destroy_result': ([result], None),
            'duckdb_row_count': ([result], ctypes.c_uint64),
            'duckdb_column_count': ([result], ctypes.c_uint64),
            'duckdb_value_is_null': ([result, ctypes.c_uint64, ctypes.c_uint64], ctypes.c_bool),
            'duckdb_value_varchar': ([result, ctypes.c_uint64, ctypes.c_uint64], pointer),
            'duckdb_free': ([pointer], None),
            'duckdb_disconnect': ([ctypes.POINTER(pointer)], None),
            'duckdb_close': ([ctypes.POINTER(pointer)], None),
        }
        for name, (arguments, returns) in signatures.items():
            function = getattr(n, name)
            function.argtypes, function.restype = arguments, returns
        self.database, self.connection = pointer(), pointer()
        try:
            require(n.duckdb_open(os.fsencode(database), ctypes.byref(self.database)) == 0,
                    'native DuckDB open failed')
            error = ctypes.create_string_buffer(256)
            require(n.grv_native_load_static_extensions(self.database, error, len(error)) == 0,
                    'independent native static extension load failed')
            require(n.duckdb_connect(self.database, ctypes.byref(self.connection)) == 0,
                    'native DuckDB connect failed')
            self.query('SET autoinstall_known_extensions=false')
            self.query('SET autoload_known_extensions=false')
            # Default timezone is UTC; don't require unshipped ICU merely to
            # format UTC values. Exact temporal oracle uses epoch_us below.
        except BaseException:
            self.close()
            raise

    def query(self, sql):
        result = self.Result()
        try:
            require(self.native.duckdb_query(self.connection, sql.encode(), ctypes.byref(result)) == 0,
                    'independent native query failed')
            rows = []
            for row in range(self.native.duckdb_row_count(ctypes.byref(result))):
                values = []
                for column in range(self.native.duckdb_column_count(ctypes.byref(result))):
                    if self.native.duckdb_value_is_null(ctypes.byref(result), column, row):
                        values.append(None)
                    else:
                        value = self.native.duckdb_value_varchar(ctypes.byref(result), column, row)
                        require(bool(value), 'native value allocation failed')
                        try:
                            values.append(ctypes.string_at(value).decode('utf-8'))
                        finally:
                            self.native.duckdb_free(value)
                rows.append(values)
            return rows
        finally:
            self.native.duckdb_destroy_result(ctypes.byref(result))

    def close(self):
        if self.connection:
            self.native.duckdb_disconnect(ctypes.byref(self.connection))
        if self.database:
            self.native.duckdb_close(ctypes.byref(self.database))


def check_bundle(bundle):
    manifest = json.loads((bundle / 'bundle.json').read_text())
    require(manifest.get('profile') == 'release', 'bundle must use actual release profile')
    for relative, expected in manifest['sha256'].items():
        path = bundle / relative
        require(not path.is_symlink() and path.is_file(), 'unsafe bundle artifact')
        require(path.resolve().is_relative_to(bundle), 'artifact outside bundle')
        with path.open('rb') as f:
            require(hashlib.file_digest(f, 'sha256').hexdigest() == expected, 'bundle digest mismatch')
    library = bundle / 'adapters/duckdb/bin/lib' / (
        'libgrv_duckdb_guard.dylib' if platform.system() == 'Darwin' else 'libgrv_duckdb_guard.so')
    for relative in ('bin/grv', 'adapters/duckdb/bin/grv-adapter-duckdb',
                     'adapters/salesforce/bin/grv-adapter-salesforce', str(library.relative_to(bundle))):
        require(relative in manifest['sha256'], 'required artifact not digest anchored')
    return manifest, library


def sf_query(runner, org, query):
    value = runner.json(['sf', 'data', 'query', '--target-org', org, '--query', query,
                         '--json'], 'independent-sf-query')
    require(value.get('status') == 0 and isinstance(value.get('result'), dict), 'SF query failed')
    result = value['result']
    require(result.get('done') is True and isinstance(result.get('records'), list) and
            result.get('totalSize') == len(result['records']), 'SF query incomplete')
    return result['records']


SF_FIELDS = [('id', 'Id'), ('name', 'Name'), ('text', 'GrvText__c'),
             ('long_text', 'GrvLongText__c'), ('int_value', 'GrvInt__c'),
             ('decimal_value', 'GrvDecimal__c'), ('date_value', 'GrvDate__c'),
             ('datetime_value', 'GrvDateTime__c'), ('picklist', 'GrvPicklist__c'),
             ('bool_value', 'GrvBool__c'), ('formula', 'GrvFormula__c'),
             ('partition_date', 'GrvPartitionDate__c'), ('status', 'GrvStatus__c'),
             ('rel_id', 'GrvRel__c'), ('_month_', None)]
SF_TYPES = ['VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'BIGINT', 'DECIMAL(38,10)',
            'DATE', 'TIMESTAMP WITH TIME ZONE', 'VARCHAR', 'BOOLEAN', 'VARCHAR',
            'DATE', 'VARCHAR', 'VARCHAR', 'VARCHAR']


def canonical(name, value):
    if value is None:
        return None
    if name in ('decimal_value', 'int_value'):
        require(not isinstance(value, float), 'float cannot participate in exact oracle')
        return Decimal(str(value))
    if name == 'datetime_value':
        from datetime import datetime, timezone
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(timezone.utc)
    if name == 'bool_value':
        require(value in (True, False, 'true', 'false'), 'invalid boolean')
        return value is True or value == 'true'
    return str(value)


def sf_oracle(runner, org):
    generator = load_module('grv_sf_oracle', ROOT / 'spec/fixtures/release-validation/salesforce/generate.py')
    related = sf_query(runner, org, 'SELECT Id, Name FROM GrvFixRel__c ORDER BY Name')
    require(len(related) == 10, 'expected ten related fixture rows')
    ids = {row['Name']: row['Id'] for row in related}
    require(len(ids) == 10 and all(re.fullmatch(r'[A-Za-z0-9]{18}', v) for v in ids.values()),
            'invalid related IDs')
    fields = ', '.join(source for _, source in SF_FIELDS if source)
    records = sf_query(runner, org, f"SELECT {fields} FROM GrvFix__c WHERE GrvStatus__c = 'Active' ORDER BY Name")
    require(len(records) == 900, 'expected full 900-row base fixture')
    expected = []
    actual_names = []
    for i in range(generator.ROWS):
        if i % 10 == 7:
            continue
        values = generator.grvfix_grv_values(i)
        record = records[len(expected)]
        require(re.fullmatch(r'[A-Za-z0-9]{18}', record['Id']) is not None, 'invalid fixture ID')
        values['id'] = record['Id']  # Only assigned identities come from live raw source.
        values['rel_id'] = ids.get(f'GRVFIXREL-{i:03d}') if i < generator.REL_ROWS else None
        row = [canonical(name, values[name]) for name, _ in SF_FIELDS]
        source_row = [canonical(name, record[source] if source else record['GrvPartitionDate__c'][:7])
                      for name, source in SF_FIELDS]
        require(source_row == row, 'raw SF source differs from independent generated oracle')
        actual_names.append(record['Id'])
        expected.append(row)
    require(len(set(actual_names)) == 900, 'duplicate fixture IDs')
    return expected


def assert_schema(native, table, names, types):
    description = native.query('DESCRIBE ' + table)
    # Schema contains only this fixed harness projection, never source values.
    require([(r[0], r[1]) for r in description] == list(zip(names, types)),
            'independent pulled schema mismatch: ' + repr([(r[0], r[1]) for r in description]))


def lifecycle(args, runner, cloud, bundle, library, work, report):
    environment = dict(runner.environment, GRV_ADAPTERS_DIR=str(bundle / 'adapters'))
    cli = str(bundle / 'bin/grv')
    def call(*arguments, offline=False):
        env = dict(environment)
        if offline:
            env.update(PATH='', GRV_ADAPTERS_DIR=str(work / 'absent-adapters'),
                       GRV_GCS_ACCOUNT='unavailable-replay-account',
                       GRV_GCS_PROJECT='unavailable-replay-project', HOME=str(work / 'offline-home'))
        value = runner.json([cli, '--json', *arguments], 'grv-' + str(arguments[0]), env)
        require(value.get('ok') is True and isinstance(value.get('result'), dict), 'invalid GRV success JSON')
        return value['result']
    for name in ('duckdb', 'salesforce'):
        descriptor = call('adapter', name, 'capabilities')['adapter']
        require(descriptor == report['manifest']['adapters'][name], 'bundle capabilities mismatch')
    call('init', '--grv', args.write_root)
    state = work / 'state'
    engine, destination = work / 'build.duckdb', work / 'pulled.duckdb'
    declaration = work / 'duckdb-push.yml'
    declaration.write_text(
        'declaration_version: 1\nkind: push\ndataset: production_duckdb\nadapter: duckdb\n'
        f'connection: {{database: {json.dumps(str(engine))}}}\n'
        'build: {execution: managed}\ntables:\n'
        "  - name: rows\n    source: {sql: 'SELECT i::BIGINT AS value FROM range(1,4) AS t(i)'}\n"
        '    columns: [{name: value, type: int64}]\n'
        "  - name: empty\n    source: {sql: 'SELECT 1::BIGINT AS value WHERE false'}\n"
        '    columns: [{name: value, type: int64}]\n')
    attempts = []
    for kind in ('published', 'no-op'):
        attempt = str(uuid.uuid4())
        result = call('push', '--grv', args.write_root, '--decl', declaration,
                      '--state', state, '--attempt', attempt)
        require(result['outcome']['kind'] == kind and result['outcome']['revision'] == '1' and
                result['replayed'] is False and result.get('mode') == 'build',
                'unexpected production DuckDB publication/no-op')
        attempts.append((attempt, result))
    call('verify', 'production_duckdb', '--grv', args.write_root, '--full')
    pull = work / 'duckdb-pull.yml'
    pull.write_text('declaration_version: 1\nkind: pull\ndataset: production_duckdb\nadapter: duckdb\n'
                    f'connection: {{database: {json.dumps(str(destination))}}}\n'
                    'target: {schema: proof}\ntables: [{name: rows}, {name: empty}]\n')
    call('pull', '--grv', args.write_root, '--decl', pull, '--state', state)
    native = NativeDuckDB(library, destination)
    try:
        for table, expected in [('rows', [['1'], ['2'], ['3']]), ('empty', [])]:
            assert_schema(native, 'proof.' + table, ['value'], ['BIGINT'])
            require(native.query('SELECT value FROM proof.' + table + ' ORDER BY value') == expected,
                    'independent DuckDB row mismatch')
    finally:
        native.close()
    report['duckdb'] = {'publication': True, 'noop': True, 'cloud_full_verify': True,
                        'independent_pulled_rows_and_schema': True, 'terminal_replay': False}
    if args.only == 'all':
        # Identity-only query: never ask org display to return accessToken.
        identity = sf_query(runner, args.sf_org, 'SELECT Id FROM Organization')
        require(identity == [{'attributes': identity[0].get('attributes'), 'Id': args.sf_org_id}],
                'SF organization ID mismatch')
        oracle = sf_oracle(runner, args.sf_org)
        base = (ROOT / 'spec/fixtures/release-validation/salesforce/decl/push-base.yml').read_text()
        require(base.count('org: grv-fixture') == 1 and base.count('transport: auto') == 1,
                'versioned SF declaration changed')
        sf_decl = work / 'sf-push.yml'
        sf_decl.write_text(base.replace('org: grv-fixture', 'org: ' + args.sf_org)
                          .replace('transport: auto', 'transport: rest'))
        sf_attempt = str(uuid.uuid4())
        sf_result = call('push', '--grv', args.write_root, '--decl', sf_decl, '--state', state,
                         '--attempt', sf_attempt)
        require(sf_result['outcome']['kind'] == 'published' and
                sf_result.get('mode') == 'extract', 'SF full publication mismatch')
        call('verify', 'grv_sf_fixture', '--grv', args.write_root, '--full')
        sf_db, sf_pull = work / 'sf.duckdb', work / 'sf-pull.yml'
        sf_pull.write_text('declaration_version: 1\nkind: pull\ndataset: grv_sf_fixture\nadapter: duckdb\n'
                           f'connection: {{database: {json.dumps(str(sf_db))}}}\n'
                           'target: {schema: sfproof}\ntables: [{name: grvfix}]\n')
        call('pull', '--grv', args.write_root, '--decl', sf_pull, '--state', state)
        native = NativeDuckDB(library, sf_db)
        try:
            names = [name for name, _ in SF_FIELDS]
            assert_schema(native, 'sfproof.grvfix', names, SF_TYPES)
            expressions = [('epoch_us("datetime_value")' if n == 'datetime_value'
                            else '"' + n + '"') for n in names]
            raw = native.query('SELECT ' + ', '.join(expressions) +
                               ' FROM sfproof.grvfix ORDER BY name')
            from datetime import datetime, timedelta, timezone
            observed = []
            for row in raw:
                values = []
                for name, value in zip(names, row):
                    if name == 'datetime_value':
                        values.append(None if value is None else
                                      datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value)))
                    else:
                        values.append(canonical(name, value))
                observed.append(values)
            require(observed == oracle, 'SF pulled data differs from exact independent oracle')
        finally:
            native.close()
        sf_noop_attempt = str(uuid.uuid4())
        sf_noop = call('push', '--grv', args.write_root, '--decl', sf_decl, '--state', state,
                       '--attempt', sf_noop_attempt)
        require(sf_noop['outcome']['kind'] == 'no-op' and
                sf_noop['outcome']['revision'] == sf_result['outcome']['revision'],
                'SF fresh no-op mismatch')
        report['salesforce'] = {'full_base_declaration': True, 'transport': 'rest', 'rows': 900,
                                'independent_generated_and_raw_source_oracle': True,
                                'independent_pulled_rows_and_schema': True,
                                'noop': True, 'terminal_replay': False}
    engine.unlink()
    declaration.unlink()
    # Delete the entire owned store before replay: PATH is empty, source and
    # declarations are gone, adapter root unavailable, cloud credentials denied.
    report['pre_replay_cleanup'] = cloud.cleanup()
    for attempt, original in attempts:
        replay = call('push', '--grv', args.write_root, '--decl', declaration,
                      '--state', state, '--attempt', attempt, offline=True)
        require(replay == dict(original, replayed=True), 'offline terminal replay mismatch')
    if args.only == 'all':
        sf_decl.unlink()
        for attempt, original in [(sf_attempt, sf_result), (sf_noop_attempt, sf_noop)]:
            replay = call('push', '--grv', args.write_root, '--decl', sf_decl,
                          '--state', state, '--attempt', attempt, offline=True)
            require(replay == dict(original, replayed=True), 'SF offline terminal replay mismatch')
        report['salesforce']['terminal_replay'] = True
    require(not cloud.inventory(), 'offline replay recreated cloud objects')
    report['duckdb']['terminal_replay'] = True


def execute(args):
    validate_args(args)
    output = Path(args.output).resolve()
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    output.chmod(0o700)
    report = {'format_version': 1, 'scope': 'production-bundle-gcs-lifecycle',
              'owned_root': args.write_root, 'account': args.account, 'project': args.project,
              'sf_org': args.sf_org, 'sf_org_id': args.sf_org_id, 'selector': args.only,
              'ok': False, 'limitations': LIMITATIONS[:], 'cleanup': {'absence_proved': False}}
    if args.only == 'duckdb':
        report['limitations'].append('MISSING: full SF publication/pull exact oracle (explicit --only duckdb).')
    private_json(output / 'result.json', report)  # Durable authorization before any cloud effects.
    environment = {k: v for k, v in os.environ.items()
                   if k in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL', 'CLOUDSDK_CONFIG')}
    environment.update(GRV_GCS_ACCOUNT=args.account, GRV_GCS_PROJECT=args.project,
                       CLOUDSDK_CORE_DISABLE_PROMPTS='1', CLOUDSDK_PAGER='')
    runner = Runner(output, environment)
    cloud = Cloud(args, runner)
    owned = False
    try:
        bundle = Path(args.bundle).resolve(strict=True)
        manifest, library = check_bundle(bundle)
        # Persist provenance without claiming notice/readiness approval.
        report['manifest'] = {'profile': manifest['profile'], 'source_commit': manifest.get('source_commit'),
                              'adapters': manifest['adapters']}
        require(not cloud.inventory(), 'prefix preexists; refusing ownership/deletion')
        owned = True
        report['ownership_confirmed'] = True
        private_json(output / 'result.json', report)
        with tempfile.TemporaryDirectory(prefix='grv-production-', dir=output) as scratch:
            lifecycle(args, runner, cloud, bundle, library, Path(scratch), report)
        report['ok'] = True
    except Exception as error:
        # Exception payloads can contain credentials or source rows. Do not log.
        # ValidationError messages are fixed harness strings, never raw peer
        # output; all other exceptions remain content-suppressed.
        report['failure'] = {'type': type(error).__name__,
                             'detail': str(error) if isinstance(error, ValidationError)
                             else 'suppressed; see diagnostic phase/status'}
    finally:
        if owned:
            try:
                report['cleanup'] = cloud.cleanup()
            except Exception:
                report['ok'] = False
                report['cleanup'] = {'absence_proved': False, 'failure': 'owned-prefix cleanup failed'}
        private_json(output / 'result.json', report)
    return report


def main():
    args = parser().parse_args()
    try:
        report = execute(args)
    except Exception:
        raise SystemExit('validation preflight failed (details suppressed for credential safety)') from None
    print(str(Path(args.output).absolute() / 'result.json'))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
