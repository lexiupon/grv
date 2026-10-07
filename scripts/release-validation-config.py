"""Read explicit, private release-validation coordinates (never credentials)."""
import json
import os
from pathlib import Path
import re
import stat


class ConfigError(ValueError):
    pass


def load_config(path=None):
    path = path or os.environ.get('GRV_RELEASE_VALIDATION_CONFIG')
    if not path:
        raise ConfigError('GRV_RELEASE_VALIDATION_CONFIG is required')
    try:
        before = Path(path).lstat()
        if not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o600 or before.st_uid != os.getuid():
            raise ConfigError('config must be an owned regular nonsymlink file with mode 0600')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'r', encoding='utf-8') as source:
            opened = os.fstat(source.fileno())
            if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
                raise ConfigError('config changed while opening')
            text = source.read(65537)
        if len(text) > 65536:
            raise ConfigError('config exceeds size bound')
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ConfigError('duplicate config key')
                result[key] = value
            return result
        value = json.loads(text, object_pairs_hook=unique)
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ConfigError('cannot read private validation config') from None
    fields = {'sf': {'org', 'org_id'}, 'gcs': {'root', 'account', 'project'},
              's3': {'root', 'profile', 'region'}, 'archive': set()}
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ConfigError('config requires sf, gcs, s3 and archive sections')
    for section, keys in fields.items():
        item = value[section]
        if not isinstance(item, dict):
            raise ConfigError('invalid config section')
        if section == 'archive':
            # Archive schema belongs to the evidence archiver, never credentials.
            def no_secrets(obj):
                if isinstance(obj, dict):
                    for key, entry in obj.items():
                        if re.search(r'token|secret|password|credential|access.?key', key, re.I):
                            raise ConfigError('credentials are forbidden in validation config')
                        no_secrets(entry)
                elif isinstance(obj, list):
                    for entry in obj:
                        no_secrets(entry)
            no_secrets(item)
            continue
        if set(item) != keys or any(not isinstance(v, str) or not v or v.strip() != v for v in item.values()):
            raise ConfigError('invalid scope fields (coordinates only; no credentials)')
    if not re.fullmatch(r'[A-Za-z0-9]{18}', value['sf']['org_id']):
        raise ConfigError('SF org_id must be an 18-character identity')
    for section, scheme in [('gcs', 'gs'), ('s3', 's3')]:
        root = value[section]['root']
        if not re.fullmatch(scheme + r'://[A-Za-z0-9._-]+/[A-Za-z0-9._/-]+', root) or root.endswith('/') or any(p in ('', '.', '..') for p in root.split('/')[3:]):
            raise ConfigError('cloud root must be an exact nonempty prefix, not a bucket or URL with credentials')
    return value
