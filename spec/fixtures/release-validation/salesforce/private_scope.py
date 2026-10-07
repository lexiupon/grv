"""Explicit ignored coordinate configuration; no credentials or defaults."""
import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    'private_validation_scope', Path(__file__).resolve().parents[4] / 'scripts/release-validation-config.py')
CONFIG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONFIG)


def salesforce_scope():
    scope = CONFIG.load_config()['sf']
    return scope['org'], scope['org_id']
