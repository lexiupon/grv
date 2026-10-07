#!/usr/bin/env bash
# Reset only the fixture namespace in an explicitly pinned disposable org.
# Provisioning performs capacity checks before deleting records, then uses
# bulk delete/load in lookup-safe order and strictly verifies the oracle.
set -euo pipefail
cd "$(dirname "$0")"
ORG="${1:?usage: reset.sh <username-or-alias> <expected-org-id> [--ignore-storage-limit]}"
ORG_ID="${2:?usage: reset.sh <username-or-alias> <expected-org-id> [--ignore-storage-limit]}"
shift 2
exec python3 provision.py --org "$ORG" --expected-org-id "$ORG_ID" "$@"
