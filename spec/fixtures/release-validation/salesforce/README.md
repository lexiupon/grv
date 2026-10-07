# Salesforce release fixture (disposable org)

Versioned source fixture for REL-SF-* validation, never arbitrary production
records. All live commands must explicitly target a disposable org. **Do not
use `sample-org` or another production identity, including as a Dev Hub, without
separate authorization.**

## Contents

- `force-app/`: two custom objects, fields and `GrvFixAccess` permission set.
- `generate.py`: deterministic CSV inputs and independent expected stored/GRV
  values. `grvfix_values(i)` is the input; `grvfix_stored_values(i)` explicitly
  defines source storage semantics; `grvfix_grv_values(i)` defines GRV values.
- `manifest/value-manifest.json`: versioned oracle, inputs and stored values
  for boundary rows, full-population aggregates, selections and CSV hashes.
- `decl/`: base (900 Active), all (1,000), empty (absent-name filter), and
  relationship pushes; all use alias `grv-fixture`.
- `provision.py`: deploy -> CLI permission assignment -> access/capacity checks
  -> bulk cleanup/load -> strict verification of **all 1,010 rows**, fields,
  formula values, lookup links, selections and unique names/Ids. Records
  actual REST page sizes and verification evidence.
- `reset.sh`: invokes the same pinned-org provisioning procedure.
- `test_fixture.py`: offline regression tests.

## Requirements and limitations

Use a disposable org with custom objects, Metadata API deployment privileges,
REST/Bulk access, and **at least about 3 MB available data storage** for the
1,010 records (about 2 KiB/custom record plus headroom). Do not infer storage
from CSV file length. A Developer Edition org can deploy the schema yet have
only 5 MB of data storage; actual capacity and reload success must be verified. The
script checks capacity *before* deleting existing fixture records. Storage
accounting is asynchronous; this is a conservative planning check, not a
promise that a server will accept every subsequent load.

The user approved a tenfold scale reduction for correctness testing: manifest
version 3 has **1,000 main + 10 related** rows, retaining scalar boundary rows,
null classes and three partitions. The old larger-run evidence is preserved,
not rewritten. `00D000000000001AAA` was cleanly reloaded and strictly verified
on 2026-10-07 with zero mismatches; limits reported 5 MB total, 3 MB remaining.
Use `--ignore-storage-limit` only for the reviewed conservative preflight waiver;
actual service failures still fail. No performance claim is made.

The earlier missing-field diagnosis was incorrect: deploying metadata did
not grant field-level security. `sf org assign permset` fixed visibility.
The assignment object is `PermissionSetAssignment`, not `PermissionSetAssign`.
Neither missing fields nor missing Dev Hub objects proves a broken org.

### What this fixture actually covers

- Null text and CSV empty text (Salesforce stores both as null), Unicode,
  emoji, duplicate strings, and a long-text field with a ~143-byte payload.
- Exact integers, small scaled decimals and a large exactly representable
  decimal `99999999.1250000000` (not the decimal(18,10) maximum).
- Null, pre-epoch, epoch and far-future dates/timestamps; far-future values are
  not claimed as platform maxima (Salesforce's documented custom Date range
  is 1700-01-01 through 4000-12-31). This Bulk ingestion path stores timestamps
  at **whole-second** precision. Input milliseconds and expected stored values
  are separately represented; the verifier does not silently round mismatches.
- Non-null checkboxes (omitted CSV values become false), formulas, lookups,
  filters, 900 Active rows and three monthly partitions; nine Active lookup rows.
- Real REST pagination via supported `Sforce-Query-Options: batchSize=200`: all-row
  readback pages `[200,200,200,200,200]`, Active pages `[200,200,200,200,100]`.
  Default-page production CLI checks run separately.
- Eligible numeric/ID/date/bool Bulk projection uses test-requested maxRecords=100
  and unchanged production parser/journal: nine real pages, 900 exact rows and
  explicit empty completion. This is not rich nullable-text Bulk qualification
  or SDK wire/credit coverage; production default remains maxRecords=1000.

**Not covered by this source fixture:** non-null empty strings, nullable
booleans, preserved sub-second timestamps, arbitrary maximum-scale large
numeric values, or proven multi-result **Bulk query** boundaries. Cover
unsupported scalar domains in another fixture. Bulk ingest success is not
Bulk acquisition/page-boundary evidence; demonstrate query boundaries in
REL-SF-04, or retain that part as a release blocker.

Reference: Salesforce documents [Date formats and ranges](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_select_dateformats.htm),
[one-second DateTime precision](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/primitive_data_types.htm),
and [explicit Dev Hub enablement](https://developer.salesforce.com/docs/atlas.en-us.sfdx_dev.meta/sfdx_dev/sfdx_setup_enable_devhub.htm).
Empty-text normalization and omitted-checkbox behavior here are measured for
this fixture/load path; the checkbox metadata explicitly specifies `false`.

## Provision a new disposable org

1. Obtain a disposable org meeting the capacity requirements. Alternatively,
   enable **Dev Hub** in a *personal* Developer Edition org (Setup -> Dev Hub),
   authorize it as `grv-personal-hub`, and create an isolated auto-expiring
   scratch org:
   ```console
   sf org login web --alias grv-personal-hub --instance-url https://login.salesforce.com
   sf org create scratch --target-dev-hub grv-personal-hub \
     --definition-file config/project-scratch-def.json \
     --alias grv-fixture --duration-days 30 --wait 10
   ```
   Enabling Dev Hub is irreversible: review this choice in the personal org.
   Dev Hub must be enabled and the user needs scratch-org permissions/quota.
   `ScratchOrgInfo` being unavailable usually means Dev Hub is not enabled or
   accessible; it does not mean the org is defective. Check the new scratch
   org's actual storage limits; do not assume creation solves capacity.
2. Authorize a non-scratch disposable org, or repoint the fixture alias:
   ```console
   sf org login web --alias grv-fixture --instance-url https://login.salesforce.com
   # For the currently verified, already authorized disposable org:
   sf alias set grv-fixture=fixture-user@example.invalid
   ```
   Record its username and 18-character Id in the validation worksheet. Use
   an isolated CLI auth store for release execution with no production orgs.
   Never put tokens/passwords in arguments, logs or versioned artifacts.
3. Set the authorized org Id and provision (this resets fixture records):
   ```console
   export GRV_SALESFORCE_TEST_ORG_ID='<18-character disposable org Id>'
   python3 provision.py --org grv-fixture \
     --expected-org-id "$GRV_SALESFORCE_TEST_ORG_ID"
   ```
   For the current org, append `--ignore-storage-limit` per the reviewed
   decision. This waives only preflight capacity, not service/load errors.
   Permission deployment/assignment is performed automatically. Equivalent
   explicit CLI commands are:
   ```console
   sf project deploy start --target-org grv-fixture \
     --source-dir force-app/main/default --wait 10
   sf org assign permset --name GrvFixAccess --target-org grv-fixture
   ```
   Required `GrvStatus__c` is intentionally absent from field grants (Salesforce
   rejects grants to universally required fields). Formula access is read-only.
   The permission set grants CRUD only on the two fixture objects, no global
   administration permission. Re-runs detect the existing assignment.
4. Check or verify without reloading:
   ```console
   python3 provision.py --org grv-fixture --expected-org-id "$GRV_SALESFORCE_TEST_ORG_ID" --skip-load
   python3 provision.py --org grv-fixture --expected-org-id "$GRV_SALESFORCE_TEST_ORG_ID" --verify-only
   python3 -m unittest -v test_fixture
   python3 generate.py
   python3 generate.py --check
   ```
   `--skip-load` deploys/assigns/checks access; `--verify-only` makes no org
   mutations. Strict readback uses `Decimal` when decoding raw REST JSON,
   never float conversion followed by a permissive rounding tolerance.
5. Canonicalize reviewed metadata changes separately:
   ```console
   sf project retrieve start --target-org grv-fixture \
     --metadata CustomObject:GrvFix__c --metadata CustomObject:GrvFixRel__c \
     --metadata PermissionSet:GrvFixAccess
   ```
   Review retrieved XML before committing; do not overwrite the seed silently.

Generated `data/*.csv` (~2.7 MB) and `manifest/provision-evidence.json` are
ignored by Git. The latter records org identity, API version, counts, actual
REST pages, storage limits and strict readback status. A verify-only run is
not evidence of a successful clean provisioning run.

## Mutation and reset (REL-SF-05)

Only mutate allowlisted fixture records. For example:
```console
sf data update record -o grv-fixture -s GrvFix__c \
  -r '<Id of GRVFIX-00001>' -v 'GrvStatus__c=Inactive'
sf data update record -o grv-fixture -s GrvFix__c \
  -r '<Id of GRVFIX-00001>' -v 'GrvPartitionDate__c=2026-08-15'
./reset.sh grv-fixture "$GRV_SALESFORCE_TEST_ORG_ID"
```
Expected post-mutation values are the pristine oracle with that mutation
applied. Reset uses bulk operations, not thousands of individual DELETE
requests. The reviewed storage waiver is explicit:
`./reset.sh grv-fixture "$GRV_SALESFORCE_TEST_ORG_ID" --ignore-storage-limit`.
Verify the reset before relying on it for destructive lifecycle cases.
Cleanup can instead discard the disposable org (or delete the scratch org).
Bulk query jobs do not modify source rows.
