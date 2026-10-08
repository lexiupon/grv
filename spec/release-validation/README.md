# Release validation operations

This directory is the reusable operational index for release validation.
The normative contracts and `../grv-v1-release-validation.md` remain authoritative.
The initial macOS release gate proposal below is a **scoped preview policy**, not
completion of the full-v1 plan. Publishing always needs separate approval.

## Private live configuration and archived artifacts

Real live Salesforce identities, GCS account/project/bucket and S3 profile/region/bucket
belong only in the ignored owner-only `.release-validation.local.json` file.
Set `GRV_RELEASE_VALIDATION_CONFIG` to its absolute path before live recipes.
The required sections are `sf` (`org`, `org_id`), `gcs` (`root`, `account`, `project`),
`s3` (`root`, `profile`, `region`) and `archive` (`root`). Cloud roots must name
an exact nonempty authorized prefix. This file is **coordinates, not credentials**;
tokens/passwords/access keys are refused. Fixtures and tests never default to a
real account. Synthetic examples in this tree are not authorization. Historical
Git history is subject to the explicitly requested local privacy rewrite;
retain the original history and old-to-new identity mapping privately outside Git.
No remote force-push is authorized by a local rewrite.

`artifacts` can be a symlink to one run-specific child of the configured archive
root. Evidence/packaging tooling checks this approved alias instead of accepting
arbitrary symlink escapes. New identity-bound work uses canonical paths. Moving
existing journals does not authorize rewriting their immutable identities: retain
the old baseline and create a new baseline in the archive. A local OneDrive move
does not itself prove remote synchronization, retention guarantees or a backup.
Raw private evidence may contain account identities and needs restricted access;
never publish it alongside the distributable without review.

## Keep three things separate

1. **Test catalog:** `catalog.json` versions repeatable commands, safety limits,
   gate associations and change triggers. It is not evidence that a test ran.
   `../fixtures/release-validation/scenarios.json` owns exact normative
   selector/assertion mappings; do not duplicate or infer those here.
2. **Current decision:** `STATUS.md` summarizes the six gates, limitations and
   next actions. Update after a meaningful test/fix/review batch, not every
   console line. It links to immutable evidence; it never replaces it.
3. **Evidence:** `evidence/first-pass.json` indexes preserved historical logs.
   New per-run JSON and private logs live under ignored
   `artifacts/release-validation/<candidate>/`. Each record is append-only:
   unique run ID, command/catalog revision, result/exit code, UTC interval,
   source/spec hashes, actual binary hashes when supplied, environment identity,
   assertions/limitations, faults, cleanup and log SHA-256. Failures remain;
   retries create new records. Missing historical facts are null, not guessed.

Git stores the policy/catalog, reviewed sanitized evidence index and decisions.
Large logs/binaries belong in durable access-controlled release/CI artifact
storage, linked by URI and digest from the reviewed dossier. Local ignored
`artifacts/` is useful working storage **but is not a backup**. Before approval,
archive logs, candidate bundles and receipts durably, record retention/location,
then review and commit a sanitized dossier/index under `evidence/<candidate>/`.
Keep it for the supported release lifetime and at least through its successor's
compatibility qualification. Never upload raw credentials/auth stores or private
source records. No remote upload is authorized just by this document.

## Initial macOS ARM64 scoped release gates

| Gate            | Required closure                                                                                                                                                                                                                                                              |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G1 Candidate    | Reviewed source/lockfile/contracts frozen; release-profile build; candidate full automated/native/scripts/lint run; declared MSRV proved or explicitly revised.                                                                                                               |
| G2 Installation | Relocated release bundle, protected directory/tarball installs, CLI and both production adapters/native dependencies load without build tree/cache/network INSTALL; representative production workflow.                                                                       |
| G3 Distribution | Exact shipped dependency notices/licenses/provenance, checksums and artifact/secret/fixture scan. No preview exemption.                                                                                                                                                       |
| G4 Safety       | Exact omission/complete snapshot behavior; destination commit/receipt and publication ambiguity; accepted/terminal source-free replay and mismatch refusal; cancellation/stopped writers. Live Salesforce mutation/omission/reset/restored oracle. Target the remaining gaps. |
| G5 Integrations | Production candidate lifecycle for each advertised provider/transport: independent exact values/schema/bytes/no-op/replay. Successful eligible Bulk/paging or explicit reviewed qualification limitation; primitives/fixture CLI do not qualify production integration.       |
| G6 Review       | Exact critical scenario mappings; explicit noncritical deferrals, capability/platform/recovery limits, immutable evidence archive and compatibility baseline. Separate approval for final tag/destination/publication.                                                        |

A green suite is not alone a gate closure. Unknown outcomes can be correct
safety results, but do not prove eventual cleanup/progress. No actual contract
failure is reclassified as a preview limitation. Claims must match shipped
capabilities; capability narrowing must be reviewed, not achieved by hiding an
unrun test. Full-v1 still requires its full register/matrix and must not be
claimed by this initial release.

## Rerun policy

Every release, including a small patch:

- Freeze the candidate and review the diff against the last qualified baseline.
- Run automated workspace/native guard/format/lint/script gates, build and inspect
  the actual bundle, verify installation/loading, checksums/notices and secret
  scan. Run compatibility/replay against retained previous-release fixtures.
- Run a representative production local workflow from the relocated bundle.
- Add the change-triggered gates below. Classify uncertainty as requiring rerun.

| Changes                                                                             | Additional mandatory evidence                                                                                                                                        |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Capture, scalar conversion, schema, canonical writer, Arrow/Parquet/native versions | Exact scalar/canonical/empty/boundary tests; actual extraction/pull; relevant identity/old-schema compatibility and affected adapter live acquisition.               |
| Publication, ownership, receipts, journal, retry, locks, cancellation, GC/recovery  | Affected deterministic fault/omission/transaction/replay/race tests; production process-death coverage; affected real backend ambiguous/precondition behavior.       |
| Backend/auth/HTTP/helper or cloud dependency changes                                | Affected provider primitives, production lifecycle/no-op/replay, isolation/sanitization, fault and owned cleanup tests.                                              |
| Salesforce Describe/SOQL/conversion/transport/auth changes                          | Pinned-org exact REST/auto/relationship/empty acquisition; wrong identity and refusal; successful Bulk where claimed; relevant checkpoint/loss/mutation/reset gates. |
| Packaging/install/capability manifest/toolchain/OS/native/extension/license changes | Clean-environment loading/relocation, install protection, notice inventory, toolchain/MSRV and affected production workflows.                                        |
| Contract/format/default/identity/CLI changes                                        | Exact affected normative mappings; old persisted data/receipt/attempt compatibility and request-mismatch checks.                                                     |
| Documentation only                                                                  | Validate links/register/claims; any executable/schema/config change is not documentation-only. A new release still gets the candidate/package baseline.              |

Do a full in-scope provider/safety qualification for the first release, broad
changes, a new advertised platform/provider, or uncertain impact. For a small
well-understood patch, earlier provider evidence may be referenced as a
**reviewed baseline**, not relabeled as a new pass. Record the exact previous
candidate, diff, dependency/native changes, reviewer rationale and skipped
matrix cells. Do not use elapsed time alone to decide equivalence. Repeated
full passes can be planned periodically once we know release cadence; no
recurring service operations are scheduled by this policy.

## Recording and checking

```console
# Catalog + historical index: no services or builds
python3 scripts/validation-evidence.py check

# After executing a named command in a protected shell, record a new run.
# Capture the actual command/env overrides and UTC times; do not include tokens.
python3 scripts/validation-evidence.py record --catalog-id workspace-native \
  --log artifacts/release-validation/rc1/workspace.log --result passed \
  --exit-code 0 --started-at 2026-10-07T12:00:00Z \
  --finished-at 2026-10-07T12:05:00Z \
  --details /path/to/sanitized-run-details.json \
  --out-dir artifacts/release-validation/rc1/runs
python3 scripts/validation-evidence.py check --runs artifacts/release-validation/rc1/runs
```

Details must include `command`, `assertions`, `limitations`, `cleanup`,
`environment`, `binary_sha256` and `fault` (null is allowed when honestly unknown
or not applicable). Describe setup variables by nonsecret identity/path; use
process-memory tokens, never record auth output. The tool records the repository
snapshot **at recording time**, not proof of the snapshot at execution time;
freeze before execution and independently bind binary hashes for qualification.
It does not run tests, sanitize arbitrary data, validate semantic assertions,
certify completeness or authorize publication. Inspect logs/details before use.
Historical imported records explicitly lack frozen-source/binary provenance.
