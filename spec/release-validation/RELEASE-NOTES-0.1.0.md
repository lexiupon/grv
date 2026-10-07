# GRV 0.1.0 — macOS ARM64 initial scoped release (draft)

**Candidate qualification pending. Not publication approval.** Replace the
identity fields below from the final candidate dossier; do not infer them
from historical tests.

## Included

Production CLI with local/S3/GCS storage and process-based DuckDB/Salesforce
adapters. Guarded DuckDB 1.5.6 and pinned signed aws/httpfs extensions are
packaged; transfers do not download or INSTALL extensions. Third-party notice
files accompany the bundle under a reviewed conservative upstream-release
notice policy. No claim of an exact reconstructed linked SBOM.

Representative flows: Salesforce snapshot -> publication -> DuckDB local
materialization; managed DuckDB SQL -> snapshot publication; fixed S3-file views
and external build lifecycle where the named candidate gates qualify them.
Exact receipts support offline terminal replay without reapplying old rows.

## Installation and runtime requirements

macOS ARM64 only. The current native library Mach-O requires **macOS 26.0**;
the current validation host is macOS 26.7.1. Do not claim macOS11 support from
the CLI/extension minimum alone. Final bundle records the maximum minimum OS
across all shipped binaries; a reviewed rebuild is needed to lower it.
Other platforms are deferred. Keep the CLI and adapter trees
in protected, owner-controlled directories; do not grant group/world write.
The native library/extensions travel with the DuckDB adapter. Directory/tarball
adapter installation is explicit and out of band; replacement requires
--replace. Verification must use the distributed checksums and reviewed
installation instructions for the exact tarball.

Salesforce authentication uses the supported Salesforce CLI/private credential
helper path; obtain source permissions separately. Cloud backends require
explicit appropriate credentials and region/project/account configuration.
GRV write credentials and DuckDB S3-reader credentials are separate concerns.
Do not use production sources for the disposable validation fixture.

Package signing/notarization: **not currently claimed**. Official DuckDB extension
signatures are verified; that is not macOS application signing/notarization.
Final download/checksum/installation destinations must be approved separately.

## Important semantics and limits

- Scoped preview, not complete-client-v1 or four-platform/full-matrix approval.
- Salesforce auto uses deterministic REST-first selection. Rich nullable-text
  Bulk projections can refuse CSV null/empty ambiguity rather than corrupt
  values. Numeric/ID/date/bool eligible Bulk paging is separately exercised.
- Snapshot acquisition is nonresumable. Ambiguous or failed acquisition cannot
  be silently reissued under the same attempt. Use a new attempt only after
  reviewing the reported outcome and durable state.
- Publication omission is allowed only from an accepted complete snapshot.
  Empty snapshots differ from failed/partial captures.
- Receipt replay returns the original immutable outcome. It does not rewind
  newer destination revisions or requery deleted sources.
- Recovery/GC can report busy/unknown/waiting when writer-stop or remote-effect
  evidence is insufficient. Expiry alone is not proof of stopped writes.
- GCS-backed data materializes locally; GCS views are not advertised.
- S3 defaults to multipart streaming. Explicit `GRV_S3_UPLOAD_MODE=single-put`
  uses conditional PUT without multipart APIs. Its buffered per-object default
  is **1 GiB** (1,073,741,824 bytes), configurable through
  `GRV_S3_SINGLE_PUT_MAX_BYTES` up to 5,000,000,000 bytes. This default targets
  machines with at least 64 GiB RAM; concurrent uploads add memory demand and
  the limit is not an RSS cap. Lower it explicitly on smaller machines.
  Oversized/source-failed objects refuse before writing; uncertain writes are
  never automatically retried through another protocol. Four-action-only IAM
  principal qualification is not currently claimed; KMS/bucket policies may
  require more permissions. Single-PUT mode does not inventory preexisting
  multipart uploads.
- Service-fault tests include synthetic response suppression and controlled
  process exits. They do not establish exhaustive network-loss/cancel schedules
  or performance capacity.

## Candidate identity and evidence (fill after freeze)

- Source commit: pending
- Bundle/archive SHA-256: pending
- Host/minimum-runtime support: pending clean-environment review
- Build toolchain/MSRV: declared Rust 1.89; candidate execution pending
- Candidate test/install/provider outcomes: pending
- Durable evidence archive and retention: pending
- Known qualified capabilities: exact bundle capability inventory pending

The six gates in README.md and exact scope in 0.1.0-SCOPE.md determine acceptance.
A passed historical baseline is not proof for changed release bytes. No known
contract failure is waived by labeling the release a preview.
