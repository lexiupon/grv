# Initial macOS ARM64 release status

Scope: macOS ARM64 0.1.0, macOS 26.0+, tested host 26.7.1.
This is not full-v1, four-platform or complete normative matrix qualification.
Reviewed local commits/freezes and explicitly configured disposable validation
writes are authorized. External push/tag/release publication is not authorized.
Actual service coordinates belong only in ignored private configuration; examples
and sanitized historical indices never authorize a real account or bucket.

## Current decision

The prior clean candidate `92f7eae95551d69ad550273daf98984fc7b07e88`
passed 514 workspace tests (22 ignored), 42 script tests, eight native tests,
strict Clippy, Rust 1.89 all-target checking, installation/relocation, notices,
artifact audit, compatibility and actual production GCS DuckDB/Salesforce
lifecycle. Its archive hash and receipts remain historical evidence.

Subsequent changes add explicit conditional single-PUT S3 mode, a configurable
**1 GiB** default object limit, private live coordinates, and an approved
run-specific artifact archive alias. Follow-up production multipart and
single-PUT S3 lifecycles passed: exact DuckDB values/schema and independent
900-row Salesforce oracle, publication/full verify/local pull/no-op and
source-free replay after 41 owned objects were deleted per run. Those live
passes used the earlier 16 MiB implementation, not the current final bytes.
The replacement profile supports object and multipart inventory; the earlier
profile's AccessDenied preflight remains a retained failure, not a blocker now.

Latest focused evidence: 42 storage tests (five ignored), strict storage Clippy,
47 script tests and 14 fixture tests passed. The whole workspace follow-up run
timed out after 1800 seconds; 57 completed families/283 passed were observed,
but this is **not** a whole-suite pass. The leftover local workspace was retained
under ignored evidence rather than staged or deleted. No active suite is assumed.

## Updated release activities

1. Review changes, format/private-coordinate/fixture/secret checks, reconcile
   this status/catalog/notes, and commit an explicit file list locally.
2. Rebuild clean release bytes; record source/toolchain/native/extension identity.
   Complete serialized workspace/native/scripts/format/Clippy/Rust1.89 gates
   with process-group-aware evidence and sufficient timeout.
3. Requalify affected cloud/local workflows from final bytes: conditional S3
   primitives, both production upload protocols, configured limits and an actual
   default-sized 1 GiB object; owned UUID absence/cleanup. GCS/local reruns follow
   backend/packaging impact. No IAM or bucket-retention changes.
4. Run the named S3 fixed-file-view/native-reader/external-build gates if those
   capabilities remain advertised. Same explicitly configured writer/reader
   profile is allowed, but proves no independent least-privilege reader role.
5. Strict bundle/notice/secret audit, fresh-HOME directory/tar installation,
   relocation/local production flow and archived canonical compatibility replay.
   Generate final tar/checksums/candidate dossier and review exact claims.
6. Seal private evidence. Remote archive synchronization and retention are
   delegated to a separate user-managed process, not GRV release blockers.
   Review publication destination/tag and request separate push/tag/upload approval.

## Gate board

| Gate | Status and remaining closure |
|---|---|
| G1 / #18 | Previous clean candidate passed; current changes need clean freeze/build and complete automated/MSRV reruns. |
| G2 / #19 | Installation/relocation tooling and previous candidate passed; rerun against final tar/bundle. No independent physical-host claim. |
| G3 / #20 | Notice blocker closed: pinned upstream licenses plus reviewed 44 engine/55 extension third-party payloads. Recheck final hashes/audit; no exact linked SBOM/legal certification claim. |
| G4 / #21 | Scoped critical safety review closed: partial acquisition, commit cancel/EOF, GC corruption, helper FD isolation and replay proof. Relevant tests rerun with candidate; no exhaustive race/network claim. |
| G5 / #22, #30 | Current S3 permission blocker resolved; earlier multipart/single-PUT lifecycle passed. Final bytes/default-large-object and advertised view/build qualification remain. |
| G6 / #24 | Private coordinates and archive move complete; canonical compatibility baseline created/replayed. Final dossier, remote synchronization, retention and publication review remain. |

## Explicit qualification limits

- Single PUT buffers an entire admitted object; 1 GiB default targets >=64 GiB
  RAM deployments. Concurrent uploads/transient reallocation add memory pressure;
  this is not a process RSS cap or unbounded streaming. Override the byte limit
  for smaller hosts. Larger objects refuse before a PUT; multipart is the default
  protocol and never retried automatically as single PUT after ambiguity.
- Single-PUT mode creates no multipart uploads but does not inventory/claim the
  absence of preexisting multipart uploads. Current-object absence and multipart
  absence are separate evidence. Version histories/delete markers are not reclaimed.
- The broader tested profile is not proof of exactly four-action IAM restrictions;
  KMS or bucket policy may require more permissions. Either test a restricted
  principal or retain this limitation; no silent IAM modification.
- Salesforce fixture is 1,000 main/10 related with 900 Active rows, correctness
  not performance evidence. Existing storage/access blockers are closed. Real
  failed cleanup/storage attempts remain historical, not successful reclamation.
- Nine reviewed mappings out of 169 scenarios/51 release cases; 160 gaps are not
  automatically 160 critical missing tests. Do not claim full-matrix completion.
- Native minOS is 26.0 despite lower CLI/extension minima. No macOS application
  signing/notarization claim; official DuckDB extension signatures are separate.

## Archive and provenance

`artifacts` is an ignored alias to one run-specific child of the user-approved
private archive root. All 17,707 prior files were moved by same-filesystem rename;
106 retained receipt log digests still match. Local move does **not** prove remote
synchronization or permanent retention; the user delegates those to a separate
process and does not require GRV to verify them. Raw evidence can contain private account
coordinates and must not be publicly redistributed without review.

Old identity-bound baselines remain unchanged; an alias is not authority to
rewrite identities. A new canonical-path archived baseline was created and
immutable push/pull replay passed. Final candidate must replay that baseline.
Per-run UUID receipts retain failures/timeouts, UTC intervals, source snapshots,
binary hashes and cleanup evidence. Checkers validate structure/digests, not
release readiness. A complete local Git-history privacy rewrite is now explicitly required by the
user, including former account coordinates, company-specific examples and commit
identity metadata. Keep original evidence/old-to-new mappings privately outside
Git; no external force-push is authorized. A new post-rewrite candidate identity
and impact qualification are required.
