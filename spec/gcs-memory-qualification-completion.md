# GCS-specific memory qualification completion

Status: **agreed memory scope complete; pause before unrelated qualification**.
This concludes tasks49/51 and the memory portions37/43, under the user-approved
GCS-specific metadata policy. It does not conclude the broader in-process
transition45, cloud security/recovery matrix, packaging or release acceptance.
No capability advertisement, commit, push or publication.

## Accepted contract and boundaries

The automatic internal GCS metadata policy uses8MiB weighted logical JSON
admission, depth64,8MiB raw documents,1024 retained document IDs and bounded
runtime/state families. Generated SQL/caller copies are admitted before adapter
amplification. Raw frame/document/durable JSON shape is admitted before typed
parse where GCS identity is known; outbound metadata and cumulative reference
expansion before copies. Durable refusal remains Unknown/OutcomeUnknown, never
absence/repair/truncation; known terminal refusal during cancellation requires
joined cleanup/channel closure and recovery, never false Stopped acknowledgement.

This is **per-family logical content/structure admission plus finite ownership
multiplicity**, not a shared8MiB process limit or measured physical capacities.
Generic bootstrap/prebinding parsing remains separately bounded by existing wire
limits before the GCS root is known. Local/S3 default policy is unchanged.
Host-owned documents, arbitrary independent runtimes/custom serializers, malloc
capacity/overhead and every stock engine/source allocation are not covered by an
invented whole-host cap.512MiB engine/16GiB spill are accepted partial controls;
64KiB collector is soft and4MiB chunk check is postallocation. No allocator
replacement/quota, helper process, transport or wire/durable version was added.

Transfer source/scratch remain separate: GCS-root staged export reserves full
encoded capacity plus decoded native source within the source allowance; Arrow
conversion has its own admission. Actual BuildStore export destroys/reopens the
old invocation Engine before local acquisition/fetch under retained workspace
ownership. Source reviews support this Engine-owned boundary; they do not prove
arbitrary escaped owners absent, signed HTTPFS binary/source correspondence or
universal RSS control. QueryEnd/Close/interrupt/reopen alone are not immutable
rollback/outcome proof. Mandatory full-object preparation and exact authority,
stock cooperative deadline limitation and best-effort GCS object identity remain.

## Evidence and corrections

- Offline native105passed/12ignored; build22, extraction9, wire5library20protocol,
  SDK18library3runtime; strict affected native/nonnative Clippy, fmt/diff,
  stale-peerCLI1 and12ctest pass. Counterexample ctests retain their limitation,
  not universal allocation acceptance.
- Actual SDK/native GCS-selected session regression: repeated exact reopen,
  channel restart with durable candidate, acceptance and changed-identity
  refusal; independent stored session/completion/digest verification.
- Three durable node-amplification regressions (build record, pull receipt,
  omitted-target contract): raw below2MiB but40000empty nodes refuse before
  typed decode; original history/view replay and DB/lock assertions preserved.
- Stable offline `~/grv-gcs-memory-offline-rdvyuo0g`: seven direct prebuilt test
  binaries pass, eleven pre/post hashes unchanged. Earlier104stage `gab585t_`
  retained. Failed driver `lr3ctckw` omitted required fixture environment; nine
  extraction failures happened before fixture execution. No product fix inferred;
  failed evidence preserved and corrected driver passed.
- Stable authorized synthetic live pair `~/grv-gcs-metadata-stable-kiaj7dhn`:
  two tests pass, three artifact hashes unchanged; one owned generation deleted,
  all-version inventory empty/bucket404. No IAM/key/organization-policy change.
- **Final current-artifact actual held lifecycle**
  `~/grv-held-gcs-y_2_qyrt`: direct prebuilt
  `live_external_gcs_build_fixed_view_inputs_self_base_accepted_export_restart_and_terminal_replay`,
  exit0,1passed/0failed,1870.18seconds. Parent read result/summary/cleanup/hash
  records and independently compared all five saved/current artifact SHA256:
  test, conformance CLI, GCS fixture, revision6 native library, signed HTTPFS.
  No rebuild or artifact edit during lifecycle. All-version inventory empty and
  bucketHTTP404. Final cleanup reported0generation deletions after prefix cleanup;
  this is not a count of all deletions. Published dependency holds remain retained,
  not abort/prune/release/expiry proof. Prior `dw_ypv61` is not promoted.

Scoped source reviews:52e0 aggregate model;435b1 export/cancel ordering;
d58c durable admission finding corrected and1d8a omitted-contract fix verified.
Earlier327be8 expansion/lifecycle andbbf792 cancellation findings were fixed and
regression-tested. Review scopes/tool limits and lack of independent test reruns
remain recorded in the linked checkpoints. A passing held test supplements,
not replaces, admission/ownership/refusal evidence.

## Pause and remaining work

Nothing is running. The agreed GCS-specific metadata/transfer memory work is
accepted on these artifacts and boundaries; **pause now**. Broader setup/security,
cooperative expiry/outcome/crash/WAL, scanner/rebind/cache isolation, held terminal
cleanup, S3 matrix, install/platform/package/MSRV and release gates remain open.
This completion does not add hard native stop, guaranteed savings, generation-
pinned stock range reads or universal credential erasure. Source correspondence
and generic/local/S3 independent resource qualification are not silently closed.
Evidence checker remains catalog42/records94/digests matched, release readiness
not assessed. Traceability register source-change refusal is retained; no silent
refresh/publication approval.

Related: [metadata policy](gcs-metadata-policy.md),
[aggregate inventory](gcs-aggregate-metadata-checkpoint.md),
[source lifetime](gcs-source-lifetime-checkpoint.md),
[execution decision](gcs-in-process-execution-decision.md).
