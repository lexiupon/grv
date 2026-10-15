# Package27 terminal held-GCS slice — reviewed bounded execution plan

**Status: bounded independent-slice revision under parent review.** Earlier
whole-matrix failures are preserved in `gcs-held-terminal-qualification-blocker.md`.
No passing live slice result yet. This is only
one slice of27, not completion of run-expiry/lost-ack/cancel or ordinary-consumer
coverage. Publication/default/capability changes remain unapproved.

## Actual finite assertions

New `crates/grv-conformance/tests/gcs_held_terminal_cli.rs`, exact ignored symbol
`live_gcs_confirmed_hold_abort_then_source_gc_release_and_prune`. The same
lifecycle is exercised locally by a nonignored test first; owned-root validator
has an independent offline negative test. Initial new-root Store setup uses
max clock skew0 and pending grace1 (test-specific configuration, not defaults).

Actual CLI managed upstream revision1 -> actual GcsView pull/full preparation
-> external session prepare -> actual confirmed hold record and run control.
Upstream revision2 publication cannot redirect the fixed input. Actual sourceGC
must preserve old version1 data/manifest, no `.pruned`, active hold/release absent.
Actual session abort must seal the run while hold stays active and becomes
releasable; no derived allocations/output/version/publication. Repeat abort must
preserve terminal facts. Actual **sourceGC** commits release then prunes oldv1
while currentv2 remains; repeat GC preserves release marker bytes/validator and
hold record, no revision rewind/new output.

Holds never expire; open run expiry follows store parameters, not native300s
reader lease. Consumers cannot release holds directly. `RunPhase::Sealed` is
not SDK `Stopped`; this abort path does not qualify in-flight native cancellation.
Protected versions cannot simultaneously be asserted pruned. Confirmed backend
hold is read/validated, not merely supplied proof. Published holds historical
success is a different retained artifact stage, not these terminal facts.

## Independent finite-slice execution (not a blind deadline increase)

Preserve the original full ignored matrix and its failed evidence. New
`--slice protect` and `--slice release` gates each require a **fresh independent
uniquely owned bucket/child**, not restart/effects retry on failed resources:

- Protect: `live_gcs_confirmed_hold_protect_then_abort`, actual acquisition,
  source revision advance, sourceGC protection, abort/sealed run. Active hold
  and no release marker remain; parent fixture teardown is **not** protocol
  hold release. Stop before post-abort sourceGC release.
- Release: `live_gcs_sealed_hold_source_gc_release_prune_and_repeat`, actual
  acquisition/confirmed hold and source advance, abort/sealed run, sourceGC
  release/prune and repeatedGC/idempotence. Omit only earlier open-runGC
  protection (covered by independent protect gate); exact immutable hold,
  release operation target, tombstone/current version assertions remain.

Each gate retains1800s **whole-test** finite harness budget and process-group
containment, never lease renewal/budget reset or shipping hard-stop. Explicit
safe command ordinals/stage labels/monotonic durations distinguish aggregate
budget from per-call stalls; no private args or provider output in labels.
Independent gates do not demonstrate original nine-command same-fixture full
continuity. They may qualify only their named finite assertions; full matrix
cannot be relabeled passing. Parent reviews branches and both local equivalents
before freezing any live run. No production cache/transport/default/retry change.

## Parent-owned resources and freeze

Driver `/tmp/grv-task27-terminal-live.py` default offline-plan only. Explicit
`--execute --freeze <0600freeze>` after parent final checks. Named freeze:
current terminal integration binary, CLI, fixture-duckdb-gcs-build, unchanged
native guard (`2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`),
both pinned signed extensions/manifest, test/fixture source, driver and this plan.
Check exact test list and exactly one passing test; pre/post/every launch hashes.
The fresh diagnostic stage additionally freezes
`/tmp/grv-task27/terminal-diagnostics.py` (12 named artifacts total). At180s
checkpoints while the leader is still owned/unreaped, sample only approved
same-process-group binaries. Persist **fixed category labels**, not arbitrary
symbol text, addresses, argv, environment, headers, locals or memory. Private
transient raw files must be removed on ordinary completion/error; abrupt harness
kill may leave private raw files and requires explicit inspection/removal before
reuse, not a universal no-copy/erasure guarantee. Sampling may perturb scheduling;
records are debugging only, never latency/hard-stop/acceptance evidence. Strict
parent offline ownership/canary/atomic-output tests precede freeze. Diagnostic
failure is recorded, not silently passing. Product execution authority unchanged.
No edit/build/Cargo/concurrent native lifecycle while live/held work runs.
Proof process group timeout is harness containment, not shipping hard-stop;
child processes are reaped before cleanup. No worker detach or effects retry.

Only `jiacao@on.sinch.com` / `entit-finance-prod`, project828148855924. Projected
explicit acquisition/expiry/override validation before cloud-resource intent.
No token in argv/env/log/digest/raw helper/provider output/receipt/view/documents,
no keys/impersonation/ambient fallback or existing IAM/ACL/policy/versioning change.
Token copies are memory-only/best effort, not universal heap erasure.

New unique bucket `grv-gcs-held-<24lowerhex>`; absent404 precedes protected atomic
marker/create/child intent. New-bucket-only EUROPE-WEST1, uniform access,
public access prevention, soft-delete retention0, Delete lifecycle age1, owner
label as approved scaffold. Marker and project checked before operations/cleanup.
Exactly `gs://<bucket>/terminal`, empty inventory before initialization. Test
creates only this admitted GRV child using normal durable run/version/hold
protocols and never deletes bucket/unknown resources. Driver alone owns cleanup,
not a Drop silently ignoring errors.

Generated child keys are not predetermined individual uploads: the protected
empty-child admission intent bounds actual GRV writes. After all native/CLI work
joins, driver strictly inventories all versions, refuses keys outside `terminal/`,
journals exact key/size/generation and conditionally deletes each known version.
Strict paginated inventory/no duplicate/cyclic pages; marker verified first.
Observe complete empty all-version inventory, bucket delete then404. Failure
retains protected journal; response-lost bucket creation still recovers only its
verified owned marker. Already-absent recovery records404 without inventing an
unobserved inventory. No wildcard unknown-resource cleanup.

REST fixed verified-TLS `storage.googleapis.com`, no proxy/redirect/retry,
bounded duplicate-refusing responseJSON/finite calls. Shipping reader unchanged
fixed-host exact read-only OAuth policy and original cooperative lease. Native
held CLI observations do not prove protection from privileged replacement,
per-range generation pinning, finite native/HTTP overrun or immutable rollback.

## Before acceptance

Parent actual source review, local same-path lifecycle, strict conformance
checks and builds precede freeze. Only actual ignored exact live pass plus
stable artifacts/complete cleanup can qualify this27 slice. Lost-ack/create-
confirm uncertainty, real run expiry, cancellation and ordinary consumer
special/evolution cases remain open; no package27 completion from this plan.
