# GCS post-secret setup failure and revocation checkpoint

Status: passing synthetic setup-fault slices, not complete OAuth isolation or
shipping acceptance. Revision5 helper behavior below is historical; current
revision6 in-process code retains the native revocation checks and quarantines
unproved cleanup without process exit. See
[gcs-in-process-transition-checkpoint.md](gcs-in-process-transition-checkpoint.md).
Public wire, durable and binding-schema versions remain unchanged.

## Behavior

Native setup can fail after creating its private temporary secret. The old
best-effort DROP with swallowed errors could return a reusable-denial response
without proof that credentials had been revoked. Catch now:

1. Marks exact GCS state failed.
2. Attempts private secret DROP.
3. Separately observes absence in the actual memory secret storage; a failed
   DROP is not assumed to prove either absence or retention.
4. Restores allowed paths/external access and the user guard; restoration
   uncertainty likewise prevents reusable setup failure.
5. Returns -1 only for authoritative setup failure with proved absence and
   restoration; -2 means whole-owner fence required. Dedicated helper exits125
   on -2; parent EOF handling must fence before reopen. Exit is not rollback.

Revision5 is required because an old helper would misinterpret -2 as an ordinary
reusable failure. Native library/helper ABI revision checks fail closed rather
than dual-decode old behavior. No public contract migration/version bump.

## Source-including synthetic fault proof

`native/gcs_setup_proof.cpp`, compiled with test-only `GRV_GCS_SETUP_PROOF`,
exercises actual configure source with signed stock HTTPFS and a synthetic token:

- Failure immediately after temporary secret installation.
- Failure after HTTP request policy installation.
- Failure after exact filesystem installation.

Each proves secret absence, exact failed state, restored guard/path/external
access, generic error without token, denied secret introspection/read admission,
and local
BEGIN/ROLLBACK recovery. Forced DROP failure keeps the secret and yields -2,
not reusable -1. Proof-only fault switches do not exist in shipping library or
helper; no production environment toggle/network or credential acquisition.

Rust subprocess test proves the -2 status handler terminates the whole process
with125 instead of returning a complete Failed response. This tests the exact
helper status handler, not a full combined injected native/helper receipt race.
Existing lock/EOF recovery proofs remain separate.

A missing-extension LOAD has no guaranteed inspectable HTTP secret support and
may yield -2. Its artifact recovery test now fences/reopens BEFORE asserting
BEGIN/ROLLBACK. The original same-child recovery assertion failed after stricter
revocation; corrected semantics retained, not waived.

## Validation and limitations

- Native113passed/22ignored; seven explicit recovery/setup artifacts passed.
- Post-secret signed setup proof passed; ten ctest native proofs passed.
- Strict affected native/conformance Clippy passed.
- Revision5 live pull/self-preparation after review at
  `~/grv-shipping-gcs-6h97jpuz` passed (priorw0to3lh0/475845y8 retained),
  owned generation/bucket deleted, empty all-version inventory and404.
- Broad `gcs_ --ignored` invocation was invalid for three live tests without
  required account/project/source coordinates; it also observed one unresolved
  compute-deadline fence. That source-free test passed a separate exact rerun;
  the transient unresolved result is NOT universal fence qualification. Keep
  unresolved ownership quarantine behavior and deadline matrix open.

Independent review identified an uncontained guard-removal call inside catch;
it is now protected and restoration uncertainty requires -2. Read-admission
attempts after every injected failure also refuse before network. Retained HTTP
parameter/token copies and direct surviving filesystem handle paths are not all
covered by this proof.

The completed retry review verified memory-storage absence semantics, failed
state checks on scan/filesystem/request boundaries, weak-state policy callback,
helper125 exit/parent fence-before-reopen, and revision coordination. It found two
further containment edges: inspection callback could set absence before its
transaction completion threw, and preamble guard lookup/path copy could escape
the C ABI. Both are fixed: inspection exceptions clear proof, and an outer catch
returns -2 for preamble uncertainty. Added synthetic callback-after-observation
and preamble exceptions both require -2 and refuse read admission. Callback
injection exercises classification, not an actual engine COMMIT-failure test.
Updated proof,113native/22ignored,7recovery,10ctest and livepair pass after fixes.
Review supports only this bounded slice, not full credential-erasure acceptance.

Remaining: full logs/views/receipts/external artifacts secret canaries, real
fresh/concurrent OAuth, all setup restoration/fence injected races and security
sign-off. Stock SQL/native credential copies are not all proven wiped. Prior
full held lifecycle predates revision5; final stable artifact pair acceptance
still required. Capability stays unadvertised; no release/publication implied.
