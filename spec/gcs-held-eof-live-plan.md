# Package27 — independent GCS-storage held CLI EOF gate

Draft under code review; no live result. Exact ignored
`live_gcs_confirmed_hold_survives_managed_cli_eof_and_incomplete_attempt_refusal`
in `crates/grv-conformance/tests/gcs_held_cancel_cli.rs`.

## Scope

New owned GCS root, real CLI managed source publication, **default local
materialization pull** with full source preparation, actual core dependency hold
and confirmed consumer RunControl, supported managed native compute invocation.
No managed nonempty GcsView preparation: that path intentionally requires verified
local inputs. This is GCS storage plus local materialization/native work, not
remote native-range cancellation or external ExecuteBuild (forbidden by protocol).

Owning test kills only its unreaped CLI, which closes SDK channel; wait for native
owner to release real workspace lock. Kill is an injected CLI-loss event, not an
SDKStopped acknowledgement or shipping hardstop. Fixture execution counter plus
source-free durable Executing prove dispatch, not exact native IO entry.
Quiescent protected journal read after owning CLI died must show confirmed
matching identity. One exact-attempt recovery must refuse BUILD_INCOMPLETE,
without second fixture execution. Independent durable record has no candidate,
completion or outcome before/after recovery. Actual immutable hold bytes+validator
unchanged across recovery **after loss**, active/unreleased. Stable control identity
preserved; legitimate renewal CAS/expiry/mutation changes are allowed. No derived
allocation/version. Holds do not expire and fixture teardown is not hold release.

## New resources and authority

Only `gs://grv-gcs-held-<24lowerhex>/terminal` in a newly created bucket with absent
child; strict canonical coordinate, account `jiacao@on.sinch.com`, project
`entit-finance-prod` / `828148855924`. Existing reviewed new-bucket-only scaffold:
EUROPE-WEST1, uniform access, public access prevention, soft-delete0, Delete age1,
ownership label. No key, impersonation, ambient fallback, existing IAM/ACL/policy/
versioning changes or production reads.

Parent driver `/tmp/grv-task27-held-eof-live.py` defaults offline. Execute only
`--execute --slice eof --freeze <protectedmanifest>`. Freeze actual current test
binary/CLI/fixture/guard/signed extensions/notices/source/driver/diagnostics/plan
(12 artifacts), list exact ignored symbol, pre/post/current checks. No edits,
builds or other lifecycle overlaps actual live stage. Selected keyless acquisition
before resource intent; tokens memory-only, no raw stderr/argv/environment bearer,
fixed TLS host/no redirects/proxy/retry. Diagnostic categories only, no raw reports.

Whole test harness budget1800s, pre-invocation marker budget600s for actual GCS
storage preparation (local20s), native-owner lock release10s. These are finite
harness assertions/containment, not shipping hard deadlines/per-call latency
claims. No blind whole-budget increase. Any assertion/timeout fails this gate;
no effects retry/relabeling. Stop/reap owned process group, inventory every known
version under admitted `terminal/`, refuse unknown resources, delete exact
generations, observed empty inventory and bucket404. Cleanup failures retained.

No package27 closure/full samefixture matrix pass, GcsView cancellation, actual
wire cancel_ackStopped, universal crash recovery or publication promotion.
