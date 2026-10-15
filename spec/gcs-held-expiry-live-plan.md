# Package27 — small real run-expiry test plan

**Under parent review; no live result yet.** Exact ignored test
`live_gcs_expired_confirmed_run_refuses_renew_without_releasing_hold`, new
`crates/grv-conformance/tests/gcs_held_expiry_cli.rs`. This is run expiry, not
OAuth/native-reader deadline expiry or hold expiry. Holds never expire.

## Named assertions

Fresh exclusively owned GRV child initialized with maxclockskew0, pendinggrace1,
**max lease TTL180s** live (30s local). Test-only new-root initialization, no
existing-root policy/default change. Actual managed source SELECT1/publish,
actual GcsView pull/full preparation, actual external CLI session preparation
confirms exact hold and open run. Native/adapter processes close before wait.
Real SystemClock/monotonic finite wait until actual durable expiry **plus1s**;
no fake clock, timestamp overwrite, background renewal or authority refresh.
Require still-live observed run before wait; if too short/excessive fail, do not
renew or retry effects. Distinguish storage runTTL from original native≤300s
reader execution lease. Expiry loop positive sleeps; finite180s+5s wait allowance.

Actual `session renew` must refuse with `EXTRACTION_INCOMPLETE`, not false
`STATE_CONFLICT`/Stopped/clean absence. Original control/hold/sourceLATEST bytes
and backend validators preserved, private immutable context/journal unchanged;
strict derived inventory has no allocation/version/publication, release marker
checkedNotFound, run staysOpen+confirmed butexpired, hold active/unreleasable.
Ordinary backend credential reads still occur; no claim of no provider total or
engine/source restart. Whole cloud tree is not copied/claimed immutable; exact
named snapshots and derived inventory only. No GC/abort/release. Fixture cleanup
is parent-owned synthetic teardown, **not protocol hold release**.

## Resource and execution gates

Only selected <jiacao@on.sinch.com> / entit-finance-prod, project828148855924.
Driver `/tmp/grv-task27-expiry-live.py` default offline; `--execute --slice expiry
--freeze <protectedfreeze>` only after parent actualsource/localtests/lint review.
Twelve artifact freeze: expiry binary/currentCLI/fixture/nativeguard/signed
httpfs+aws/manifest/driver/diagnostic module/newtest/fixture source/thisplan.
Guard2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f unchanged.
Exact test selection/one passingtest/pre-everylaunch-post-current hashes.
No Cargo/edit/rebuild while lifecycle live. Finite1800s whole-test process-group
watchdog/reap containment, not shipping hard-stop. No effects retry/budget reset.
Diagnostics fixed-category only, observer-affected/no latency/cancel claim;
raw files private/transient strictcleanup, abrupt harnesskill limitations retained.

New unique `grv-gcs-held-<24lowerhex>` bucket/empty exact `/terminal` child,
checked404-beforeprotected atomic marker/create/child intent. New-bucket-only
EUROPE-WEST1, uniformaccess/publicaccessprevention, softdeleteretention0,
Delete lifecycleage1, ownershiplabel; no existingIAM/ACL/policy/versioning change,
keys/impersonation/ambientfallback. Tokenmemoryonly noargv/env/log/documents/raw
helperoutput/receipts, best-effort copies notuniversal heap erasure.
FixedverifiedTLSstorage.googleapis.com no proxy/redirect/retry; strictboundedJSON.
Afterprocessreap marker/projectverification, strictly inventoryallversions exact
admittedterminal keys, journalkey/size/generation thenconditionalknown-generation
deletes, observedemptyinventory/bucketdelete404. Refuse unknownkey/marker mismatch,
no test-sideDrop/unknownwildcard deletion. Failure protected recoveryjournal.

No package27 completion from this slice: actualGCS ambiguousholdfault, in-flight
cancel and ordinary-consumer special/evolution residuals remain separately gated.
Original wholematrix failed evidence remains failed; no publication/capability.
