# Package27 — held GCS-view managed cancellation gate

Draft under parent review; no live result yet. Exact ignored symbol
`live_gcs_held_gcs_view_cancellation_refuses_reexecution_after_supported_interrupt`
in `crates/grv-conformance/tests/gcs_held_cancel_cli.rs`. Sibling gates qualified in
`spec/gcs-held-terminal-checkpoint.md` keep their own freezes and are not re-run.

## Finite scope

Real push and full preparation through a real cloud backend on one fresh exclusively
owned canonical child. The protected engine-adjacent reader configuration document is
published before any stage from the fixed explicit selectors only, is re-read and
checked field by field, and stays credential-free; the production strict loader must
resolve it to exactly those selectors. Because the backend is external, core
reconstructs the input as a private GcsView regardless of the local materialization, so
the read path is the stock pinned signed HTTPFS path with its own independently acquired
selected-account OAuth credential. No token appears in argv, environment, logs, durable
JSON or the reader configuration document.

Execution is managed (external `ExecuteBuild` is refused by design). The owned CLI child
is killed and reaped so the SDK sees EOF. The protected journal is read only from a
quiescent state, after the owning writer closed and the workspace lock became acquirable
again; only contention is treated as pending.

Required assertions: nonvacuous durable Executing dispatch bound to the single exact
marker for this attempt; no candidate, completion or outcome; one same-attempt retry
refused; exactly one dispatch line survives, so no retried effects; the real core hold at
its real key for the current revision stays active and unreleasable, with no release
marker and no `.pruned` tombstone; hold bytes and validator invariant after the loss and
across recovery; consumer session unchanged; only the run-control fixed typed identity
compared across recovery, because a legitimate renewal CAS may change expiry and mutation
fields; nothing published on the consumer side.

## What this does not prove

The marker proves dispatch, not entry into a native read. Cooperative cancellation only:
no hard native stop, no finite stock-HTTP overrun guarantee, no `SdkStopped`
acknowledgement claim, no atomicity claim, no per-range generation pinning, no protection
against privileged same-key or same-schema replacement, no universal credential erasure.
The whole-test and child-wait bounds are harness containment, not shipping deadlines.
This is one independent slice; it does not qualify the original full same-fixture matrix,
whose failures remain failures.

## Failed stage retained

First live attempt `/Users/jiacao/grv-task27-held-cancel-xbopf8vv` failed after 250.89 s
and cleaned up fully: 15 exact-generation deletes, observed empty all-version inventory,
bucket `grv-gcs-held-813ae2865a874d1680475384` returned 404. Real push (154.4 s), real pull
(15.4 s), the owned managed child was dispatched, killed and reaped, and the quiescent
journal was read.

Cause: the test asserted the consumer run control's own `base_revision == 1`. That run
belongs to `derived`, whose managed build was interrupted and never committed, so its base
revision is legitimately 0; the held input citation is upstream revision 1, which the test
asserts separately and which passed. This was a wrong test expectation, not a production
defect, and the fixed typed identity across recovery compared equal. The assertion now
compares each snapshot against the pre-recovery snapshot instead of a hardcoded 1.
The failure is not retroactively passed; the corrected test runs on a fresh freeze and a
fresh owned bucket.

## Resources, freezes and cleanup

Only the new owned `gs://grv-gcs-held-<24lowerhex>/terminal` child. Fixed account
`jiacao@on.sinch.com`, project `entit-finance-prod`, project number `828148855924`. No
fallback, ADC substitution, impersonation, keys, production reads, or existing
IAM/ACL/policy/versioning changes. New-bucket-only reviewed scaffold: EUROPE-WEST1,
uniform access, public access prevention, soft-delete 0, Delete lifecycle age 1, ownership
label. Fresh root clock skew 0, grace 1, default run TTL; the native reader lease is
separately bounded and never renewed.

Driver `/tmp/grv-task27-held-cancel-live.py` defaults to offline. `--execute --slice
cancel --freeze <protected manifest>` only after parent code review, local tests, strict
checks and a fresh 12-artifact freeze. The exact symbol must list exactly once; pre, post
and current hashes must match. No edits, builds, agents or other lifecycles overlap the
live stage. Selected acquisition completes before any resource intent or mutation, over
the fixed HTTPS host with no proxy, redirect or retry.

Finally the owned process group is stopped and reaped, all versions under the admitted
child are inventoried, unknown resources refuse deletion, only exact key plus generation
pairs are deleted, and an empty inventory plus bucket 404 must be observed. No test-side
cloud cleanup. Failing stages and cleanup errors are retained; no effects retry and no
relabeling. No package27 closure, no capability promotion, no commit, no publication.
