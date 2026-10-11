# GCS absolute in-flight deadline qualification blocker

Status: unresolved release blocker. `gcs-view` remains unadvertised. This is not
permission to replace the signed stock HTTPFS transport, relax the deadline
contract or publish a capability.

## Executable finding

Revision 3's engine `HTTPUtil::Request` hooks do not intercept the official
signed HTTPFS extension's private implementations. Revision 4 binds pinned
HTTPFSParams to a virtual SendRequest delegate, fixing that request-entry seam.
The delegate checks exact admitted URL/read method/deadline, resets stock clients,
uses remaining-lease timeout fields and checks GET content delivery.

Signed stock curl has connect/low-speed timeouts but no hard transfer timeout.
Signed stock httplib admits bounded stalled HEAD and trickling GET tests, but
continuously trickling response headers bypass the per-read timeout. With a
250 ms lease, the synthetic header case returned after **3792 ms**. Late results
are rejected, but the request/owner remains in flight. This fails bounded stop/
join acceptance; it must not be reclassified as a harmless performance limitation.

Reproduce offline with signed HTTPFS and the optional native proof target:

```sh
python3 crates/grv-adapter-duckdb/native/test_gcs_stock_request.py \
  --proof /protected/build/grv_gcs_stock_request_proof \
  --httpfs /protected/extensions/httpfs.duckdb_extension
```

The default includes the failing header gate. The explicit
`--skip-known-header-blocker` option runs only supplemental partial proofs and
prints the omitted gate. No cloud credentials are used: loopback synthetic bearer,
direct read, redirects, stalled HEAD, trickling GET, expired/unlisted authority
and header trickle. Zero redirect-destination and inactive-authority requests
are required.

## Independent review and fixes

The review found a potential shared HTTPFSParams timeout/retries race under
parallel reads, plus temporary-secret retention after partial setup failure.
Source fixes construct private per-request params and concrete GET/HEAD request
copies, dispose clients on all exits, wipe policy-owned bearer copies, and attempt
temporary-secret revocation before restoring the failed owner guard. Partial
signed-stock tests pass. Full production rebuild/live qualification of these
changes is pending; the ongoing parent-held test's native library is immutable.

The exact pinned upstream params/header and Clone path preserve the policy
binding; supported-target ABI/destruction/transport qualification remains open.
Stock-owned credential/SQL copies cannot be assumed zeroized simply because the
policy-owned buffers are wiped.

## Why existing cancellation is insufficient

DuckDB interrupt does not cancel the synchronous stock HTTP client. Pull/build
workers join their native owner threads; a blocked request prevents that join.
SDK stopped acknowledgement correctly follows successful worker stop/join, so
it cannot claim stopped evidence for this blocked owner.

Adapter-host IPC deadlines are absolute during a read/write, but sending frames
resets them. Cancel timeout alone does not terminate the child. Drop eventually
signals the process group TERM/KILL and waits, without proving stopped descendants
or an immutable operation-wide GCS deadline. ExternalInvocation's retained
workspace descriptor, non-reaping leader observation, checked ownership recovery
and stopped-writer evidence are the stronger precedent.

## Proposed next design decision

Keep signed stock transport, but isolate each GCS native owner in an exclusively
owned executable process with bounded typed IPC and one monotonic operation
budget. Trigger cooperative cancellation and process-group escalation within
that budget; preserve ownership until exit/native-lock reacquisition proves the
fence. Failure to prove the fence leaves the outcome unresolved and prohibits
workspace mutation; no thread detach, asynchronous thread kill or ownership theft.

For same-process library use, NativeEngine must not claim a hard fence without
that process boundary. BuildWorker's arbitrary closures need explicit commands
if moved across IPC. Consider disposable child-owned scan staging followed by
normal transactional import; prove this preserves fixed inputs, snapshot and
receipt semantics before adopting it. Killing an owner of the real workspace
transaction is not rollback evidence: reopen/recover and resolve immutable
attempt/invocation receipts rather than replaying effects blindly.

Ordinary independently configured DuckDB consumers retain their own authority
and expiry limits; GRV cannot promise to stop arbitrary external readers.

This is an architectural/contract decision, not a small timeout adjustment.
Review the exact guarantee: deadline-triggered escalation and refusal of late
results can be implemented, but OS termination/reaping at an exact instant is
not universally guaranteed. Preserve fail-closed unresolved outcomes rather
than silently relaxing acceptance. Qualify DNS, TCP/TLS, status/headers, body,
chunk framing/trailers, cancellation, descendants and commit/recovery races.
