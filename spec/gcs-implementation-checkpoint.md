# GCS implementation checkpoint

Status: implementation in progress; not a release qualification or capability
advertisement. No push, tag or publication is recorded by this checkpoint.

## Implemented

- Explicit protected gcloud reader selectors; bounded zeroizing token/expiry
  acquisition, 300-second monotonic execution lease and 120-second expiry margin.
- Coordinated GCS file/materialization records, canonical `gs://` provenance,
  fixed-host HTTPS derivative, full-object preparation and authenticated bucket
  metadata admission refusing Requester Pays. JSON API omitted default-false
  billing was confirmed live; null, malformed and explicit true remain rejected.
- Signed stock HTTPFS temporary bearer setup; no HMAC keys or policy exceptions.
- Exact read-only scanner/file admission and handle epochs, guarded pulls,
  source-free replay, empty refresh, held external-build dispatch.
- Guard revision 3 pinned engine request hook: exact URL/read/deadline checks,
  remaining-lease timeout and cached-client disposal before stock client creation.
  Signed stock clients/transport remain unchanged. Synthetic watchdog rollback.

## Evidence and limitations

- Earlier native shipping-path owned one-row GCS pull passed, including direct,
  nested-unlisted/glob denial, source-free reopen replay and empty refresh.
  Bucket/object generation removed; empty all-version inventory and HTTP404
  confirmed. Private evidence: `~/grv-shipping-gcs-27mbgze_`.
- After user reauthentication, revision-3 guarded live pull passed with explicit
  `jiacao@on.sinch.com` / `entit-finance-prod`, without fallback. Nonempty GCS
  external self-input preparation also passed: one-row output seeding and private
  remote view reads, unheld-request rejection, source-free reopen, abort and
  repeated terminal cleanup, plus injected precommit rollback leaving no durable
  session. This test supplies confirmed-hold evidence directly; it does not
  establish actual parent revision-hold creation/release or external execution/
  publication. Private evidence: `~/grv-shipping-gcs-mcwbxcbc`; owned generation
  deleted, all-version inventory empty and bucket HTTP404 verified.
- Revision 3 native proofs: **9/9 passed**, including expiry between requests,
  exact wrong-URL/write rejection and unchanged ordinary-stock control.
- Native adapter regression: **76 passed**, credential/fixture-dependent tests
  ignored; synthetic-token watchdog interruption/rollback passed separately.
- API/storage/core foundation suites were passed during implementation. Latest
  focused core build **5 passed**, cloud GCS **11 passed / 3 ignored**; later
  all-core reruns were interrupted by execution timeouts, not claimed complete.
- Strict workspace/native all-target Clippy passed before final documentation.

## Remaining release gates

Actual revision-3 live cloud correctness/pushdown matrix; protected S3 shipping
pushdown measurements; held GCS build lifecycle tests; in-flight timeout and
cancel/commit races; faults/resources/overwrite behavior; ordinary DuckDB
consumption; supported-target/MSRV/package/install qualification. Capability
advertisement and release remain blocked. No implicit publication approval.
