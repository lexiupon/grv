# GCS incompatible peer and durable record refusal checkpoint

Status: package24 scoped acceptance completed after parent review (2026-10-13).
The [history refusal checkpoint](gcs-peer-history-refusal-checkpoint.md) records
the current CLI/native matrix and reviewed evidence. Earlier counts below are
historical stages, not current totals. No wire or binding version bump,
migration, reset or dual-version negotiation was added.

## Located contracts

- Host bootstrap compares installed manifest name/package/interface offer set/
  binding-schema version to the child's Identified message before Ready. No
  common interface is a separate later check; mismatch tests must not confuse
  its UnsupportedCapability branch with AdapterFailure.
- BuildStore.fixed requires current package identity, interface1 and binding
  schema1 plus exact root/workspace/database coordinate. Stored build records
  deserialize into closed typed records and validate before session use.
- PullStore binding/resolve require metadata header(version1,ownership1), closed
  PullReceipt parsing and exact recorded request/package identity. Unknown
  materialization values cannot become local or another remote access mode.
- Wire inline frames and resolved chunked documents use the same typed access
  decoding. Unknown access values are rejected before prepare dispatch.

## Executable passing negatives

1. `grv-adapter-wire/tests/protocol.rs` unknown remote access spellings in inline
   and chunked-document prepare-build payloads fail Codec/typed expansion before
   dispatch. A valid known GCS value still decodes; no alternate mode fallback.
2. `grv-adapter-api/tests/build.rs` known local/S3/GCS materializations decode,
   unknown historical/alias values fail. Parser proof only, not CLI sideeffects.
3. Native pull tests persist a genuine empty GCS view receipt without auth, then
   inject unknown receipt mode/field, header0/ownership0, recorded package or
   binding-schema mismatch. Source-free reopen refuses. Database and ownership
   witness bytes remain exactly unchanged; no reader config is created.
4. Native build tests persist a managed build session on a GCS root without
   auth/helper, then inject unknown state/field or mismatched request package/
   schema. Session reopen refuses and database/witness bytes remain unchanged.
5. Host conformance uses the actual existing fixture and authentication marker.
   Manifest[1,2] vs handshake[1], binding schema2 vs1, and manifest[2] vs[1]
   fail before a Session is returned. Package directory/file snapshot unchanged,
   authentication marker absent. The last case is identity mismatch, not proof
   of matching offers with no common interface.

6. A stale recorded registry options schema limited to local/S3 explicitly
   refuses GCS; its enum stays unchanged. No automatic schema extension or
   materialization reinterpretation. This is host registry validation, not a
   complete mismatched-capability CLI execution.

Serial validation: native adapter101passed/16ignored (includes subsequent fence
slice tests); API19 tests; wire17 tests; full host conformance21passed plus four
host library tests. Subsequent host conformance22passed includes actual matching
manifest/handshake interface[2] with no common host offer; public
UnsupportedCapability and unchanged package snapshot beforeReady/dispatch.
Latest native111passed/21ignored after memory/fence slices. Affected strict
API/wire/native/host and conformance Clippy passed. No cloud resources or credentials
were used in these negatives.

## Residual gates

New end-to-end CLI slice in `grv-conformance/tests/peer_refusal_cli.rs` passes:
a dedicated fixture agrees on package/interface/schema identity and advertises
GCS capability while options enum remains local/S3. Offline locate/bind/receipt
lookup are supported without target reads. CLI rejects GCS with
INVALID_DECLARATION before auth/preparation/apply/command hooks; entire fixture,
root, target canary and state directory bytes/entries remain unchanged. Explicit
host authentication control fires the forbidden-hook marker, proving the canary
is live. This fixture executes no cloud/native database operations; parent
separate validation target test and native-feature strict conformance Clippy pass.

The additional prior-binding/history/recovery slices are now covered by the
reviewed history checkpoint: bound identity/workspace, source-free known GCS
receipt replay despite stale current options, terminal-journal missing/unknown
receipt refusal, mismatched fixed identities, malformed history and native
stored receipt/binding faults. Auth/effect and startup controls are positive-tested.
CLI offline locate/bind/receiptlookup
intentionally precede current defaults/options validation to preserve source-free
historical receipt replay. Refusal-before-authentication/mutation is required,
not refusal before those offline read-only operations; do not reorder them. These slices do not establish every
incompatible peer combination or final security sign-off. Keep capability gated.
