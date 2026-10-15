# GCS caller metadata pre-copy admission checkpoint

Status: narrow logical admission and reported copy-multiplier fixes implemented
and tested. Task48 is complete for this slice; whole memory acceptance is open. **Not total memory qualification**. No global allocator quota or new
engine/RSS guarantee. Native library revision6 unchanged.

## Narrow implementation

`src/gcs_metadata_budget.rs` walks borrowed standard GRV serde documents through
`serde_json::to_writer` into a counting writer. No JSON Value, serialized Vec,
canonical string or typed clone is created by the counting writer. Serialized
string/escape content costs one logical byte per emitted byte; nonstring
structure costs256bytes per byte, conservatively reserving small-item/container
cost rather than using wire bytes alone. Depth≤64, aggregate reserve≤8MiB.
These are logical accounting weights, not measured target allocator capacities.
Already-owned arbitrary caller capacities are not forcibly reduced or included
in a claimed total-library/RSS maximum.

GCS callsites charge before newly owned copies:

- Build discovery before validation, transaction and request/discovery clones.
- External preparation before reservation/session clone/import effects, including
  repeated input/base contract/file logical content. Runtime preparation admits
  before credential setup; embedded Store calls perform their own admission.
- Build execute before stored session comparison/dispatch.
- Pull fixed-request before declaration clone; conversion before per-file native
  plan clones counts repeated contract/schema/partition/verified-file copies.
- Native PullPlan validation rejects amplified caller metadata, not only generated
  SQL. Existing local/S3 calls do not run this GCS accounting.

Borrowed metadata admission must not be confused with the decoder8MiB budget:
these separately address caller-owned/new-copy documents versus native encoded
2MiB responses decoded into row/cell/string storage. Caller budgets do not fit
inside the same32MiB transfer reserve by assertion. Maps/queues, wire parsing,
SDK output conversion and source-library retained buffers remain separate gaps.

## Validation and limitations

Tests reject many empty nodes (40,000 strings), oversized string content, excessive
nesting and accumulated repeated copies without constructing an intermediate
serialization buffer. Escaped backslash/quote/braces and multibyte strings admit
normally. Store regression refuses amplified discovery before reservation or
build-session effects and still permits source-free SELECT42.

Latest native97passed/12ignored and strict native/conformance Clippy/fmt/diff
pass. Live pair `~/grv-shipping-gcs-ham6gzfh` passed both guarded pull and nonempty
external self-preparation, owned one generation removed, inventoryempty/bucket404.
That live run preceded the added repeated build-input/base clone accounting.
Final-stage live pair `heaabrj6` subsequently passed both tests with owned one
generation removed/empty inventory/bucket404. Stale-registry CLI refusal also
reran passing. No final-stage held pass is invented.

Independent review `c776d1c0-2b7e-4df` confirmed real pre-copy logical admission
but found three incomplete copy multipliers: matching base-file Vec plus remote
SourceFile/partition copy; returned/stored-session base contracts outside discovery;
and selected-partition JSON repeating tuples/table names. Added explicit charges
for both extra base-file copies, partition/schema content, self-output seeding,
both session base-contract copies and selected-partition/table-name duplication.
Focused follow-up confirmed file/partition and selected-partition charges but
found one residual discovery-derived copy: existing reservation and its local
clone coexist with preparation, returned session and stored session. There are
five instances, not four. Added `include_copies(discovery,4)` beyond the original
preparation's charge, with an explicit five-instance comment. A regression proves
four1.7MiB discoveries admit while the fifth refuses. Parent checked the retained
reservation/local/session lifetimes against that accounting; no universal approval.

Updated99native/12ignored and strict native/conformance Clippy pass. Latest live
pair `ywo05jpl` passes both tests/owned one generation removed/inventoryempty404.
Earlier `9qy1vzpv` remains evidence for its pre-fifth-copy stage. Non-native strict
Clippy also passes; no warning is hidden by the native feature.
New regression admits a3MiB original preparation but refuses its session/stored-
session copy aggregate before metadata/import effects, preserving ROLLBACK and
SELECT42. Initial regression test hit RequestMismatch at discovery due macOS
/tmp vs /private/tmp identity; fixture corrected to crate-local temp directory.
That initial development failure is not a passing admission test.

Parent inspected pinned serde_json1.0.151 `to_writer`, `serialize_str`, escape
fragment writing: these paths write borrowed string fragments directly, with no
full encoded String/Vec. This is not a statement about arbitrary custom Serialize
implementations or every serializer branch/dependency/derived-record allocation.

Do not claim complete coverage of every newly owned copy, engine storage,
arbitrary serializers,
allocator overhead, runtime queues/state or process RSS. `document_ids` and
runtime connection/session cardinality, wire decoded/canonical clones and SDK
pre-length serialized frame construction remain open under task37. Embedded
callers cannot assume a host-wide allocator cap. Original incompatible-history
refusal and offline replay ordering must remain; no silent repair/reset/drop of
malformed evidence.

Actual held lifecycle `dw_ypv61` passed before this Rust stage with pre/post native,
fixture, CLI, test and signed-extension equality/owned404. It does not automatically
qualify later admission changes. All current cloud runs have ended. No commit,
publication, credential/IAM change or capability advertisement. User requested
pause after the memory scope is genuinely complete; this narrow checkpoint alone
is insufficient to trigger that completion.
