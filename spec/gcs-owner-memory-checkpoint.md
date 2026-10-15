# GCS owner memory-bound checkpoint

Status: reduced bounded buffers and passing allocation-boundary slices; **not a
universal pair-wide high-water/RSS proof**. Task37 remains open. Native DuckDB
memory/staging quotas and caller document limits remain separate budgets.

## Superseding execution decision

[Approved in-process direction](gcs-in-process-execution-decision.md) removes the
additional GCS native-helper requirement and declines production adoption of the
helper-only allocator policy. Current native revision6 code is in-process; see
[gcs-in-process-transition-checkpoint.md](gcs-in-process-transition-checkpoint.md).
Revision6 live pair `296qgt4m` passes/owned cleanup404; actual prebuilt held
lifecycle `dw_ypv61` passed1test/1962.69s with five unchanged artifacts/owned404.
Neither is inferred from prior helper passes. Later generated-SQL pre-copy
admission is a separate Rust stage; whole caller/queue/source accounting remains
open in [memory lifetimes checkpoint](gcs-in-process-memory-lifetimes-checkpoint.md).
Historical32MiB pair-wide candidate accounting must be reassessed for the simpler
path. Independently bounded transfer/metadata buffers remain required; no hard
total-engine/RSS cap is claimed. These evidence records are not erased or
reclassified as passing memory acceptance. Pause after memory scope completes.

## Problem and changes

The earlier8MiB JSON response frame stored numeric byte arrays: simultaneous
child send frame, parent receive frame, child2MiB raw output, parent decoded2MiB
reply and query copies exceeded24MiB before serde scratch/decoded metadata.
Raw2MiB metadata could additionally amplify to over10MiB of nested row/cell
headers for many empty cells. A32MiB admission reservation did not prove safety.

The private helper and parent now use:

- JSON commands capped at **2MiB+64KiB**. Aggregate command strings capped at
  **2MiB**, not2MiB per field.4096 location count enforced during serde decoding,
  including empty strings. Escape expansion may cause refusal before effects.
- Raw responses with4-byte big-endian length, one tag (Failed/Done/false/true/
  Bytes), and at most **2MiB** payload. Send borrows child bytes, receive performs
  one exact-size allocation returned directly. No JSON numeric-array staging or
  decoding allocation; malformed/oversized/partial replies refuse and fence.
- Closed Open field `response_encoding=raw-tagged-metadata`: old helper refuses
  the unfamiliar field before database open; new helper rejects missing field.
  No dual decoding, automatic negotiation or public contract version bump.
- Isolated GCS metadata decoding first validates all tags/UTF8/lengths and
  accounts row-vector/cell/string logical owned storage within **8MiB** before
  allocation. Fallible exact reservations replace infallible metadata reserves.
  Ordinary local/S3 decoded limits remain unchanged.

## Executable slices

- Exact2MiB response test pauses sender before destroying its allocation; parent
  and child payload capacities simultaneously total4MiB. No staging copy.
- Raw response tags, malformed lengths/tags/booleans, fragmentation, partial EOF,
  blocked read/write, cancellation and absolute deadlines exercised.
- Worst-case escaped logical2MiB SQL refuses bounded frame without capacity
  growth. Command aggregate overflow refuses before sending/FFI.
- Metadata empty-cell amplification refused. Last admitted nested-allocation
  boundary stays within8MiB measured Vec capacities; next row refused. Local
  decoder control remains accepted, preserving local/S3 behavior.
- Rebuilt helper serially; native109passed/18ignored after additional decoder
  and concurrency/setup tests; five recovery primitives and four isolated
  descriptor/self-base/cancellation integration slices passed. Synthetic two-
  owner cancellation/fresh reopen and failed-extension-LOAD sanitized error/
  rollback/absent DB token tests also passed explicitly. No native library rebuild.

## Accounting residual

Protocol section5.5 scopes source/scratch to transfer buffering, not totalRSS;
parent metadata and engine caches require separately bounded budgets. Caller
already-owned documents cannot simply disappear from whole-invocation accounting
by relabeling them: their maxima and duplication still need independent bounds.
A same-process caller can retain an arbitrary-capacity String, so no universal
32MiB total-library-call heap claim is valid. Only newly admitted bridge and
transfer allocations can be qualified against that reservation.

The raw-response phase uses at most child2MiB output + parent2MiB received bytes

- parent8MiB decoded storage, **12MiB** logical buffers, before retained caller
  query, native source/query copies, other documents and allocator overhead. The
  command phase has parent and child up to2.0625MiB frame buffers plus originals,
  serde decoded strings/escape scratch, argument CString/remapped SQL and vector
  headers. Not every buffer is live in every phase: JSON receive buffers drop
  before native dispatch, but sender buffers may overlap child dispatch.

Independent review found actual rejection-path gaps: copying oversized borrowed
SQL before aggregate validation and serde internally-tagged Content buffering
many empty strings before field-level count checks. Borrowed-argument validation
now precedes all isolated SQL/relation/staging copies; remoteURLs are accumulated
under a budget without redundant CString copies. Allocation-free JSON structural
preflight (bounded arrays/maps/depth/node/string counts) now runs BEFORE typed
serde decoding, including unknown nested content.100000 empty locations within
frame size are refused at preflight; valid4096-location aggregate boundary
passes. These newer guards were built/tested in a separate validation target;
the full-held rerun retained its earlier raw-response helper artifact and
passed1839.11s at `~/grv-held-gcs-9zqrg2qn`, emptyversions/bucket404. It does not
silently cover these latest guards. Post-run helper/library hashes verified;
pre-run hashes absent, so no independent before/after equality claim.
No shared native library rebuild.
Native schema decoding also validates declared count against exact6-byte type
records before Vec reservation, including maliciousu32MAX count/trailing bytes.
Latest native112passed/21ignored; seven explicitly executed recovery/setup budget
slices and four isolated descriptor/self-base slices passed. Supported allocation
capacities and caller documents remain part of the outstanding acceptance.
Descriptor remapping now builds one exact-reserved CString byte vector rather
than removal/replacementString plus CString copies. Latest guarded helper in
separate validation target passed authorized live pull/self-preparation at
`~/grv-shipping-gcs-_791jmu6`; owned generation/bucket emptyversions404. The
completed main-target held rerun used its unchanged earlier artifact; later
source guards are not inferred covered by that lifecycle pass.

Full pair-wide caller/document lifetimes, serde string capacity growth, setup/
configuration and supported-target allocator costs require independent review
and high-water qualification.32MiB stays a candidate admission threshold until
these are accounted. Logical capacity checks are not malloc overhead/RSS bounds.
Native C++ configured-GCS metadata now streams fetched chunks rather than
retaining the entire SELECT result, and checks VARCHAR length before copying.
A64KiB stock collector softthreshold and4MiB adoptedchunk refusal reduce
retention but do not prevent upstream largechunk/expression allocation. Cleanup
requires positive QueryEnd witness and interruption/fetch on refused partial
streams; Close alone was found insufficient. See
[gcs-native-metadata-streaming-checkpoint.md](gcs-native-metadata-streaming-checkpoint.md).
Native112passed/22ignored; live pull/self-preparation and ZSTD65536-row scanner
passed/cleaned404. Whole pair-wide resource proof remains open, and prior held
pass predates this library change. Never rebuild during held execution.

## Executable upstream allocation evidence

Standalone `native/metadata_allocation_proof.cpp` now tracks allocations forwarded
through stock DuckDB default allocator functions (public allocator reference,
only a single-owner proof process). All oversized trial replies are refused.
A40,000,000-byte repeat at the trusted metadata seam nevertheless requests one
67,108,864-byte allocation and reaches134,504,464 tracked simultaneous bytes;
6,000,000 bytes reaches17,063,952. Billion-row bounded BIGINT prefix peaks516,096.
A40,000,000-byte malformed stored receipt in metadata-authorized `_grv.pull_attempts`
with LIMIT2 reaches67,411,968 and also requests67,108,864 before bounded refusal.
These figures exclude STL/other allocators/headers/RSS and are lower-bound
counterexamples, not complete memory maxima. This demonstrates why post-fetch
chunk refusal cannot be presented as preallocation bounding. Proof SQL is not
claimed directly reachable from a shipping user declaration. The malformed
existing durable-row case is a real metadata-read shape, but not complete GRV
reopen/discovery-route testing. Legitimate writer limits alone cannot bound
corrupted existing data; production closed query/source/durable maxima still
need a separate reachability/accounting pass.
At that stage no shipping allocator replacement or production enforcement was
introduced. Follow-up Rust durable-copy admission is detailed below.
See [streaming checkpoint](gcs-native-metadata-streaming-checkpoint.md) and
`release-validation/gcs-native-resource-fault-checkpoint.json` for12ctest results
and actual WALcommit/invalidated-database rollback-error slices.

## Follow-up: separate result-copy and storage costs

Expanded standalone instrumentation covers BOTH default and DBConfig allocator
paths. Fresh-reopen scalar byte-length and CASE candidates still request
40,001,536 bytes and reach about81MiB tracked storage when reading a40million
stored value. Previous default-only figures must not be presented as complete
engine allocation accounting. A16MiB engine limit refuses this storage request
with largest admitted allocation262,144; the same setting does NOT cap a
40million string expression (largest67,108,864, trackedpeak134,815,760). FullSTL/
otherallocators/headers/RSS remain outside either measurement.

GCS isolated `durable_metadata_query` now uses one CASE/error projection for
selected receipt/binding/session rows before result copying. NULL/duplicates/
absence and original row-selection are preserved, oversized rows are errors
rather than filtered away, complete VARCHAR row bytes counted. Local/S3 paths
unchanged. This reduces result retention but does not solve upstream storage
allocation or expression limits. Explicit tests preserve exact UTF8 boundary,
callerOutcomeUnknown vsNotCommitted, source-free fence/reopen and DB/witness
bytes. Native114pass24ignored,12ctest,7recovery2durableslices/strictClippy; latest
livepair `~/grv-shipping-gcs-73ki_c3w` passed/cleanup404. A validator regression
failed live first (`s2gqhiii`,retainedcleaned404), fixed legitimate sha256digits.

Next: source-expression/generated-query reachability, engine allocation budget
coverage and whole bridge/caller-document accounting. User requests pause when
memory acceptance is actually completed; task37 is NOT complete yet. No universal
32MiB claim, no automatic migration or resource expansion.

Historical live/held evidence predates this private IPC change; artifact-only
recovery reruns do not silently requalify that full live lifecycle.
