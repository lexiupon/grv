# GRV Adapter Process Protocol v1

|         |            |
|---------|------------|
| Status  | draft      |
| Version | 1          |
| Date    | 2026-10-05 |
| Extends | GRV Client v1 |

## Purpose and relationship to the authoring specification

[GRV Client v1](grv-client-v1.md) defines language-neutral adapter obligations;
[the execution companion](grv-client-v1-execution.md) defines their ordering,
identity, receipt, locking, and recovery requirements. This document mechanizes
those obligations between the CLI (the **parent**) and an installed **adapter
process**. Both companions remain normative. [GRV v2](grv-storage-v2.md) alone defines
the storage layout; this protocol creates no new GRV object or commit marker.

The parent owns GRV backend operations, holds, runs, claims, leases, capture
writing, and publication. The adapter owns connection/authentication, source
jobs, destination execution/receipts, engine locks, private engine sessions,
and stopped-writer attestation. The core does not interpret adapter-specific
SQL or catalogs.

**Process adapters and built-ins.** Every installed adapter discovered under
§1 runs as a supervised child process and speaks this protocol. A v1 built-in
adapter (DuckDB, Salesforce) MAY instead be linked into the CLI behind the
Client's logical lifecycle interface. A linked built-in MUST:

- register the same name, package, interface, and binding-schema versions,
  capabilities, served schemas, and commands that it would serve in
  `identified` (§3);
- keep the same authority split as a process adapter: it never reads or writes
  GRV coordination objects, never receives owner, lease, or claim tokens, and
  never performs GRV mutations; and
- pass the Client's and execution companion's logical conformance suites and
  the operation-level scenarios of §9.

Channel, framing, and data-plane requirements (§§2, 5, and the corresponding
§9 scenarios) apply only to process adapters. There is no daemon mode. A
command that needs process-adapter work spawns one process; recovery may spawn
a replacement **after** fencing the old work. A recorded terminal outcome with
no pending hook bypasses source binding, authentication, and acquisition, and
may bypass adapter start entirely. Process-local or in-memory handles are never
recovery evidence, whether the adapter is linked or spawned.

**Scope.** V1 process adapters require a Unix-domain stream `socketpair` on
Linux or macOS. No descriptor passing or shared memory is used. A missing
required primitive is refused before transfer mutation with
`UNSUPPORTED_CAPABILITY`. Windows is deferred. Capitalized requirement words
have the meanings in RFC 2119 and RFC 8174.

### Trust boundary

Adapters are operator-installed, semi-trusted executable code. The process
split provides supervision and separates protocol authority; it is **not an OS
sandbox** and does not confine a same-user process's filesystem access.

| Crosses the channel | Never crosses the channel |
|---------------------|---------------------------|
| effective declarations, contracts, non-secret source/destination mappings | GRV backend credentials or signed credential-bearing URLs |
| canonical root **identity**, workspace/attempt/run IDs and request digests | owner, lease, and claim tokens |
| verified local staging file metadata; exact S3 data-file URIs for S3-view readers | GRV controls, protected contexts containing tokens, or authority to mutate GRV |
| adapter results, durable checkpoint references, batch payloads | authentication credentials, private keys, or session cookies |

Canonical root coordinates are non-secret identity data required for workspace
binding. Receiving them does not authorize GRV access. Adapters MUST NOT read
or write GRV coordination objects. The sole direct GRV data-reader exception is
the companion's S3-view path: exact parent-verified data-file URIs may be read
using the adapter's own separately configured read-only credentials. Local
inputs are staged outside GRV. No adapter performs GRV mutations.

Adapter authentication stores, source-job/checkpoint stores, and engine databases
are adapter-owned consumer state outside GRV.
The parent holds its own durable request, capture, context, and outcome state.
Both sides make required evidence atomic and durable before advancing a phase.

Frames and metadata documents MUST NOT carry authentication material. Data rows
are operator-selected content, not authentication transport; scanning cannot
prove that arbitrary rows contain no sensitive values. The harness checks known
credential canaries in control traffic, results, stderr, and inherited state.

## 1. Installation and discovery

The user adapters root is `$XDG_CONFIG_HOME/grv/adapters`, defaulting to
`$HOME/.config/grv/adapters`. Each canonical adapter name has one directory
containing `adapter.toml`. `GRV_ADAPTERS_DIR` replaces the **entire** search path:
when set, no user or system fallback is searched. There are no per-adapter
environment overrides. Otherwise an installation may configure one explicit
system root at lower search priority; the user entry shadows the system entry.
The effective roots and winning manifest are shown by `adapter list`.

```toml
name = "salesforce"
version = "1.4.0"
interface_versions = [1]
binding_schema_version = 1
entrypoint = "/opt/sf-adapter/sf-adapter"
entrypoint_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
```

The manifest is closed and all fields except `entrypoint_sha256` are required.
`name` matches `^[a-z0-9][a-z0-9_-]{0,63}$` and its directory name.
`version` is a nonempty package-version string; versions compare by exact
string equality, not ordering. `interface_versions` is a nonempty unique array
of positive safe integers. `binding_schema_version` is one positive safe
integer. `entrypoint` is absolute or resolved relative to the manifest directory.

Rules:

- The entrypoint may be anywhere. Its location is installation information,
  never a declaration/attempt identity input.
- Resolve symlinks and check the actual objects. User-root manifest directories
  and manifests are owned by the invoking user; system-root ones are root-owned.
  Neither they nor their resolved ancestor chain may be group/world-writable;
  ancestors may be owned by root or the invoking user. The executable and its
  resolved ancestor chain satisfy the same trusted-owner/non-writable rule.
  Reject non-regular manifests and non-regular/non-executable entrypoints.
- A configured digest is lowercase SHA-256. Hash an opened executable and
  execute that verified object where the platform supports it. Otherwise
  recheck its identity immediately before pathname execution and refuse changes.
  A pathname recheck is tamper detection, not a race-free guarantee. A script's
  digest does not cover its interpreter, libraries, or other runtime dependencies.
- Manifest name, package version, binding version, and interface-version set
  MUST agree with `identified`. A mismatch is `ADAPTER_FAILURE` before binding.
- Transfers do not install or download. `grv adapter install <path|tarball>`
  is an out-of-band installation operation that applies these checks and
  smoke-tests the handshake. `adapter list` is manifest-only; capabilities and
  namespaced commands use the handshake.

## 2. Transport, grammar, and channel state

### 2.1 Spawn and supervision

The parent creates `socketpair(AF_UNIX, SOCK_STREAM)` and gives the adapter end
to the child as fd 3. All other inherited descriptors are closed, except
stderr's capture pipe and explicitly supervised engine-lock descriptors inside
the adapter's own descendant tree (§6). Child stdin/stdout are attached to
`/dev/null`; closing fd 0/1 outright must not let later opens accidentally
become stdio. No protocol traffic uses stdio.

The child environment is built from an allowlist: `PATH`, `HOME`, the XDG
config/state/cache variables, `TMPDIR`, locale variables, and `TZ`. Values
containing credentials are excluded. GRV root/state/backend variables, cloud
credential variables, and backend credential descriptors are not inherited.
Adapters load their credentials from their own stores. The working directory
is the resolved manifest directory, never the GRV root or parent staging area.

The parent drains and discards raw stderr by default; it never tees raw child
bytes. An explicitly enabled diagnostic capture uses a bounded protected
consumer-state log, never an automatically rendered result. Public diagnostics
use sanitized `error` frames. Stderr draining is independent of channel
processing and renewal; excess output is discarded with a truncation flag,
not allowed to block work. Diagnostic capture can retain accidentally emitted
secrets and is not part of the claimed authentication separation.
No command requires an interactive stdio prompt; login uses the adapter's own
browser/device flow and returns sanitized protocol results.

Spawn the adapter in a separate process group. It MUST NOT detach work from
supervision. The parent signals the group and tracks exit; the adapter supervises
and reaps its descendants, keeps lock ownership with every database user, and
marks inherited channel fd 3 close-on-exec for engine children. Channel EOF
means parent loss: stop/await local work, preserve existing durable evidence,
and close handles. Remote work requires its own durable identity and fencing;
process-group termination does not prove a remote job stopped.

### 2.2 JSON framing and types

A frame is one compact UTF-8 JSON object followed by exactly one LF. Embedded
newlines are escaped. Reject BOMs, blank lines, duplicate keys, trailing JSON
values, unknown members, non-finite numbers, and invalid UTF-8. Frame limits
count JSON bytes plus LF: 1 MiB normally, 16 MiB for `identified`. Oversized
metadata uses §2.4, never an oversized frame. Truncated frames are failures.
The only exception to "frames are JSON lines" is `batch`: its LF is followed
immediately by a binary payload of exactly its declared `size` (§2.5). The
next frame begins after the last payload byte.

The frame tables in §§3–4 and the control tables in §§2.4 and 5 are normative
closed-object grammars. Every listed field is required unless suffixed `?`;
the suffix is notation, not part of its name. All frames have `msg`. Operation
requests/responses/events also have `req`; handshake and document controls do
not. There is **no per-frame attempt member**: `hello.attempt` scopes the channel.

Common types:

| Type | Wire representation |
|------|---------------------|
| `J` | JSON-compatible value; objects closed by the named common or served schema |
| `Doc<T>` | exactly `{"inline": T}` or `{"document_id": UUID}`; §2.4 verifies the latter |
| `SafeInt` | JSON integer `0..2^53-1`, parsed without rounding |
| `Req` | JSON integer `1..2^53-1` |
| `U64` | canonical decimal string `0..2^63-1`; no sign or leading zeros |
| `Revision` | `U64`; requested selector is `"latest"` or `Revision` |
| `UUID` | canonical lowercase v4 or v7 UUID |
| `RunId` | canonical uppercase ULID per GRV §6 |
| `Digest` | 64 lowercase hexadecimal SHA-256 characters |
| `Time` | validated RFC 3339 UTC timestamp ending in `Z` |
| `Name` | canonical GRV identifier |
| `Handle` | nonempty, non-secret, channel-local opaque string |
| `Details(point, mode)` | value validated by the served schema at that point/mode |
| `Mode` | `"extract"`, `"managed_build"`, `"external_build"`, `"pull"`, `"inspect"`, or `"command"` |

Byte lengths, counts, and revisions use decimal strings; slot indices,
sequence numbers, request IDs, schema versions, and metadata chunk indices use
safe JSON integers. Each batch has at most `2^31-1` rows. Sequence exhaustion
fails rather than wrapping. Objects taken from the declaration or existing
completion/result schemas retain those schemas' representation.

### 2.3 Correlation and legal states

Channel states are `handshaking → idle ↔ operating → closing → closed`.
After `ready`, only the parent starts ordinary operations. Request IDs are
strictly increasing and never reused on that channel. At most **one ordinary
request** is outstanding. A request accepts its listed events and exactly one
terminal result or request-scoped `error`. `extract_started` and checkpoints
are nonterminal. Repeating `req` on events/acks is required correlation,
not duplicate request allocation.

A result must match the outstanding request and its frame kind. Unknown
request IDs, wrong-role frames, duplicate terminal results, and unexpected
events are `PROTOCOL_FAILURE`. The bounded exception is cancellation of the
most recently retired request (§4.13); this resolves completion/cancel races.
Batch/checkpoint acknowledgements are controls for the outstanding stream.
No ordinary next request or `close` is sent until terminal evidence and
consumer draining are complete. If cancellation was sent, its matching ACK
must also arrive before another ordinary request or close; otherwise escalate
shutdown without retiring the cancellation correlation. No streaming events
follow a terminal result.

Document controls, cancellation controls, and channel-level errors may occur
while an ordinary request is outstanding. They do not create another operation.
On normal shutdown `close_result` precedes adapter exit 0. A nonzero/abnormal
exit or missing close result is a shutdown error, but never erases an already
established commit or accepted completion.

### 2.4 Bounded metadata documents

Large declarations, SQL, plans, file lists, checkpoints, sessions, and results
use `Doc<T>`. Either side may transmit a document after `ready`, before the
frame that references it. These controls have no `req` and no binary payload:

| Frame | Direction | Fields beyond `msg` |
|-------|-----------|---------------------|
| `document_begin` | either | `document_id: UUID, size: U64, sha256: Digest` |
| `document_chunk` | same sender | `document_id: UUID, index: SafeInt, data: string` |
| `document_end` | same sender | `document_id: UUID` |
| `document_ack` | receiver | `document_id: UUID` |

Bytes are one UTF-8 JSON value, canonicalized with RFC 8785 before transfer.
`data` is canonical RFC 4648 base64, with padding and no whitespace, decoding
to at most 512 KiB; chunks are nonempty, dense from 0, and sum exactly to
`size`. Verify length, SHA-256, strict JSON parsing, and the referencing
type/schema before use. ACK confirms complete byte receipt, not semantic
acceptance or execution. References are legal only after ACK. IDs are unique
per channel; each document is consumed by exactly one subsequent operation
request/result/event, then released. Replays upload new channel-local IDs.

Uploads belong to the active operation or the next operation when idle; they
are not unrelated background traffic. After cancellation, the channel is not
reused for ordinary work except clean close. The adapter stops metadata
transmission before its cancel ACK, which follows all bytes already sent.
Both sides discard partial/unconsumed documents at that barrier. If the transfer
cannot be drained safely, close the transport and use the signal ladder.
Request/channel errors likewise discard their documents and close the channel.

V1 limits each document to 64 MiB, one incomplete upload and one completed
unconsumed document per sender. No row data or secrets may use this mechanism.
The parent checks expanded declaration size before authentication or GRV
mutation; exceeding that supported request limit is `INVALID_DECLARATION`.
If adapter-produced metadata exceeds the limit, return `ADAPTER_FAILURE` with
known effects preserved. Upload timeout/rejection closes the channel with
`PROTOCOL_FAILURE`; affected mutating work is resolved through §7.
The served registry itself must fit `identified`'s 16 MiB bootstrap limit.

### 2.5 Binary batch payloads

Each side has one serialized channel writer. A `batch` frame (§5.1) is written
as its JSON line, its LF, and then exactly `size` raw payload bytes, with
ordinary partial-write handling. The writer emits the frame and its payload
contiguously: no other frame, control, or document chunk may be interleaved
inside a payload. No other frame has a payload.

The receiver parses the `batch` line first, validates its fields (including
`size` against `max_batch_bytes`), and then reads exactly `size` bytes into
its own buffer, reserved in advance up to `max_batch_bytes`. It validates only
that copy (§5.2). It never reads ahead past the payload into the next frame
before the payload is complete. A payload shorter than `size`, EOF inside a
payload, or a `size` outside the permitted range is `PROTOCOL_FAILURE`. The
receiver does not rely on stream write boundaries: frames and payloads may
arrive fragmented or coalesced in any way.

`EPIPE` and EOF are handled without allowing `SIGPIPE` to terminate the
parent. An invalid or truncated frame or payload closes the channel; the
receiver discards any partially read payload.

## 3. Handshake and registration

The fixed bootstrap grammar is `hello → identified → ready`; refusal is a
channel-level `error` followed by channel shutdown, with no `close` request.
Bootstrap itself has a deadline (§7); it is not an ordinary cancellable request.

### 3.1 Frame grammar

| Frame | Sender | Fields beyond `msg` |
|-------|--------|---------------------|
| `hello` | parent | `interface_versions: [Req], core: {name: string, version: string}, attempt: UUID, resources: Resources` |
| `identified` | adapter | `name: Name, package_version: string, interface_versions: [Req], binding_schema_version: Req, capabilities: Capabilities, registry: Registry, commands: [CommandDescriptor]` |
| `ready` | parent | `interface_version: Req, binding_schema_version: Req, resources: Resources` |

`Resources` is closed:
`{slots: Req, max_batch_bytes: U64, max_source_unit_bytes: U64, max_scratch_bytes: U64}`.
Defaults are 8 slots, 67108864 batch bytes, 67108864 source-unit bytes, and
67108864 scratch bytes. Slots are `1..64`; batch size is a multiple of 8 in
`262144..67108864`; source/scratch budgets are positive and no larger than
67108864 bytes each. The offered values are repeated unchanged in `ready`.
Resources are budgets, not a request to allocate all buffers at handshake.
Parent-owned metadata/Parquet/consumer budgets are accounted separately (§5).

For example, this is a complete bootstrap request (no `req` field):

```json
{"msg":"hello","interface_versions":[1],"core":{"name":"grv","version":"0.3.0"},"attempt":"359c6d0f-a9c1-4ae6-b804-0742a5e2b9de","resources":{"slots":8,"max_batch_bytes":"67108864","max_source_unit_bytes":"67108864","max_scratch_bytes":"67108864"}}
```

`Capabilities` is closed and contains:
`push, pull, managed_build, external_build, resumable_extract, inspect_connection,
after_publish` (booleans);
`source_consistency` (`"snapshot"`, `"capture_window"`, or `"none"`);
`pull_write_modes` (unique array of `"replace" | "append"`);
`pull_materializations` (unique array of `"local" | "s3-view"`);
`pull_recovery` (`"transactional" | "journaled" | null`);
and `data_plane` (array of strings).
Unsupported directions have empty corresponding mode arrays/null recovery.
Build capabilities require `push`. Pull requires a recovery contract.
V1 accepts `data_plane` exactly `["stream"]` (§2.5, §5); other values are
refused.

### 3.2 Served schemas, defaults, and commands

`Registry` is `{schema_bundle: J, points: [PointDescriptor]}`.
The bundle is a JSON Schema draft 2020-12 resource with local `$defs`.
Every `$ref` is a JSON Pointer within this resource; external references,
dynamic references, filesystem loads, and network retrieval are prohibited.
Existing adapter schemas with relative references must be bundled at installation/
handshake. The parent validates schema structure with an offline metaschema and
bounds validation time/memory. JSON Schema `default` is an annotation, not a
normalization instruction.

A point descriptor is closed:
`{point: string, mode: string, schema_pointer: string, default_value: J,
has_default: boolean}`. `schema_pointer` addresses the bundle;
`default_value` is null when `has_default` is false. Descriptors are unique by
(point, mode). Modes are `extract, managed_build, external_build, pull,
inspect, command`. Point names are:

- Binding: `connection, options, table_source, target, table_target,
  table_select, column_source, build_input`.
- Results/state: `connection_details, pull_plan, pull_result, push_result,
  inspection_result, session_details, source_identity, source_job`.

Register exactly the implemented points in each advertised mode. `table_source`
has separate extraction, managed SQL, and external mapping schemas; a union
that permits one mode's bindings in another is not sufficient. Closed object
schemas reject unknown properties; scalar selectors retain their scalar shapes.
Missing optional points get their explicit whole-point default, then validation.
Nested/default expansion beyond that is performed by `validate_binding` and
revalidated by the parent. Required missing points fail; absence of a descriptor
never makes an applicable binding an unchecked open object.

Every supported connection mode registers `connection` and `connection_details`.
Extraction registers `options, table_source, column_source, source_identity,
source_job, push_result`; builds register `options, table_source, column_source,
build_input, session_details, push_result`; pull registers `options, target,
table_target, table_select, pull_plan, pull_result`; inspection registers
`inspection_result`. Points such as SQL select or build inputs that are optional
in the authoring envelope still have schemas when their capability implements
them. Empty tuning registers a closed empty schema/default, not an absent point.
Command args/results are supplied by their command descriptor pointers.

`CommandDescriptor` is closed:
`{name: Name, requires_connection: boolean, requires_authentication: boolean,
args_schema_pointer: string, result_schema_pointer: string}`.
Pointers address the same bundle; command names are unique. Authentication
implies connection. `login` requires neither prior handle nor authentication.
The CLI sends non-secret adapter flags as raw string `argv` in the pure
`prepare_command` phase (§4.12). The adapter parses/validates args and returns
any required connection fragment before binding/authentication. The parent
revalidates both args and results. Commands must not accept credential-bearing
argv flags; authentication uses adapter stores/browser flows. No generic core
interpretation of adapter-specific flags is needed.

### 3.3 Negotiation and identity

For new work choose the highest common interface version; for unfinished retry
select the recorded version if both sides still implement it. No intersection
is `UNSUPPORTED_CAPABILITY`. V1 has one served binding-schema version, checked
against the manifest and pinned; it is **not** independently selected from an
unadvertised version set. A changed package/binding version on unfinished work
is `REQUEST_MISMATCH`. Unknown capabilities, incompatible registry points,
or unmet resource/platform requirements are refused before authentication,
engine mutation, or GRV run creation.

The adapter identity is the closed object
`{name: Name, package_version: string, interface_version: Req,
binding_schema_version: Req}`. It and resolved connection identity are hashed
exactly as the companion specifies. Paths, mtimes, channel IDs, and bootstrap
resource budgets are not additional declaration inputs.

Terminal replay is reporting recorded facts, not starting execution with a new
adapter identity. Use the recorded effective defaults, adapter identity,
connection identity, and registered result schema to compare the fixed request
and validate its recorded outcome; retain that normalization/schema evidence
with the receipt/context. Never contact a source merely to reconstruct them.
A current binary may read an old receipt only through its supported durable
evidence reader; inability to read trustworthy history is an explicit error,
not permission to execute again. No result/exit version or public code is added.


## 4. Operation catalog and lifecycle ordering

Every row below defines a parent request and its adapter terminal response.
Both contain `msg` and `req`; tables list their remaining required fields.
Large values have a named `Doc<T>` type. Schema-selected adapter details are
the only extensible portions; the surrounding objects are closed.

### 4.1 Common records

- **TableContract:** `{columns: [{name: string, type: J}], partition_keys: [Name],
  extensions: J, column_ext: J}`. Columns use **GRV v2 §4 logical types**, not
  a new Arrow-type JSON dialect. Supported values are restricted by the Client's
  type set and exact Arrow/engine round-trip rules. Extension objects follow
  their registered GRV contracts; defaults are `{}`. Names/order/types are
  exact; nullability and incidental IPC metadata do not change logical equality.
  Required extension properties are not silently discarded.
- **File:** `{table: Name, partition: J, version: U64,
  schema: {columns: [{name: string, type: J}]},
  access: "local" | "s3-view", location: string, size: U64, sha256: Digest,
  validator: string}`. Version is positive. Partition objects have exactly the
  table layout's keys and canonical values. Local locations are absolute verified
  staging paths; S3-view locations are exact credential-free S3 data-file URIs.
  File schema is that file's logical prefix schema, not a later table baseline.
- **RequestRecord:** `{attempt_id: UUID, root: string, dataset: Name,
  workspace_id: UUID, adapter_identity: AdapterIdentity,
  connection_identity: string, effective_declaration: J,
  declaration_sha256: Digest, request_sha256: Digest, registry: Registry,
  requested_revision: "latest" | Revision}`. The effective declaration follows
  the common and recorded adapter schemas. The registry is the recorded
  normalization/result-validation evidence, not a new execution registration.
- **Receipt:** `{request: RequestRecord, committed_revision: Revision,
  generation_id: UUID, pulled_at: Time, row_counts: [{table: Name, rows: U64}],
  source_contracts: [{table: Name, contract: TableContract}],
  output_contracts: [{table: Name, contract: TableContract}], adapter_result: J}`.
  Lists have unique table names; details validate against the request's recorded
  pull-result schema. This is the original immutable successful receipt, with
  its original output facts. It contains enough evidence to render the companion's
  full pull result without source reads or re-evaluating SQL.
- **BuildIdentity:** `{attempt_id: UUID, root: string, dataset: Name,
  run_id: RunId, workspace_id: UUID, declaration_sha256: Digest,
  adapter_identity: AdapterIdentity, connection_identity: string}`.
- **InputBinding:** `{alias: Name, relation: J, dataset: Name, revision: Revision,
  generation_id: UUID, contract: TableContract,
  materialization: "local" | "s3-view"}`. Relation validates against `build_input`
  for the selected mode. Revision is positive; metadata must establish a complete
  eligible identity materialization in the same bound root.
- **OutputBinding:** `{table: Name, source: J, engine_table: string,
  contract: TableContract}`. Source is the fixed mode-specific table source.
  V1 completion mappings use the existing build-completion schema's qualified-name
  grammar; the adapter supplies and validates its physical mappings.
- **BuildSession:** `{session_id: UUID, identity: BuildIdentity,
  execution: "managed" | "external", base_revision: Revision,
  inputs: [InputBinding], outputs: [OutputBinding], selected_outputs: [Name],
  self_input: boolean, adapter_details: J}`. Details validate at `session_details`
  for the execution mode. The session ID is durable, unlike a connection handle.
  Names/aliases are unique. Selected outputs are exactly the completion-required
  outputs computed by the companion's selection rules, not every prepared table.
- **Outcome:** `{kind: "published" | "no-op" | "aborted",
  revision: Revision | null, operation_id: RunId | null}`.
  Published has both identifiers; no-op has its observed predecessor revision
  and null operation; aborted has neither. This matches the companion.

`AdapterIdentity` is defined in §3.3. Counts are checked against actual consumed
rows. A build completion is exactly the object in
[grv-client-v1-build-completion.schema.json](grv-client-v1-build-completion.schema.json);
no row counts, aliases, or renamed `engine` fields are added to that object.

### 4.2 Validation and phased connection binding

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `validate_binding` | `declaration: Doc<J>, mode: Mode, schema_version: Req` | `validate_result` | `effective_declaration: Doc<J>` |
| `locate_connection` | `connection: Doc<J>, mode: Mode, run_id: RunId \| null` | `connection_located` | `connection: Doc<ConnectionLocator>` |
| `bind_connection` | `locator: Doc<ConnectionLocator>, root: string \| null, expected_identity: string \| null, expected_workspace_id: UUID \| null, mode: Mode` | `bind_result` | `handle: Handle, identity: string \| null, workspace_id: UUID \| null, binding: "bound" \| "uninitialized" \| "not_applicable", details: Doc<J>` |
| `authenticate` | `handle: Handle, expected_identity: string \| null` | `authenticate_result` | `identity: string` |

`ConnectionLocator` is `{canonical_connection: J, identity: string | null,
engine_path: string | null, session_lock_path: string | null}`. The connection
fragment validates against its mode schema. Paths are canonical absolute local
paths where applicable. For a build, the locator uses the supplied run ID for
its persistent session-lock path. Non-engine adapters use null paths and retain
their declared destination/source serialization.

`validate_binding` is pure: no authentication, filesystem I/O, engine access,
or network access. The parent has parsed/expanded common references and checked
capabilities first. The result may fill/normalize adapter fragments only; it
must preserve explicit authoring values and all parent-normalized common fields.
The parent revalidates the whole effective declaration using the served point
schemas, then its **single core normalizer** constructs contracts and plans.
A forged common-field change is `PROTOCOL_FAILURE`.
The requested mode must match the declaration/direction and `schema_version`
must equal the binding version selected by `ready`.

`locate_connection` is the metadata-only first part of binding: canonicalize
paths/aliases using local non-secret metadata, but do not authenticate, open an
engine connection, create bindings/receipts, or start source work. It may return
a null identity where authentication is needed to learn the actual system ID.
This permits the parent to acquire a build session lock before adapter engine
access without interpreting an adapter-specific connection fragment (§6).

`bind_connection` acquires required adapter workspace/destination locks before
opening any engine/evidence store. It checks canonical connection identity,
root binding and trustworthy consumer metadata, and returns a non-secret handle.
For sources whose actual ID is not yet known, the returned identity is null;
it is not replaced by an alias or fabricated locator ID. `authenticate_result`
supplies the resolved stable system identity **before** the parent pins a new
attempt or opens a GRV run. It must match an expected recorded stable identity
on retry; that comparison is deferred to authentication when offline resolution
cannot establish it. New work supplies null expected identity.
For destination evidence readers, the offline identity must be the durable
destination identity; pull support requires this unauthenticated evidence path.

Binding does not establish an engine root/workspace binding or empty receipt
store. An uninitialized engine returns null workspace ID. For a first managed
write the parent chooses and durably reserves the candidate workspace UUID in
consumer attempt state; the adapter commits it, its root binding, and the receipt
store with the first successful managed transaction. A competing established
workspace identity is `STATE_CONFLICT`, never silently substituted.
Read-only inspection does not create that reservation or binding.

There is at most one bound handle per process. `authenticate` upgrades it at
most once and is invoked only for new execution or a pending acknowledgement
that needs authentication. It never transports credentials. Receipt/session
lookup and inspection do not initiate login or re-authentication. A known
terminal outcome is returned before live source identity resolution.

### 4.3 Extraction and durable source checkpoints

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `extract` | `handle: Handle, payload: Doc<ExtractRequest>` | `source_complete` | `completion: Doc<SourceCompletion>` |

`SourceCompletion` is `{job: J, capture_window: {start: Time, end: Time},
adapter_result: J}`. Job/result validate at `source_job`/`push_result` for
extraction. The parent stores these immutable result facts with its capture and
eventual outcome so terminal replay can render the registered adapter result.

`ExtractRequest` is `{attempt_id: UUID, stream_id: UUID, root: string,
dataset: Name, run_id: RunId, declaration_sha256: Digest,
adapter_identity: AdapterIdentity, connection_identity: string,
selection: {policy: "changed" | "all"},
tables: [{name: Name, source: J, columns: J, contract: TableContract}],
resume: Checkpoint | null}`. Source/column selectors retain their effective
declaration shapes and validate at their extraction points. Partition columns
declared with `derive` are computed by the parent after receiving batches;
they are omitted from `columns` and `contract`, and the adapter never emits
them. Whole-table `selection.drop` is likewise a parent-only publication rule
and is not sent. The parent checks
and durably fixes identities and open-run ownership before sending this request.
The private `stream_id` is fresh on each extraction stream, including retries,
and is not a declaration input.

Nonterminal adapter events:

| Event | Fields beyond `msg, req` |
|-------|--------------------------|
| `extract_started` | none |
| `checkpoint` | `checkpoint_id: UUID, payload: Doc<Checkpoint>` |
| `batch` | §5.1 |
| `table_complete` | `table: Name, row_count: U64, source_identity: Doc<J>, capture: {start: Time, end: Time}` |

`Checkpoint` is `{attempt_id: UUID, adapter_identity: AdapterIdentity,
connection_identity: string,
tables: [{table: Name, snapshot_id: string, source_identity: J, capture_start: Time}],
job: J}`. Snapshot IDs are nonempty durable source identities, not timestamps
invented to label unrelated acquisitions. Source identity/job objects validate
at the served points. The adapter durably records source-job creation/resumable
identity before fetching and before sending the checkpoint. If job creation
itself is ambiguous, preserve its request identity and resolve it; do not
silently start another source job for the same attempt.

The parent persists/verifies checkpoint facts in its locked attempt state,
then sends `checkpoint_ack {req, checkpoint_id}`. No table batch or completion
is sent before an acknowledged checkpoint covers that table. A later checkpoint
can add tables or monotone job facts; it cannot replace a fixed snapshot identity.
The adapter's own checkpoint store is outside GRV, keyed by attempt and adapter/
connection identity; a fresh process reopens it. Stores are protected, atomic,
durable, and preserve unresolved evidence and pending hooks.

Tables stream contiguously. Per-table sequence numbers start at 0 in each stream,
are dense, and never repeat within that stream. No unexpected table is accepted.
Before `table_complete`, the adapter has received every ack for that table.
Before `source_complete`, every requested table completed exactly once and all
batch credits are returned. Reported table counts equal the sum of batch counts and
actual consumed rows; reported capture times are ordered UTC facts.

Zero-row tables send checkpoint coverage and `table_complete` with zero count,
but no batches. Missing completion or batches after completion produce
`EXTRACTION_INCOMPLETE`; duplicates/non-dense sequence/unexpected tables are
`PROTOCOL_FAILURE`. Schema/count disagreement is `INTEGRITY_FAILURE`.
All abort capture acceptance and publication, never imply omission.

On successful terminal evidence the parent still finishes/flushes and stops its
capture writers, verifies staged files and counts, then atomically seals the
capture receipt. Source completion is not itself that receipt or a GRV commit.

### 4.4 Pull resolution precedes source selection

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `resolve_pull` | `handle: Handle, payload: Doc<ResolvePullRequest>` | `resolve_result` | `resolution: Doc<PullResolution>` |

`ResolvePullRequest` is `{phase: "lookup" | "compare", attempt_id: UUID,
root: string, request: RequestRecord | null}`. Lookup has null request and
performs durable attempt discovery only. Compare supplies the parent-computed
fixed RequestRecord for that attempt. This two-frame sequence implements the
companion's single resolution obligation without guessing receipt versions/
defaults or source facts before reading durable evidence.

`PullResolution` is `{state: "committed" | "not_committed" | "busy" | "unknown",
request: RequestRecord | null, receipt: Receipt | null, recovery: J | null}`.
Receipt is present exactly for committed; its request equals the returned
request. `recovery` is null for transactional destinations, otherwise the
registered journal/repair evidence validated at `pull_plan`. A persisted
unfinished request is returned when present; absence in a new workspace is
distinguished from missing/corrupt history in an initialized workspace.

Lookup fences the former writer and reads the trustworthy durable evidence
store without source resolution/download, source authentication, or SQL
re-evaluation. Busy is `ENGINE_BUSY` at the command surface; unresolved durability
is `OUTCOME_UNKNOWN`; inconsistent metadata is `PROTOCOL_FAILURE`. A changed
current checkpoint or destination rows cannot establish not-committed.

For a committed receipt, the parent compares the caller's expanded declaration,
requested selector and root/connection against the recorded request using its
recorded defaults/schema/adapter identity; compare then requires the same
`request_sha256` and returns the original receipt. Only after that equality
check may the parent report replayed success. No acquisition/preparation/apply
or authentication is performed. A mismatch is `REQUEST_MISMATCH`.

For new/unfinished work, the parent validates current applicable bindings,
fixes the workspace/adapter/connection identity and request hash, and performs
compare. A mismatching recorded request is rejected. Only trustworthy
not-committed plus permitted journal repair authorizes source selection.
For an uncommitted `latest` retry, source contracts/revision may be rediscovered;
they are not substituted into the fixed request hash. Authentication for actual
destination execution, if needed, follows evidence resolution and precedes
mutation. After process loss, reopen in a fresh process and repeat resolution.

### 4.5 Pull preparation and application

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `prepare_pull` | `handle: Handle, payload: Doc<PreparePullRequest>` | `prepare_result` | `plan: Doc<PullPlan>` |
| `apply_pull` | `handle: Handle, plan: Doc<PullPlan>` | `apply_result` | `receipt: Doc<Receipt>` |

`PreparePullRequest` is `{request: RequestRecord, resolved_revision: Revision,
tables: [PullTable], files: [File], recovery: J | null}`.
`PullTable` is `{name: Name, target: J, select: J | null,
partitions: [J], source_contract: TableContract, output_contract: TableContract}`.
Its adapters' target/select objects validate at the pull points; partition
tuples are the concrete selected revision entries, not predicates inferred
from SQL. The original declaration preserves whether selection was complete,
explicitly empty, or partition-limited.

`PullPlan` is `{plan_id: UUID, request: RequestRecord,
resolved_revision: Revision, tables: [PullTable], files: [File],
refresh: "changed" | "full", recovery_contract: "transactional" | "journaled",
adapter_details: J}`. Details validate at `pull_plan` and include physical
mappings, stable ownership/role, checkpoint/diff and repair facts. Recovery
must equal the advertised capability. The parent validates and durably stores
the plan before apply; the adapter rejects a changed echoed plan.

The parent has already proved revision commitment, selected entries before
fetching, rejected selected tombstones, verified files, and resolved source/
output contracts. Files carry per-file prefix schemas and partition identities,
including empty files. Unselected files are neither listed nor fetched.
The adapter validates each reader schema, uses explicit file lists with Hive
inference disabled, and performs only permitted trailing-null padding.
Staging paths are not exposed to user SQL.

**Transactional:** DuckDB uses exactly the companion's one transaction for
all target data/catalog changes, checks over the completed resulting scope,
ownership/role and initial binding, completion checkpoint/import metadata, and
immutable successful receipt. SQL sees one fixed source/local/pre-write target
snapshot; all selections stage before any target writes. Mapping changes clear
obsolete exclusively owned targets atomically. Failure rolls back all of these;
only ambiguous commit proceeds to receipt resolution.

**Journaled:** before each possible mutation durably record its request identity,
target revision and dirty-table membership; fence every old writer and retain
the union of incomplete dirty tables. Keep consumers blocked until dataset-wide
durable completion and atomic checkpoint advancement. Rebuild/empty all dirty
tables on permitted retry, following GRV §11 and the adapter's registered recovery
contract. Failure can leave partial effects; it does not claim rollback.
The adapter must retain an immutable successful attempt outcome sufficient for
same-attempt replay. Append support is advertised only if its journal/fencing
contract prevents duplicate insertion on replay; dirty-table rebuilding alone
does not prove that guarantee.

`apply_result` is success only with the matching durable receipt. The parent may
also establish success through subsequent `resolve_pull` if the response is lost.
An error contains known-effect information only through declared recovery state;
it never fabricates a successful receipt or assumes a failed commit rolled back.

### 4.6 Build discovery before holds, then preparation

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `discover_build` | `handle: Handle, payload: Doc<DiscoverBuildRequest>` | `build_discovered` | `discovery: Doc<BuildDiscovery>` |
| `prepare_build` | `handle: Handle, payload: Doc<PrepareBuildRequest>` | `build_prepared` | `session: Doc<BuildSession>` |

`DiscoverBuildRequest` is `{identity: BuildIdentity, execution: "managed" | "external",
inputs: [{alias: Name, relation: J}],
outputs: [{table: Name, source: J, contract: TableContract}],
selected_outputs: [Name], self_input: boolean}`.
`BuildDiscovery` is `{discovery_id: UUID, identity: BuildIdentity,
inputs: [InputBinding], outputs: [OutputBinding]}`.
`PrepareBuildRequest` is `{discovery: BuildDiscovery, base_revision: Revision,
self_input: boolean, base_files: [File], input_files: [File],
holds_confirmed: boolean}`; confirmation must be true, even for an empty input set.

After the parent reserves the attempt/run index and acquires the session lock,
the adapter acquires workspace ownership and opens the preparation transaction.
Discovery selects eligible completed materialization generations and returns
their recorded contracts, logical revisions, and private output mappings.
Discovery only chooses *which* revision, contract, and generation each input
uses; it never supplies the input rows. No application
import, revision 0, foreign-root input, mutable view dependency, or guessed table
provenance is accepted. The discovery reservation/transaction remains open and
fixed across the parent's backend work; another engine user cannot refresh it.
The adapter durably records the discovery identity and selected facts before
returning them; the parent durably records those facts before creating the run.
If process loss destroys the discovery transaction after a run was created and
no matching prepared session and private inputs committed, that preparation is
incomplete. Abandon/recover its run; do not perform fresh discovery under it.

The parent creates the GRV run with exactly those fixed inputs, confirms each
whole-revision dependency hold, and verifies needed file sets, while renewing
the run. Only then does it send `prepare_build`. Cached rows never replace GRV
hold and availability checks. The adapter verifies discovery and identity
equality, then builds each private input from the held revision's
parent-verified `input_files`: local inputs are materialized from those files,
and S3 inputs are private views over the same fixed, held S3 file sets. The
adapter never copies an input from a tracking table, because ordinary engine
tables can be changed after a pull and would then no longer match the cited
revision. If a discovered materialization's recorded contract disagrees with
the logical schema of the verified input files, preparation fails with
`PROTOCOL_FAILURE` (inconsistent consumer metadata). The adapter then creates
output tables and commits the session record and the first
root/workspace/receipt-store binding atomically. Failed preparation rolls back
engine preparation; the parent seals the owned GRV run empty or leaves normal
recovery, never deletes holds as rollback.

Self-input files exist exactly when enabled and represent the prepared target
base, materialized locally without a self-hold. Managed outputs start empty
with a separate immutable self read binding; external outputs may be seeded
as the companion specifies. Holds/drop selection suppresses execution/export,
not merely row contribution from a prepared empty table.

`build_prepared` means durable engine preparation, not parent context success.
The parent writes its protected context atomically/durably before reporting
preparation success. It records the durable session ID, identities, mappings,
base/input facts and session-lock coordinates, but never sends its owner token.
A lost response/context is recovered from this exact session through §4.9.
No second discovery or preparation under the same attempt may select new inputs.

### 4.7 Managed execution returns completion, not batches

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `execute_build` | `handle: Handle, payload: Doc<ExecuteBuildRequest>` | `build_finished` | `result: Doc<BuildExecutionResult>` |

`BuildExecutionResult` is `{status: "succeeded" | "failed", completion: J | null,
row_counts: [{table: Name, rows: U64}]}`. Completion and counts share one document
so large output sets remain within the metadata transport bounds.

`ExecuteBuildRequest` is `{session: BuildSession,
queries: [{table: Name, sql: string}]}`. Queries cover exactly the selected
managed outputs and equal their fixed prepared source SQL. No query is run for
held/wholly dropped outputs. Omission-only
work has an empty query list but still produces bound completion evidence.

Before starting work the adapter durably records invocation ID, fixed query
digest and executing state. Execution is legal only for a prepared session
that has never started an invocation. An executing/incomplete session cannot
be rerun; a completed session is reopened to recover its candidate/accepted
record, not executed again.

The adapter evaluates/stages every selected output before replacing private
output contents. Queries read only immutable declared input/self bindings;
external access/autoload/install are disabled during user-query evaluation.
No query reads an earlier output query's new result. Zero-row success is explicit.

After successful invocation it stops/awaits all output writers and closes every
invocation database connection, while retaining workspace-lock ownership.
It durably writes the exact successful completion record and invocation facts
in adapter consumer state **before** emitting `build_finished`. The record's
run/workspace/declaration/mappings come from the prepared session and actual
invocation; `completed_at` records stopped-writer completion, not receipt time.
It passes the existing build-completion schema, including `kind` and exact
`{table, engine_table}` entries. Counts are separate from the record.

There are **no batch or table-completion frames in execute_build**. On query
failure status is failed, completion is null, and counts include only actually
established facts. No success record is written or accepted; the parent reports
`BUILD_INCOMPLETE`, cancels/awaits any remaining work, and abandons normally.
Cancellation or ownership loss never permits completion acceptance or rerunning
incomplete work from leftover tables.

### 4.8 Accept immutable completion before separate export

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `accept_build_completion` | `handle: Handle, session_id: UUID, completion: Doc<J>` | `completion_accepted` | `session_id: UUID, completion_sha256: Digest` |
| `export_build` | `handle: Handle, session_id: UUID, completion_sha256: Digest, stream_id: UUID` | `export_complete` | `session_id: UUID, completion_sha256: Digest, adapter_result: Doc<J>` |

The parent validates full identity, selected-output coverage and mapping equality
against its fixed context and the build-completion schema, under session/workspace
serialization. For managed work it atomically/durably stores the returned
completion JSON file; for external work the driver has already written that
file after stopping all writers/connections. Then it sends acceptance.

The adapter canonicalizes with RFC 8785 and durably stores the record and its
SHA-256 in the **engine session record**, before reopening an export snapshot.
The first accepted record is immutable; a later non-identical record is
`REQUEST_MISMATCH`. Parent and adapter digests must agree. A crash during
acceptance requires reopening the session and checking this exact stored record,
not choosing a new completion timestamp or reconstructing names from tables.

`export_build` is legal only with that accepted digest and the parent's current
ownership/finalization authorization. It reads all selected outputs from one
consistent engine read transaction and sends §5 `batch` events plus exactly
one `build_table_complete {req, table, row_count}` per selected output.
It waits for every ack before table completion and `export_complete`. Counts
must match accepted invocation counts where recorded; no capture/source identity
fields are invented for build rows. Parent schema/check/staging validation and
the GRV publishability fence remain mandatory.

The parent drains and finishes capture writers and records its durable export
plan/completion digest before allocations. Accepted staging/capture is reused
on retry; incomplete export can reread the same immutable completed outputs
while ownership is valid. A complete sealed-run retry does not reopen/export
engine tables: it verifies the existing durable plan and sealed payload.
The terminal adapter result validates at `push_result` for the session mode
and is durably retained with the accepted export/outcome for source-free replay.

### 4.9 Durable build reopen, inspection, abort, and cleanup

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `open_build` | `handle: Handle, payload: Doc<BuildIdentity>` | `build_opened` | `record: Doc<BuildRecord>` |
| `inspect_build` | `handle: Handle, payload: Doc<BuildIdentity>` | `build_inspected` | `record: Doc<BuildRecord>` |
| `abort_build` | `handle: Handle, session_id: UUID` | `build_aborted` | `session_id: UUID, writers_stopped: true` |
| `record_build_outcome` | `handle: Handle, session_id: UUID, outcome: Outcome` | `build_outcome_recorded` | `session_id: UUID` |
| `cleanup_build` | `handle: Handle, session_id: UUID` | `build_cleaned` | `session_id: UUID` |

`BuildRecord` is `{session: BuildSession,
state: "prepared" | "executing" | "completed" | "aborted",
completion: J | null, completion_sha256: Digest | null,
candidate: J | null,
row_counts: [{table: Name, rows: U64}], outcome: Outcome | null}`.
Completion/digest refer to the **accepted** engine record or are both null.
Candidate is the separately retained exact successful managed completion record
written before acceptance, or null. Open/inspect return that immutable evidence
without executing SQL. The parent may validate/write/accept a recovered candidate
while original ownership permits; executing-without-success is incomplete.
Candidate/accepted completion identities must agree when both are present.

Open/reopen validates exact recorded identities and fixed mappings; missing
required records are errors, never an instruction to create a new session.
The parent first resolves its recorded GRV publication/outcome. A recovered
context is derived from the original engine session, not current tracking data.
Inspection is strictly read-only: no completion acceptance, repair, renewal,
or writer fencing with side effects.

External preparation uses §4.6, then closes the adapter cleanly and releases
workspace ownership. The external driver owns the same workspace lock throughout
its invocation/children and performs backend-only renewals. Later finalization
uses a fresh adapter, opens the original session, accepts the actual driver
record (`engine`, `direct`, or `omission-only`), and exports without
`execute_build`. No result flag is needed once completion was accepted.

Abort is legal only after the parent resolves any recorded publication attempt;
it cannot erase a committed publication. It stops/awaits supervised managed
writers, records local abortion and refuses further execution/export, while the
parent resolves allocations and seals the run through normal GRV protocols.
Stopping external writers remains the driver's obligation; busy ownership
returns `ENGINE_BUSY`. Lost GRV ownership never licenses adoption of tables.

The parent durably records confirmed GRV outcomes before synchronizing the engine
record. Outcome synchronization is idempotent and may lag publication; failure
preserves/reports the committed outcome and is retried without export/publication.
Cleanup requires a known terminal outcome, stopped writers and no unresolved
engine commit; it deletes private execution/cache artifacts only, preserves
session/attempt/completion/outcome history, and never releases GRV holds.
Parent-only session renewal/backend inspection requires no adapter or engine
connection.

### 4.10 Read-only connection inspection

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `inspect_connection` | `handle: Handle, root: string \| null, declaration: Doc<J> \| null` | `inspect_result` | `details: Doc<J>` |

Details validate at `inspection_result`. The declaration scopes matching
materializations/imports; root identity permits foreign-root rejection.
No binding, receipt creation, repair, authentication renewal, or GRV renewal
occurs. The adapter still acquires workspace ownership before engine reads.
An unsupported inspection is `UNSUPPORTED_CAPABILITY`, not empty invented data.

### 4.11 Post-publication acknowledgement

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `after_publish` | `handle: Handle, attempt_id: UUID, declaration_sha256: Digest, outcome: Outcome` | `after_publish_result` | `acknowledged: true` |

Required only when `after_publish` is advertised. Parent records pending
acknowledgement alongside the known published/no-op result **before** calling it.
The adapter reopens fixed attempt/source-job state; it never reacquires source
data. Authenticate only if the hook needs it. Cursor advancement is allowed
only for confirmed published/no-op outcomes; aborted can release a source job
but cannot advance its cursor. Hooks are idempotent and monotone across attempts.
The parent durably clears pending state after success. Failure/death preserves
the outcome and pending hook and reports an adapter error, never republishes.

### 4.12 Namespaced commands

| Request | Request fields | Terminal response | Response fields |
|---------|----------------|-------------------|-----------------|
| `prepare_command` | `name: Name, argv: Doc<[string]>` | `command_prepared` | `call: Doc<CommandCall>` |
| `command` | `command_id: UUID, handle: Handle \| null` | `command_result` | `details: Doc<J>` |

`CommandCall` is `{command_id: UUID, name: Name, args: J, connection: J | null}`.
Prepare is pure parsing/default validation with no authentication or I/O.
Names come from served descriptors; parsed args validate against their args
pointer. Connection is non-null exactly when required and validates against the
command-mode connection point. The adapter keeps one immutable prepared call
per channel; changed/unknown/reused command IDs are protocol failures.

The parent validates the parsed call, then locates/binds its connection and
authenticates only when the descriptor requires it. Handle is non-null exactly
when connection is required. Login uses null connection/handle. The adapter
executes that prepared call once; parent validates sanitized results against
the command's result schema before producing the common envelope.

### 4.13 Cancellation, close, and errors

| Frame | Sender | Fields beyond `msg` |
|-------|--------|---------------------|
| `cancel` | parent | `req: Req` |
| `cancel_ack` | adapter | `req: Req, state: "stopped" \| "completed"` |
| `close` | parent | `req: Req` |
| `close_result` | adapter | `req: Req` |
| `error` | either | `req: Req \| null, code: string, message: string, retryable: boolean, object: J \| null` |

Cancel refers to the active or most recently retired request, never a new
request ID. A duplicate cancel for either is idempotent. Unknown/older IDs
are `PROTOCOL_FAILURE`. `cancel_ack.state: stopped` is terminal for the
cancelled operation and means all supervised local work stopped and remote
uncertainty is durably preserved; it is not merely "signal received".
Outstanding slots are abandoned, both sides release references, and no further
streaming/normal result follows that stopped acknowledgement.

The parent's single serialized channel writer freezes new batch/checkpoint
ACKs before enqueueing cancel. ACKs already written precede cancel on the
stream. Events received after that point are drained, and their payloads read
and discarded, without ACK; stopping must not depend on receiving their credits. This prevents
late ACKs from racing a stopped terminal result or the next request.

If normal completion wins, send its normal terminal result first, followed by
`cancel_ack.state: completed` if cancel was observed. The parent drains any
earlier events, preserves established outcome evidence, and accepts this one
late control for the most recently retired request. If cancellation wins,
normal success cannot follow; ambiguous destination commits are resolved through
receipts regardless of which control arrived first. A user stop need not accept
a build/extraction candidate, but it never rolls back a known destination or
GRV commit. Repeated cancel ACKs must be identical and do not advance state.

Close is an ordinary request accepted only when idle/drained or after stopped
cancellation (or completed cancellation after draining). It releases handles,
rolls back uncommitted preparation discovery,
stops/awaits descendants, closes connections and releases adapter locks, then
returns and exits 0. A second close is a protocol failure. The parent holds
its session lock until database users/writers stop, not merely until a terminal
data frame.

Errors use **only the companion's closed public code set**. Unsupported
interface, platform, or data plane is `UNSUPPORTED_CAPABILITY`; frame/transport violations
are `PROTOCOL_FAILURE`; schema/hash/count disagreement is `INTEGRITY_FAILURE`.
There are no `VERSION_MISMATCH` or `CHANNEL_FAILURE` wire/public codes.
`object` uses the output schema's structured `object_identity` or null, never
an opaque string. Exit mapping and retryable semantics are exactly the companion's.

A request error is terminal; a channel error has null req and closes the channel.
Error termination can abandon outstanding slots; the parent releases them and
sends no later batch ACKs or ordinary operations on that failed channel.
Error classification never substitutes for resolving an uncertain commit.

### 4.14 Required command sequences

- **Fresh extraction:** common validation/capabilities → validate → locate →
  bind/authenticate → pin/reserve attempt and GRV run → checkpoint handshake →
  extract/consume/complete → seal capture → core finalization/publication →
  durable outcome/pending hook → optional acknowledgement → close.
- **Pull:** common request loading → locate/bind evidence only → resolve lookup →
  compare fixed request with recorded normalization evidence (or validate/fix a
  new request) → resolve compare → **only if trustworthy not-committed**, resolve
  revision/select/verify source → authenticate execution if needed → prepare →
  durable plan → apply/receipt resolution → report original/new result → close.
- **Managed build:** validation/locate → reserve fixed attempt/run and acquire
  session lock → bind/workspace lock → discover → create run/confirm holds →
  prepare/context → execute and stop connections → validate/write completion →
  accept immutable engine record → export/drain → fixed plan → core publication →
  durable outcome synchronization/hook → close.
- **External build:** prepare/context/close; driver locks/invokes/stops/attests;
  later parent resolves GRV outcome, binds/reopens original session, accepts
  completion and exports. No managed execution is inferred or repeated.
- **Terminal replay:** compare fixed recorded request/outcome before source
  contact; return it directly, or run only a pending acknowledgement. Accepted
  capture, accepted completion, or a sealed plan never authorizes reacquisition.

When adapter evidence is needed to normalize a terminal pull replay, lookup
precedes execution validation/authentication. This is not permission to mutate
an invalid declaration. Unsupported new work is refused before any login/run/
engine mutation; old terminal facts use their recorded interface/result contract.


## 5. Data plane: credited stream batches

Only extraction and **accepted build export** produce row batches. Pull and
local build inputs use explicit verified file metadata; S3 views use their
separate reader contract. The v1 data plane is `stream`: row data travels as
binary payloads on the channel socket itself (§2.5). There is no
shared-memory, descriptor-passing, persistent-file, or typed-text output data
plane in v1.

### 5.1 Slots, batches, and acknowledgements

Slots are flow-control credits, not buffers or files. A slot starts free. The
adapter consumes one credit when it sends a batch, and the matching
`batch_ack` returns it. The adapter may reuse a slot number only after that
ack. All active slots in the one active stream share the offered limit.

| Frame | Direction | Fields beyond `msg, req` |
|-------|-----------|--------------------------|
| `batch` | adapter → parent | `table: Name, seq: SafeInt, slot: SafeInt, size: U64, rows: U64`, followed by exactly `size` payload bytes (§2.5) |
| `batch_ack` | parent → adapter | `table: Name, seq: SafeInt, slot: SafeInt` |
| `build_table_complete` | adapter → parent | `table: Name, row_count: U64` |

Slot is in `0..slots-1`. Size is positive, a multiple of 8, and no greater
than `max_batch_bytes`; rows are positive and at most `2^31-1`. Zero-row
tables use completion without a batch. A busy slot, wrong tuple, repeated or
missing sequence, unexpected table, or duplicate ack is `PROTOCOL_FAILURE`.
Sequences are per table in a stream; audit identity is
`(attempt, stream_id, table, seq)`, not a tuple reused across fresh channels.

The parent reads the payload into its own buffer, validates it, consumes every
row into its bounded Parquet writer, and releases that buffer (or hands its
rows to separately owned bounded buffers) before sending the exact ack. An
asynchronous writer cannot retain Arrow arrays backed by the receive buffer
after the ack. Ack authorizes credit reuse, not capture acceptance, durable
publication, or destination commit.

The adapter waits for all of a table's acks before table completion, and for
all of the stream's acks before successful terminal completion. The parent
reconciles batch, table, and actual sink row counts, verifies output coverage,
flushes and stops all capture writers, and verifies files before accepting a
complete capture or export. On error or stopped cancellation the parent
instead abandons outstanding credits and discards incomplete staging, without
sending stale acks.

### 5.2 Exact Arrow IPC encoding

Each payload uses the [Arrow encapsulated IPC format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc):
exactly a Schema message followed by one RecordBatch message and its body.
Each has the modern `0xFFFFFFFF` continuation marker, little-endian int32
metadata length, FlatBuffers Message metadata with MetadataVersion V5, and
8-byte padding. The schema has no body. RecordBatch buffer offsets are relative
to the **start of its body**, not the message header or whole payload.
The metadata length includes metadata padding, and the padded body length
must exactly exhaust the payload. No footer, end-of-stream marker, third message,
dictionary batch, dictionary-encoded field, compression, or big-endian schema
is permitted in v1. Schema and body buffers use the standard 8-byte IPC alignment.

Parse bounded metadata first, verifying FlatBuffers structure, message and body
lengths, buffer offsets and lengths, field-node counts, row counts, string and
binary offset monotonicity and bounds, UTF-8, and arithmetic overflow before
adoption. Reject unsupported physical layouts and types rather than relying on
a writer's choice of encoding. Use a validating Arrow decoder; offset-based
storage alone does not make a parser memory-safe.

### 5.3 Contract binding

The request's TableContract is authoritative. Convert IPC names, types and
order to the supported GRV logical representation and compare exactly. GRV
nullability rules and incidental schema metadata do not require byte-identical
FlatBuffers, but UTC semantics, units, decimal precision/scale and required
registered extensions must be preserved. No implicit cast/truncation repairs
a mismatch. Validate partition-column values and declared checks as the
companions require, in addition to structural schema equality.

Malformed IPC, size, or payload framing is `PROTOCOL_FAILURE`. Logical schema,
row count, or immutable file/hash disagreement is `INTEGRITY_FAILURE`. Parent
consumer or writer resource failure is `ADAPTER_FAILURE` unless a more specific
companion code applies; all paths release buffers and preserve already known
outcomes. Earlier staged rows do not make a partial capture acceptable.

### 5.4 Adaptive size and oversized rows

The encoded batch target `T` starts at offered `max_batch_bytes` and includes
both messages, metadata and padding, not just value buffers. Batches end at row
boundaries with encoded size ≤ T. The producer obtains a free credit **before**
encoding the next payload; retries cannot create uncredited batches.

On a catchable resource failure while encoding a payload, release the partial
payload and scratch, retry up to three times with cancellation-aware waits of
at most 100 ms each, then halve T (round down to a multiple of 8). Stop below
256 KiB with `ADAPTER_FAILURE`. Never drop or truncate a row or mark the table
complete. The adapter may retain a decreased target for subsequent batches; it
never increases above the offered maximum.

If one row plus required schema and metadata cannot fit the current target,
increase T up to the offered maximum for at most one encoding attempt for that
row; if it cannot fit or that attempt fails, fail `ADAPTER_FAILURE` with an
oversized-row diagnostic. Do not loop indefinitely halving an indivisible row.
Inability to encode even the schema is likewise a typed failure. Parent
buffer, decoding, or Parquet failures abort cleanly where catchable; the parent
does not send a success ack for unconsumed data.

Resource allocation can fail during encoding, receiving, or decoding. Kernel
OOM termination is a crash, not a guaranteed catchable typed failure; §7
governs it. No claim of immunity to OOM is made.

### 5.5 Source backpressure and memory budgets

An adapter holds at most **one source unit** beyond its credited batches, and
its retained encoded and decoded source data together fit
`max_source_unit_bytes`. Conversion and IPC scratch separately fits
`max_scratch_bytes`. If a source API page exceeds the budget, use bounded
incremental decoding or scan windows, or fail before adopting it; "one page"
alone is not a byte bound. No unbounded prefetch, hidden source queue, or
full-result buffering bypasses this rule.

With all credits occupied the producer stops fetching and encoding. Its
independent channel reader remains responsive to acks, cancel, and EOF.
Source and engine libraries' retained extraction buffers count toward the
source and scratch budgets. Database query caches and private staged build
tables have their own configured engine memory and spill limits; engine-owned
spill is not an output data plane. All selected SQL outputs may stage in engine
storage, not an unbounded adapter memory queue.

These limits bound **transfer buffering**, not total RSS: runtime overhead,
engine caches, source libraries, parent metadata and Parquet compression require
additional independently bounded budgets. The parent reserves enough consumer
memory for one maximum batch receive buffer plus its writer budget before
offering resources. It consumes at most one received batch at a time and bounds
queued metadata by credit, frame, and document caps. Batches waiting in the
socket are bounded by the credits. The parent can offer fewer or smaller slots
if its budget cannot support defaults.

### 5.6 Credit-aware liveness

Use the deadline classes in §7.1. Producer idle time is suspended **only when
all credits are occupied**. One held slot with remaining free credits does not
disable the producer deadline. Time spent genuinely credit-blocked does not
consume its remaining idle budget; a returned credit resumes it.

Each unacked slot has a consumer-progress deadline independent of producer
silence. A stuck decoder or writer therefore cannot hold a credit forever and
disable all liveness checks. Consumer progress is advancing that batch's
validated decode and staging work, not unrelated batches. No ping frame or fake
keepalive resets a deadline.

### 5.7 Payload immutability

Payload bytes are copied through the kernel socket into a buffer the parent
owns. Once sent, the producer cannot change, truncate, or revoke them, and the
parent never maps or references adapter-owned memory or files. This copy is the
v1 immutability guarantee: the parent validates and consumes exactly the bytes
it received. No platform-specific memory primitive, temporary file, or cleanup
sweep is part of the data plane, and the data plane works identically on Linux
and macOS.

### 5.8 Safety limits

Reject invalid sizes, truncated payloads, and unsupported IPC before using rows.
Frame and payload parsing never opens an adapter-supplied pathname. The parent
does open trusted installation and helper lock paths (§§1, 6) and its own
staging and state paths; this is an explicit exception to a blanket "no adapter
paths" claim.

The channel cannot prove source truth, protect against every parser/runtime bug,
or stop same-user executable code from using ambient OS permissions. A hostile
adapter can emit schema-valid false rows or exhaust its own budgets. Publication
still requires all parent validation and GRV fencing; process separation and
bounded/validated input do not constitute hostile-code sandboxing.

## 6. Configuration, identity, and locks

### 6.1 Normalization and durable identities

The parent parses YAML/reference/common shape before the handshake, then uses
served capabilities/schemas and the pure validation result in its single
normalizer. Capability checks cannot occur "before any adapter frame" because
the authoritative descriptor is served by the handshake. They occur before
authentication, execution mutation, or GRV allocation.

Fixed requests use the companion's RFC 8785 declaration/request hashing exactly:
effective defaults/local files/canonical paths, recorded adapter triple, and
resolved stable connection identity. Capture-time facts, channel resources,
attempt/run/stream IDs and chosen physical installation paths are not added
to declaration inputs. Inferred successful pull schemas are receipt output
facts; a newer uncommitted latest selection does not alter the requested selector.

Parent indexes, contexts, captures, completion files and outcomes are atomic/
durable consumer state, protected by canonical attempt/session locks. Adapter
source-job stores and engine records are separately atomic/durable. Every
mutation-capable phase has its fixed identity durably recorded before dispatch.
Missing evidence is an error; it is never reconstructed from folder/table presence.
The adapter receives projections of contexts, never the token-bearing context
itself. Run IDs are non-secret; leases/tokens and renewal records remain private.

### 6.2 Renewal and ownership loss

The parent renews GRV run/claim/dataset leases on its own timer according to
GRV §5 TTL/skew bounds, independent of channel receive, stderr, writer tasks,
or adapter speed. Between external commands the driver uses the companion's
engine-free renewal path. The adapter never renews a GRV lease.

Failed ownership renewal immediately disables new allocations, completion
acceptance, and publication under that owner and initiates cancellation.
A child that cannot stop promptly cannot cause adoption under a new run:
outputs remain session-private. A sealed complete run may only retry its fixed
publication plan; ownership expiry does not authorize a new run, rebase, new
source job or resumed incomplete build under the same attempt.

### 6.3 Session and workspace locks

Engine-specific locking belongs to the adapter, matching the Client.
`locate_connection` supplies trusted helper coordinates without opening the
engine. For build mutation/inspection, the parent first acquires the persistent
session lock for the fixed run, then `bind_connection` has the adapter acquire
the workspace lock **without waiting**, before opening any engine connection.
Paths/identities are rechecked under these locks. Contention releases acquired
locks before reporting `ENGINE_BUSY`. Discovery of an existing session may use
workspace-only read access, close it, then reacquire session followed by workspace
and recheck; it never inverts the companion's order.

For DuckDB, canonicalize symlinks, reject hard-link aliases, and use the
persistent exclusive `flock` helper at `<canonical-engine-path>.grv-lock`.
The parent session helper is
`<canonical-engine-path>.grv-session-<run-id>.lock`. Never unlink/replace either
lock file during the workspace lifetime; reject database move/replacement.
Helper paths are trusted adapter binding information, checked as regular
protected consumer lock files, never locations for reading arbitrary result data.
Read-only extraction does not establish root/workspace metadata.

The adapter holds workspace ownership throughout pull, extraction, inspection,
preparation, invocation, completion acceptance/export, abort, outcome
synchronization and cleanup whenever they open the engine. It keeps the lock
even when it temporarily closes database connections to attest completion.
Every database child inherits a reference to that same lock ownership through
the adapter helper; no descendant explicitly unlocks the shared open-file
description. Release only when all database users/writers stopped and all lock
references close. Adapter/parent death does not justify stealing a surviving
child's lock. DuckDB's own file lock is an additional busy safeguard.

The parent keeps its build session lock throughout each corresponding command
and its own backend-only renewal while that command owns it. An external driver
holds the workspace lock during invocation, **not** the session mutation lock;
backend-only renewal/inspection/pin/GC/recovery do not open an engine.
Extraction consumer-state locking and context aliases follow the implementation
companion. Other adapters implement their declared destination locks/fencing;
an expiring client lease alone is insufficient. Salesforce takes no DuckDB lock.

## 7. Failure, cancellation, and recovery

### 7.1 Deadlines and cancel ladder

Deadlines are operator-configurable, not declaration inputs. V1 defaults:

| Deadline | Default | Meaning |
|----------|---------|---------|
| bootstrap | 30 s | spawn through validated `identified`/`ready` |
| partial frame | 300 s | first byte through complete LF and, for `batch`, the last payload byte, irrespective of credits |
| document transfer | 300 s | total begin-through-ack; bounded metadata cannot drip forever |
| operation start / non-streaming response | 300 s | request through first allowed progress/terminal response |
| producer progress | 300 s | meaningful checkpoint/batch/table progress; paused only at zero credits |
| consumer progress | 300 s | per unacked batch's decode/staging progress |
| cancellation / shutdown grace | 10 s | each cooperative/signal/shutdown step |

No mandatory total duration limit is imposed on a progressing stream; an
operator may configure one. Non-streaming queries/preparation require increasing
their response deadline if expected to exceed it. First streaming progress
switches to producer/consumer deadlines; sending an initial `extract_started`
does not disable later enforcement. Unrelated document traffic or stderr does
not reset operation progress.

On timeout, operator stop, ownership loss, or parent-side abort:

1. If an ordinary request is active, send cancel and wait at most one grace
   for its matching stopped/completed acknowledgement. Preserve a winning normal
   terminal result while waiting for that ACK; absence of the ACK escalates to
   signals rather than another ordinary request. A stopped ACK means stop was
   completed, not just requested.
2. When cooperative work has stopped, close cleanly and wait a bounded grace
   for exit. If it remains active, cannot process frames, or transport failed,
   signal the **supervised process group** with SIGTERM.
3. Wait one grace, then SIGKILL the group if needed and reap/observe exit.
   During bootstrap/partial-transport loss, skip invalid cancel/close frames
   and use the signal steps directly.
4. If surviving database owners/remote writers cannot be fenced, record/report
   busy or unknown and preserve evidence. Do not start a replacement writer
   merely because the direct adapter PID died.

All waits and stderr/channel handling remain separate from parent GRV renewal
until the parent abandons/seals or loses ownership. Parent EOF makes the adapter
perform its own stop/await/release path; descendants retain file-lock ownership
until genuinely stopped. Remote cancellation is best effort unless the declared
destination contract proves fencing.

Required evidence is durable **before mutation/job dispatch**, not first written
by a cancel handler. Killing local processes therefore preserves a path to
resolution; it does not by itself prove destination rollback or remote-job death.

### 7.2 Operation/phase recovery matrix

An unexpected exit, EOF/truncated frame, resource death, or unresolved deadline
closes the failed channel. Resolve known durable effects before choosing the
final public code. The adapter exit status never selects the CLI status.

| Lost phase | Required next step / public classification |
|------------|---------------------------------------------|
| handshake | no mutation; `ADAPTER_FAILURE` for death/timeout, `UNSUPPORTED_CAPABILITY` for explicit version/platform refusal |
| validation / locator / prepare-command parsing | no execution effects; `ADAPTER_FAILURE` for death, `INVALID_DECLARATION` for established invalid input |
| bind / authentication | no managed binding creation; close/fence users, then `ADAPTER_FAILURE` or `ENGINE_BUSY`; no GRV run yet |
| pull lookup / compare / prepare | no new application; fence and resolve original attempt first; `ENGINE_BUSY`, `PROTOCOL_FAILURE` or `OUTCOME_UNKNOWN` where evidence is untrustworthy |
| pull apply / lost apply result | reopen under destination serialization, recover engine/journal and resolve immutable receipt; report known success or busy/unknown, never inferred rollback |
| build discovery / preparation | resolve exact attempt index/session; recover matching committed preparation/context, otherwise abandon owned run or normal recovery; `ADAPTER_FAILURE`/busy/unknown; never select new fixed inputs under an existing run |
| managed execution | replay durable successful candidate if one exists and ownership allows acceptance; otherwise `BUILD_INCOMPLETE` after stopping writers; no rerun from leftover tables |
| completion acceptance | reopen and compare accepted canonical record/digest; retry identical acceptance if absent and trustworthy; inconsistent identity is `REQUEST_MISMATCH` |
| build export | accepted record remains fixed; discard incomplete export and reread same stable outputs only with valid ownership; absent stable evidence is `BUILD_INCOMPLETE` |
| extraction | no incomplete capture acceptance; `EXTRACTION_INCOMPLETE`; only §7.3 continuation is legal |
| build open / inspect | no invented state or repair; `ADAPTER_FAILURE`/busy, or `PROTOCOL_FAILURE` for corrupt required records |
| build abort / outcome sync / cleanup | resolve known session/GRV outcome first; preserve records and known effects; retry idempotent state synchronization/cleanup only after stopped writers |
| namespaced command | `ADAPTER_FAILURE` unless a more specific declared result establishes effects; arbitrary commands are not automatically replayable |
| after-publish hook | preserve/report known GRV outcome and durable pending hook; `ADAPTER_FAILURE`; retry only the fixed idempotent hook |
| close / exit after established result | preserve successful receipt/accepted completion/GRV outcome; report shutdown `ADAPTER_FAILURE` if needed, never label committed work rolled back |

Protocol/transport failure is `PROTOCOL_FAILURE`, but an in-flight commit still
requires resolution. A malformed response cannot prove commit or rollback.
A terminal result validated before a later shutdown error remains evidence:
report known committed fields with any nonzero cleanup/shutdown error according
to the companion's partial-result rules. There is no blanket "nothing published"
rule after an already committed operation.

On unresolved preparation, do not delete GRV controls/holds as rollback. Source
GC alone releases holds. A replacement adapter opens original durable IDs;
it does not interpret a new handle as a new attempt or permission to rebind root.

### 7.3 Extraction resumption and stream loss

The table is the retry unit, never an individual lost batch. Same-attempt
continuation requires unchanged fixed identities, valid original ownership,
and acknowledged durable source identity that can reproduce the **same snapshot**.
Keep complete tables only after their staged files/counts/hashes are durable and
verified; discard all partial staging for each incomplete table and reproduce
that whole table with a fresh stream ID and sequence starting at 0.
Prior acknowledgements do not forbid retransmission in this new stream.

Do not resend an acknowledged batch within a live stream; an uncertain/lost ack
closes that stream instead of guessing credit ownership. New channels upload
their metadata/checkpoints again but cannot replace recorded source identity.
If the source snapshot cannot be reopened, a new attempt acquires **all declared
tables** as a new complete snapshot; it cannot combine fresh affected tables
with unrelated old snapshot fragments. Missing/corrupt accepted capture or
attempt mapping is an error. Accepted complete capture is hash-verified and
reused without re-extraction.

### 7.4 GRV outcomes and external writers

Only the parent's backend operation commits GRV. Adapter frames cannot commit,
roll back or rebase a revision. Resolve publication through the recorded
operation and committed chain under normal GRV fencing before another attempt.
A pending source acknowledgement never authorizes republication.

External drivers remain responsible for actual invocation success, stopped
writers, workspace ownership and between-command renewal. Attestation is not
independent proof of model correctness. Lost/recovered runs cannot adopt private
tables; complete sealed runs may publish only when they match the durable plan.
Remaining native/remote ownership is busy/unknown, not canceled by lease expiry.

## 8. Versioning and evolution

- `interface_version` versions frame grammar and semantics; any incompatible
  change requires a new version. V1 closed objects have no additive lane.
  The bootstrap grammar is fixed for v1; supporting later interfaces must
  preserve a mutually understood bootstrap before interpreting new operation
  shapes. An incompatible bootstrap also requires an explicitly new bootstrap
  contract, not emission of unknown members to a v1 peer.
- `binding_schema_version` versions served adapter fragments/default rules/
  result schemas. One version is served per install in v1 and pinned alongside
  package/interface versions; it is not silently negotiated or downgraded.
- New work selects highest common interface; unfinished work selects its fixed
  supported version and rejects package/binding/connection drift. Terminal
  outcomes use their recorded evidence/normalization contract.
- The v1 data-plane offer is exactly `["stream"]`. Unknown values refuse
  capability. A future interface may add another plane, such as shared memory,
  without changing a v1 adapter.
- Public output/code versions remain the companion's. Adapter-only diagnostic
  distinctions are messages/details under existing codes, not new public enums.

## 9. Conformance

An adapter passes the companion's common and capability-specific suites plus
the scenarios below. Scenarios about the channel, framing, payloads, process
supervision, or inherited process state apply only to process adapters; a
built-in linked into the CLI (Purpose section) passes the operation-level
scenarios through the logical lifecycle interface. A reference **mock parent**
drives process-adapter obligations. A separate fake/faulting adapter exercises
the **real parent** parser, payload handling, staging, timers, renewal and
outcome coordinator; mock-parent success alone cannot establish parent safety.
Cross-language golden JSON and Arrow fixtures exercise the normative tables and
the stream transport on Linux and macOS.

The harness injects known credential canaries into both authentication stores
and the parent's backend environment and inherited descriptors, scans control documents/results/diagnostics,
and verifies that raw stderr is not publicly teed. It does not claim universal
secret detection in arbitrary dataset content.

| Scenario | Required result |
|----------|-----------------|
| disjoint interface versions / unsupported platform | `UNSUPPORTED_CAPABILITY`; no operation or mutation |
| manifest/handshake identity or version mismatch | `ADAPTER_FAILURE`; no binding |
| unknown capability/data-plane offer | refusal without fallback |
| user/system root shadowing and explicit root override | one deterministic winning manifest; override suppresses every fallback |
| unsafe owner/group permissions, changed executable after hashing | refusal under §1 checks; no spawn of an observed changed object |
| external schema reference or wrong mode's table source | offline rejection before auth/mutation |
| optional defaults / inline versus file declarations | same normalized effective request and identity |
| namespaced flags and requires-connection/auth descriptors | adapter parsing, capability checks and sanitized result validation |
| terminal pull after source pruning or authentication expiry | original receipt without source/authentication/preparation/apply |
| pull receipt under changed request/default/source selector | `REQUEST_MISMATCH`; no reapplication |
| latest pull without trustworthy prior commitment | no source resolution until not-committed is established |
| initial managed transaction fails | no root/workspace binding or successful receipt left behind |
| ambiguous first commit / later checkpoint advancement | fence/recover and return original immutable receipt if committed |
| initialized receipt store is missing | `PROTOCOL_FAILURE`/unknown, never empty history |
| journaled second-table failure or delayed old writer | preserve dirty union, block consumers, fence/repair; no claimed rollback |
| journaled append capability | prove replay cannot duplicate insertions or refuse capability |
| foreign-root/application input during build discovery | reject before run/hold/output execution |
| tracking pull between external preparation and invocation | immutable prepared generation is used |
| pruning races with source hold confirmation | fail preparation or retain complete held input under GRV rules |
| discovery response / preparation commit / context write lost | exact original identities recovered, or incomplete preparation abandoned |
| held/dropped/explicit-empty/omission-only build | exact selected-output coverage, correct existing completion schema |
| second output query fails | no successful candidate or accepted completion; `BUILD_INCOMPLETE` |
| source/self/output query dependency violates contract | fail before output writes; no implicit provenance |
| successful build emits rows before completion acceptance | parent rejects; export is a separate phase |
| wrong run/workspace/digest/mapping in candidate or driver record | rejected before export/allocation |
| acceptance response lost or retry changes completion timestamp | recover identical stored record, or `REQUEST_MISMATCH` |
| no writers remain but database connection still open at attestation | completion forbidden until connection closure |
| external session reopened in fresh process | original session/accepted digest exported without managed invocation |
| incomplete execution leaves prepared/seeded tables | no inferred completion or rerun/adoption |
| adapter loses GRV owner during execution/export | parent disables allocation/publication and cancels/awaits writers |
| build export response lost / accepted capture present | fixed outputs or accepted capture reused, never SQL rerun |
| context copies / engine aliases / hard-linked database | canonical session/workspace lock serialization; hard-link aliases rejected |
| adapter or driver dies with live database child | child's lock/file ownership remains busy; no stolen access |
| engine-free renew during an external invocation | session/backend renewal without workspace contention |
| closed/unknown/duplicate frame fields or non-safe numerics | `PROTOCOL_FAILURE`; no parser rounding or invented defaults |
| started event followed by repeated stream req | valid correlation; request stays active until terminal/drain |
| byte-fragmented or coalesced frames and payloads, partial writes | exact frame and payload boundaries on Linux and macOS |
| payload shorter than `size`, EOF mid-payload, or `size` above `max_batch_bytes` | `PROTOCOL_FAILURE`; partial payload discarded, channel closed |
| another frame or document chunk interleaved inside a payload | `PROTOCOL_FAILURE` |
| peer closes while parent writes | handled EPIPE/EOF, not parent SIGPIPE death |
| metadata >1 MiB, malformed chunks/digest, >64 MiB declaration | bounded chunking or pre-mutation rejection; no unbounded buffering |
| standard V5 Schema + RecordBatch fixture across languages | correct body-relative offsets and identical logical rows |
| legacy prefix, compression, dictionaries, extra messages or offset overflow | structural rejection without parser crash |
| bad schema/count/partition values or total table count | abort capture/export; no partial acceptance |
| slot reuse/ack mismatch/non-dense seq or terminal before draining | `PROTOCOL_FAILURE`; no invalid completion acceptance |
| zero-row extraction/build table | explicit completion accepted under ordinary empty rules |
| throttled, all credits occupied, and oversized source page | source/scratch budgets enforced; reader still receives cancel |
| producer silent with only one busy slot | producer deadline remains armed |
| all credits occupied / one consumer stuck | producer timing pauses; independent consumer timeout still cancels |
| encode/receive/decode/Parquet failure or oversized single row | bounded retry/typed failure where catchable; kernel death follows crash recovery |
| cancel races with committed apply result | preserve result/resolve receipt; late most-recent cancel is idempotent |
| cancel races with batch/checkpoint ACK or next request | serialized pre-cancel ACKs precede cancel; later events drained without ACK; matching cancel ACK required before another request |
| cancel ACK before actual writer stop | conformance failure; ACK is not merely signal receipt |
| handshake timeout / partial frame / close timeout | applicable bounded ladder without illegal req-less cancel |
| checkpoint lost before ACK / crash mid-table | no unacknowledged-snapshot adoption; whole-table retry or fresh full attempt |
| callback failure after publication / shutdown failure after receipt | known commit plus durable pending/cleanup error; no duplicate work |
| abort after ambiguous GRV publication actually committed | report committed outcome; do not mark unpublished |
| cleanup while commit/outcome or writers remain unresolved | preserve evidence and return busy/unknown |
| any control frame/document contains credential canary | conformance failure |
| backend credentials inherited or raw credential stderr rendered | conformance failure |
| malformed adapter/public error object or new error enum | reject/map using the existing companion output schema |

## 10. Later additions

- Additional output data planes for languages without an Arrow IPC writer,
  introduced under a new interface contract.
- An optional shared-memory or descriptor-passing data plane, if measured
  throughput shows the v1 stream plane is a bottleneck. It would carry its own
  platform immutability rules and be negotiated through `data_plane`.
- Windows transport.
- Parallel extraction with explicit correlation/credit rules.
- Remote adapters with authenticated transport and a deliberately revised trust
  boundary; local socket and process semantics are not presumed portable over a network.
- A daemon mode, if separately specified; v1 uses per-command supervised processes.
