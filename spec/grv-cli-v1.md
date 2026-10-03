# GRV Client v1

|         |            |
|---------|------------|
| Status  | draft      |
| Version | 1          |
| Date    | 2026-10-03 |
| Extends | GRV v2     |

## Relationship to the GRV spec

This document specifies the **GRV client**: a command-line tool for moving
dataset state between GRV and a local transformation engine, inspecting
history, and managing retention and recovery. It defines no new GRV storage
layout. Pull checkpoints and attempt receipts, build-session contexts and
completion records, and engine working tables
are consumer state outside GRV; session runs, claims, dependency holds, and
publications use the existing
[GRV v2 specification](grv-v2.md).

`push` is the CLI command for exporting engine output and publishing a GRV
revision. **Publication** and **publish step** refer to the underlying GRV
protocol.

The documents are reviewed together. Where they disagree, GRV v2 wins and this
specification is amended. The client MUST implement the complete protocols it
uses, including recovery and fencing; the algorithms below do not replace
those protocols.

## Overview

GRV is the durable ingest and release layer. The engine is the build layer:
it consumes ordinary tables and produces ordinary tables. A pipeline driver
coordinates the commands and the engine through an explicit **build session**:

```text
GRV inputs → pull → prepare session → external build → push(session) → GRV product
```

- `pull` resolves one dataset revision and applies the specified refresh to
  its managed engine tables and checkpoint in **one transaction**.
- `session prepare` fixes the build's inputs and target base revision,
  confirms their dependency holds, and prepares private engine tables.
- The driver runs the engine against those tables, renews the run lease, and
  records which outputs completed successfully after all writers have stopped.
- `push --session --build-result` exports the completed build, finalizes and seals its
  GRV run, and applies the resulting change set.
- Inspection commands describe committed state, engine materializations,
  history, and retention. `pin`, `unpin`, `gc`, and `recover` use the core
  coordination protocols.

Models remain GRV-unaware. The driver supplies ordinary input and output table
names from the session context; the client never invokes the engine itself.

## Scope

### In scope

- A shared declaration format, Arrow schema contract, backend adapter, and
  DuckDB engine adapter.
- One GRV root for the entire engine workspace, all inputs, and all products.
- One dataset per pull invocation, with one transaction for all its refresh
  changes; separate datasets have separate completion checkpoints.
- One target dataset per build session and publication.
- Full or incremental refreshes within the same dataset transaction boundary.
- Session preparation, inspection, renewal, and abandonment, with `push`
  performing finalization.
- Root initialization; dataset and revision inspection, metadata diffs,
  engine status, and format/integrity verification.
- Explicit pins on committed dataset revisions, pin release, one-dataset GC,
  and recovery of interrupted dataset operations and runs.

### Out of scope

- Transformations and engine invocation, which belong to the pipeline driver.
- Raw ingestion, row-level diffs, and automatic retention policies.
- CLI mutations of table, partition, or individual-version pins; root
  parameter updates, restoration, and dataset retirement.
- Cross-root reads or derivation, multi-dataset atomic refreshes, and batch
  declaration-directory invocation.
- Concurrent external processes accessing one native DuckDB file, server
  adapters, network-hosted engine files, and per-output dependency narrowing.
- Other engine adapters until their transaction and type guarantees are specified.

## The shared core

### The interface

```console
grv init --grv=<GRV_DIR> [--max-clock-skew-seconds N] [--max-lease-ttl-seconds N] [--pending-grace-seconds N]
grv ls --grv=<GRV_DIR> [<dataset>] [--table <table>] [--revision N]
grv show <dataset> --grv=<GRV_DIR> [--revision N] [--retention]
grv status <dataset> --grv=<GRV_DIR> [--engine=<store>]
grv log <dataset> --grv=<GRV_DIR> [--limit N]
grv diff <dataset> --grv=<GRV_DIR> --from N --to <N|latest>
grv verify <dataset> --grv=<GRV_DIR> [--revision N] [--full]
grv pull --decl=<yaml> --grv=<GRV_DIR> --engine=<store> [--revision N] [--target <schema>] [--attempt <id>]
grv session prepare --session=<context.json> --decl=<yaml> --grv=<GRV_DIR> --engine=<store> [--target <dataset>]
grv session show --session=<context.json>
grv session renew --session=<context.json>
grv session abort --session=<context.json>
grv push --session=<context.json> [--build-result=<result.json>]
grv pin <dataset> --grv=<GRV_DIR> --revision N --reason <text> [--pin <id>]
grv unpin <dataset> --grv=<GRV_DIR> --revision N --pin <id>
grv gc <dataset> --grv=<GRV_DIR> [--dry-run | --apply]
grv recover <dataset> --grv=<GRV_DIR> [--run <run-id>] [--dry-run]
```

The `session` group owns preparation, inspection, renewal, and abandonment;
`push --session` finalizes and publishes a completed build. Its first
finalization requires `--build-result`; retries may use the already accepted
completion record described below.
A session is required even for direct exports or omission-only publications.
Such a driver may have no transformation step and no external inputs.
For a direct export, the driver populates the session's private output tables
after preparation; finalization does not adopt pre-existing shared tables.

- **`--grv`** is a local path, `s3://bucket/prefix`, or `gs://bucket/prefix`.
  Every dataset reference is a canonical GRV dataset name relative to this
  root. Full dataset paths, dataset URIs, and per-declaration root overrides
  are rejected. Source and target `grv` names must equal their declaration's
  `dataset`, except for a target dataset override resolved during preparation.
- **`--decl`** names one declaration file. Its effective configuration,
  including resolved overrides and code fingerprints, is fixed at preparation.
- **`--engine`** identifies one DuckDB database. The client binds the engine's
  managed state to the canonical GRV root on first managed write. Commands and
  session inputs from another root are rejected; repointing requires a new workspace.
  V1 accepts a local persistent native database file, not `:memory:`, a network
  filesystem, or a server connection. The binding includes a generated
  `workspace_id` UUID, preserved for the workspace's lifetime.
- **`--revision N`** selects one committed revision for commands that accept
  it. Omitting it selects the current revision once, where a default is
  supported. Selection does not create a GRV retention pin; only `pin` does.
- **`--target`** overrides the engine target schema for pull or the product
  dataset at session preparation. Finalization cannot retarget a session.
- **`--session`** names a durable local context file emitted by `session prepare`.
  It identifies a committed engine session record and its normal GRV run.
  Later commands load the fixed root, engine, declarations, and mappings from it.
- **`--attempt`** supplies a canonical UUID for a pull retry. Without it the
  client generates an ID and reports it on stderr before any refresh mutation.
  Internal retries keep that ID; a new invocation without it requests a new pull.
- **`--build-result`** names the driver's durable JSON completion record. It is
  consumer state, never a GRV run file or a publication commit marker.
- Backend credentials use the backend's normal environment configuration.

Paths and URIs are canonicalized before comparison. Equivalent spellings of
one root do not create separate identities; resolving credentials never changes
which root a context names.

### DuckDB process ownership

V1 serializes **all database access** between the CLI and external engine
invocations using one non-expiring OS process lock for the workspace. The
adapter and driver use the same lock identity, derived from the canonical
database path; symlink aliases resolve to it and hard-link aliases are rejected.
The persistent lock file is `<canonical-engine-path>.grv-lock`; participants
use an exclusive `flock` and never unlink or replace it while the workspace
exists. The database file must not be moved or replaced during its lifetime.
The driver must implement this lock protocol or use an adapter-provided helper.
DuckDB's own file lock remains an additional safeguard. See
[DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency).

Every command that opens the engine holds the workspace lock until its
connections close. The driver holds it throughout the external invocation,
including child processes, and releases it only after all database users and
writers have stopped and all connections have closed. Contention returns
`ENGINE_BUSY` (exit 3), without engine mutation; v1 does not queue or steal a
live lock. A driver crash does not authorize a new engine user while an old
process still has the database open: DuckDB connection contention is also busy.

`session renew` requires no engine connection or workspace lock. It validates
the self-contained context and GRV identities, then renews the run through the
backend. It therefore continues during a build. Backend-only inspection, pin,
GC, and recovery commands likewise do not open DuckDB. `status --engine`,
`session show`, preparation, pull, push, abort, and engine cleanup require the
workspace lock. A busy engine inspection may report already observed GRV facts
in its partial error result; it cannot report an unobserved checkpoint.

The session mutation lock below is distinct from the workspace lock. If both
are needed, acquire the session lock first and attempt the workspace lock
without waiting; release locks before reporting busy. Neither lock is a GRV
lease. This model permits one active external invocation per engine file;
private tables preserve provenance but do not permit concurrent process access.

### Common command behavior

All commands validate `grv.json` and use the backend contract. Only `init`
initializes a root; other commands report an uninitialized or damaged root
rather than inventing configuration. Dataset arguments are canonical names
within the selected root. Backend-only commands do not require an engine or
declaration. Read-only commands never establish an engine binding, acquire
leases, complete pending operations, create receipts, or change retention.

Listings discover objects; they do not prove commitment, availability, or an
operation's success. Revision-based commands resolve a committed revision by
the receipt or chain rules in GRV §7. A command samples `LATEST` once for each
default or `latest` selector and reports the resolved number. Read-only
commands may use the last committed revision while an operation is pending,
and report the observed coordination state. They take no reader pin: later GC
can make data unavailable during or after inspection. A root-wide listing and
an engine/GRV comparison are observations, not a cross-dataset transaction.

When a default selector observes `LATEST.revision: 0`, inspection and
verification use the empty initial state without looking for a revision-0
parquet. A missing `LATEST` is reported as missing coordination metadata, not
silently converted into revision 0. Creation of a new target's initial `LATEST`
belongs to the mutating core protocol.

All commands support `--json`. Stdout contains one JSON document conforming to
[the output schema](grv-cli-v1-output.schema.json), with `output_version: 1`,
`command`, canonical `root` (null until known), `ok`, `exit_status`, `result`,
and `errors`. Command names are `init`, `ls`, `show`, `status`, `log`, `diff`,
`verify`, `pull`, `session prepare`, `session show`, `session renew`,
`session abort`, `push`, `pin`, `unpin`, `gc`, and `recover`; an unrecognized
command uses `unknown` in an error envelope. Results identify resolved
revisions and partial progress; errors have stable codes and affected object
identities. Human-readable output presents the same
facts. Progress goes to stderr. Inspection output omits credentials and owner
tokens; the protected session context still contains the token needed by its
driver. Collections are ordered by canonical object identifiers, with revision
history newest first.

Successful results contain every field required by their command's schema;
`errors` is empty and `exit_status` is 0. Error results are null or a typed
subset of that command's result fields: an absent field means its value or
effect was not established, never rollback or a default value. Arrays in a
partial result contain only established observations or effects, not a claim
that enumeration finished. `errors[0]` is the primary error and determines the
exit status; subsequent errors are ordered by object identity. Every error
contains `code`, `message`, nullable `object`, and `retryable`. `retryable`
means the same fixed request can be retried when the blocking condition clears;
it is not permission to change a session base or to ignore an unknown outcome.
Revision/version numbers and byte totals use canonical decimal strings in
normalized results to preserve int64 precision; revisions and versions must
still fit GRV's `0..2^63-1` and `1..2^63-1` ranges respectively. Core metadata
embedded in `details` retains GRV's JSON representation with credentials and
owner/lease tokens removed; its returned fields follow the core contracts.

Exit status is `0` for a completed request, including an informational stale
status, a diff with changes, a dry run, or an idempotent no-op. Nonzero statuses
are `2` for invalid arguments or declarations, `3` for conflict, busy resources,
or lost ownership, `4` for not-found or unavailable requested state, `5` for
integrity or protocol failure, and `6` for backend/engine failure or an outcome
that could not be resolved. Partial mutation is reported with a nonzero status
and its known committed effects; the client never reports rollback of an
already committed GRV operation. Existing attempt identities must be resolved
before a retry starts another attempt.

V1 error codes are closed; additions or incompatible output changes require a
new output version. Producers emit only schema-defined fields; consumers must
select a supported version before interpreting a response.

| code | exit | meaning |
|------|------|---------|
| `INVALID_ARGUMENT` | 2 | Invalid command, flags, or identifier |
| `INVALID_DECLARATION` | 2 | Invalid declaration, unsupported type/extension, or failed declared check |
| `REQUEST_MISMATCH` | 2 | Attempt or completion identity reused with another fixed request |
| `BUILD_INCOMPLETE` | 2 | Missing successful completion evidence for a selected output |
| `ENGINE_BUSY` | 3 | Workspace/process lock or DuckDB connection is busy |
| `STATE_CONFLICT` | 3 | Target ownership, output-plan, or GRV state conflict |
| `OWNERSHIP_LOST` | 3 | Required run, claim, or dataset ownership was lost |
| `NOT_FOUND` | 4 | Requested root, dataset, revision, session, or scoped pin is absent |
| `UNAVAILABLE` | 4 | Committed data needed by the command is missing or pruned |
| `INTEGRITY_FAILURE` | 5 | Hash, schema, or immutable-object contents are inconsistent |
| `PROTOCOL_FAILURE` | 5 | Malformed coordination, uncommitted references, or inconsistent consumer metadata |
| `BACKEND_FAILURE` | 6 | Backend operation failed without another established classification |
| `ENGINE_FAILURE` | 6 | Engine operation failed without another established classification |
| `OUTCOME_UNKNOWN` | 6 | A committing attempt cannot yet be resolved |

### JSON result contracts

The output schema defines each command's full and partial result types, shared
object identities, state entries, session records, retention explanations,
and progress records. All producers must validate their envelope against it.
Formats such as UTC timestamps are validated as well as JSON structure;
cross-field invariants below remain mandatory even where JSON Schema cannot
express them. Core and registered extension objects are the only open metadata
objects; client-defined result objects have no unspecified fields.

| command | result contract |
|---------|-----------------|
| `init` | Creation flag, core format/version, and effective root parameters |
| `ls` | Scope, nullable dataset/revision, and object-identified items with observed coordination/details |
| `show` | Revision/predecessor/operation, state entries, per-table layout/baseline/revision schema and provenance, optional retention explanation |
| `status` | Current state, lease/pending operation, runs, and optional engine binding/materializations/sessions |
| `log` | Limit, `has_more`, and newest-first committed entries with change counts and run IDs |
| `diff` | Resolved endpoints, `changed`, entry changes, table membership changes, and known/unknown schema comparisons |
| `verify` | Resolved scope, full-hash mode, named check outcomes, and unavailable objects |
| `pull` | Attempt/workspace IDs, fixed scope/selector, committed revision/generation/time, and `replayed` |
| `session prepare`, `session show` | Canonical context/engine paths and the session's fixed inputs, mappings, completion, plan, attempt, outcome, and observed run |
| `session renew` | Dataset/run, open phase, renewed expiry, and `renewed: true` |
| `session abort` | Dataset/run, known terminal outcome, and finalized entries |
| `push` | Dataset/run, accepted completion digest, known published/no-op outcome, and `replayed` |
| `pin`, `unpin` | Fully scoped pin/audit/release information and `no_op`; unpin also reports remaining protections |
| `gc` | Mode, per-version progress/reasons, releasable/released holds, completed operations, byte estimates, and waiting work |
| `recover` | Mode/scope, per-run progress, completed operations, pending and waiting work |

`replayed` means the existing recorded result was returned rather than a new
refresh/publication. `no_op` means the requested retention mutation was already
satisfied. A session outcome of `no-op` means no revision publication occurred;
its `revision` reports the observed predecessor, and `operation_id` is null.
`published` reports the committed revision/operation; `aborted` has neither.
Logical schemas in normalized table/schema-comparison results are
`{columns: [...]}` objects using GRV's ordered column representation.
Session `export_plan` is a normalized summary of its durable plan: prepared
base, completion digest, write/reuse/empty contributions with nullable assigned
version/run identities, omissions, and held tables. It is not a substitute for
the authoritative allocation records. Input mappings identify each source
table and materialization generation; `input_revisions` deduplicates the
external holds by dataset/revision, independently of those mappings.
Version progress `bytes` is null when unknown; `known_bytes` sums only known
sizes of eligible/decided prune candidates, not protected versions or an
assertion of physical disk space reclaimed. `has_more` is established by
observing the next committed predecessor after the requested log limit.

For example, a successful first pull returns:

```json
{
  "output_version": 1,
  "command": "pull",
  "root": "/data/grv",
  "ok": true,
  "exit_status": 0,
  "result": {
    "attempt_id": "359c6d0f-a9c1-4ae6-b804-0742a5e2b9de",
    "workspace_id": "b2c81f94-45a6-4cb3-b937-82916c1e0842",
    "dataset": "mt_month_stats",
    "target_schema": "raw",
    "revision_mode": "tracking",
    "materialization_mode": "local",
    "requested_revision": "latest",
    "committed_revision": "42",
    "generation_id": "c2dc35d6-05ae-4ff8-95db-8d4aaf6f1b88",
    "pulled_at": "2026-10-03T10:00:00Z",
    "replayed": false
  },
  "errors": []
}
```

### The declaration: two layers

Each dataset has one declaration conforming to
[the declaration schema](grv-cli-v1-declaration.schema.json), with required
`declaration_version: 1` and `kind: pull` or `kind: push`. The **type schema**
is Apache Arrow, either inline or referenced as a serialized `.arrow`/`.ipc`
schema. The **envelope**
is the client convention for locations, partition keys, refresh or publication
scope, and data checks. It is not a new type system.

Parse YAML 1.2 using JSON-compatible values. Reject duplicate keys, unknown
fields, non-string map keys, custom tags, aliases, and non-finite numbers.
There is no environment interpolation, implicit include, or URI-based schema
fetch. A schema reference is `{ipc: <path>}`: a local path resolved relative to
the declaration directory, containing exactly one encapsulated Arrow IPC
Schema message, with no record batches or trailing messages. This is the
encoding produced by [Arrow Schema.serialize](https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema.serialize),
not an IPC data file inferred from its suffix. Referenced schemas must meet
the same type restrictions as inline schemas. Arrow field names and order are
preserved; Arrow nullability does not substitute for a declared `not_null`
check, and metadata does not introduce an unsupported GRV logical type.

Inline fields are ordered `{name, type, ext?}` objects. V1 spellings are
`bool`, `date32`, `int64`, `double`, `utf8`, `binary`, `decimal128(p,s)`,
`timestamp(ms)`, `timestamp(us)`, `timestamp(ns)`, `timestamp(ms,UTC)`, and
`timestamp(us,UTC)`, with no whitespace inside type spellings. Decimal
precision is 1–38 and scale is 0–precision. Other types, timezone spellings,
nested types, and TIME mappings are unsupported in v1. IPC references do not
expand this supported set. Column `ext` and table `extensions` use the
registered GRV extension contracts; referenced-schema column properties are
supplied through the table's `column_ext` map. Inline `ext` and `column_ext`
must not both address the same column.

Defaults are `refresh: changed`, `target.mode: local`, `self_input: false`,
`selection.policy: changed`, empty input/selector/check lists, empty extension maps,
and empty `code_fingerprints`. `code_fingerprints` maps nonempty driver-defined
labels to lowercase SHA-256 strings; it is fixed before preparation, not
inferred from output tables. Unknown fields inside registered extension
configuration follow that extension's contract. All other declaration objects
are closed. Pull table names, push output mappings, input aliases, schema field
names, partition keys, and target physical names must be unique in their scope;
engine comparisons account for DuckDB's identifier equality. Quote identifiers
when generating SQL; declaration strings are never SQL expressions.
Engine schema/table names and aliases match `^[a-z0-9_][a-z0-9_-]*$`;
qualified input names have exactly the form `schema.table`. GRV names retain
their stricter core grammar. `_grv` and prepared session namespaces cannot be
pull targets. Empty or duplicate resolved schema fields, invalid decimal scale,
unknown `column_ext` fields, inconsistent source/target dataset names, or a push
schema list that does not match its output mappings are declaration errors.

The effective declaration expands defaults, resolves overrides and local paths,
replaces IPC references with ordered supported fields, and preserves required
extension properties. Its `declaration_sha256` is the lowercase SHA-256 of
that JSON value canonicalized through [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785).
Credentials and owner tokens are excluded. Pull request identity additionally
includes canonical root, workspace ID, target schema, and the requested
revision selector (`latest` or an explicit decimal string). A session's
private mappings and fixed input revisions are recorded separately and cannot
be changed by replacing its declaration file.

`checks` contains `{table, not_null: [column, ...]}` objects, with nonempty,
unique column lists. Table names refer to GRV table names in the declaration;
columns must exist in its effective schema. Pull checks cover every declared
table after schema alignment, regardless of full/changed refresh; push checks
cover completed outputs selected for export, including explicit empty outputs.
V1 defines no SQL-expression or row-count check syntax. Schema validation is
required even when a table is held or no business-data checks are configured.

Schema validation uses GRV's logical types, including timestamp precision and
UTC semantics. Partition columns `_{key}_` are ordinary string columns and
must be included in the effective schema; their names are retained in the
engine. Files with older prefix-compatible schemas are expanded with nulls
before declaration checks. A declaration cannot silently project away GRV
columns or cast values to pass validation.

### The type mapping (Arrow → engine)

The engine adapter preserves both values and the declared GRV logical type
when importing and exporting. V1's supported DuckDB mappings are:

| Arrow / GRV meaning | DuckDB |
|---------------------|--------|
| `date32` | `DATE` |
| `int64` | `BIGINT` |
| `double` | `DOUBLE` |
| `utf8` | `VARCHAR` |
| `binary` | `BLOB` |
| `decimal128(p, s)` | `DECIMAL(p, s)` |
| `timestamp(ms)` without timezone | `TIMESTAMP_MS` |
| `timestamp(us)` without timezone | `TIMESTAMP` |
| `timestamp(ns)` without timezone | `TIMESTAMP_NS` |
| timezone-aware `timestamp(ms)` or `timestamp(us)` | `TIMESTAMPTZ` |
| `bool` | `BOOLEAN` |

The adapter retains the declared unit and UTC flag in consumer schema metadata,
so a native engine type does not determine the export schema by itself. Exports
must be exactly representable in the declared type; a millisecond declaration,
for example, rejects values with a nonzero sub-millisecond component.
Timezone-aware values denote instants and use UTC for interchange; unadjusted
values retain their local-time meaning.

DuckDB has no native timestamp type combining nanosecond precision with timezone
awareness. V1 rejects that combination before materialization or version
allocation. It likewise rejects any other type it cannot preserve faithfully,
including an ambiguous Arrow representation of a GRV TIME UTC flag. Additional
mappings require explicit round-trip rules; there are no implicit truncations
or conversions to strings. See [DuckDB timestamp types](https://duckdb.org/docs/current/sql/data_types/timestamp).

Writers also honor GRV table extensions and column `ext` properties. An
unsupported required extension makes publication fail before acquiring claims
or registering schemas, as GRV §3 requires.

## `grv init`

Initialize one empty GRV root through GRV §2's conditional creation of
`grv.json`. CLI defaults for a new root are 30 seconds of maximum clock skew,
900 seconds of maximum lease TTL, and 604800 seconds of pending grace. Values
are integer seconds; skew is nonnegative, TTL and grace are positive, and TTL
must exceed skew so a conforming lease can be renewed. These values configure
the store; they are not per-command timing overrides.

Before creation, confirm that `datasets/` is empty. A missing `grv.json` with
existing dataset objects is a damaged store and fails initialization. If
another initializer wins the conditional create, reread and validate its
configuration. An existing valid root makes `init` idempotent; explicitly
provided parameters must match it, while omitted flags accept its stored
values. Updating an existing root's parameters is outside v1.

Report the canonical root, format/version, effective parameters, and whether
the root was created or already initialized. Initialization creates no dataset,
revision, engine binding, or build session.

## Inspection commands

### `grv ls`

With no dataset argument, discover dataset names under `datasets/` and report
their observed current revision and retirement state. Missing coordination
metadata is reported as absent/unknown, never as proof of an empty committed
dataset. Listings may include an unpublished dataset being prepared by a
writer; its physical presence does not imply a published state.

With a dataset argument, resolve the requested revision and list the tables
present in its state. With `--table`, list that table's canonical partition
objects and selected version/run identities. `--table` requires a dataset;
`--revision` is valid only with a dataset. Physical table folders or orphan
versions do not become state members merely because a listing finds them.
Empty versions remain members. An empty revision produces an empty table list.

### `grv show`

Describe one committed revision, defaulting to the observed current revision:
its predecessor, publication operation, table/partition membership, selected
versions and producing runs. Include layouts, the current registered schema
baselines, and the revision's logical table schemas and provenance when the
referenced manifests can be read. Label baseline and revision schemas
separately; a later baseline extension is not evidence of a historical schema.

Inspection remains useful after pruning: retained revision/run/operation
records are displayed, while unavailable manifests, data, or schema details
are explicitly marked unavailable or not checked. It does not claim full
integrity verification. Corrupt records are errors, not omitted output.

`--retention` lists pin IDs and scopes, active/released status, creation audit
information and reasons, dependency holds and their consumer datasets/runs,
and the observed protections of the selected revision's versions. Include
current-state protection, supersession grace, and applicable table, partition,
or version pins created by other conforming tools. Missing supersession
receipts are reported as protection whose grace has not yet started; inspection
does not create them. Releasable holds are distinguished from released holds.
Unreadable protection records make the explanation incomplete and return a
protocol/backend error rather than claiming the data is unprotected.

### `grv status`

Report the dataset's observed current revision, retirement, dataset lease and
pending operation, and discovered run controls with their phases and lease
expiry. An expired lease makes recovery eligible; it does not prove recovery
has happened. Report run IDs and holders without exposing owner tokens.

With `--engine`, read the bound engine's checkpoints, mappings, and session
records in one consistent engine read transaction, then compare them with the
observed GRV state. Show each tracking/historical materialization's committed
revision, generation, and target mapping; distinguish current, behind,
historical, and uninitialized state. Inconsistent metadata is an error.
An unbound engine is reported as uninitialized, without binding it or guessing
provenance from its ordinary tables. A foreign-root binding is rejected.

Session status includes its fixed base/input revisions, recorded outcome or
pending publication attempt, and observed GRV run phase. Table existence cannot
prove that an engine job succeeded. A behind materialization or a healthy open
run is informational and leaves the command successful.

### `grv log`

Walk the committed `previous_revision` chain from the observed current revision,
newest first. `--limit` defaults to 20 and must be a positive integer. Each
entry reports revision/predecessor, creation time, publication operation,
change summary, contributing run IDs, and optional reason/code metadata from
the retained records. Orphan revision files are excluded. A missing or invalid
chain record is an error; the client never invents a gap or repairs history.

Log does not require historical data to remain available. A revision's
commitment is a historical fact; its data availability is separate and may be
unknown until checked. A dataset at revision 0 has an empty log.

### `grv diff`

Resolve both endpoints in the same dataset. `--from` is an explicit revision
number; `--to` is an explicit revision or `latest`, resolved once. Revision 0
is accepted as the empty initial state. Every nonzero endpoint must be
committed; an orphan path is not an endpoint.

Compare revision entries by canonical `(table, partition)`: report added,
removed, or changed selections with before/after version and run IDs, and
table membership changes. Logical schema differences are reported when the
manifests needed to establish them are available; otherwise mark those details
unknown. The revision-entry comparison still works after data pruning and
does not require warehouse materialization or row-level comparison.

A changed version is a selection change, not proof that row values differ.
Return `changed` explicitly in JSON. Both an equal diff and a diff with changes
exit successfully; invalid or unavailable endpoint records are errors.

## `grv verify`

Resolve one committed revision and check its referenced versions through GRV
§4–§7 and §9: revision metadata and uniqueness, layouts/partition identifiers,
manifests, sealed run/control agreement and entries, schema baselines,
confirmed dependency holds and resolvable references, and all manifest-listed
files. Check tombstones and file sizes/validators. A validator mismatch requires
full SHA-256 verification even in the default mode. `--full` computes every
data file's SHA-256 in the selected revision regardless of a matching validator.
Provenance checks validate commitment, selector membership, and confirmed active
holds as the core fence requires. Deduplicate repeated version/file checks
within this invocation.

This is format and integrity verification, not engine execution or declaration
business-data checks. Report scope, resolved revision, checks performed,
unavailable objects, and integrity errors. A pruned/missing data object is
unavailable; a present object with inconsistent metadata or a failed hash is
an integrity failure. No observed tombstone is ignored because residual data
still exists. A reference never committed or a malformed coordination record
is a protocol failure.

Verification acquires no pin or lease and makes no repairs. Its success
describes the objects checked during the invocation, not future retention or
a root-wide snapshot. Concurrent pruning is surfaced as unavailability. Users
who need continuing availability first establish a revision pin.

## `grv pull`

### The pull declaration

```yaml
# <decl>/mt_month_stats.yml
declaration_version: 1
kind: pull
dataset: mt_month_stats
source:
  grv: mt_month_stats
target:
  engine: duckdb
  schema: raw
  mode: local                   # local | s3-view
refresh: changed                # full | changed; both commit in one transaction
tables:
  - name: mt_month_stats
    schema:
      - { name: period,       type: date32 }
      - { name: impressions,  type: int64 }
      - { name: clicks,       type: int64 }
      - { name: spend,        type: 'decimal128(18,4)' }
      - { name: _period_,     type: utf8 }
    partition_keys: [period]
checks:
  - table: mt_month_stats
    not_null: [period, impressions, clicks, spend, _period_]
```

Each declared GRV table maps to `<target.schema>.<table.name>`; an explicit
`target_table` on a table entry may override that mapping. Mappings must be
unique. The declaration covers every table in the resolved revision; an
undeclared source table is an error. Declared or previously managed tables
absent from that revision are emptied, not silently left at an older state.
When a declaration changes a target mapping, the same refresh transaction
empties the old owned target and updates its ownership record. The old mapping
cannot be silently abandoned or taken over by another dataset.

`partition_keys` must match `.layout.json`. `_period_` contains the canonical
string partition value; `period` remains an independent data column of type
`date32`. The client does not infer a date encoding from the partition name.

### The pull algorithm

1. **Identify and resolve the request.** Read `grv.json` and validate the
   effective declaration. Acquire the workspace lock, open DuckDB, and
   validate its root/workspace binding and consumer metadata. Select the
   caller's `--attempt` or generate and report a fresh UUID. Look up that ID
   in `pull_attempts` **before resolving or downloading source data**. A
   matching committed receipt returns its original outcome; a different
   request returns `REQUEST_MISMATCH`. A retry never re-applies a successful
   attempt or rewinds newer materializations. If no receipt exists, resolve
   one committed revision. A historical revision's supersession
   receipt may prove commitment; otherwise walk the chain from `LATEST`.
   A revision parquet existing at a named path is not sufficient: orphan
   revisions are not dataset states. Revision 0 denotes an unpublished,
   empty initial state.
2. **Prepare the refresh.** Resolve all partitions and every manifest-listed
   data file, reject `.pruned` versions, and check logical schemas and data
   rules. Local mode downloads and verifies every file's size and SHA-256
   into staging outside the managed tables. Any source failure aborts before
   refresh. S3-view verification follows the backend rules below.
3. **Plan under serialization.** Retain the workspace lock throughout this
   invocation; no other engine command or external build can replace its
   metadata. Read the current committed checkpoint and ownership records;
   an earlier diff is only a hint. Conflicting target ownership or historical/tracking
   mappings are errors.
4. **Apply and commit.** Use the selected `attempt_id` and open **one DuckDB
   transaction**. Apply the whole specified refresh: all affected tables,
   partitions, schema changes, table or view creation, omitted-table
   emptying, ownership records, per-table `pull_meta`, and the dataset
   completion checkpoint, and immutable successful attempt receipt. Run all
   declaration checks over the resulting managed scope inside that transaction.
   Commit once after every change succeeds. Any failure rolls everything
   back; per-table commits or swaps followed by a separate checkpoint write
   are forbidden. Initial root/workspace binding is also part of this first
   managed transaction; a failed first pull cannot leave a bound partial workspace.

For `refresh: full`, rebuild every managed table from the target revision.
For `refresh: changed`, diff against the committed revision and update changed,
added, and removed partitions, reconciling the complete target table schemas.
Neither strategy exposes a partial dataset. Tables unaffected by an incremental
refresh are still represented by the new completion checkpoint.

An ambiguous commit is resolved by reopening the database under the workspace
lock, allowing normal DuckDB recovery, and reading `pull_attempts[attempt_id]`.
A matching receipt proves success even after a crash and subsequent refreshes.
Absence proves no commit only after the prior writer is fenced by process/file
ownership and the complete, trustworthy receipt store is readable. Then
replan from the current checkpoint; without an explicit revision selector, an
uncommitted retry may resolve a newer `LATEST`. If the prior process still owns
the file, report busy; if durability or metadata cannot be established, report
`OUTCOME_UNKNOWN` or `PROTOCOL_FAILURE`, never infer rollback from a different
current checkpoint. Return the committed receipt's revision/generation and
`replayed: true` on replay, even if that generation is no longer materialized
or its source has since been pruned. A new pull requires a new attempt ID.

The CLI reports success only after commit is known. This is GRV §11's
transactional adapter path; the receipt is a committed result, not a separately
committed dirty-table journal. Receipts are retained for the workspace's
lifetime and are not removed by session/cache cleanup. A missing receipt table
in a previously initialized workspace is inconsistent metadata, not an empty
history. If checkpoints or per-table materialization metadata are missing or
untrustworthy but the binding, ownership, and receipt history remain trustworthy,
rebuild the full managed scope, including emptying obsolete managed tables;
never infer contents from an old watermark. A rebuild does not recreate lost
attempt history or resolve an unknown outcome.
If root identity or target ownership cannot be established, reject the refresh
and require restored metadata or a new workspace rather than guessing which
shared tables to clear.

The DuckDB adapter uses explicit transaction boundaries for data, catalog,
and metadata changes; see [DuckDB transaction management](https://duckdb.org/docs/current/sql/statements/transactions).

### Consumer metadata

All client-managed metadata lives in the engine's reserved `_grv` schema.
The root binding, table ownership, schema qualifiers, and checkpoints are
updated with the tables they describe. Conceptual records include:

```text
pull_checkpoint:
  dataset, target_schema, revision_mode, materialization_mode,
  committed_revision, attempt_id, generation_id, pulled_at

pull_meta:
  dataset, grv_table, target_table,
  grv_revision, generation_id, logical_schema

pull_attempts (immutable, keyed by attempt_id across this workspace):
  attempt_id, workspace_id, request_sha256, dataset, target_schema,
  revision_mode, materialization_mode, requested_revision,
  committed_revision, generation_id, pulled_at
```

`revision_mode` distinguishes tracking from historical materializations;
`materialization_mode` is `local` or `s3-view`. Table mappings are part of the
checkpoint scope. Each successful refresh has a new
canonical UUID `generation_id`. Every per-table record in that scope refers to
its completed generation, including empty or omitted tables. A build session accepts only
complete, mutually consistent metadata and mappings.

`request_sha256` hashes the canonical pull request identity defined above,
including the effective declaration and `refresh` strategy. It excludes the
attempt ID itself. `requested_revision` is `latest` or an explicit decimal
string, so a retry of `latest` compares the original request rather than a newly
sampled revision. The committed receipt contains the fixed successful target.
Failed transactions insert no success receipt. Duplicate receipt IDs are
rejected in the refresh transaction; an existing receipt is never overwritten.

`pull_meta` describes the current materialization. Preparation copies its
provenance into a fixed session context; final publication never rereads it to
infer which inputs an earlier build consumed. Revision parquets are retained
by GRV even after version pruning, so enumerating historical partition/version
references does not require duplicating them in `pull_meta`.

### Tracking and historical materializations

- **Tracking:** refresh the declaration's tracking targets and advance their
  checkpoint in the same transaction.
- **Historical:** `--revision N` refreshes separate targets, defaulting to
  `<tracking_schema>_revision_<N>` unless overridden. It has its own checkpoint
  and leaves tracking targets and their checkpoint untouched.

A historical target that overlaps a tracking target or another dataset's
managed target is rejected before mutation. A warning is insufficient.
Likewise, a tracking refresh cannot take ownership of historical targets.
Named revisions select fixed contents but remain subject to availability;
choosing a revision number does not guarantee retention.

### Completeness

After a successful pull, all managed tables and their checkpoint describe one
resolved source revision. Omitted partitions, empty versions, and omitted
tables contribute no rows. A referenced but unavailable version fails the
refresh, rather than being treated as omission.

An engine transaction observes one complete dataset generation. V1 serializes
external readers/builds and pulls on the same database file; it does not promise
concurrent access by separate processes. A build selects a completed generation
during preparation and uses its private session inputs. A pull between
preparation and engine invocation cannot change those private inputs.

### Backends and S3 views

All modes use GRV's backend contract, including durable local read-back.

Every Parquet reader, including the expressions saved in S3 views, uses an
explicit file list and `hive_partitioning=false` at the reader call. Directory
names such as `version=17` and partition paths must not create, replace, or
cast data columns. The adapter validates each file's GRV logical schema before
combining it with other files. Files within a version have identical schemas;
selected versions must be prefix-compatible with the resolved table schema.
Generate an explicit ordered projection of every logical column, adding typed
nulls only for permitted trailing columns absent in an older version. No
`SELECT *`, automatic schema widening, implicit lossy casts, or reader-generated
columns become managed table columns. A validated implementation may use
`union_by_name`, but that option does not replace per-file schema validation
and explicit logical projection. See [DuckDB Parquet reader options](https://duckdb.org/docs/current/data/parquet/overview#parameters).

Preserve the actual file values of ordinary columns and `_{key}_` columns;
validate the latter against the manifest partition. An empty selected file
still has its manifest's partition identity. This contract applies to local
staging files even if their temporary paths happen to look partitioned.

- **Local mode** materializes verified rows into DuckDB. Later reads need no
  GRV downloads. During preparation, source holds are still required: a
  cached copy is not a substitute for retaining auditable GRV inputs.
- **S3-view mode** creates views over the exact files listed by the resolved
  manifests, in the same transaction as the checkpoint. It never uses
  directory globs or silently ignores missing files. Before commit, every
  file must verify by size and validator or SHA-256 according to GRV §4;
  schemas and tombstones are checked as well. Validators that differ require
  full hash verification, even though the normal path avoids downloading
  all data. This mode is valid only for S3 sources.

A standalone S3 view has GRV's best-effort reader semantics and can become
unavailable after pruning. A prepared build's confirmed dependency holds
protect its source revisions for the build and subsequent GRV retention.
The session's private views reference the same fixed file sets, not live
tracking views. No broad source write permission or reader pin is needed for
this protection; consumers create holds only in their own allowed subfolder.
All query failures are surfaced. S3-view builds require network access and
pay remote read costs; local mode is the default.

## `grv session`

`session prepare` creates the context described below. `session renew` renews
its normal open run lease from the context and backend alone, without opening
DuckDB. `session abort` abandons publication through the
normal run/claim protocols. `push --session --build-result` exports and publishes its completed
build; these operations share the fixed context and session serialization.

`session show` is read-only: validate the context's identities, read its engine
record consistently, and describe the fixed declaration, target/base, input
revisions and holds, private mappings, export plan, recorded attempt/outcome,
and observed GRV run phase. Report missing or inconsistent records and expired
or lost ownership explicitly. It does not renew, recover, infer engine success,
or resolve an ambiguous committing CAS. Inspection never emits the owner token.

### Preparation and context

A **session** is one normal GRV run in one product dataset plus its private
engine workspace. Its context is fixed before the engine invocation. The
pipeline driver calls `session prepare`, waits for success, runs the engine,
and renews the run until the client seals it. Preparation holds the workspace
lock through its engine transaction and durable context creation. It creates
the root/workspace binding and complete empty receipt store atomically if this
is the first managed write to an otherwise unbound engine.

Before mutating a preparation, select its new run ID or discover the run ID of
an existing recorded preparation, then acquire that session's mutation lock
before the workspace lock. Any engine read needed solely to discover a retry
identity uses the workspace lock alone and releases it before acquiring the
session lock; reread the record under both locks before mutation. Never invert
the lock order. A context recovered from an existing record must match the
same effective declaration, canonical engine/root, and target override.

Preparation performs the following steps:

1. Validate the effective push declaration, supported types and extensions,
   root binding, and non-retired target. In one engine transaction, select
   completed input materializations and their metadata. Input aliases and
   output mappings must be unique and must belong to the bound GRV root.
2. Initialize a new target's `LATEST` as GRV §8 permits, then read its
   `LATEST.revision` as `base_revision` (0 for a new target). Use the selected
   run ID and generate an owner token and a retention id for each distinct
   external source revision.
   Create its normal open GRV run control with these fixed inputs before
   creating any dependency hold. Source revision 0 is not a committed input;
   an empty source must have an explicitly published empty revision.
3. Create and confirm every hold through GRV §9, then CAS
   `holds_confirmed: true`. If a cached input's GRV revision has already become
   unavailable, preparation fails; the driver must refresh and prepare again.
   An empty input set also completes the normal holds-confirmation transition.
4. Create private input tables and output tables under namespaces unique to
   this run. Local inputs are stable copies made from the transaction's
   selected materializations; S3 inputs are private views over held file sets.
   Record their fixed schemas and mappings with the prepared engine session,
   and commit that engine transaction.
5. Materialize the context file atomically and durably from the committed
   session record. A retry may recreate an identical context for that session;
   it must not overwrite another context. Return success only when the engine
   record, context, and holds are established.

The context records the canonical root, engine path and workspace ID, run id
and owner token, target dataset and base revision, `declaration_sha256` and code
fingerprints, each input's dataset/revision/retention id and materialization
generation, and the private engine input/output mappings. It is consumer
state; no new GRV object type or run-control field is introduced. It contains
the immutable GRV run identity and backend coordinates needed for engine-free
renewal; renewal validates those against the current control. Preparation
retries recover the same recorded session or start a fresh run; they never
change an existing run's fixed inputs or base.

If preparation fails after creating a run, the client seals it without entries
when it still owns it, or leaves it to normal run recovery. It does not delete
the control or source holds as rollback. No build may start from a failed
preparation.

With `self_input: true`, preparation seeds the declared private output tables
from exactly the target base revision, so a model can read its previous product
output. This self-input is recorded by `base_revision`, not an external input
or a self-hold. Those rows must be materialized locally before the build,
because base revisions receive no additional GRV retention. A missing required
base version fails preparation. Without self-input, or for a new product,
outputs start empty with the declared schemas.
Self-input never comes from a separately pulled tracking or historical copy
of the product; such a copy might differ from the prepared target base.

Private input tables remain immutable for the session. Output tables belong
exclusively to that session's engine invocation. Ordinary pulls cannot target
session namespaces, and different sessions cannot share output tables. The
driver supplies these mappings to the engine through its normal configuration;
models do not need GRV APIs. Physical copies cost engine storage but avoid
making build inputs depend on the current tracking generation. In v1 a pull
can run between preparation and invocation, or after invocation, but cannot
open the same database while the external engine owns it.

### Driver lifecycle and fencing

The driver owns the session through the build and export. It calls
`session renew` often enough to satisfy GRV §5's TTL and clock-skew bounds.
The CLI renews the run during long preparation and finalization operations;
the driver renews between commands, including throughout the engine invocation.
Renewal uses the recorded owner token and the run control's current validator;
losing ownership or encountering recovery stops engine work and new version
allocations under that session. Sealing ends renewal and all allocations;
it still permits publication retries of a complete sealed run. Neither the
context file nor an engine table is evidence that the driver still owns an
open GRV run.

Mutating CLI operations on one session are serialized by a non-expiring local
process lock, released by process exit. Its persistent file is
`<canonical-engine-path>.grv-session-<run-id>.lock`; use exclusive `flock`,
never unlink it during the workspace lifetime, and acquire it without waiting.
Copies or aliases of a context therefore use the same lock. Contention is busy.
This includes renewal, finalization, and abort;
the operation holding the lock performs its own renewals as needed. An expiring
client-side lock is insufficient. A retry first resolves the durable session
plan and any recorded publication attempt before starting new work.

The driver does not hold this session mutation lock for the external invocation;
it holds the workspace lock and performs backend-only renewals between commands.

Finalization is called only after the engine invocation completed successfully
and all writers to its private output tables have stopped. The driver supplies
the completion record below to attest these conditions and the completed
outputs; the CLI cannot infer engine success from the presence of a table.
Outputs remain stable during export; the client exports them from one consistent
engine read
transaction. A lease cannot fence arbitrary delayed engine writes, which is
why output namespaces are private and the driver must wait for its writers.

### Build completion record

Before first finalization, the driver writes one atomic, durable JSON file
conforming to [the build-result schema](grv-cli-v1-build-result.schema.json).
It is written only after the invocation has succeeded, all output writers and
database connections have stopped, and the driver has established which
declared outputs completed. A direct export uses `kind: direct`; an
omission-only publication uses `kind: omission-only` and an empty completion
list. Each still has a driver-assigned invocation ID. Example:

```json
{
  "result_version": 1,
  "run_id": "01M3KQA080R6Y8C2D9F0G1H2J3",
  "workspace_id": "b2c81f94-45a6-4cb3-b937-82916c1e0842",
  "declaration_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "kind": "engine",
  "invocation_id": "build-2026-10-03-001",
  "status": "succeeded",
  "writers_stopped": true,
  "completed_at": "2026-10-03T10:15:00Z",
  "completed_outputs": [
    {"table": "fct_x", "engine_table": "grv_out_01m3kqa080r6y8c2d9f0g1h2j3.fct_x"},
    {"table": "dim_y", "engine_table": "grv_out_01m3kqa080r6y8c2d9f0g1h2j3.dim_y"}
  ]
}
```

The run ID, workspace ID, declaration digest, and every physical output mapping
must match the prepared session. Entries are unique by declared GRV table name
and must refer to declared outputs. For `changed` or `all`, every declared
output not held or wholly dropped must have a completion entry. For `explicit`,
every table addressed by an `include` or `empty` selector needs one; other
tables are not exported. Tables named only by drop selectors and absent from
the output mappings require no completion. Held or wholly dropped tables must
not be listed.
A completed output may contain zero rows; an empty or self-input-seeded table
without completion evidence is **not** a completed output. Explicit empty
versions also require evidence for their selected output table. Missing
evidence returns `BUILD_INCOMPLETE` before any version allocation or sealing.

`push` validates the supplied record under the session and workspace locks and
durably stores its canonical JSON and SHA-256 in the engine session record
before staging an export. The first accepted record is immutable for that
session; later files must canonicalize identically or fail `REQUEST_MISMATCH`.
Once recorded, `--build-result` may be omitted on retry. If no record exists,
omitting the flag fails even when prepared output tables exist. A terminal
outcome or recorded publication attempt is resolved first; an already
committed publication never requires the driver to recreate a lost file.
The fixed export plan includes the accepted completion digest. Session/cache
cleanup preserves both records and any retryable attempt/outcome.

The record is a driver's attestation, not an independent proof of model
correctness or a GRV commit. The driver must derive completion from its actual
invocation results rather than fill the list from preparation's table names.
The client still validates schemas, declared checks, ownership, and the full
GRV publishability fence. Accepting a record authorizes neither later output
writes nor resuming a build after run recovery.

On a driver crash, normal GRV run recovery may take over and seal the run.
A late engine process can leave private working tables, but cannot modify a
new session's outputs or authorize new GRV allocations with the lost owner
token. The client never adopts an expired/recovered build by reading the new
`LATEST` and replacing its base revision. A lost or incomplete session requires
a new preparation and engine invocation. A retry may finish publication of
an already complete sealed run only after verifying it matches the session's
durable export plan exactly; it cannot resume the build or extend that run.

`session abort` stops publication, resolves outstanding allocations through
the normal claim/run protocols, and seals and materializes the run without
publishing it. Before export, this normally means an empty run. Already
finalized allocations remain entries of the sealed run; abort cannot erase
finalization proofs or change an already sealed payload. The CLI records the
aborted outcome in consumer session state and refuses further finalization
through that context. Only source GC releases holds, when GRV §9 permits it;
the client never deletes holds or writes source release markers.

Abort first resolves any recorded publication attempt. If it already committed,
the command reports that committed outcome; abort cannot undo it. Stopping the
external engine remains the driver's responsibility.

Private engine tables may be cleaned up after the session's outcome is known
and all its writers have stopped. Engine-cache cleanup does not change GRV
version, pin, or hold retention. It preserves the session record and recorded
attempt/outcome needed to retry a still-valid context.

## `grv push`

### The push declaration

```yaml
# <decl>/mt.yml
declaration_version: 1
kind: push
dataset: mt
source:
  engine: duckdb
  tables:
    - { table: fct_x, grv_table: fct_x }
    - { table: dim_y, grv_table: dim_y }
inputs:
  - { table: raw.mt_month_stats, as: mt_month_stats }
self_input: false                  # true seeds outputs from the prepared base
target:
  grv: mt
selection:
  policy: changed                 # changed | all | explicit
  include: []                     # required for explicit
  drop: []                        # omissions, including whole tables
  hold: []                        # tables kept unchanged
  empty: []                       # explicit zero-row partition versions
schema:
  - name: fct_x
    columns:
      - { name: period,  type: date32 }
      - { name: value,   type: double }
      - { name: _period_, type: utf8 }
    partition_keys: [period]
  - name: dim_y
    columns:
      - { name: id, type: int64 }
    partition_keys: []
derived_from: session
```

`inputs` names the completed engine source tables the build may consume;
`as` supplies an alias in the private input namespace. It must cover all GRV
sources actually read other than declared self-input. Listing the target
dataset among these external inputs is rejected. `self_input` defaults to
false. The source output `table` names are unqualified aliases
resolved in the session's private output namespace, not shared tracking or
working tables. Preparation returns the concrete mappings to the driver.

The `schema` uses the same Arrow layer as pull. Output partition keys are fixed
by the target layout, and partitioned outputs contain the reserved string
`_{key}_` columns. Their canonical values identify output groups; the client
rejects missing, null, or invalid partition identifiers. It does not infer
partition encodings from ordinary data columns. Tables are created by the
normal immutable layout and schema registration protocols on first export.
An unpartitioned output always has the group `{}`, even with zero rows, and
exports an empty version when appropriate. For partitioned outputs, a zero-row
group has no rows to identify it; `selection.empty` supplies that identity.
An absent group alone does not imply omission.

### Finalization algorithm

Under the session and workspace locks, first return an already recorded terminal
outcome or resolve a recorded publication attempt. Otherwise use the steps
below. A complete sealed-run retry skips steps 1–3 and verifies and materializes
the existing sealed payload in step 4; an incomplete sealed run cannot continue.

1. **Check the session and export.** Verify its recorded identities and fixed
   declaration, ownership of the open run, accepted completion record, and
   exclusive output mappings. Require completion for every selected output
   before deriving even an empty unpartitioned group. Read outputs from a
   stable engine snapshot, validate schemas and
   checks, and stage complete parquet files. Derive provenance from the fixed
   session inputs, never the current `pull_meta` or source `LATEST`.
2. **Plan against the prepared base.** Determine contributed output partitions,
   equality-proven versions, explicit omissions and empty versions. Staging
   must describe full snapshots of those partitions. Changes during the build
   do not change the plan's base. Each affected pair has at most one action.
   Durably record this fixed export plan and completion digest before allocating
   versions, and record their allocation identities as work proceeds. Retries resolve
   existing allocations before creating replacements. A sealed-run retry
   verifies the recorded plan and entries and skips export and allocation.
3. **Finalize versions.** For every new version, perform the complete GRV §5
   claim cycle: acquire, reserve a number, create the allocation record,
   register its schema, create all data files, renew, create the manifest last,
   verify the files and absence of a tombstone, release as finalized, and CAS
   the allocation record to finalized. Manifests already contain `run_id`,
   `claim_token`, all per-file hashes/sizes/validators, and `derived_from` with
   its confirmed retention ids. Unknown outcomes use GRV's proofs.
4. **Seal the run.** Resolve all remaining allocation records, CAS-seal the
   control with exactly its finalized entries, and materialize the immutable
   run file, as GRV §6 requires. Publication cannot precede this step. A retry
   may reuse the identical sealed run; recovery that excluded expected work is
   an incomplete build, not permission to append entries to the sealed file.
5. **Publish the change set.** Acquire the target's lease, finish pending
   operations, reject retirement, and recompute from its current predecessor
   using the fixed session base. Check every contributed output pair for
   intervening changes, including changes later restored, under GRV §8.
   Equality-proven pairs skipped by dedup also receive this check; dedup cannot
   conceal a concurrent change. Explicit omissions are guarded by
   `expected_revision` equal to the prepared base. After the no-op decision
   below, execute the complete GRV §8 publish protocol, including its revision
   reservation, validation, publish description, revision parquet, `LATEST`
   CAS, and supersession receipt. Before issuing the commit CAS, durably
   record the attempt's operation id and reserved revision in consumer session
   state. A retry reacquires the dataset lease to fence delayed requests and
   resolves that attempt from the committed chain before issuing another.
6. **Report the outcome.** Record the known revision or no-op in consumer
   session state. This may lag the GRV commit and is not its commit marker.
   An ambiguous publication is resolved from the committed revision chain;
   retries must recover the session's recorded publication attempt before
   starting another. Manifests and run files are never rewritten afterward.

A lease takeover without a conflicting state change may be retried with the
same sealed run under a fresh dataset lease, subject to GRV's availability
checks. A state conflict requires a fresh session and build; the client must
not automatically rebase existing engine outputs. A retirement or unavailable
version is surfaced rather than repaired by publishing incomplete state.

### Selection and no-op rules

- **`changed`** is the default: export eligible output partitions and reuse an
  existing base version only when complete content and logical-schema equality
  are proven. Comparing only `data.parquet` is insufficient for a multi-file
  version. If equality cannot be established, produce a new version; equality
  must not be guessed from row count or a partial hash.
- **`all`** creates new versions for all eligible output partitions, including
  byte-identical data.
- **`explicit`** restricts outputs to `include` selectors, each naming a whole
  table or a `(table, partition)` pair. A missing requested output is an error.
- **`drop`** uses GRV omission selectors. A missing exported partition alone
  does not imply removal; the driver must declare removal explicitly.
- **`hold`** excludes named tables from output contributions and keeps their
  predecessor entries unchanged. This declaration field is unrelated to GRV
  dependency holds.
- **`empty`** names explicit pairs for which a zero-row version is produced
  with the table's registered schema. It retains membership and terminates
  the partition's data. An empty request does not rely on a nonexistent row
  group to identify its partition.

Selectors use canonical GRV table names and partition objects. Overlapping
contributions, omissions, holds, or empty requests are rejected. The client
skips a new revision only if the **complete resulting state** equals the
predecessor after conflict checks, including omissions. An omission-only
change still publishes a revision; having no new versions is not a no-op test.
For a true no-op, release the dataset lease and record the result without a
revision publication. A number already reserved while acquiring the lease
remains reserved, as GRV requires; it may be skipped.

The selector grammar is closed: whole-table selectors are `{table: <name>}`;
pair selectors are `{table: <name>, partition: {key: <string>, ...}}`.
`include` and `drop` accept either form, `hold` only the whole-table form, and
`empty` only the pair form. `{}` is the partition of an unpartitioned table.
Partition objects contain exactly the layout's keys and canonical string
values; key order is reconstructed from the layout. `include` and `empty`
must address declared output mappings; `drop` and `hold` may address existing
target tables without output mappings. Unknown tables or keys are errors.
For `explicit`, `include` must be nonempty; for other policies it must be empty.
`empty` pairs in explicit mode must be covered by `include`.

Compute selection before generating output groups: held and wholly dropped
tables contribute no automatic output, including precreated empty tables.
Apply `include` as an eligibility filter, then derive groups only from selected,
completed outputs. An `include` selector is not itself a contribution and may
cover an `empty` request. A pair explicitly named by `empty` must have zero
rows; a nonempty matching group is an error. A partition omission overlapping
an exported or explicitly empty group is an error. Duplicate or intersecting
selectors within one list and conflicts between hold/drop/include/empty are
rejected before allocation. Whole-table drop and hold suppress export of that
table rather than conflict merely with its precreated table's existence.

For example, these are three independent selector shapes; the drop's table
must already exist and the hold must not overlap selected outputs:

```yaml
selection:
  policy: explicit
  include: [{table: fct_x, partition: {period: '2026-10'}}]
  drop: [{table: old_dimension}]
  hold: [{table: dim_y}]
  empty: [{table: fct_x, partition: {period: '2026-10'}}]
```

### Schemas and provenance

New versions may append top-level columns only. Removing, renaming, retyping
(including precision or UTC changes), reordering, changing nested types, or
changing partition keys requires a new GRV table. Registration and extension
properties follow the durable table-wide baseline; failed registrations are
not rolled back. Engine-specific code fingerprints and invocation artifacts
belong in the optional engine metadata, respecting which run fields were
fixed at creation.

Every new derived version cites **all** the run's confirmed external input
holds, using whole-revision selectors. V1 defines no narrower dependency
mapping grammar. This conservative provenance can retain more upstream data;
narrowing requires a later specification. The sources must be
committed revisions in this root, the references must resolve, and the
consumer dataset must satisfy GRV's acyclic derivation rule. Self-input is
represented by the target base revision and never by a self-hold.

GRV history is the history of record. Engine working snapshots and session
copies are disposable execution state; they do not define a second product
version history.

## Revision retention: `grv pin` and `grv unpin`

V1 creates and releases pins only on committed dataset revisions. A pin keeps
that revision's version data available through GC until its release. It has no
expiry and does not change `LATEST`, freeze publications, or select an engine's
tracking revision. Dependency holds on its versions may retain entire upstream
revisions transitively. Ordinary pulls create no pins; build sessions use
their own automatic dependency holds.

### Creating a pin

`pin` requires an explicit positive `--revision` and nonempty `--reason`.
It follows GRV §10 in full: acquire the dataset lease, finish pending work,
prove commitment and availability, create the `pin` operation description,
commit through `LATEST.pending`, durably materialize the pin marker, and clear
the pending operation. A pruned or unavailable revision fails; pinning cannot
restore deleted data. Pins on available revisions of a retired dataset remain
valid operations under the core spec.

The CLI generates a fresh canonical UUID pin ID unless `--pin` supplies one.
The identity is `(dataset, revision, pin_id)`: IDs are scoped to the revision's
existing `.pins/` directory, not a dataset-wide registry. Caller-supplied IDs
support reliable retries; an ID cannot be reused after release in that scope.
Validate the exact addressed records under the dataset lease; do not scan all
other revisions to prove global uniqueness. The same UUID in another revision
is a distinct pin and cannot be released by this request. An existing active
pin with the same ID, revision,
and reason makes the request an idempotent no-op. A conflicting identity or
reason fails rather than editing the record. Each independently generated ID
creates independent protection, even for the same revision and reason.

Report a generated ID on stderr before attempting the committing CAS, and
include it in the final success/error result. Internal retries retain it.
Callers retain the dataset, revision, and ID and retry that complete identity;
a new invocation without `--pin` requests a new
pin. An unknown commit is resolved through GRV §8's lease reacquisition and
durable marker proof before another attempt is made.

The result includes dataset, revision, pin ID, reason, active status, and the
core creation audit fields. `created_by` is the core's tool/version identity;
it is not a per-person ownership or authorization system. Store permissions
govern who can perform retention mutations. `show --retention` exposes these
records and the other protections affecting a revision.

### Releasing a pin

`unpin` requires an explicit positive `--revision` and one pin ID. Under the
dataset lease, finish pending work and read that revision's
`.pins/<id>.json` and optional release marker directly, validating path,
scope, and operation identity. A missing scoped ID fails even if the same ID
exists elsewhere; inconsistent records fail. V1 rejects non-revision pin
scopes and never falls back to dataset-wide discovery. Commit the normal `unpin`
operation and durably materialize its release marker before reporting success.
An already released matching pin is an idempotent no-op.

Release affects only that pin. Other pins, current-state protection, grace,
and dependency holds continue to apply. Data is reclaimed only by a later GC
decision after all applicable protections are gone. The result reports the
released pin and observed remaining protections; it never promises an exact
deletion time or byte saving. Pin records are retained, and released IDs are
never reused within their scope. The CLI never treats unpinning as releasing
dependency holds.

## `grv gc`

GC operates on one named dataset. With no mode flag it performs `--dry-run`;
`--dry-run` and `--apply` are mutually exclusive. Both modes read the same core
retention rules and report candidate `(table, partition, version)` identities,
reasons for protection or eligibility, pending coordination, releasable holds,
and byte estimates when known. Unknown sizes are reported separately. Revision,
run, operation, layout, schema, claim, pin, and hold history is retained.

### Preview

A dry run is read-only. It acquires no lease, recovers no run, releases no hold,
creates no supersession receipt, and writes no intent or tombstone. Report
versions eligible now separately from those dependent on hold release or run
recovery. A missing supersession receipt keeps the affected revision protected
for this preview: an applying GC would first write the receipt and start grace.
Unreadable protection records prevent an eligibility claim and are errors.

The preview is an observation, not a committed deletion plan. New pins, holds,
publications, retirement, or recovery can change it. `--apply` always recomputes
under the actual dataset lease; it does not accept a dry-run file as authority.

### Applying GC

Use the complete GRV §8–§10 protocol: acquire the dataset lease and complete
pending operations, reread parameters, write missing supersession receipts,
recover eligible expired local runs as needed, release only provably releasable
source holds through committed operations, and compute protected versions.
Recovery of a consumer run in another dataset is not part of this command;
report holds whose release requires that consumer's recovery.

For each candidate batch, commit `prune_intent`, re-list holds, then commit a
`prune` decision naming only still-unprotected targets from that intent.
Durably create every decided tombstone before clearing pending. Only then
delete those targets' data files and manifests, as the core permits outside
the lease. Sweep residual data below existing tombstones using the same
authority. Never delete coordination/history objects or reuse numbers.

A lost lease before the prune decision authorizes no deletion. A known committed
decision remains replayable after a crash; its tombstones and physical cleanup
may finish later. Resolve ambiguous operation commits before continuing. Report
decided/tombstoned versions separately from completed physical deletion, along
with released holds and unresolved cleanup. A deletion failure returns an error
with known committed progress; it cannot roll back the prune decision. A dataset
with no eligible work is a successful no-op.

## `grv recover`

Recover interrupted coordination for one dataset through GRV §6 and §8. With
no `--run`, complete any existing pending dataset operation under a valid
dataset lease, then discover and recover eligible expired run controls and
materialize missing sealed run files. `--run` restricts the command to that
specific run and does not recover unrelated dataset operations or other runs.
`--dry-run` reports observed eligibility and proposed repairs without mutations.

Recovery rechecks every control and validator before taking ownership. A live
run is left to its owner. For expired runs, CAS into or take over `recovering`
under the core lease rules, resolve allocation records using claim-release
proofs or the expired-claim verification protocol, and CAS-seal and materialize
exactly the authoritative finalized entries. Sealed files must match their
controls. Claims still valid must be allowed to finish or expire; run takeover
is not evidence that a partition writer has stopped.

No invocation force-expires a valid run, claim, or dataset lease. Untargeted
healthy live runs are reported as skipped. A requested live run, a busy dataset
operation, or an unresolved allocation that must wait returns busy with known
progress; retry proceeds only when the core rules permit ownership. The client
renews leases throughout recovery work it owns. Where the core rules resolve a
missing finalization proof as `unproven`, record that outcome rather than
salvaging a version. Corrupt required records are protocol errors.

Recovery makes no new publication, selection, prune, pin, or hold-release
decision. It only replays effects already authorized by a
pending operation and completes the core run-recovery transitions. Physical
cleanup of tombstoned data remains GC's responsibility. A recovered sealed run
is not evidence of a successful complete engine build; `push` can resume only
when that run matches its existing durable session plan. Recovery never changes
a session's fixed inputs/base or adopts arbitrary working tables.

The result identifies discovered and targeted resources, recovered/sealed runs,
completed operation IDs, skipped live work, and waiting/failed work. Repeating
recovery of an already complete scope succeeds without changing sealed payloads.

## Dependencies on GRV v2

Changes to these constructs require joint review:

- Root initialization and parameters; backend reads, durable read-back,
  conditional writes, ambiguous outcomes, and listings (§1–§2).
- Layout identifiers, partition duplication, table extensions, logical
  schemas, and durable schema baselines (§3–§4).
- The complete claim cycle, allocation records, owner leases, run recovery,
  sealing, and the publishability fence (§5–§6).
- Committed versus orphan revisions, `LATEST` leases, durable operations,
  revision reservations, history-based conflicts, and publication recovery
  (§7–§8).
- Source hold confirmation, retention ids, immutable provenance, and GC-only
  hold release (§9–§10).
- Revision pin creation/release, retirement state inspection, kept revisions,
  grace, prune intent/decision replay, and physical GC cleanup (§8–§10).
- Transactional dataset refreshes and build input/output stability (§11).

## Properties and trade-offs

- Pull commits one dataset's refresh and checkpoint together. Incremental
  refresh changes the amount of work, not the transaction boundary.
- Private sessions separate build provenance from mutable tracking state and
  allow pulls between preparation and invocation without changing the selected
  inputs. V1 serializes database access and permits one external invocation
  per engine file; a concurrent pull or engine inspection reports busy.
- A pipeline driver must prepare, renew, finish or abandon the session and
  configure its private mappings and attest each selected output's completion.
  Directly inspecting already-built shared
  tables cannot reconstruct a missing build context.
- Session copies consume engine storage; S3 views avoid copies but need network
  access, verification, and confirmed source holds for builds.
- Sessions and pulls stay within one GRV. Cross-root portability requires a
  new workspace and is not an implicit provenance feature.
- Type support is explicit and lossless. Unsupported mappings fail rather than
  silently changing values or GRV schemas.
- Explicit Parquet reader settings preserve file schemas and columns regardless
  of directory names. Per-file validation precedes permitted null-padding.
- Durable transactional attempt receipts make pull retries independent of the
  current checkpoint. Small consumer histories remain until workspace reset.
- All outputs conservatively cite all declared external inputs; v1 accepts
  additional retention instead of an unspecified dependency-narrowing language.
- Inspection observes committed metadata without changing retention or
  coordination. Metadata history remains useful when historical data has been
  pruned; metadata comparisons do not assert row equality or future availability.
- Revision pins are explicit, independent, and permanent until released. Their
  transitive source retention can exceed the selected product's own size;
  `show --retention` and GC previews make those protections visible. Unpin
  requires the full dataset/revision/ID identity and performs no global ID search.
- GC and recovery use existing per-dataset fences. Dry runs are observations;
  applying commands recheck their authority and report irreversible progress.

## Later additions

`restore` may select an older available state into a new revision, preserving
the committed history; `retire` may expose the core's terminal retirement
operation. Neither is specified as a v1 command. Automatic retention policies,
mutations of broader pin scopes, row-level diffs, root-parameter management,
server/shared-process engine adapters, concurrent build databases, and narrower
per-output dependency mappings also require a later CLI specification.
These deferred interfaces do not
change the corresponding core storage semantics.

## Required conformance scenarios

| scenario | required result |
|----------|-----------------|
| Failure while refreshing the second table | All table, schema, ownership, and checkpoint changes roll back |
| Ambiguous pull commit | Fence the prior writer, recover DuckDB, and resolve the immutable attempt receipt |
| A committed pull crashes, then another pull advances the checkpoint | Retry with the original ID returns the original receipt without reapplying or rewinding |
| A successful pull's source data is later pruned | Receipt replay still returns its committed outcome without downloading again |
| A pull retry changes its declaration, target, or requested selector | Reject reuse with `REQUEST_MISMATCH` |
| Receipt metadata is missing in a previously initialized workspace | Report inconsistent metadata; do not infer rollback or invent an empty history |
| A pull changes tracking inputs between preparation and invocation | The session reads its fixed private input generation |
| Pull or engine inspection runs while the external engine owns DuckDB | Return `ENGINE_BUSY`, with no engine mutation or invented engine observation |
| Session renewal runs while the external engine owns DuckDB | Renew using context/backend only, without opening DuckDB |
| A driver exits but its engine child still owns the database | Respect the remaining file owner; do not steal access or fence it with lease expiry |
| Different context copies mutate the same session | Serialize on the canonical engine/run lock, not the context-file spelling |
| A Parquet file is below partition and `version=N` directories | Read only its logical file columns with Hive inference disabled |
| An older compatible version lacks appended columns | Add typed nulls after per-file validation; reject retyped or reordered schemas |
| A successful partial invocation leaves a declared output untouched | Reject missing completion evidence before allocation; do not publish its prepared emptiness |
| A selected output genuinely completed with zero rows | Accept its completion evidence; publish an empty unpartitioned version or explicitly named empty partition |
| Self-input seeds an output that the invocation does not complete | Seeded table existence does not authorize export |
| A completion record names another run/workspace/digest/mapping | Reject before staging or allocation |
| Push retries with its accepted completion record and no result file | Reuse the immutable accepted record and fixed plan |
| A retry supplies a different completion record | Reject `REQUEST_MISMATCH`; preserve the accepted record |
| An omission-only session has no output tables | Require a bound omission-only completion record with an empty completion list |
| Explicit include covers an explicit empty pair | Treat include as eligibility; require completion and zero rows, without duplicate contribution |
| A held or wholly dropped output has a precreated table | Suppress automatic export; table existence causes no empty contribution |
| Source pruning before preparation confirms holds | Preparation fails; cached rows cannot authorize derived publication |
| A source GC runs after session hold confirmation | The held input revision remains complete |
| Another publication changes and then restores an output partition | The old session conflicts |
| A publication contains only an effective omission | A new revision is committed |
| A historical target overlaps tracking tables | Reject before mutation |
| An input belongs to another GRV root | Reject preparation |
| A timestamp mapping would truncate nanoseconds or change UTC semantics | Reject before table mutation or version allocation |
| A driver loses ownership while its engine process continues | No new allocations under that owner; private outputs cannot affect another session |
| A crash occurs after GRV publication but before consumer result recording | Retry recovers the existing committed publication |
| Abort follows an ambiguous publication that actually committed | Report the committed outcome; do not mark it unpublished |
| A sealed run matches the durable export plan exactly | Resume publication without changing inputs, outputs, or run entries |
| A recovery seals the run without an expected output version | Require a new session and build |
| A model requests self-input | Seed from the prepared target base, with no self-hold |
| Initialization finds dataset objects without `grv.json` | Report a damaged root; create no configuration |
| Initialization repeats against a valid root | Reuse its configuration; reject conflicting explicit parameters |
| Listing discovers orphan version directories | Do not include them in a committed revision's table/partition state |
| Historical manifests have been pruned | Log and revision-entry diff still work; unavailable schema/provenance details are labeled |
| Status inspects an unbound engine or a pending operation | Report it without binding, renewing, or repairing anything |
| Full verification encounters concurrent pruning | Report unavailable objects; create no automatic pin or repair |
| Two independent pins protect one revision | Releasing one leaves the other protection active |
| Pin creation retries with the same active ID and matching request | Return that pin without creating another protection |
| Pin creation attempts to reuse a released ID in the same revision | Reject reuse; preserve the release record |
| The same pin UUID exists in two revisions | Treat them independently; unpin addresses only the specified revision |
| Unpin's ID exists only in another revision | Return `NOT_FOUND`; perform no dataset-wide lookup or release |
| Unpin receives a table/partition/version pin ID | Reject the unsupported mutation scope |
| GC dry run finds a missing supersession receipt | Report protection and write no receipt or other object |
| A new hold or pin appears after a GC preview | Applying GC rechecks protection and preserves protected versions |
| Physical deletion fails after a committed prune decision | Report tombstoned data and incomplete cleanup; retry preserves the decision |
| Recovery targets an unexpired run or claim | Respect its lease; report waiting/busy work rather than force takeover |
| Recovery resolves an expired run | Seal its authorized entries without publishing or changing its base/inputs |
| Session inspection sees an ambiguous publication attempt | Report the attempt read-only; a mutating retry resolves it |
| A declaration has duplicate YAML keys, unknown fields, an invalid selector, or an unsupported Arrow type | Reject before engine or GRV mutation |
| A declared not-null check fails in an otherwise unchanged table | Roll back the whole pull, including the checkpoint and attempt receipt |
| A command emits success or a partial error in JSON | Validate its command-specific schema, error/exit correspondence, precision, and token redaction |
