# GRV Client v1 Execution Semantics

## Relationship to the authoring specification

This normative companion describes how the CLI, core, and adapters implement
[the user surface and extension contract](grv-client-v1.md). No CLI is implemented.
Users author that document's declarations; they do not implement the protocols
below. The managed runner performs build-driver duties for managed builds.
The explicit external integration API retains the same guarantees.
[GRV v2](grv-storage-v2.md) remains authoritative and no storage layout is added.

The [adapter process protocol](grv-adapter-protocol-v1.md) defines the local
channel implementing these obligations. See the [specification guide](README.md)
for document scopes and version relationships.

### Normalized plans and identity

Validate the common envelope and registered adapter points, resolve capabilities,
expand local column/SQL files and defaults, then bind stable connection identity.
Parse YAML 1.2 into JSON-compatible values, rejecting duplicate keys, aliases,
custom tags, non-string keys, non-finite values, and unknown fields. References
are local and one-level only; no environment interpolation or remote includes.
IPC references contain one Arrow Schema message with no trailing batches/messages.
Unsupported Arrow types/extensions fail before acquisition or mutation.

One core normalizer produces a fixed execution plan for inline/file contracts
and every adapter. The following names are **internal plan fields**, not a
second authoring format:

- `target.grv` and `source.grv` are derived from the sole public `dataset`.
- `source.mode` is `extract` when no `build` is present, otherwise `build`.
- `source.tables` contains resolved source/output mappings. Push `schema`
  contains the ordered Arrow contracts derived from table `columns`.
- Build `inputs`, `self_input`, and `code_fingerprints` come from `build`.
  `derived_from: session` is a core provenance rule, never user assertion.
- Pull `target.write` derives from `write` (default `replace`);
  `target.mode` derives from adapter materialization options (default `local`).
  Pull source schemas/layouts and selected partition tuples come from verified
  selected versions or a declared/trustworthy empty-source template.
- Pull table `select.schema` is the resolved output `columns`; source `schema`
  is the selected-source contract. Normalization expands SQL files to query text.
- `refresh` is a work plan: `changed` only for complete identity replacement
  under `options.refresh: auto`; otherwise `full`. It is not a user-required
  switch. `scope_mode: selected` means at least one explicit partition selector;
  `transform_mode: sql` means at least one table query.

Mappings, aliases, and physical destinations must be unique. GRV names follow
its canonical grammar. DuckDB destination/schema names follow
`^[a-z0-9_][a-z0-9_-]*$`; qualified local relations have exactly `schema.table`.
Quote identifiers. `_grv`, `grv_source`, `grv_target`, `grv_input`, `grv_self`,
and private session namespaces cannot be extraction sources or pull targets.
Input field selectors are identifiers, not expressions. Source predicates are
one adapter-language row predicate; joins/subqueries require managed build SQL.

Each output column has one resolved mapping and no undeclared columns. Source
selectors may be repeated for distinct output names. Exact types, ordered output,
partition columns, and registered extensions must pass core validation. Checks
name the operation's output columns, not removed source-only fields. An identity
pull's optional output columns assert equality; they never authorize projection.

`declaration_sha256` hashes canonical effective authoring configuration using
[RFC 8785](https://www.rfc-editor.org/rfc/rfc8785): expand defaults and local files,
normalize source revision to `latest` or a decimal string, and resolve paths.
Adapter package/interface/binding-schema versions and stable connection identity
are fixed additionally. Secrets are stored through adapter authentication, not
in the declaration or hash. A reused alias cannot retarget an unfinished attempt.
Pull request identity includes root, workspace, destination mappings/role,
selected partitions, output/expected contracts, effective adapter options, and
requested source selector. Generated attempt/run IDs and source facts learned
only during capture are not declaration inputs.

On pull replay, compare the fixed request and recorded adapter identity before
source resolution/download. Inferred source schemas are output facts of the
successful receipt, not mutable inputs to its request hash. For an uncommitted
`latest` retry they may be rediscovered with a newer revision. A terminal receipt
can be returned without the source or re-authentication; restoring receipts is
required if they are lost. Push accepted captures/plans remain fixed on retry.

### Command surfaces

Ordinary push/pull/adapter and inspection commands are defined in the main spec.
Advanced flags are `--attempt <uuid>` and `--state <dir>` on push,
`--attempt <uuid>` on pull, and root initialization parameters below.
The explicit integration API is:

```console
grv session prepare --session <context.json> --decl <yaml> --grv <root> [--state <dir>] [--attempt <uuid>]
grv session show --session <context.json>
grv session renew --session <context.json>
grv session abort --session <context.json>
grv push --session <context.json> [--build-result <result.json>]
```

`push --decl` performs extraction or a managed build and publication.
`build.execution: external` requires explicit preparation/invocation/finalization.
A session's target dataset and adapter connection are fixed in the declaration;
there are no `--engine`/`--target` overrides. Pull `--revision <N|latest>` only
changes its source selector. This companion's engine process/catalog details
and JSON build-result file describe the initial DuckDB integration; other build
adapters supply registered completion/mapping details through the same logical
SDK boundary. Core publication validates fixed inputs/completion in either case.

## Runtime ownership and results

### DuckDB process ownership

V1 serializes **all database access** between the CLI and external engine
invocations using one non-expiring OS process lock for the workspace. The
adapter and driver use the same lock identity, derived from the canonical
database path; symlink aliases resolve to it and hard-link aliases are rejected.
The persistent lock file is `<canonical-engine-path>.grv-lock`; participants
use an exclusive `flock` and never unlink or replace it while the workspace
exists. The database file must not be moved or replaced during its lifetime.
The managed runner uses the adapter lock helper; an advanced external driver
must implement the same protocol or use that helper.
DuckDB's own file lock remains an additional safeguard. See
[DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency).

Every command that opens the engine holds the workspace lock until its
connections close. The runner or external driver holds it throughout the engine invocation,
including child processes, and releases it only after all database users and
writers have stopped and all connections have closed. Contention returns
`ENGINE_BUSY` (exit 3), without engine mutation; v1 does not queue or steal a
live lock. A driver crash does not authorize a new engine user while an old
process still has the database open: DuckDB connection contention is also busy.

`session renew` requires no engine connection or workspace lock. It validates
the self-contained context and GRV identities, then renews the run through the
backend. It therefore continues during a build. Backend-only inspection, pin,
GC, and recovery commands likewise do not open DuckDB. `status --decl`,
`session show` for a build, build preparation, DuckDB pull/extraction,
build finalization/abort, and engine cleanup require the workspace lock.
Salesforce extraction sessions never open DuckDB or acquire its lock. A busy engine inspection may report already observed GRV facts
in its partial error result; it cannot report an unobserved checkpoint.

The session mutation lock below is distinct from the workspace lock. If both
are needed, acquire the session lock first and attempt the workspace lock
without waiting; release locks before reporting busy. Neither lock is a GRV
lease. This model permits one active external invocation per engine file;
private tables preserve provenance but do not permit concurrent process access.

### Common command behavior

GRV-facing commands validate `grv.json` and use the backend contract. Adapter
registry, capability, and namespaced commands need no GRV root and report
`root: null`; login may modify adapter authentication state. Only `init`
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
[the output schema](grv-client-v1-command-output.schema.json), with `output_version: 1`,
`command`, canonical `root` (null until known), `ok`, `exit_status`, `result`,
and `errors`. Command names are `init`, `ls`, `show`, `status`, `log`, `diff`,
`verify`, `pull`, `session prepare`, `session show`, `session renew`,
`session abort`, `push`, `adapter list`, `adapter capabilities`, `adapter command`,
`pin`, `unpin`, `gc`, and `recover`; an unrecognized
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
| ------ | ------ | --------- |
| `INVALID_ARGUMENT` | 2 | Invalid command, flags, or identifier |
| `INVALID_DECLARATION` | 2 | Invalid declaration, unsupported type/extension, or failed declared check |
| `REQUEST_MISMATCH` | 2 | Attempt or completion identity reused with another fixed request |
| `BUILD_INCOMPLETE` | 2 | Missing successful build completion evidence |
| `UNSUPPORTED_CAPABILITY` | 2 | Adapter does not implement the requested direction or write behavior |
| `EXTRACTION_INCOMPLETE` | 6 | Source acquisition has not completed all declared outputs |
| `ADAPTER_FAILURE` | 6 | Adapter authentication, source, or destination operation failed |
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
| --------- | ----------------- |
| `init` | Creation flag, core format/version, and effective root parameters |
| `ls` | Scope, nullable dataset/revision, and object-identified items with observed coordination/details |
| `show` | Revision/predecessor/operation, state entries, per-table layout/baseline/revision schema and provenance, optional retention explanation |
| `status` | Current state, lease/pending operation, runs, and optional registered adapter state |
| `log` | Limit, `has_more`, and newest-first committed entries with change counts and run IDs |
| `diff` | Resolved endpoints, `changed`, entry changes, table membership changes, and known/unknown schema comparisons |
| `verify` | Resolved scope, full-hash mode, named check outcomes, and unavailable objects |
| `pull` | Adapter/write mode, attempt/workspace IDs, fixed scope/selector, committed revision/generation/time, and `replayed` |
| `session prepare`, `session show` | Context/state location, adapter/mode, nullable engine path, fixed inputs, mappings or capture, completion, plan, attempt/outcome, and observed run |
| `session renew` | Dataset/run, open phase, renewed expiry, and `renewed: true` |
| `session abort` | Dataset/run, known terminal outcome, and finalized entries |
| `push` | Adapter/mode, nullable extraction attempt ID, dataset/run, completion digest, known published/no-op outcome, and `replayed` |
| `adapter list`, `adapter capabilities` | Adapter identity/version, directions, consistency, supported write modes, and command names |
| `adapter command` | Adapter identity, namespaced command name, and separately validated redacted adapter result |
| `pin`, `unpin` | Fully scoped pin/audit/release information and `no_op`; unpin also reports remaining protections |
| `gc` | Mode, per-version progress/reasons, releasable/released holds, completed operations, byte estimates, and waiting work |
| `recover` | Mode/scope, per-run progress, completed operations, pending and waiting work |

`replayed` means the existing recorded result was returned rather than a new
refresh/publication. `no_op` means the requested retention mutation was already
satisfied. A session outcome of `no-op` means no revision publication occurred;
its `revision` reports the observed predecessor, and `operation_id` is null.
`published` reports the committed revision/operation; `aborted` has neither.
Status uses nullable `adapter_state: {adapter, details}`. DuckDB registers its
engine binding/materialization/session/import details; other adapters need not
invent engine fields. Sessions report registered `adapter_context`; generic
mapping identifiers are adapter-owned, with physical grammar checked by its
schema. Every build/extraction transfer carries its retryable attempt UUID.
Logical schemas in normalized table/schema-comparison results are
`{columns: [...]}` objects using GRV's ordered column representation.
Extraction sessions report null engine paths and mappings, and a nullable
`capture` summary with source identity, per-table capture times/counts/hashes,
and extraction job IDs. `completion_sha256` identifies the sealed capture
receipt for extraction, or the accepted build result for builds.
Adapter command `details` are validated by the named adapter's versioned schema.
Pull results always include common adapter/attempt/workspace, source
revision, generation/time, and replay fields. Every adapter, including built-ins,
reports `adapter_result` validated by its registered result schema. DuckDB
details include target schema, write/transform/scope/materialization modes, and
the concrete selected partition tuples. The core result has no adapter branches.

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
    "adapter": "duckdb",
    "attempt_id": "359c6d0f-a9c1-4ae6-b804-0742a5e2b9de",
    "workspace_id": "b2c81f94-45a6-4cb3-b937-82916c1e0842",
    "dataset": "mt_month_stats",
    "requested_revision": "latest",
    "committed_revision": "42",
    "generation_id": "c2dc35d6-05ae-4ff8-95db-8d4aaf6f1b88",
    "pulled_at": "2026-10-03T10:00:00Z",
    "replayed": false,
    "adapter_result": {
      "target_schema": "raw",
      "write_mode": "replace",
      "transform_mode": "identity",
      "materialization_mode": "local",
      "scope_mode": "complete",
      "source_partitions": [
        {
          "table": "mt_month_stats",
          "partition": {
            "period": "2026-09"
          }
        }
      ]
    }
  },
  "errors": []
}
```

### The type mapping (Arrow → engine)

The engine adapter preserves both values and the declared GRV logical type
when importing and exporting. V1's supported DuckDB mappings are:

| Arrow / GRV meaning | DuckDB |
| --------------------- | -------- |
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

With `--decl`, ask the registered adapter to inspect its connection. DuckDB
reads its checkpoints, mappings, and session
records in one consistent engine read transaction, then compare them with the
observed GRV state. Show each latest/fixed-selector materialization's committed
revision, generation, and target mapping; distinguish current, behind,
fixed, application-import, and uninitialized state. Append, partition-selected, and SQL scopes
report their successful import attempts rather than claiming that all destination
rows form an identity materialization of the latest revision. Inconsistent metadata is an error.
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

## Pull orchestration and DuckDB execution

The authoring selection, SQL bindings, write behavior, and empty-source rules
are defined in the main spec. The following protocol implements those rules.
Core orchestration is adapter-neutral: static validation → identity → destination
outcome resolution → fixed source selection → verified provider → destination
apply/outcome resolution. The numbered algorithm below is DuckDB's transactional
implementation, not a required DuckDB workspace for other adapters.

### Source selection and contracts

Resolve one committed revision and read its revision entries. Select declared
tables, then apply each table's exact partition tuple OR-list, before requesting
manifests/data. No predicate is inferred from SQL. Validate layouts for selector
keys. Unselected data or tombstones do not affect this import's availability;
revision commitment remains a metadata fact even when unrelated versions were
pruned. Only selected files are downloaded/hashed. SQL does not read unselected
rows. A selected partition/version must be complete, available, and valid.

Discover selected-source schemas from selected Parquet versions, validate each
against its durable baseline, and choose the longest selected prefix schema.
Null-padding follows GRV; a later baseline growth never invents a historical
source column. Apply `expect` assertions and resolve exact output contracts.
When selection has no versions, use `expect.columns` or the trustworthy prior
source contract recorded for this binding to create empty typed relations; a
missing template is an invalid declaration, not source-schema inference.
The receipt reports concrete selected partition tuples and resolved contracts.
An explicit empty partition list is still a selected application scope.

Reserved `grv_source`/`grv_target` SQL schemas are transaction-local and removed
before commit. An existing schema of either name is `STATE_CONFLICT`; never
replace it. Materialize verified local source relations before restricted query
evaluation. Bind missing targets as empty typed relations. Stage every query
result before any destination write, under one pre-write DuckDB snapshot.
External access/autoload/install are disabled during queries; adapters validate
resolved supported dependencies/functions, including local view expansion.
Do not claim hostile-SQL isolation. S3-view identity mode uses its separate
reader rules and does not evaluate user queries.

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
2. **Prepare the refresh.** Resolve the partition-selected entries and every manifest-listed
   data file in the selected source scope, reject `.pruned` versions, and check
   source logical schemas. Derive source/output contracts as above and ask the adapter for its physical
   mappings/ownership plan. Destination not-null checks run after selection and
   writes, not against source-only columns. Local mode verifies size and SHA-256
   into staging outside the managed tables. Any source failure aborts before
   refresh. S3-view verification follows the backend rules below.
3. **Plan under serialization.** Retain the workspace lock throughout this
   invocation; no other engine command or external build can replace its
   metadata. Read the current committed checkpoint and ownership records;
   an earlier diff is only a hint. Conflicting destination ownership or identity/application roles are errors;
   the source selector does not create a different destination scope.
4. **Apply and commit.** Use the selected `attempt_id` and open **one DuckDB
   transaction**. Bind private source/pre-write destination relations and stage
   all SQL selections first. For complete identity replacement, apply the whole refresh:
   all affected tables,
   partitions, schema changes, table or view creation, omitted-table
   emptying, ownership records, per-table `pull_meta`, and the dataset
   completion checkpoint, and immutable successful attempt receipt. Run all
   declaration checks over the resulting managed scope inside that transaction.
   Commit once after every change succeeds. Any failure rolls everything
   back; per-table commits or swaps followed by a separate checkpoint write
   are forbidden. Initial root/workspace binding is also part of this first
   managed transaction; a failed first pull cannot leave a bound partial workspace.
   For SQL or partition-selected replacement, replace each exclusively owned
   destination with its staged query result/selected source. For append, create missing targets or validate existing
   ordinary tables, then insert selected rows without clearing existing rows.
   Commit destination binding/import metadata, checks, and the immutable attempt
   receipt in the same transaction. Neither append nor SQL/partition-selected replacement creates
   an eligible identity materialization checkpoint. Rolled-back work has no
   success receipt. Remove transient source/target schemas before commit.

For complete identity replacement `refresh: full`, rebuild every managed table from the target
revision. For complete identity replacement `refresh: changed`, diff against the committed revision and update changed,
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
Root binding, exclusive identity/replacement ownership, append bindings,
application import metadata, and receipts are updated with their corresponding
table mutations in the same transaction. Conceptual records include:

```text
pull_checkpoint:
  dataset, target_schema, write_mode, transform_mode, revision_mode, materialization_mode,
  committed_revision, attempt_id, generation_id, pulled_at

pull_meta:
  dataset, grv_table, target_table,
  grv_revision, generation_id, logical_schema

pull_attempts (immutable, keyed by attempt_id across this workspace):
  attempt_id, workspace_id, request_sha256, dataset, target_schema,
  write_mode, transform_mode, revision_mode, materialization_mode, requested_revision,
  committed_revision, generation_id, pulled_at

import_bindings:
  dataset, target_schema, mappings, write_mode, transform_mode, scope_mode, output_schema

import_attempt_details (keyed by successful attempt):
  source_revision, source_partitions, resolved_source_contracts, output_contracts,
  declaration_sha256, query_sha256, table_counts
```

`write_mode` is `replace` or `append`; `transform_mode` is `identity` or `sql`.
`revision_mode` is `latest` or `fixed`, describing the requested source selector
for all writes. It is not an ownership key and never routes/renames targets.
SQL/partition-selected replacement and append store import metadata, not an
eligible identity checkpoint. Replacement ownership scope is stable across requests: bound root/workspace,
dataset, adapter, and adapter destination namespace (`target.schema` for DuckDB).
Mappings, source selectors, and query text identify the request, not the owner.
Mapping changes update that scope transactionally, clearing obsolete owned targets
and claiming new unowned targets under normal checks. Its identity/application
role remains fixed; role changes require a new destination namespace. Independent
replacement bindings of one dataset use different destination namespaces.
`materialization_mode` is `local` or `s3-view`. Table mappings are part of the
checkpoint scope. Each successful refresh has a new
canonical UUID `generation_id`. Every per-table record in that scope refers to
its completed generation, including empty or omitted tables. A build session accepts only complete, mutually consistent replacement
metadata and mappings; application import receipts are not eligible inputs.

`request_sha256` hashes the canonical pull request identity defined above,
including the effective declaration and adapter options, not an automatically
chosen diff plan or inferred source facts. It excludes the
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

### Completeness

After complete identity replacement, every table and checkpoint describes the complete
declared scope at one revision. SQL/partition-selected replacement and append instead attest the
completed query-defined import at that revision, with table counts and receipts.
Existing append rows can originate from other operations; they are not asserted
to belong to the selected source revision. Omitted partitions, empty versions, and omitted
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
All query failures are surfaced. Advanced external S3-view builds require network access and pay remote read
costs. Managed SQL builds locally materialize the held file sets before restricted
query evaluation. Local mode is the default.

## `grv session`

A GRV publication session fixes one target dataset/base, declaration, adapter
identity, run, completion, and export/publication attempts. Its mode determines
how source data is obtained; its GRV run and publishability rules are common.
Authentication sessions and source jobs remain adapter state.

`session prepare` accepts extraction or external DuckDB build declarations.
Managed builds use the same internal preparation lifecycle through the runner.
`session renew` uses context/backend only. `session abort` resolves any recorded
publication first, then abandons through normal run/claim protocols. It cannot
undo a committed revision. `push --session` completes an extraction or finalizes
an attested build, using the fixed context without new declaration overrides.

`session show` validates identities and reads the authoritative consumer record
under its mode-specific lock. It reports adapter/mode, target/base, inputs,
private mappings or capture, completion, export plan, attempt/outcome, and
observed run phase. It does not renew, recover, infer completion, or resolve an
ambiguous committing CAS. Inspection never emits credentials or owner tokens.

### Extraction session preparation and capture

1. Validate common and adapter declarations/capabilities before side effects.
   Select/report the attempt UUID and lock its consumer-state directory. A
   matching recorded attempt recovers its context/result; a changed fixed
   request returns `REQUEST_MISMATCH`. Resolve the adapter connection identity.
2. Initialize the target's `LATEST` if needed and read its base revision. Create
   an ordinary open GRV run with no GRV inputs; confirm the empty hold set.
   Record adapter/package identity, declaration/source configuration, source
   consistency, run/base/owner, and attempt in durable consumer state. Write
   the protected context atomically. No DuckDB build workspace is created.
3. On `push`, extract all declared tables and stage core-written Parquet outside
   GRV. Renew the run throughout acquisition; ownership loss stops publication.
   The adapter reports explicit successful completion even for empty tables.
   A failed table/page cannot become an omission or an empty snapshot.
4. Seal a durable capture receipt with each output's schema, partition groups,
   row counts, capture start/end times, source/job identity, and staged-file
   sizes/hashes. Stop all capture writers first. Its SHA-256 becomes the session
   completion digest; capture files are immutable after acceptance.
5. Finalize/publish using the common push protocol. The receipt is completion
   evidence, not a commit marker. Store the resolved result in consumer state
   after GRV commitment is established; adapter checkpoints follow that result.
   If `after_publish` is required, record pending acknowledgement alongside the
   known publication outcome before calling it; record completion durably.

The state directory contains `push/<attempt-id>/` with context, source-job
state, staged files, capture receipt, fixed plan, and result. File creation and
updates are atomic/durable; persistent `session.lock` uses exclusive nonblocking
`flock` and is never replaced or unlinked. Context aliases use this same lock.
These records are outside GRV and may not be inferred from folder listings.
The core owns lease renewal while a transfer command holds the session lock.
Between commands, an external driver can call `session renew` if it keeps a
prepared extraction session open. Salesforce sessions never acquire engine locks.

An accepted complete capture is reused on retry after verifying its hashes;
it is not queried again against a changing source. An incomplete capture may
resume only with the adapter's recorded resumable source identity. If that is
unavailable, abandon and start a new attempt. Expired/recovered ownership does
not authorize capture continuation or a new run under the same fixed attempt.
If a complete sealed run already matches its durable plan, publication may
resume under the existing core retry rules. Missing/corrupt required capture or
attempt state is an error, not authorization to silently re-extract.

`push --decl --attempt` creates/reuses this session and performs the lifecycle
in one command. `session prepare` can expose the same context explicitly.
Preparation and push retries resolve the recorded outcome before contacting the
source. Salesforce alias changes or adapter version changes reject an unfinished
retry. A recorded terminal publication result can be returned without reacquisition.
If a required post-publication hook is pending, same-attempt retry replays only
that idempotent acknowledgement with the fixed adapter state (authentication
may be needed for this hook), then records its completion. Hook failure reports
the known committed outcome plus an adapter error; it never republishes or
re-extracts. With no pending hook, receipt replay requires no re-authentication.

### DuckDB build session preparation and context

A **build session** is one normal GRV run in one product dataset plus its
private DuckDB engine workspace. The rest of this build subsection and its
driver/completion contract apply only to normalized `source.mode: build`. Its context is
fixed before the engine invocation. The
managed runner prepares internally, or the external driver calls
`session prepare`, waits for success, runs the engine, and renews the run until
the client seals it. Preparation holds the workspace
lock through its engine transaction and durable context creation. It creates
the root/workspace binding and complete empty receipt store atomically if this
is the first managed write to an otherwise unbound engine.

Before mutating a preparation, select its new run ID or discover the run ID of
an existing recorded preparation, then acquire that session's mutation lock
before the workspace lock. Any engine read needed solely to discover a retry
identity uses the workspace lock alone and releases it before acquiring the
session lock; reread the record under both locks before mutation. Never invert
the lock order. A context recovered from an existing record must match the
same effective declaration and canonical engine/root, with the declared target.

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

Every build preparation selects/reuses a transfer UUID. A core attempt index
in the selected push state store outside GRV/database records the fixed request,
canonical connection, and chosen run ID durably before opening a GRV run.
This reservation is not an engine binding or a prepared-success checkpoint;
engine preparation can still roll back without losing the retry identity.
The engine session and completed context later reference this same index entry.
Lookup/discovery releases the workspace lock before reacquiring session then
workspace locks; under both locks the recorded identities are rechecked. Under its lock,
a matching attempt recovers the fixed context/outcome; a mismatching declaration
or connection returns `REQUEST_MISMATCH`. A lost attempt mapping is not permission
to create a new run under the same identity. Managed push reports that UUID and
uses it for retry; external finalization takes it from the prepared context.

The context records the canonical root, engine path and workspace ID, run id
and owner token, target dataset and base revision, `declaration_sha256` and code
fingerprints, each input's dataset/revision/retention id and materialization
generation, and the private engine input/output mappings. It is consumer
state; no new GRV object type or run-control field is introduced. It contains
the immutable GRV run identity and backend coordinates needed for engine-free
renewal; renewal validates those against the current control. Preparation retries recover the same recorded session. A fresh run requires a
new attempt identity; retries never change an existing run's fixed inputs or base.

If preparation fails after creating a run, the client seals it without entries
when it still owns it, or leaves it to normal run recovery. It does not delete
the control or source holds as rollback. No build may start from a failed
preparation.

With normalized `self_input: true`, preparation materializes exactly the target
base revision locally. External model execution seeds private output tables from
those rows. Managed execution keeps a separate immutable `grv_self` read copy
and initializes private output tables empty; query results later replace those
outputs completely. This self-input is recorded by `base_revision`, not an external input
or a self-hold. Those rows must be materialized locally before the build,
because base revisions receive no additional GRV retention. A missing required
base version fails preparation. Without self-input, or for a new product,
outputs start empty with the declared schemas. Managed output writes always
replace the selected private output contents rather than append to seed rows.
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

### Managed runner execution

The runner prepares the same fixed inputs/holds/base and private outputs as an
external session. It holds adapter workspace ownership, supervises cancellation
and run renewal, and asks the adapter to evaluate the declared output queries.
DuckDB binds private inputs as `grv_input.<alias>` and, if enabled, immutable
locally materialized base copies as `grv_self.<output table>`. An existing schema
with either name conflicts. Those display bindings are not storage provenance.

Each query is one read-only statement. Resolved table dependencies are limited
to declared input/self bindings; external data and ordinary mutable working
relations cannot bypass provenance. All selected output query results are
staged and validated before replacing private output table contents. Each query
produces a complete relation, never an append onto prior-state rows. Query order cannot
introduce undeclared output dependencies. Suppressed hold/drop outputs are not
executed or attested. A successful zero-row query completes its output.

For local/previously S3-view inputs, materialize the held fixed file sets locally
before applying the user-query execution restrictions. Acquire private copies
under the preparation transaction; no subsequent tracking pull changes them.
If self-input is enabled, keep a separate immutable base copy for query reads;
never let a later output query observe an earlier query's new output.

Record successful invocation, verified selected outputs, stopped writers, and
fixed mappings internally using the completion contract below. No user-produced
completion file or renewal loop is required. On query failure, cancellation,
or ownership loss, stop/await writers, accept no completion, and abandon through
normal run recovery; never rebuild an incomplete attempt from leftover tables.
An accepted complete capture/export plan is reused on retry instead of rerunning
SQL. Cleanup preserves unresolved state and core retention evidence.

### External driver lifecycle and fencing

For an external integration, the driver owns the session through build/export.
The managed runner performs these obligations itself for managed builds. It calls
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

Before first finalization, the managed runner or external driver writes one
atomic, durable JSON file
conforming to [the build-completion schema](grv-client-v1-build-completion.schema.json).
It is written only after the invocation has succeeded, all output writers and
database connections have stopped, and the driver has established which
declared outputs completed. A driver-populated build with no model step uses `kind: direct`; an
omission-only build publication uses `kind: omission-only` and an empty completion
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
Once recorded, `--build-result` may be omitted on retry. For external finalization with no record, omitting the flag fails even when
prepared output tables exist. Managed finalization uses the runner's accepted
record without asking the user for a file. A terminal
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

## Push finalization

Extraction and managed-build declarations use ordinary `push --decl`.
Advanced external builds use prepared contexts and `push --session --build-result`.
Each reaches the same finalization algorithm after an explicit completion boundary.
The common normalizer derives output schemas/mappings and build inputs from the
public table-centric declaration; no separate user schema or provenance assertion
is accepted. Build queries/aliases and partition fields follow the main spec.

### Finalization algorithm

Use the extraction state lock, or the build session and DuckDB workspace locks,
as appropriate. Under those locks, first return an already recorded terminal
outcome or resolve a recorded publication attempt. Otherwise use the steps
below. A complete sealed-run retry skips steps 1–3 and verifies and materializes
the existing sealed payload in step 4; an incomplete sealed run cannot continue.

1. **Check the session and export.** Verify its recorded identities and fixed
   declaration, ownership of the open run, accepted completion record, and
   exclusive output mappings or accepted capture. Require completion for every
   selected output before deriving an empty group. Build mode reads one stable
   engine snapshot, validates schemas/checks, and stages complete Parquet.
   Extraction mode validates the already sealed capture, schemas/checks, and
   file hashes without reopening the source. Derive provenance from the fixed
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

### Build selection and no-op rules

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

These selector lists apply to build declarations. Extraction declarations
accept only `selection.policy: changed | all` and no selector lists; their
complete-snapshot membership rules are specified below.

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

### Extraction snapshot membership

An extraction declaration defines a complete snapshot of its target dataset:
all declared source tables succeed, their selected rows form full partition
snapshots, and the resulting dataset contains exactly the declared tables and
captured partition groups. The adapter's row filter is evaluated before
mapping/grouping. An unpartitioned zero-row table publishes an empty version.
A partitioned zero-row table contributes no groups and removes its old groups.

The core derives omission selectors for base tables no longer declared and
base partitions absent from the complete capture. They are guarded by the
prepared base revision. A failed/missing output cannot authorize any omission.
A source declaration is consequently the authoritative snapshot definition,
not a patch to unrelated tables in the same dataset. Use a separate dataset
when independent sources need independent refresh ownership.

`selection.policy: changed` reuses provably equal versions and creates new
versions for other groups; `all` creates new versions for every captured group.
Both have identical snapshot membership. Equality proof follows the core's
logical row/schema rules, never capture time or file names alone. A declaration
with zero source tables is invalid. Held/drop/include/empty selector lists are
build-only; extraction determines absence from successful full acquisition.

Extraction publication requires the committed predecessor to remain the
prepared base. Any intervening dataset revision, even to another table, returns
`STATE_CONFLICT`; a new session/attempt and capture are required. A no-op still
checks that base and proves all selected contents and membership unchanged.
This prevents an implicit rebase from combining snapshots of independently
captured source states. The shared publisher enforces the base check under
its dataset lease before the normal GRV publish steps.

No source watermark is advanced just because files were downloaded or staged.
A publication crash is resolved through the recorded GRV operation/chain before
recording consumer success. Run-control metadata is immutable from creation:
`metadata.grv_cli` may record the adapter/version, resolved source identity,
declaration/query fingerprints, and extraction attempt identity known then.
Capture end times, job IDs discovered later, and receipt hashes remain in the
durable consumer capture record; they cannot be added to run metadata at seal
time. No new GRV artifact layout is defined. Source provenance is not fabricated
as `derived_from` references to
external Salesforce or website identities. Extraction has no GRV dependency
holds. DuckDB build mode retains the actual fixed GRV holds and derivation rules.

### Schemas and provenance

New versions may append top-level columns only. Removing, renaming, retyping
(including precision or UTC changes), reordering, changing nested types, or
changing partition keys requires a new GRV table. Registration and extension
properties follow the durable table-wide baseline; failed registrations are
not rolled back. Adapter/source capture metadata or engine-specific fingerprints and artifacts
belong in permitted run metadata, respecting which run fields were
fixed at creation.

For build mode, every new derived version cites **all** the run's confirmed external input
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

## Required conformance scenarios

| scenario | required result |
| ---------- | ----------------- |
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
| Another binding overlaps an owned replacement scope | Reject before mutation regardless of source selector |
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
| Salesforce receives a pull declaration | Reject `UNSUPPORTED_CAPABILITY` before source login or mutation |
| An extraction fails on the second object/page | Accept no capture receipt; publish no partial snapshot or omission |
| A successful source query genuinely selects zero rows | Record completion and publish the defined empty snapshot |
| A Salesforce case stops matching the declared filter | The next full snapshot removes it from dataset state |
| A source alias or adapter version changes on an unfinished retry | Reject `REQUEST_MISMATCH`; preserve the fixed source identity |
| A retry has a sealed complete capture | Verify and reuse it without querying the changing source again |
| A captured decimal cannot be represented exactly | Reject the whole extraction before allocation |
| Another revision commits during an extraction | Reject snapshot publication against the stale prepared base |
| A table/partition disappears from a successful extraction snapshot | Omit it with the prepared-base guard; failures never authorize omission |
| A declaration maps two source columns to one output name | Reject duplicate/incomplete mappings before source acquisition |
| DuckDB append SQL excludes an existing business ID | Preserve existing rows; insert only query-selected rows |
| Incoming rows duplicate an ID and SQL uses a window rule | Apply the declared rule; infer no extra business-key policy |
| SQL append runs again with the same successful attempt ID | Return its receipt without re-evaluating SQL or duplicating rows |
| SQL append runs with a fresh attempt against the same revision | Evaluate current local conditions; infer no snapshot-ledger no-op |
| Two output queries inspect destination rows | Both see the same pre-write state; no query sees another output's inserts |
| Append SQL fails while writing its second target | Roll back rows, schema/binding metadata, checks, and receipts together |
| Append produces zero rows | Commit a successful zero-row import receipt |
| Append targets an existing ordinary table with a different schema | Reject before inserting or altering existing application rows |
| SQL selects a partition column from one chosen revision | Read only its manifest-selected versions with Hive inference disabled |
| SQL uses DML, multiple statements, or external-read functions | Reject before destination mutation |
| SQL output names/order/types differ from the resolved output column contract | Reject without silent projection or lossy casts |
| A build tries to use append, partition-selected, or SQL-import targets as identity GRV inputs | Reject unsupported provenance; do not infer a complete source materialization |
| A namespaced adapter command returns JSON | Validate the common envelope and registered redacted adapter-result schema |

| New scenario | Required result |
| --------- | ----------------- |
| Pull uses a fixed revision | Write the declared destination; create no generated historical schema |
| Same owned replacement scope changes latest/fixed selector | Permit the explicit request without changing ownership or target naming |
| Partition-selected import excludes a pruned unrelated month | Acquire/validate only selected entries; unrelated data availability does not fail the import |
| Selected partition itself is pruned | Fail unavailable before destination mutation |
| An explicit empty partition list has no known source contract | Request `expect.columns`; never invent an older revision schema from the current baseline |
| A baseline appended a column after the selected old revision | Discover the schema from selected versions; do not assert the later column existed |
| Identity replacement gains a partition selector or SQL | Require a separate application scope; never retain build-input eligibility |
| An adapter adds unknown binding fields | Reject through its registered closed schema without modifying the common envelope |
| Built-in and third-party bindings use equivalent capabilities | Follow the same registration/validation/lifecycle/result paths |
| A column/SQL file changes on unfinished retry | Reject changed effective request; preserve accepted capture/outcome evidence |
| SQL file contains two statements or an external-scanning local view | Reject before writes using the adapter parser/execution dependency contract |
| Managed build SQL fails on its second output | Accept no complete build result and publish no output from that invocation |
| Managed build owner is lost while execution remains active | Stop publication/allocation, cancel/await writers, never adopt its tables |
| Managed build completes an empty output | Attest completion internally and apply normal empty output rules |
| Source acknowledgement fails after confirmed publication | Preserve/report committed outcome; retry acknowledgement idempotently without re-extraction |
| Adapter cancellation sees an unresolved destination commit | Preserve receipts/journal and resolve outcome; never report assumed rollback |
| A replacement declaration changes its mapping members | Update the stable owner scope atomically and clear obsolete owned targets |
| Managed self-input query omits an old row | Replace private outputs with the complete result; do not retain or duplicate seeded rows |
