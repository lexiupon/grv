# GRV Client v1 Execution Semantics

## Relationship to the authoring specification

This normative companion specifies how the CLI, the transfer core, and adapters
implement [the user surface and extension contract](grv-client-v1.md) (the
client document). No CLI is implemented. Users author the client document's
declarations; they do not implement the protocols below. The managed runner
performs the build-driver duties for managed builds, and the explicit external
integration API keeps the same guarantees. [GRV v2](grv-storage-v2.md) remains
authoritative, and this companion adds no storage layout.

The [adapter process protocol](grv-adapter-protocol-v1.md) defines the local
channel that carries these obligations between the CLI and an adapter. See the
[specification guide](README.md) for document scopes and version relationships.
Terms such as *attempt*, *fixed request*, *capture*, *materialization*, and
*export plan* are used as defined in the client document's *Terms* section.

### Normalized plans and identity

This section defines how the core turns a declaration into one fixed execution
plan, and how that plan is identified so that retries repeat the same request.

#### Parsing and validation order

Normalization runs in this order:

1. Validate the common envelope and the registered adapter points, and resolve
   capabilities.
2. Expand local column and SQL files, and apply defaults.
3. Bind the stable connection identity.

YAML and reference parsing follows these rules:

- Parse YAML 1.2 into JSON-compatible values.
- Reject duplicate keys, aliases, custom tags, non-string keys, non-finite
  values, and unknown fields.
- References are local and one level deep. There is no environment
  interpolation and no remote include.
- An IPC reference contains exactly one Arrow Schema message, with no trailing
  batches or other messages.

Unsupported Arrow types and extensions fail before acquisition or mutation. The
supported types are defined in the client document, *Output columns and types*.

#### Internal plan fields

One core normalizer produces a fixed execution plan for inline and file
contracts and for every adapter. The following names are **internal plan
fields**, not a second authoring format:

- `target.grv` and `source.grv` are derived from the sole public `dataset`.
- `source.mode` is `extract` when no `build` is present; otherwise it is
  `build`.
- `source.tables` contains the resolved source and output mappings. The push
  `schema` contains the ordered Arrow contracts derived from the tables'
  `columns`.
- Build `inputs`, `self_input`, and `code_fingerprints` come from `build`.
  `derived_from: session` is a core provenance rule, never a user assertion.
- Pull `target.write` derives from `write` (default `replace`). Pull
  `target.mode` derives from the adapter's materialization options (default
  `local`).
- Pull source schemas, layouts, and selected partition tuples come from
  verified selected versions, or from a declared or trustworthy empty-source
  template.
- A pull table's `select.schema` is the resolved output `columns`; its source
  `schema` is the selected-source contract. Normalization expands SQL files to
  query text.
- `refresh` is a work plan, not a user-required switch. It is `changed` only
  for complete identity replacement under `options.refresh: auto`; otherwise it
  is `full`.
- `scope_mode: selected` means at least one explicit partition selector.
  `transform_mode: sql` means at least one table query.

#### Names and mappings

- Mappings, aliases, and physical destinations must be unique.
- GRV names follow GRV's canonical grammar.
- DuckDB destination and schema names follow `^[a-z0-9_][a-z0-9_-]*$`. A
  qualified local relation has exactly the form `schema.table`. Quote
  identifiers.
- `_grv`, `grv_source`, `grv_target`, `grv_input`, `grv_self`, and private
  session namespaces cannot be extraction sources or pull targets.
- Each output column has exactly one resolved mapping, and there are no
  undeclared columns. A source selector may be repeated for distinct output
  names.
- Exact types, ordered output, partition columns, and registered extensions
  must pass core validation.

Field selectors, source predicates, `derive` mappings, checks, and the
optional `columns` assertion of an identity pull follow the client document,
*Output columns and types* and *Push selection*. In short, a `derive` mapping
lets an extraction compute a `_{key}_` partition column from another output
column. The core computes derived columns from the mapped batch values before
partition grouping. Derived columns are not part of the contract sent to the
adapter; the adapter never produces or sees them.

#### Declaration and request identity

`declaration_sha256` hashes the canonical effective authoring configuration
using [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785). Before hashing:

- expand defaults and local files;
- normalize the source revision to `latest` or a decimal string; and
- resolve paths.

The adapter identity (package, interface, and binding-schema versions) and the
stable connection identity are hashed together with the canonical declaration
into `declaration_sha256`, so a changed adapter version or connection changes
the fixed request. Secrets are stored
through adapter authentication, never in the declaration or the hash. A reused
alias cannot retarget an unfinished attempt.

Pull request identity includes the root, workspace, destination mappings and
role, selected partitions, output and expected contracts, effective adapter
options, and requested source selector. It is hashed as `request_sha256`
(*Consumer metadata*).

Generated attempt and run IDs, and source facts learned only during capture,
are not declaration inputs. Inferred source schemas are output facts of a
successful pull receipt, not mutable inputs to its request hash. An uncommitted
`latest` retry may rediscover them with a newer revision. Accepted push
captures and export plans remain fixed on retry.

A pull retry checks its receipt before any source contact (*The pull
algorithm*, step 1). A terminal receipt can be returned without the source and
without re-authentication. If receipts are lost, the operator must restore
them from backup; until then, a replay of an affected attempt fails with
`PROTOCOL_FAILURE` and is never re-executed.

### Command surfaces

Ordinary push, pull, adapter, and inspection commands are defined in the client
document. The advanced flags are:

- `--attempt <uuid>` and `--state <dir>` on push;
- `--attempt <uuid>` on pull; and
- the root initialization parameters in *`grv init`*.

The explicit integration API is:

```console
grv session prepare --session <context.json> --decl <yaml> --grv <root> [--state <dir>] [--attempt <uuid>]
grv session show --session <context.json>
grv session renew --session <context.json>
grv session abort --session <context.json>
grv push --session <context.json> [--build-result <result.json>]
```

- `push --decl` performs an extraction or a managed build, and its
  publication.
- `build.execution: external` requires explicit preparation, invocation, and
  finalization.
- A session's target dataset and adapter connection are fixed in the
  declaration. There are no `--engine` or `--target` overrides.
- A pull's `--revision <N|latest>` changes only its source selector.

This companion's engine process and catalog details, and its JSON build-result
file, describe the initial DuckDB integration. Other build adapters supply
registered completion and mapping details through the same logical SDK
boundary. In either case, core publication validates the fixed inputs and the
completion.

## Runtime ownership and results

This part defines who may open the DuckDB database, which locks each command
takes, and the shape of every command result.

### DuckDB process ownership

#### Workspace lock

V1 serializes **all database access** between the CLI and external engine
invocations with one non-expiring OS process lock per workspace.

- The adapter and the driver use the same lock identity, derived from the
  canonical database path. Symlink aliases resolve to it. Hard-link aliases are
  rejected.
- The persistent lock file is `<canonical-engine-path>.grv-lock`. Participants
  take an exclusive `flock` on it and never unlink or replace it while the
  workspace exists.
- The database file must not be moved or replaced during its lifetime.
- The managed runner uses the adapter lock helper. An advanced external driver
  must implement the same protocol or use that helper.
- DuckDB's own file lock remains an additional safeguard. See
  [DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency).

How the lock is held:

- Every command that opens the engine holds the workspace lock until its
  connections close.
- The runner or external driver holds the lock throughout the engine
  invocation, including child processes. It releases the lock only after all
  database users and writers have stopped and all connections have closed.
- Contention returns `ENGINE_BUSY` (exit 3) without engine mutation. V1 does
  not queue or steal a live lock.
- A driver crash does not authorize a new engine user while an old process
  still has the database open. DuckDB connection contention is also reported
  as busy.

Which commands take it:

- `status --decl`, `session show` for a build, build preparation, DuckDB pull
  and extraction, build finalization and abort, and engine cleanup require the
  workspace lock.
- `session renew` requires no engine connection and no workspace lock. It
  validates the self-contained context and GRV identities, then renews the run
  through the backend. It therefore continues during a build.
- Backend-only inspection, pin, GC, and recovery commands likewise do not open
  DuckDB.
- Salesforce extraction sessions never open DuckDB or acquire its lock.

A busy engine inspection may report already observed GRV facts in its partial
error result. It cannot report an unobserved checkpoint.

#### Session mutation lock

Mutating CLI operations on one build session are serialized by a non-expiring
local process lock, released by process exit. Extraction sessions do not use
this lock; they serialize on the `session.lock` in their state directory
(*Extraction session preparation and capture*).

- Its persistent file is `<canonical-engine-path>.grv-session-<run-id>.lock`.
  Use an exclusive `flock`, never unlink the file during the workspace
  lifetime, and acquire it without waiting. Copies or aliases of a context
  therefore use the same lock.
- Contention returns `ENGINE_BUSY`. This includes renewal, finalization, and abort. The
  operation holding the lock performs its own renewals as needed.
- An expiring client-side lock is insufficient.
- A retry first resolves the durable session plan and any recorded publication
  attempt before starting new work.
- The driver does not hold this lock for the external invocation. It holds the
  workspace lock and performs backend-only renewals between commands.

#### Lock ordering

The session mutation lock is distinct from the workspace lock. When a command
needs both:

1. Acquire the session lock first.
2. Attempt the workspace lock without waiting.
3. If either is busy, release the locks already acquired before reporting busy.

Never invert this order. An engine read needed only to discover a retry
identity (for example, the run ID of an existing preparation) uses the
workspace lock alone and releases it before acquiring the session lock. The
command then rereads the record under both locks before any mutation.

Neither lock is a GRV lease. This model permits one active external invocation
per engine file. Private tables preserve provenance but do not permit
concurrent process access.

### Common command behavior

These rules apply to every command: root handling, revision resolution, the
JSON envelope, errors, and exit statuses.

#### Roots and read-only commands

- GRV-facing commands validate `grv.json` and use the backend contract.
- Adapter registry, capability, and namespaced commands need no GRV root and
  report `root: null`. Login may modify adapter authentication state.
- Only `init` initializes a root. Other commands report an uninitialized or
  damaged root rather than inventing configuration.
- Dataset arguments are canonical names within the selected root.
- Backend-only commands require neither an engine nor a declaration.
- Read-only commands never establish an engine binding, acquire leases,
  complete pending operations, create receipts, or change retention.

#### Observations and revision resolution

- Listings discover objects. They do not prove commitment, availability, or an
  operation's success.
- Revision-based commands resolve a committed revision by the receipt or chain
  rules in GRV §7.
- A command samples `LATEST` once for each default or `latest` selector and
  reports the resolved number.
- Read-only commands may use the last committed revision while an operation is
  pending, and report the observed coordination state. They take no reader pin,
  so later GC can make data unavailable during or after inspection.
- A root-wide listing and an engine-to-GRV comparison are observations, not a
  cross-dataset transaction.
- When a default selector observes `LATEST.revision: 0`, inspection and
  verification use the empty initial state without looking for a revision-0
  parquet.
- A missing `LATEST` is reported as missing coordination metadata, not
  silently converted into revision 0. Creating a new target's initial `LATEST`
  belongs to the mutating core protocol.

#### Output envelope

All commands support `--json`. Stdout then contains one JSON document
conforming to [the output schema](grv-client-v1-command-output.schema.json),
with `output_version: 1`, `command`, canonical `root` (null until known), `ok`,
`exit_status`, `result`, and `errors`.

- Command names are `init`, `ls`, `show`, `status`, `log`, `diff`, `verify`,
  `pull`, `session prepare`, `session show`, `session renew`, `session abort`,
  `push`, `adapter list`, `adapter capabilities`, `adapter command`, `pin`,
  `unpin`, `gc`, and `recover`. An unrecognized command uses `unknown` in an
  error envelope.
- Results identify resolved revisions and partial progress. Errors have stable
  codes and affected object identities.
- Human-readable output presents the same facts. Progress goes to stderr.
- Inspection output omits credentials and owner tokens. The protected session
  context still contains the token its driver needs.
- Collections are ordered by canonical object identifiers. Revision history is
  newest first.
- Revision numbers, version numbers, and byte totals use canonical decimal
  strings in normalized results to preserve int64 precision. Revisions must
  still fit GRV's `0..2^63-1` range and versions its `1..2^63-1` range.
- Core metadata embedded in `details` keeps GRV's JSON representation, with
  credentials and owner and lease tokens removed. Its returned fields follow
  the core contracts.

#### Success, partial, and error results

- A successful result contains every field its command's schema requires.
  `errors` is empty and `exit_status` is 0.
- An error result is null or a typed subset of that command's result fields.
  An absent field means its value or effect was not established. It never
  means rollback or a default value.
- Arrays in a partial result contain only established observations or effects.
  They do not claim that enumeration finished.
- `errors[0]` is the primary error and determines the exit status. Subsequent
  errors are ordered by object identity.
- Every error contains `code`, `message`, nullable `object`, and `retryable`.
  `retryable` means the same fixed request can be retried when the blocking
  condition clears. It is not permission to change a session base or to ignore
  an unknown outcome.

#### Exit statuses

Exit status is `0` for a completed request, including an informational stale
status, a diff with changes, a dry run, or an idempotent no-op. Nonzero
statuses are:

| exit | meaning |
| ------ | --------- |
| `2` | Invalid arguments or declarations |
| `3` | Conflict, busy resources, or lost ownership |
| `4` | Not-found or unavailable requested state |
| `5` | Integrity or protocol failure |
| `6` | Backend or engine failure, or an outcome that could not be resolved |

Partial mutation is reported with a nonzero status and its known committed
effects. The client never reports rollback of an already committed GRV
operation. Existing attempt identities must be resolved before a retry starts
another attempt.

#### Error codes

V1 error codes are closed. Adding a code, or making an incompatible output
change, requires a new output version. Producers emit only schema-defined
fields. Consumers must select a supported version before interpreting a
response.

| code | exit | meaning |
| ------ | ------ | --------- |
| `INVALID_ARGUMENT` | 2 | Invalid command, flags, or identifier |
| `INVALID_DECLARATION` | 2 | Invalid declaration, unsupported type or extension, or failed declared check |
| `REQUEST_MISMATCH` | 2 | Attempt or completion identity reused with another fixed request |
| `BUILD_INCOMPLETE` | 2 | Missing successful build completion evidence |
| `UNSUPPORTED_CAPABILITY` | 2 | Adapter does not implement the requested direction or write behavior |
| `EXTRACTION_INCOMPLETE` | 6 | Source acquisition has not completed all declared outputs |
| `ADAPTER_FAILURE` | 6 | Adapter authentication, source, or destination operation failed |
| `ENGINE_BUSY` | 3 | Workspace lock, session mutation lock, extraction `session.lock`, or DuckDB connection is busy |
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
object identities, state entries, session records, retention explanations, and
progress records. All producers must validate their envelope against it.
Formats such as UTC timestamps are validated as well as JSON structure. The
cross-field invariants below remain mandatory even where JSON Schema cannot
express them. Core and registered extension objects are the only open metadata
objects; client-defined result objects have no unspecified fields.

| command | result contract |
| --------- | ----------------- |
| `init` | Creation flag, core format and version, and effective root parameters |
| `ls` | Scope, nullable dataset and revision, and object-identified items with observed coordination and details |
| `show` | Revision, predecessor, and operation; state entries; per-table layout, baseline schema, revision schema, and provenance; optional retention explanation |
| `status` | Current state, lease and pending operation, runs, and optional registered adapter state |
| `log` | Limit, `has_more`, and newest-first committed entries with change counts and run IDs |
| `diff` | Resolved endpoints, `changed`, entry changes, table membership changes, and known or unknown schema comparisons |
| `verify` | Resolved scope, full-hash mode, named check outcomes, and unavailable objects |
| `pull` | Adapter and write mode, attempt and workspace IDs, fixed scope and selector, committed revision, generation, and time, and `replayed` |
| `session prepare`, `session show` | Context and state location, adapter and mode, nullable engine path, fixed inputs, mappings or capture, completion, plan, attempt and outcome, and observed run |
| `session renew` | Dataset and run, open phase, renewed expiry, and `renewed: true` |
| `session abort` | Dataset and run, known terminal outcome, and finalized entries |
| `push` | Adapter and mode, nullable extraction attempt ID, dataset and run, completion digest, known published or no-op outcome, and `replayed` |
| `adapter list`, `adapter capabilities` | Adapter identity and version, directions, consistency, supported write modes, and command names |
| `adapter command` | Adapter identity, namespaced command name, and separately validated redacted adapter result |
| `pin`, `unpin` | Fully scoped pin, audit, and release information, and `no_op`; unpin also reports remaining protections |
| `gc` | Mode, per-version progress and reasons, releasable and released holds, completed operations, byte estimates, and waiting work |
| `recover` | Mode and scope, per-run progress, completed operations, pending and waiting work |

#### Field semantics

- `replayed` means the existing recorded result was returned rather than a new
  refresh or publication.
- `no_op` means the requested retention mutation was already satisfied.
- A session outcome of `no-op` means no revision publication occurred. Its
  `revision` reports the observed predecessor, and `operation_id` is null.
  `published` reports the committed revision and operation. `aborted` has
  neither.
- Status uses nullable `adapter_state: {adapter, details}`. DuckDB registers
  its engine binding, materialization, session, and import details. Other
  adapters need not invent engine fields.
- Sessions report registered `adapter_context`. Generic mapping identifiers
  are adapter-owned, with their physical grammar checked by the adapter's
  schema.
- Every build or extraction transfer carries its retryable attempt UUID.
- Logical schemas in normalized table and schema-comparison results are
  `{columns: [...]}` objects using GRV's ordered column representation.
- Extraction sessions report null engine paths and mappings, and a nullable
  `capture` summary with source identity, per-table capture times, counts, and
  hashes, and extraction job IDs.
- `completion_sha256` identifies the sealed capture receipt for an extraction,
  or the accepted build result for a build.
- Adapter command `details` are validated by the named adapter's versioned
  schema.
- Pull results always include the common adapter, attempt, workspace, source
  revision, generation, time, and replay fields. Every adapter, including
  built-ins, reports `adapter_result` validated by its registered result
  schema. DuckDB details include the target schema; the write, transform,
  scope, and materialization modes; and the concrete selected partition
  tuples. The core result has no adapter branches.
- Session `export_plan` is a normalized summary of the durable export plan:
  prepared base, completion digest, write, reuse, and empty contributions with
  nullable assigned version and run identities, omissions, and held tables. It
  is not a substitute for the authoritative allocation records.
- Input mappings identify each source table and materialization generation.
  `input_revisions` deduplicates the external holds by dataset and revision,
  independently of those mappings.
- Version progress `bytes` is null when unknown. `known_bytes` sums only the
  known sizes of eligible or decided prune candidates. It does not include
  protected versions and does not assert physical disk space reclaimed.
- `has_more` is established by observing the next committed predecessor after
  the requested log limit.

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

This section defines how the DuckDB adapter stores each declared type. The
mapping from declaration types to GRV logical types, and the rule for
unsupported types, are in the client document, *Output columns and types*. The
engine adapter preserves both the values and the declared GRV logical type when
importing and exporting. V1's supported DuckDB mappings are:

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

- The adapter keeps the declared unit and UTC flag in consumer schema metadata,
  so a native engine type does not by itself determine the export schema.
- Exports must be exactly representable in the declared type. For example, a
  millisecond declaration rejects values with a nonzero sub-millisecond
  component.
- Timezone-aware values denote instants and use UTC for interchange. Unadjusted
  values keep their local-time meaning.
- DuckDB has no native timestamp type that combines nanosecond precision with
  timezone awareness. V1 rejects that combination before materialization or
  version allocation.
- V1 likewise rejects any other type it cannot preserve faithfully, including
  an ambiguous Arrow representation of a GRV TIME UTC flag.
- Additional mappings require explicit round-trip rules. There are no implicit
  truncations and no conversions to strings. See
  [DuckDB timestamp types](https://duckdb.org/docs/current/sql/data_types/timestamp).

Writers also honor GRV table extensions and column `ext` properties. An
unsupported required extension makes publication fail before acquiring claims
or registering schemas, as GRV §3 requires.

## `grv init`

`grv init` initializes one empty GRV root through GRV §2's conditional creation
of `grv.json`. Its parameters configure the store; they are not per-command
timing overrides. Values are integer seconds:

| Parameter | CLI default for a new root | Constraint |
| --------- | --------- | --------- |
| Maximum clock skew | 30 | Nonnegative |
| Maximum lease TTL | 900 | Positive, and greater than the skew so a conforming lease can be renewed |
| Pending grace | 604800 | Positive |

Creation rules:

- Before creation, confirm that `datasets/` is empty. A missing `grv.json`
  with existing dataset objects is a damaged store, and initialization fails.
- If another initializer wins the conditional create, reread and validate its
  configuration.
- An existing valid root makes `init` idempotent. Explicitly provided
  parameters must match it; omitted flags accept its stored values.
- Updating an existing root's parameters is outside v1.

Report the canonical root, format and version, effective parameters, and
whether the root was created or already initialized. Initialization creates no
dataset, revision, engine binding, or build session.

## Inspection commands

These commands report committed GRV state and observed coordination. They
follow the read-only rules in *Common command behavior*.

### `grv ls`

Without a dataset argument, `ls` discovers dataset names under `datasets/` and
reports each one's observed current revision and retirement state.

- Missing coordination metadata is reported as absent or unknown, never as
  proof of an empty committed dataset.
- Listings may include an unpublished dataset being prepared by a writer. Its
  physical presence does not imply a published state.

With a dataset argument, `ls` resolves the requested revision and lists the
tables present in its state. With `--table`, it lists that table's canonical
partition objects and their selected version and run identities.

- `--table` requires a dataset. `--revision` is valid only with a dataset.
- Physical table folders or orphan versions do not become state members merely
  because a listing finds them.
- Empty versions remain members. An empty revision produces an empty table
  list.

### `grv show`

`show` describes one committed revision, by default the observed current
revision. It reports the revision's predecessor, publication operation, table
and partition membership, selected versions, and producing runs. When the
referenced manifests can be read, it also includes layouts, the current
registered schema baselines, and the revision's logical table schemas and
provenance. Baseline and revision schemas are labeled separately: a later
baseline extension is not evidence of a historical schema.

Inspection remains useful after pruning. Retained revision, run, and operation
records are displayed, while unavailable manifests, data, or schema details are
explicitly marked unavailable or not checked. `show` does not claim full
integrity verification. Corrupt records are errors, not omitted output.

`--retention` explains what protects the revision's versions. It lists:

- pin IDs and scopes, active or released status, and creation audit
  information and reasons;
- dependency holds and their consumer datasets and runs; and
- the observed protections of the selected revision's versions, including
  current-state protection, supersession grace, and applicable table,
  partition, or version pins created by other conforming tools.

A missing supersession receipt is reported as protection whose grace has not
yet started; inspection does not create it. Releasable holds are distinguished
from released holds. Unreadable protection records make the explanation
incomplete and return a protocol or backend error, rather than a claim that the
data is unprotected.

### `grv status`

`status` reports the dataset's observed current revision, retirement, dataset
lease and pending operation, and discovered run controls with their phases and
lease expiry. An expired lease makes recovery eligible; it does not prove that
recovery has happened. Run IDs and holders are reported without owner tokens.

With `--decl`, `status` asks the registered adapter to inspect its connection.
DuckDB reads its checkpoints, mappings, and session records in one consistent
engine read transaction, then compares them with the observed GRV state:

- For each materialization with a latest or fixed selector, show its committed
  revision, generation, and target mapping. Distinguish current, behind, fixed,
  application-import, and uninitialized state.
- Append, partition-selected, and SQL scopes report their successful import
  attempts, rather than claiming that all destination rows form an identity
  materialization of the latest revision.
- Inconsistent metadata is an error.
- An unbound engine is reported as uninitialized, without binding it or
  guessing provenance from its ordinary tables. A foreign-root binding is
  rejected.

Session status includes the fixed base and input revisions, the recorded
outcome or pending publication attempt, and the observed GRV run phase. Table
existence cannot prove that an engine job succeeded. A behind materialization
or a healthy open run is informational and leaves the command successful.

### `grv log`

`log` walks the committed `previous_revision` chain from the observed current
revision, newest first. `--limit` defaults to 20 and must be a positive
integer. Each entry reports the revision and predecessor, creation time,
publication operation, change summary, contributing run IDs, and optional
reason and code metadata from the retained records.

- Orphan revision files are excluded.
- A missing or invalid chain record is an error. The client never invents a
  gap or repairs history.
- Log does not require historical data to remain available. A revision's
  commitment is a historical fact; its data availability is separate and may
  be unknown until checked.
- A dataset at revision 0 has an empty log.

### `grv diff`

`diff` resolves both endpoints in the same dataset. `--from` is an explicit
revision number. `--to` is an explicit revision or `latest`, resolved once.
Revision 0 is accepted as the empty initial state. Every nonzero endpoint must
be committed; an orphan path is not an endpoint.

The comparison is by canonical `(table, partition)` revision entry:

- Report added, removed, or changed selections with before and after version
  and run IDs, and report table membership changes.
- Report logical schema differences when the manifests needed to establish
  them are available; otherwise mark those details unknown.
- The revision-entry comparison still works after data pruning. It does not
  require warehouse materialization or row-level comparison.
- A changed version is a selection change, not proof that row values differ.

Return `changed` explicitly in JSON. Both an equal diff and a diff with changes
exit successfully. Invalid or unavailable endpoint records are errors.

## `grv verify`

`verify` checks the format and integrity of one committed revision through GRV
§4–§7 and §9. It is not engine execution and does not run a declaration's
business-data checks.

It checks, for the resolved revision and its referenced versions:

- revision metadata and uniqueness;
- layouts and partition identifiers;
- manifests;
- sealed run and control agreement, and run entries;
- schema baselines;
- confirmed dependency holds and resolvable references;
- all manifest-listed files, including tombstones and file sizes and
  validators; and
- provenance: commitment, selector membership, and confirmed active holds, as
  the core fence requires.

A validator mismatch requires full SHA-256 verification, even in the default
mode. `--full` computes every data file's SHA-256 in the selected revision
regardless of a matching validator. Repeated version and file checks are
deduplicated within one invocation.

Report the scope, resolved revision, checks performed, unavailable objects, and
integrity errors. Classify findings as follows:

- A pruned or missing data object is unavailable.
- A present object with inconsistent metadata or a failed hash is an integrity
  failure. No observed tombstone is ignored because residual data still exists.
- A reference that was never committed, or a malformed coordination record, is
  a protocol failure.

Verification acquires no pin or lease and makes no repairs. Its success
describes the objects checked during the invocation, not future retention or a
root-wide snapshot. Concurrent pruning is surfaced as unavailability. Users who
need continuing availability first establish a revision pin.

## Pull orchestration and DuckDB execution

This part specifies how the core and the DuckDB adapter execute a pull. The
user-visible selection, SQL bindings, write behavior, and empty-source rules
are defined in the client document (*Pull selection and write behavior*, *SQL
bindings and transaction*, and *SQL execution boundary*).

Core orchestration is adapter-neutral and runs in this order:

1. static validation;
2. identity;
3. destination outcome resolution;
4. fixed source selection;
5. verified source provider;
6. destination apply and outcome resolution.

The numbered pull algorithm below is DuckDB's transactional implementation. It
does not require other adapters to use a DuckDB workspace.

### Source selection and contracts

This section states how the core implements the client document's selection
rules before any destination is touched.

1. Resolve one committed revision and read its revision entries.
2. Select the declared tables, then apply each table's exact partition-tuple
   OR-list, before requesting manifests or data. No predicate is inferred from
   SQL. Validate layouts for the selector keys.
3. Download and hash only the selected files. A selected partition or version
   must be complete, available, and valid. Unselected data and tombstones do
   not affect this import's availability, and revision commitment remains a
   metadata fact even when unrelated versions were pruned. SQL does not read
   unselected rows.
4. Discover the selected-source schemas from the selected Parquet versions,
   validate each against its durable baseline, and choose the longest selected
   prefix schema. Null-padding follows GRV. A later baseline growth never
   invents a historical source column.
5. Apply `expect` assertions and resolve the exact output contracts.

When the selection has no versions, the client document's empty-source
contract applies. The trustworthy prior source contract it permits is the one
recorded for this binding.

The receipt reports the concrete selected partition tuples and the resolved
contracts. An explicit empty partition list is still a selected application
scope.

The SQL bindings and the query restrictions are those of the client document.
The DuckDB implementation adds two rules:

- The reserved `grv_source` and `grv_target` schemas are transaction-local and
  are removed before commit.
- An existing schema named `grv_source` or `grv_target` is a
  `STATE_CONFLICT`. Never replace it.

### The pull algorithm

1. **Identify and resolve the request.**
   - Read `grv.json` and validate the effective declaration.
   - Acquire the workspace lock, open DuckDB, and validate its root and
     workspace binding and its consumer metadata.
   - Use the caller's `--attempt`, or generate and report a fresh UUID.
   - Look up that ID in `pull_attempts` **before resolving or downloading
     source data**, comparing the fixed request and the recorded adapter
     identity. A matching committed receipt returns its original outcome. A
     receipt for a different request returns `REQUEST_MISMATCH`. A retry never
     re-applies a successful attempt or rewinds newer materializations.
   - If no receipt exists, resolve one committed revision. A historical
     revision's supersession receipt may prove commitment; otherwise walk the
     chain from `LATEST`. A revision parquet existing at a named path is not
     sufficient, because orphan revisions are not dataset states. Revision 0
     denotes the unpublished, empty initial state.
2. **Prepare the refresh.**
   - Resolve the selected partition entries and every manifest-listed data
     file in the selected source scope. Reject `.pruned` versions, and check
     source logical schemas.
   - Derive the source and output contracts as in *Source selection and
     contracts*, and ask the adapter for its physical mappings and ownership
     plan.
   - Destination not-null checks run after selection and writes, not against
     source-only columns.
   - Local mode verifies size and SHA-256 into staging outside the managed
     tables. S3-view verification follows *Backends and S3 views*.
   - Any source failure aborts before the refresh.
3. **Plan under serialization.** Keep the workspace lock throughout this
   invocation, so that no other engine command or external build can replace
   its metadata. Read the current committed checkpoint and ownership records;
   an earlier diff is only a hint. Conflicting destination ownership, or
   conflicting identity and application roles, are errors. The source selector
   does not create a different destination scope.
4. **Apply and commit.** Use the selected `attempt_id` and open **one DuckDB
   transaction**.
   - Bind the private source relations and the pre-write destination
     relations, and stage all SQL selections first.
   - For complete identity replacement, apply the whole refresh in this
     transaction: all affected tables and partitions, schema changes, table or
     view creation, emptying of omitted tables, ownership records, per-table
     `pull_meta`, the dataset completion checkpoint, and the immutable
     successful attempt receipt. Run all declaration checks over the resulting
     managed scope inside the transaction. Commit once, after every change
     succeeds. Any failure rolls everything back. Per-table commits or swaps
     followed by a separate checkpoint write are forbidden. The initial root
     and workspace binding is also part of this first managed transaction, so a
     failed first pull cannot leave a bound partial workspace.
   - For SQL or partition-selected replacement, replace each exclusively owned
     destination with its staged query result or selected source.
   - For append, create missing targets or validate existing ordinary tables,
     then insert the selected rows without clearing existing rows.
   - For SQL or partition-selected replacement and for append, commit the
     destination binding and import metadata, checks, and the immutable attempt
     receipt in the same transaction. Neither creates an eligible identity
     materialization checkpoint.
   - Rolled-back work has no success receipt.
   - Remove the transient source and target schemas before commit.

For complete identity replacement, the refresh plan (*Internal plan fields*)
chooses the strategy:

- `refresh: full` rebuilds every managed table from the target revision.
- `refresh: changed` diffs against the committed revision and updates changed,
  added, and removed partitions, reconciling the complete target table
  schemas.

Neither strategy exposes a partial dataset. Tables unaffected by an incremental
refresh are still represented by the new completion checkpoint.

#### Resolving an ambiguous commit

1. Reopen the database under the workspace lock, allowing normal DuckDB
   recovery.
2. Read `pull_attempts[attempt_id]`. A matching receipt proves success, even
   after a crash and subsequent refreshes.
3. Absence proves no commit only after the prior writer is fenced by process or
   file ownership and the complete, trustworthy receipt store is readable. Then
   replan from the current checkpoint. Without an explicit revision selector,
   an uncommitted retry may resolve a newer `LATEST`.

If the prior process still owns the file, report busy. If durability or
metadata cannot be established, report `OUTCOME_UNKNOWN` or
`PROTOCOL_FAILURE`. Never infer rollback from a different current checkpoint.

On replay, return the committed receipt's revision and generation with
`replayed: true`, even if that generation is no longer materialized or its
source has since been pruned. A new pull requires a new attempt ID.

#### Receipts and lost metadata

- The CLI reports success only after the commit is known. This is GRV §11's
  transactional adapter path: the receipt is a committed result, not a
  separately committed dirty-table journal.
- Receipts are retained for the workspace's lifetime and are not removed by
  session or cache cleanup.
- A missing receipt table in a previously initialized workspace is
  inconsistent metadata, not an empty history.
- If checkpoints or per-table materialization metadata are missing or
  untrustworthy, but the binding, ownership, and receipt history remain
  trustworthy, rebuild the full managed scope, including emptying obsolete
  managed tables. Never infer contents from an old watermark. A rebuild does
  not recreate lost attempt history or resolve an unknown outcome.
- If root identity or target ownership cannot be established, reject the
  refresh and require restored metadata or a new workspace, rather than
  guessing which shared tables to clear.

The DuckDB adapter uses explicit transaction boundaries for data, catalog, and
metadata changes; see
[DuckDB transaction management](https://duckdb.org/docs/current/sql/statements/transactions).

### Consumer metadata

All client-managed metadata lives in the engine's reserved `_grv` schema. Root
binding, exclusive identity and replacement ownership, append bindings,
application import metadata, and receipts are updated with their corresponding
table mutations in the same transaction. The conceptual records are:

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

#### Modes and checkpoints

- `write_mode` is `replace` or `append`. `transform_mode` is `identity` or
  `sql`. `materialization_mode` is `local` or `s3-view`.
- `revision_mode` is `latest` or `fixed`. It describes the requested source
  selector for all writes. It is not an ownership key and never routes or
  renames targets.
- SQL or partition-selected replacement and append store import metadata, not
  an eligible identity checkpoint.
- Table mappings are part of the checkpoint scope.
- Each successful refresh has a new canonical UUID `generation_id`. Every
  per-table record in that scope refers to its completed generation, including
  empty or omitted tables.
- A build session accepts only complete, mutually consistent replacement
  metadata and mappings. Application import receipts are not eligible inputs.

#### Replacement ownership

Replacement ownership follows the scope and role rules in the client document,
*SQL bindings and transaction*. The ownership scope is stable across requests
and is keyed by the bound root and workspace, dataset, adapter, and adapter
destination namespace (`target.schema` for DuckDB). Mappings, source
selectors, and query text identify the request, not the owner. When a mapping
change updates the scope, it claims new unowned targets under normal checks.

#### Request hash and receipts

- `request_sha256` hashes the canonical pull request identity defined in
  *Declaration and request identity*, including the effective declaration and
  adapter options. It does not hash an automatically chosen diff plan or
  inferred source facts. It excludes the attempt ID itself.
- `requested_revision` is `latest` or an explicit decimal string, so a retry of
  `latest` compares the original request rather than a newly sampled revision.
- The committed receipt contains the fixed successful target. Failed
  transactions insert no success receipt.
- Duplicate receipt IDs are rejected in the refresh transaction. An existing
  receipt is never overwritten.

#### Materialization provenance

`pull_meta` describes the current materialization. Preparation copies its
provenance into a fixed session context; final publication never rereads it to
infer which inputs an earlier build consumed. GRV retains revision parquets
even after version pruning, so enumerating historical partition and version
references does not require duplicating them in `pull_meta`.

### Completeness

This section states what a committed pull attests about the destination.

- After complete identity replacement, every table and the checkpoint describe
  the complete declared scope at one revision.
- SQL or partition-selected replacement and append instead attest the
  completed query-defined import at that revision, with table counts and
  receipts. Existing append rows can originate from other operations; they are
  not asserted to belong to the selected source revision.
- Omitted partitions, empty versions, and omitted tables contribute no rows. A
  referenced but unavailable version fails the refresh rather than being
  treated as an omission.

An engine transaction observes one complete dataset generation. V1 serializes
external readers, builds, and pulls on the same database file; it does not
promise concurrent access by separate processes. A build selects a completed
generation during preparation and uses its private session inputs, so a pull
between preparation and engine invocation cannot change those inputs.

### Backends and S3 views

All modes use GRV's backend contract, including durable local read-back.

#### Parquet reader rules

- Every Parquet reader, including the expressions saved in S3 views, uses an
  explicit file list and `hive_partitioning=false` at the reader call.
- Directory names such as `version=17` and partition paths must not create,
  replace, or cast data columns.
- The adapter validates each file's GRV logical schema before combining it
  with other files. Files within a version have identical schemas. Selected
  versions must be prefix-compatible with the resolved table schema.
- Generate an explicit ordered projection of every logical column, adding
  typed nulls only for permitted trailing columns absent in an older version.
- No `SELECT *`, automatic schema widening, implicit lossy casts, or
  reader-generated columns become managed table columns.
- A validated implementation may use `union_by_name`, but that option does not
  replace per-file schema validation and explicit logical projection. See
  [DuckDB Parquet reader options](https://duckdb.org/docs/current/data/parquet/overview#parameters).
- Preserve the actual file values of ordinary columns and `_{key}_` columns,
  and validate the latter against the manifest partition. An empty selected
  file still has its manifest's partition identity.

All of the reader rules above apply to local staging files too, even if their
temporary paths happen to look partitioned.

#### Materialization modes

- **Local mode** is the default. It materializes verified rows into DuckDB, so
  later reads need no GRV downloads. During preparation, source holds are still
  required: a cached copy is not a substitute for retaining auditable GRV
  inputs.
- **S3-view mode** is valid only for S3 sources. It creates views over the
  exact files listed by the resolved manifests, in the same transaction as the
  checkpoint. It never uses directory globs or silently ignores missing files.
  Before commit, every file must verify by size and validator, or by SHA-256,
  according to GRV §4; schemas and tombstones are checked as well. Differing
  validators require full hash verification, even though the normal path
  avoids downloading all data.

#### S3 views and retention

- A standalone S3 view has GRV's best-effort reader semantics and can become
  unavailable after pruning.
- A prepared build's confirmed dependency holds protect its source revisions
  for the build and for subsequent GRV retention. The session's private views
  reference the same fixed file sets, not live tracking views.
- This protection needs no broad source write permission and no reader pin.
  Consumers create holds only in their own allowed subfolder.
- All query failures are surfaced.
- Advanced external S3-view builds require network access and pay remote read
  costs. Managed SQL builds materialize the held file sets locally instead
  (*Input materialization*).

## `grv session`

A GRV publication session fixes one target dataset and base, the declaration,
the adapter identity, the run, the completion, and the export and publication
attempts. Its mode determines how source data is obtained. Its GRV run and
publishability rules are common to all modes. Authentication sessions and
source jobs remain adapter state.

- `session prepare` accepts extraction or external DuckDB build declarations.
  Managed builds use the same internal preparation lifecycle through the
  runner.
- `session renew` uses only the context and the backend.
- `session abort` resolves any recorded publication first, then abandons the
  session through the normal run and claim protocols. It cannot undo a
  committed revision (see *Abort* under *Build completion record*).
- `push --session` completes an extraction or finalizes an attested build. It
  uses the fixed context, without new declaration overrides.
- `session show` validates identities and reads the authoritative session
  record under its mode-specific lock. It reports the adapter and mode, target
  and base, inputs, private mappings or capture, completion, export plan,
  attempt and outcome, and observed run phase. It does not renew, recover,
  infer completion, or resolve an ambiguous committing CAS. Inspection never
  emits credentials or owner tokens.

### Extraction session preparation and capture

An extraction session runs these steps:

1. **Validate and identify.** Validate the common and adapter declarations and
   capabilities before side effects. Select and report the attempt UUID, and
   lock its consumer-state directory. A matching recorded attempt recovers its
   context and result. A changed fixed request returns `REQUEST_MISMATCH`.
   Resolve the adapter connection identity.
2. **Prepare.** Initialize the target's `LATEST` if needed, and read its base
   revision. Check that every base table is declared or dropped
   ([Extraction snapshot membership](#extraction-snapshot-membership)). Create
   an ordinary open GRV run with no GRV inputs, and confirm the empty hold set.
   Record in durable consumer state the adapter and package identity, the
   declaration and source configuration, source consistency, the run, base,
   and owner, and the attempt. Write the protected context atomically. No
   DuckDB build workspace is created.
3. **Extract.** On `push`, extract all declared tables and stage core-written
   Parquet outside GRV. Renew the run throughout acquisition; ownership loss
   stops publication. The adapter reports explicit successful completion even
   for empty tables. A failed table or page cannot become an omission or an
   empty snapshot.
4. **Seal the capture.** Stop all capture writers first. Then seal a durable
   capture receipt with each output's schema, partition groups, row counts,
   capture start and end times, source and job identity, and staged-file sizes
   and hashes. The receipt's SHA-256 becomes the session completion digest.
   Capture files are immutable after acceptance.
5. **Publish.** Finalize and publish using the common push protocol (*Push
   finalization*). The capture receipt is completion evidence, not a commit
   marker. Store the resolved result in consumer state after GRV commitment is
   established; adapter checkpoints follow that result. If the adapter requires
   `after_publish`, run it as described in *After-publish hook*.

#### State directory and locking

The state directory contains `push/<attempt-id>/` with the context, source-job
state, staged files, capture receipt, fixed plan, and result.

- File creation and updates are atomic and durable.
- The persistent `session.lock` uses an exclusive nonblocking `flock` and is
  never replaced or unlinked. Context aliases use this same lock.
- These records are outside GRV and may not be inferred from folder listings.
- If the state directory is lost (for example, on an ephemeral CI runner), its
  attempts cannot be retried or inspected. Their GRV runs are recovered by the
  normal run-recovery protocol, and nothing in GRV becomes inconsistent.
- The core owns lease renewal while a transfer command holds the session lock.
  Between commands, an external driver can call `session renew` if it keeps a
  prepared extraction session open.

#### Retries and capture reuse

- `push --decl --attempt` creates or reuses this session and performs the
  whole lifecycle in one command. `session prepare` can expose the same context
  explicitly.
- Preparation and push retries resolve the recorded outcome before contacting
  the source. A recorded terminal publication result can be returned without
  reacquisition. With no pending hook, receipt replay requires no
  re-authentication.
- A Salesforce alias change or an adapter version change rejects an unfinished
  retry.
- An accepted complete capture is reused on retry after its hashes are
  verified. It is not queried again against a changing source.
- An incomplete capture may resume only with the adapter's recorded resumable
  source identity. If that is unavailable, abandon the attempt and start a new
  one.
- Expired or recovered ownership does not authorize capture continuation or a
  new run under the same fixed attempt.
- If a complete sealed run already matches its durable plan, publication may
  resume under the existing core retry rules.
- Missing or corrupt required capture or attempt state is an error, not
  authorization to silently re-extract.

#### After-publish hook

When the adapter requires `after_publish`:

1. Before calling the hook, record a pending acknowledgement alongside the
   known publication outcome.
2. Call the hook.
3. Record its completion durably.

If the hook is still pending, a same-attempt retry replays only that idempotent
acknowledgement with the fixed adapter state, then records its completion. The
hook may need authentication. A hook failure reports the known committed
outcome plus an adapter error; it never republishes or re-extracts. The wire
form of the call is in the adapter process protocol, §4.11.

### DuckDB build session preparation and context

A **build session** is one normal GRV run in one product dataset, plus its
private DuckDB engine workspace. The rest of this subsection, and the driver
and completion contracts that follow, apply only to normalized
`source.mode: build`. The session's context is fixed before the engine
invocation.

- The managed runner prepares internally. Otherwise the external driver calls
  `session prepare`, waits for success, runs the engine, and renews the run
  until the client seals it.
- Preparation holds the workspace lock through its engine transaction and
  durable context creation.
- If this is the first managed write to an otherwise unbound engine,
  preparation atomically creates the root and workspace binding and a complete
  empty receipt store.

#### Attempt index and locks

Every build preparation selects or reuses an attempt UUID. Before a GRV run is
opened, a core attempt index in the selected push state store, outside GRV and
outside the database, durably records the fixed request, the canonical
connection, and the chosen run ID.

- This reservation is neither an engine binding nor a prepared-success
  checkpoint. Engine preparation can still roll back without losing the retry
  identity. The engine session and the completed context later reference this
  same index entry.
- Before mutating a preparation, select its new run ID or discover the run ID
  of an existing recorded preparation. Then acquire that session's mutation
  lock and the workspace lock as specified in *Lock ordering*: discovery
  releases the workspace lock before reacquiring the session lock and then the
  workspace lock.
- Under both the session lock and the workspace lock, recheck the recorded
  identities. A matching attempt recovers the fixed context and outcome. A mismatching declaration or
  connection returns `REQUEST_MISMATCH`.
- A context recovered from an existing record must match the same effective
  declaration and the canonical engine and root, with the declared target.
- A lost attempt mapping is not permission to create a new run under the same
  identity.
- Managed push reports the attempt UUID and uses it for retry. External
  finalization takes it from the prepared context.

#### Preparation steps

1. **Validate and select inputs.** Validate the effective push declaration,
   supported types and extensions, root binding, and non-retired target. In one
   engine transaction, select the completed input materializations and their
   metadata. Input aliases and output mappings must be unique and must belong
   to the bound GRV root. Reject self-inputs and observable derivation cycles
   ([Schemas and provenance](#schemas-and-provenance)).
2. **Open the run.** Initialize a new target's `LATEST` as GRV §8 permits, then
   read its `LATEST.revision` as `base_revision` (0 for a new target). Use the
   selected run ID, and generate an owner token and a retention ID for each
   distinct external source revision. Create the normal open GRV run control
   with these fixed inputs before creating any dependency hold. Source revision
   0 is not a committed input; an empty source must have an explicitly
   published empty revision.
3. **Confirm holds.** Create and confirm every hold through GRV §9, then CAS
   `holds_confirmed: true`. An empty input set also completes the normal
   holds-confirmation transition. If a cached input's GRV revision has already
   become unavailable, preparation fails; the driver must refresh and prepare
   again.
4. **Create private tables.** Create private input tables and output tables
   under namespaces unique to this run, and load the inputs as described in
   *Input materialization*. Record the fixed schemas and mappings with the
   prepared engine session, and commit that engine transaction.
5. **Write the context.** Materialize the context file atomically and durably
   from the committed session record. A retry may recreate an identical context
   for that session; it must not overwrite another context. Return success only
   when the engine session record, the context, and the holds are established.

#### Input materialization

Build inputs always come from held GRV files, never from tracking tables:

- Local inputs are loaded from the held revision's GRV data files, which the
  core has verified by size and SHA-256 for the input table's selected entries.
- S3 inputs are private views over the same held file sets. Managed builds,
  however, materialize the held, verified GRV file sets locally for both local
  and S3-view inputs before applying the user-query execution restrictions.
- Tracking tables are never copied. The selected materialization only
  identifies the input's dataset, revision, contract, and generation.
- Private copies are made under the preparation transaction. A tracking table
  modified after its pull therefore cannot leak into a build that cites the
  held revision, and no later pull into the tracking tables changes the copies.
- If the materialization's recorded contract differs from the verified files'
  logical schema, fail with `PROTOCOL_FAILURE` (inconsistent consumer
  metadata).

#### Context contents

The context records:

- the canonical root, engine path, and workspace ID;
- the run ID and owner token;
- the target dataset and base revision;
- `declaration_sha256` and the code fingerprints;
- each input's dataset, revision, retention ID, and materialization
  generation; and
- the private engine input and output mappings.

The context is consumer state; it introduces no new GRV object type or
run-control field. It contains the immutable GRV run identity and the backend
coordinates needed for engine-free renewal, and renewal validates those against
the current control. Preparation retries recover the same recorded session. A
fresh run requires a new attempt identity. Retries never change an existing
run's fixed inputs or base.

#### Preparation failure

If preparation fails after creating a run, the client seals the run without
entries when it still owns it, or leaves it to normal run recovery. It does not
delete the control or the source holds as rollback. No build may start from a
failed preparation.

#### Self-input

With normalized `self_input: true`, preparation materializes exactly the target
base revision locally.

- External model execution seeds the private output tables from those rows.
- Managed execution keeps a separate immutable `grv_self` read copy and
  initializes the private output tables empty. Query results later replace
  those outputs completely. Managed output writes always replace the selected
  private output contents rather than append to seed rows.
- Self-input is recorded by `base_revision`, not as an external input or a
  self-hold.
- The rows must be materialized locally before the build, because base
  revisions receive no additional GRV retention. A missing required base
  version fails preparation.
- Self-input never comes from a separately pulled tracking or historical copy
  of the product, because such a copy might differ from the prepared target
  base.

Without self-input, or for a new product, outputs start empty with the declared
schemas.

#### Session isolation

- Private input tables remain immutable for the session.
- Output tables belong exclusively to that session's engine invocation.
  Different sessions cannot share output tables, and ordinary pulls cannot
  target session namespaces.
- The driver supplies these mappings to the engine through its normal
  configuration. Models do not need GRV APIs.
- Physical copies cost engine storage, but they keep build inputs independent
  of the tracking tables' current generation.
- In v1 a pull can run between preparation and invocation, or after
  invocation, but it cannot open the same database while the external engine
  owns it.

### Managed runner execution

The managed runner prepares the same fixed inputs, holds, base, and private
outputs as an external session. It holds adapter workspace ownership,
supervises cancellation and run renewal, and asks the adapter to evaluate the
declared output queries.

DuckDB binds private inputs as `grv_input.<alias>` and, if self-input is
enabled, the immutable locally materialized base copies as
`grv_self.<output table>`. An existing schema with either name conflicts.
These display bindings are not storage provenance.

Query evaluation follows these rules:

- Each query is one read-only statement.
- Resolved table dependencies are limited to the declared input and self
  bindings. External data and ordinary mutable working relations cannot bypass
  provenance.
- All selected output query results are staged and validated before they
  replace private output table contents.
- Each query produces a complete relation, never an append onto prior-state
  rows.
- Query order cannot introduce undeclared output dependencies. With
  self-input, queries read the separate immutable base copy; a later output
  query never observes an earlier query's new output.
- Suppressed hold and drop outputs are not executed or attested.
- A successful zero-row query completes its output.

Inputs are materialized locally from held GRV files as described in *Input
materialization*.

Completion and failure:

- The runner records the successful invocation, verified selected outputs,
  stopped writers, and fixed mappings internally, using the contract in *Build
  completion record*. No user-produced completion file or renewal loop is
  required.
- On query failure, cancellation, or ownership loss, the runner stops writers
  and waits for them to stop, accepts no completion, and abandons the run
  through normal run recovery. It never rebuilds an incomplete attempt from
  leftover tables.
- An accepted complete capture or export plan is reused on retry instead of
  rerunning SQL.
- Cleanup preserves unresolved state and core retention evidence.

### External driver lifecycle and fencing

For an external integration, the driver owns the session through build and
export. The managed runner performs these obligations itself for managed
builds.

Renewal and ownership:

- The driver calls `session renew` often enough to satisfy GRV §5's TTL and
  clock-skew bounds. The CLI renews the run during long preparation and
  finalization operations. The driver renews between commands, including
  throughout the engine invocation.
- Renewal uses the recorded owner token and the run control's current
  validator.
- Losing ownership, or encountering recovery, stops engine work and new
  version allocations under that session.
- Sealing ends renewal and all allocations. It still permits publication
  retries of a complete sealed run.
- Neither the context file nor an engine table is evidence that the driver
  still owns an open GRV run.

CLI mutations of one session are serialized by the session mutation lock
(*DuckDB process ownership*). During the external invocation the driver holds
the workspace lock, not the session mutation lock.

Finalization preconditions:

- Finalization is called only after the engine invocation completed
  successfully and all writers to its private output tables have stopped.
- The driver supplies the completion record (*Build completion record*) to
  attest these conditions and the completed outputs. The CLI cannot infer
  engine success from the presence of a table.
- Outputs remain stable during export. The client exports them from one
  consistent engine read transaction.
- A lease cannot fence arbitrary delayed engine writes. That is why output
  namespaces are private and the driver must wait for its writers.

### Build completion record

The completion record is the attestation that a build invocation succeeded,
that all writers stopped, and which outputs completed. Before first
finalization, the managed runner or external driver writes one atomic, durable
JSON file conforming to
[the build-completion schema](grv-client-v1-build-completion.schema.json). It
is written only after:

- the invocation has succeeded;
- all output writers and database connections have stopped; and
- the driver has established which declared outputs completed.

A driver-populated build with no model step uses `kind: direct`. An
omission-only build publication uses `kind: omission-only` and an empty
completion list. Each still has a driver-assigned invocation ID. Example:

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

#### Validation and coverage

- The run ID, workspace ID, declaration digest, and every physical output
  mapping must match the prepared session.
- Entries are unique by declared GRV table name and must refer to declared
  outputs.
- Which outputs need a completion entry depends on the selection policy:
  - For `changed` or `all`, every declared output that is not held or wholly
    dropped must have a completion entry.
  - For `explicit`, every table addressed by an `include` or `empty` selector
    needs one. Other tables are not exported.
  - Tables named only by drop selectors and absent from the output mappings
    require no completion.
  - Held or wholly dropped tables must not be listed.
- A completed output may contain zero rows. An empty or self-input-seeded table
  without completion evidence is **not** a completed output. Explicit empty
  versions also require evidence for their selected output table.
- Missing evidence returns `BUILD_INCOMPLETE` before any version allocation or
  sealing.

#### Acceptance and immutability

- `push` validates the supplied record under the session and workspace locks.
  Before staging an export, it durably stores the record's canonical JSON and
  SHA-256 in the engine session record.
- The first accepted record is immutable for that session. Later files must
  canonicalize identically or fail with `REQUEST_MISMATCH`.
- Once a record has been stored, `--build-result` may be omitted on retry. For
  external finalization with no stored record, omitting the flag fails even
  when prepared output tables exist.
- Managed finalization uses the runner's accepted record without asking the
  user for a file.
- A terminal outcome or a recorded publication attempt is resolved first. An
  already committed publication never requires the driver to recreate a lost
  file.
- The fixed export plan includes the accepted completion digest. Session and
  cache cleanup preserve both records and any retryable attempt and outcome.

#### What the record proves

The record is a driver's attestation. It is not an independent proof of model
correctness, and it is not a GRV commit.

- The driver must derive completion from its actual invocation results, rather
  than fill the list from preparation's table names.
- The client still validates schemas, declared checks, ownership, and the full
  GRV publishability fence.
- Accepting a record authorizes neither later output writes nor resuming a
  build after run recovery.

#### Driver crash and lost sessions

- On a driver crash, normal GRV run recovery may take over and seal the run.
- A late engine process can leave private working tables. It cannot modify a
  new session's outputs or authorize new GRV allocations with the lost owner
  token.
- The client never adopts an expired or recovered build by reading the new
  `LATEST` and replacing its base revision.
- A lost or incomplete session requires a new preparation and engine
  invocation.
- A retry may finish publication of an already complete sealed run only after
  verifying that it matches the session's durable export plan exactly. It
  cannot resume the build or extend that run.

#### Abort

`session abort` first resolves any recorded publication attempt. If that
attempt already committed, the command reports the committed outcome; abort
cannot undo it. Otherwise abort:

1. stops publication;
2. resolves outstanding allocations through the normal claim and run
   protocols;
3. seals and materializes the run without publishing it (before export, this
   normally means an empty run); and
4. records the aborted outcome in consumer session state, and refuses further
   finalization through that context.

Already finalized allocations remain entries of the sealed run. Abort cannot
erase finalization proofs or change an already sealed payload. Only source GC
releases holds, when GRV §9 permits it; the client never deletes holds or
writes source release markers. Stopping the external engine remains the
driver's responsibility.

#### Cleanup

Private engine tables may be cleaned up after the session's outcome is known
and all its writers have stopped. Engine-cache cleanup does not change GRV
version, pin, or hold retention. It preserves the session record and the
recorded attempt and outcome needed to retry a still-valid context.

## Push finalization

Every push ends in the same finalization algorithm after an explicit
completion boundary. Extraction and managed-build declarations use ordinary
`push --decl`. Advanced external builds use prepared contexts and
`push --session --build-result`. The common normalizer derives output schemas,
mappings, and build inputs from the public table-centric declaration; no
separate user schema or provenance assertion is accepted. Build queries,
aliases, and partition fields follow the client document.

### Finalization algorithm

Take the extraction state lock, or the build session lock and the DuckDB
workspace lock, as appropriate. Under those locks, first return an already
recorded terminal outcome or resolve a recorded publication attempt. Otherwise
run the steps below. A complete sealed-run retry skips steps 1–3 and verifies
and materializes the existing sealed payload in step 4. An incomplete sealed
run cannot continue.

1. **Check the session and export.**
   - Verify the session's recorded identities and fixed declaration, ownership
     of the open run, the accepted completion record, and the exclusive output
     mappings or accepted capture.
   - Require completion for every selected output before deriving an empty
     group.
   - Build mode reads one stable engine snapshot, validates schemas and
     checks, and stages complete Parquet.
   - Extraction mode validates the already sealed capture, schemas, checks,
     and file hashes without reopening the source.
   - Derive provenance from the fixed session inputs, never from the current
     `pull_meta` or the source `LATEST`.
2. **Plan against the prepared base.**
   - Determine the contributed output partitions, equality-proven versions,
     explicit omissions, and empty versions. Staging must describe full
     snapshots of those partitions.
   - Changes during the build do not change the plan's base. Each affected
     pair has at most one action.
   - Durably record this fixed export plan and the completion digest before
     allocating versions, and record allocation identities as work proceeds.
     Retries resolve existing allocations before creating replacements.
   - A sealed-run retry verifies the recorded plan and entries, and skips
     export and allocation.
3. **Finalize versions.** For every new version, perform the complete GRV §5
   claim cycle: acquire, reserve a number, create the allocation record,
   register its schema, create all data files, renew, create the manifest
   last, verify the files and the absence of a tombstone, release as
   finalized, and CAS the allocation record to finalized. Manifests already
   contain `run_id`, `claim_token`, all per-file hashes, sizes, and
   validators, and `derived_from` with its confirmed retention IDs. Unknown
   outcomes use GRV's proofs.
4. **Seal the run.** Resolve all remaining allocation records, CAS-seal the
   control with exactly its finalized entries, and materialize the immutable
   run file, as GRV §6 requires. Publication cannot precede this step. A retry
   may reuse the identical sealed run. Recovery that excluded expected work is
   an incomplete build, not permission to append entries to the sealed file.
5. **Publish the change set.**
   - Acquire the target's lease, finish pending operations, reject retirement,
     and recompute from its current predecessor using the fixed session base.
   - Check every contributed output pair for intervening changes, including
     changes later restored, under GRV §8. Equality-proven pairs skipped by
     dedup also receive this check; dedup cannot conceal a concurrent change.
   - Explicit omissions are guarded by `expected_revision` equal to the
     prepared base.
   - After the no-op decision (*Build selection and no-op rules*), execute
     the complete GRV §8 publish protocol: revision reservation, validation,
     publish description, revision parquet, `LATEST` CAS, and supersession
     receipt.
   - Before issuing the commit CAS, durably record the attempt's operation ID
     and reserved revision in consumer session state. A retry reacquires the
     dataset lease to fence delayed requests, and resolves that attempt from
     the committed chain before issuing another.
6. **Report the outcome.** Record the known revision or no-op in consumer
   session state. This record may lag the GRV commit and is not its commit
   marker. An ambiguous publication is resolved from the committed revision
   chain. Retries must recover the session's recorded publication attempt
   before starting another. Manifests and run files are never rewritten
   afterward.

After a failed publication:

- A lease takeover without a conflicting state change may be retried with the
  same sealed run under a fresh dataset lease, subject to GRV's availability
  checks.
- A state conflict requires a fresh session and build. The client must not
  automatically rebase existing engine outputs.
- A retirement or an unavailable version is surfaced, rather than repaired by
  publishing incomplete state.

### Build selection and no-op rules

A build declaration's `selection` decides which output partitions become new
versions, which base versions are reused, and which entries are omitted.

- **`changed`** is the default. Export eligible output partitions, and reuse an
  existing base version only when equality is proven by
  [Canonical encoding and equality](#canonical-encoding-and-equality).
  Comparing only `data.parquet` is insufficient for a multi-file version. If
  equality cannot be established, produce a new version; equality must not be
  guessed from row count or a partial hash.
- **`all`** creates new versions for all eligible output partitions, including
  byte-identical data.
- **`explicit`** restricts outputs to `include` selectors, each naming a whole
  table or a `(table, partition)` pair. A missing requested output is an
  error.
- **`drop`** uses GRV omission selectors. A missing exported partition alone
  does not imply removal; the driver must declare removal explicitly.
- **`hold`** excludes named tables from output contributions and keeps their
  predecessor entries unchanged. This declaration field is unrelated to GRV
  dependency holds.
- **`empty`** names explicit pairs for which a zero-row version is produced
  with the table's registered schema. It keeps membership and terminates the
  partition's data. An empty request does not rely on a nonexistent row group
  to identify its partition.

These selector lists apply to build declarations. Extraction declarations
accept only `selection.policy: changed | all` and whole-table
`selection.drop`; their complete-snapshot membership rules are in *Extraction
snapshot membership*.

#### Selector grammar

The selector grammar is closed:

- A whole-table selector is `{table: <name>}`.
- A pair selector is `{table: <name>, partition: {key: <string>, ...}}`.
  `{}` is the partition of an unpartitioned table.
- `include` and `drop` accept either form. `hold` accepts only the whole-table
  form, and `empty` only the pair form.
- Selectors use canonical GRV table names and partition objects. Partition
  objects contain exactly the layout's keys and canonical string values; key
  order is reconstructed from the layout.
- `include` and `empty` must address declared output mappings. `drop` and
  `hold` may address existing target tables without output mappings. Unknown
  tables or keys are errors.
- For `explicit`, `include` must be nonempty. For other policies it must be
  empty. `empty` pairs in explicit mode must be covered by `include`.

#### Applying selection

Compute selection before generating output groups:

1. Held and wholly dropped tables contribute no automatic output, including
   precreated empty tables. Whole-table drop and hold suppress export of that
   table, rather than conflicting merely with the existence of its precreated
   table.
2. Apply `include` as an eligibility filter. An `include` selector is not
   itself a contribution, and it may cover an `empty` request.
3. Derive groups only from selected, completed outputs.

Conflicts and errors:

- Overlapping contributions, omissions, holds, or empty requests are rejected.
- Duplicate or intersecting selectors within one list, and conflicts among
  `hold`, `drop`, `include`, and `empty`, are rejected before allocation.
- A pair explicitly named by `empty` must have zero rows. A nonempty matching
  group is an error.
- A partition omission that overlaps an exported or explicitly empty group is
  an error.

#### No-op

The client skips a new revision only if the **complete resulting state** equals
the predecessor after conflict checks, including omissions. An omission-only
change still publishes a revision; having no new versions is not a no-op test.
For a true no-op, release the dataset lease and record the result without a
revision publication. A number already reserved while acquiring the lease
remains reserved, as GRV requires; it may be skipped.

For example, these are three independent selector shapes. The drop's table
must already exist, and the hold must not overlap selected outputs:

```yaml
selection:
  policy: explicit
  include: [{table: fct_x, partition: {period: '2026-10'}}]
  drop: [{table: old_dimension}]
  hold: [{table: dim_y}]
  empty: [{table: fct_x, partition: {period: '2026-10'}}]
```

### Canonical encoding and equality

The core writes every push output group (extraction and build, under any
policy) in **canonical form**, so that equality can be proven from manifest
hashes without downloading base data:

1. **Row order.** Rows are sorted by a total order that compares columns left
   to right in output-schema order. Nulls sort first. `bool` orders false before
   true; integers, dates, and timestamps order numerically; decimals order by
   value; `double` uses IEEE 754 `totalOrder` (so `-0` precedes `+0` and NaNs
   order by bit pattern); `utf8` and `binary` order by unsigned bytes. Duplicate
   rows are kept and are adjacent.
2. **Writer.** One fixed Parquet writer configuration, identified by a
   `canonical_writer` string that includes the writer library and its version,
   encodes the sorted rows. It produces identical bytes for an identical logical
   schema and row sequence: fixed compression, encodings, page and row-group
   sizes, and footer metadata, with no timestamps or random values.
3. **Files.** Rows are split into `data.parquet`, `data-1.parquet`, … at fixed
   row-count boundaries determined by `canonical_writer`.

The `canonical_writer` value is recorded in run metadata as
`metadata.grv_cli.canonical_writer`. It is informational, and no rule depends
on it. Large outputs are sorted with the core's bounded external sort in
staging.

A staged group **equals** the base version of the same (table, partition) iff
the base manifest's `data_files` list has the same length and, entry by entry,
the same `name`, `size`, and `sha256` as the staged files. Equal bytes imply
equal logical schemas and rows. Unequal bytes prove nothing: the base may have
been written by another tool or by a different `canonical_writer`, so the group
simply becomes a new version, which is always safe. A writer upgrade therefore
costs at most one round of new versions per partition.

### Extraction snapshot membership

An extraction declaration defines a complete snapshot of its target dataset.
All declared source tables succeed, their selected rows form full partition
snapshots, and the resulting dataset contains exactly the declared tables and
captured partition groups. The client document's *Push selection* describes
this behavior for users.

- The adapter's row filter is evaluated before mapping and grouping.
- An unpartitioned zero-row table publishes an empty version. A partitioned
  zero-row table contributes no groups and removes its old groups.
- A declaration with zero source tables is invalid.

#### Omissions

- For declared tables, the core derives omission selectors for base partitions
  absent from the complete capture. Partition absence is determined from
  successful full acquisition.
- Whole-table omissions are never derived. During session preparation, after
  reading the base revision and before any source acquisition, every base
  table must be either declared in `tables` or named in `selection.drop`.
  Otherwise preparation fails with `STATE_CONFLICT`, naming the undeclared
  tables, before any GRV run is created.
- A `drop` entry must not also be declared in `tables`. A dropped table
  already absent from the base is a no-op.
- Extraction accepts `selection.drop` with whole-table selectors only.
  `include`, `hold`, `empty`, and pair-form drops are build-only.
- All omissions are guarded by the prepared base revision. A failed or missing
  output cannot authorize any omission.

A source declaration is consequently the authoritative snapshot definition of
the tables it declares or drops. It is never an implicit patch that removes
tables another declaration produced. Use a separate dataset when independent
sources need independent refresh ownership.

#### Policies and the base check

`selection.policy: changed` reuses provably equal versions and creates new
versions for other groups. `all` creates new versions for every captured group.
Both have identical snapshot membership. Equality proof follows
[Canonical encoding and equality](#canonical-encoding-and-equality), never
capture time or file names alone.

Extraction publication requires the committed predecessor to remain the
prepared base. Any intervening dataset revision, even to another table, returns
`STATE_CONFLICT`; a new session, attempt, and capture are required. A no-op
still checks that base and proves all selected contents and membership
unchanged. This prevents an implicit rebase from combining snapshots of
independently captured source states. The shared publisher enforces the base
check under its dataset lease before the normal GRV publish steps.

#### Watermarks and run metadata

- No source watermark is advanced just because files were downloaded or
  staged.
- A publication crash is resolved through the recorded GRV operation and chain
  before consumer success is recorded.
- Run-control metadata is immutable from creation. `metadata.grv_cli` may
  record the adapter and version, resolved source identity, declaration and
  query fingerprints, and extraction attempt identity known at creation.
  Capture end times, job IDs discovered later, and receipt hashes remain in the
  durable consumer capture record; they cannot be added to run metadata at
  seal time.
- No new GRV artifact layout is defined.
- Source provenance is not fabricated as `derived_from` references to external
  Salesforce or website identities. Extraction has no GRV dependency holds.
  DuckDB build mode keeps the actual fixed GRV holds and derivation rules.

### Schemas and provenance

New versions may append top-level columns only. Removing, renaming, retyping
(including precision or UTC changes), reordering, changing nested types, or
changing partition keys requires a new GRV table. Registration and extension
properties follow the durable table-wide baseline; failed registrations are not
rolled back. Adapter or source capture metadata, and engine-specific
fingerprints and artifacts, belong in permitted run metadata, respecting which
run fields were fixed at creation.

For build mode, every new derived version cites **all** the run's confirmed
external input holds, using whole-revision selectors. V1 defines no narrower
dependency mapping grammar. This conservative provenance can retain more
upstream data; narrowing requires a later specification. The sources must be
committed revisions in this root, the references must resolve, and the consumer
dataset must satisfy GRV's acyclic derivation rule. Self-input is represented
by the target base revision and never by a self-hold.

The client enforces acyclicity as far as it can observe it, during preparation
and before creating a run:

- An input whose dataset is the target dataset is rejected with
  `INVALID_DECLARATION`; it would be the self-hold GRV §9 forbids. Use
  `build.self_input` instead.
- The client then walks the derivation graph through current states,
  breadth-first over datasets not yet visited. For each input dataset, it reads
  the `LATEST` revision, the run files of the runs its entries name, and those
  runs' input datasets. Reaching the target dataset fails with
  `INVALID_DECLARATION`, reporting the cycle path. Unreadable records fail with
  the corresponding protocol or backend error rather than being skipped.

The walk costs one revision read plus that revision's distinct run files per
dataset. It sees only current states, so it cannot prove that history is
acyclic; deployments still assign datasets to layers as GRV §9 recommends.

GRV history is the history of record. Engine working snapshots and session
copies are disposable execution state; they do not define a second product
version history.

## Revision retention: `grv pin` and `grv unpin`

V1 creates and releases pins only on committed dataset revisions. A pin keeps
that revision's version data available through GC until its release.

- A pin has no expiry. It does not change `LATEST`, freeze publications, or
  select an engine's tracking revision.
- Dependency holds on its versions may retain entire upstream revisions
  transitively.
- Ordinary pulls create no pins. Build sessions use their own automatic
  dependency holds.

### Creating a pin

`pin` requires an explicit positive `--revision` and a nonempty `--reason`. It
follows GRV §10 in full:

1. acquire the dataset lease;
2. finish pending work;
3. prove commitment and availability;
4. create the `pin` operation description;
5. commit through `LATEST.pending`;
6. durably materialize the pin marker; and
7. clear the pending operation.

A pruned or unavailable revision fails; pinning cannot restore deleted data.
Pins on available revisions of a retired dataset remain valid operations under
the core spec.

Pin identity:

- The CLI generates a fresh canonical UUID pin ID unless `--pin` supplies one.
- The identity is `(dataset, revision, pin_id)`. IDs are scoped to the
  revision's existing `.pins/` directory, not to a dataset-wide registry.
- Caller-supplied IDs support reliable retries. An ID cannot be reused after
  release in its scope.
- Validate the exact addressed records under the dataset lease. Do not scan
  all other revisions to prove global uniqueness. The same UUID in another
  revision is a distinct pin, and this request cannot release it.
- An existing active pin with the same ID, revision, and reason makes the
  request an idempotent no-op. A conflicting identity or reason fails rather
  than editing the record.
- Each independently generated ID creates independent protection, even for the
  same revision and reason.

Retries:

- Report a generated ID on stderr before attempting the committing CAS, and
  include it in the final success or error result. Internal retries keep it.
- Callers keep the dataset, revision, and ID and retry that complete identity.
  A new invocation without `--pin` requests a new pin.
- An unknown commit is resolved through GRV §8's lease reacquisition and
  durable marker proof before another attempt is made.

The result includes the dataset, revision, pin ID, reason, active status, and
the core creation audit fields. `created_by` is the core's tool and version
identity; it is not a per-person ownership or authorization system. Store
permissions govern who can perform retention mutations. `show --retention`
exposes these records and the other protections affecting a revision.

### Releasing a pin

`unpin` requires an explicit positive `--revision` and one pin ID.

1. Under the dataset lease, finish pending work.
2. Read that revision's `.pins/<id>.json` and optional release marker
   directly, validating path, scope, and operation identity.
3. Commit the normal `unpin` operation and durably materialize its release
   marker before reporting success.

- A missing scoped ID fails, even if the same ID exists elsewhere.
  Inconsistent records fail.
- V1 rejects non-revision pin scopes and never falls back to dataset-wide
  discovery.
- An already released matching pin is an idempotent no-op.

Release affects only that pin. Other pins, current-state protection, grace, and
dependency holds continue to apply. Data is reclaimed only by a later GC
decision after all applicable protections are gone. The result reports the
released pin and the observed remaining protections; it never promises an exact
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

A dry run is read-only. It acquires no lease, recovers no run, releases no
hold, creates no supersession receipt, and writes no intent or tombstone.

- Report versions eligible now separately from those that depend on hold
  release or run recovery.
- A missing supersession receipt keeps the affected revision protected for
  this preview. An applying GC would first write the receipt and start grace.
- Unreadable protection records prevent an eligibility claim and are errors.

The preview is an observation, not a committed deletion plan. New pins, holds,
publications, retirement, or recovery can change it. `--apply` always
recomputes under the actual dataset lease; it does not accept a dry-run file as
authority.

### Applying GC

`--apply` uses the complete GRV §8–§10 protocol:

1. Acquire the dataset lease and complete pending operations.
2. Reread parameters.
3. Write missing supersession receipts.
4. Recover eligible expired local runs as needed.
5. Release only provably releasable source holds, through committed
   operations.
6. Compute protected versions.

Recovery of a consumer run in another dataset is not part of this command.
Report holds whose release requires that consumer's recovery.

For each candidate batch:

1. Commit `prune_intent`.
2. Re-list holds.
3. Commit a `prune` decision naming only still-unprotected targets from that
   intent.
4. Durably create every decided tombstone before clearing pending.
5. Only then delete those targets' data files and manifests, as the core
   permits outside the lease.

Sweep residual data below existing tombstones using the same authority. Never
delete coordination or history objects, and never reuse numbers.

Failures and reporting:

- A lost lease before the prune decision authorizes no deletion.
- A known committed decision remains replayable after a crash; its tombstones
  and physical cleanup may finish later.
- Resolve ambiguous operation commits before continuing.
- Report decided and tombstoned versions separately from completed physical
  deletion, along with released holds and unresolved cleanup.
- A deletion failure returns an error with known committed progress. It cannot
  roll back the prune decision.
- A dataset with no eligible work is a successful no-op.

## `grv recover`

`recover` completes interrupted coordination for one dataset through GRV §6 and
§8.

- With no `--run`, complete any existing pending dataset operation under a
  valid dataset lease, then discover and recover eligible expired run controls
  and materialize missing sealed run files.
- `--run` restricts the command to that specific run. It does not recover
  unrelated dataset operations or other runs.
- `--dry-run` reports observed eligibility and proposed repairs without
  mutations.

Recovery rechecks every control and validator before taking ownership. A live
run is left to its owner. For expired runs:

1. CAS into, or take over, `recovering` under the core lease rules.
2. Resolve allocation records using claim-release proofs or the expired-claim
   verification protocol.
3. CAS-seal and materialize exactly the authoritative finalized entries.

Sealed files must match their controls. Claims that are still valid must be
allowed to finish or expire; run takeover is not evidence that a partition
writer has stopped.

Limits on recovery:

- No invocation force-expires a valid run, claim, or dataset lease.
- Untargeted healthy live runs are reported as skipped.
- A requested live run, a busy dataset operation, or an unresolved allocation
  that must wait returns busy with known progress. Retry proceeds only when the
  core rules permit ownership.
- The client renews leases throughout recovery work it owns.
- Where the core rules resolve a missing finalization proof as `unproven`,
  record that outcome rather than salvaging a version.
- Corrupt required records are protocol errors.

Recovery makes no new publication, selection, prune, pin, or hold-release
decision. It only replays effects already authorized by a pending operation and
completes the core run-recovery transitions. Physical cleanup of tombstoned
data remains GC's responsibility. A recovered sealed run is not evidence of a
successful complete engine build; `push` can resume only when that run matches
its existing durable session plan. Recovery never changes a session's fixed
inputs or base, and never adopts arbitrary working tables.

The result identifies discovered and targeted resources, recovered and sealed
runs, completed operation IDs, skipped live work, and waiting or failed work.
Repeating recovery of an already complete scope succeeds without changing
sealed payloads.

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
- Source hold confirmation, retention IDs, immutable provenance, and GC-only
  hold release (§9–§10).
- Revision pin creation and release, retirement state inspection, kept
  revisions, grace, prune intent and decision replay, and physical GC cleanup
  (§8–§10).
- Transactional dataset refreshes and build input and output stability (§11).

## Required conformance scenarios

Each scenario below must produce its required result. Protocol-specific
scenarios are in the adapter process protocol, §9.

### Pull transactions, receipts, and ownership

| scenario | required result |
| ---------- | ----------------- |
| Failure while refreshing the second table | All table, schema, ownership, and checkpoint changes roll back |
| Ambiguous pull commit | Fence the prior writer, recover DuckDB, and resolve the immutable attempt receipt |
| A committed pull crashes, then another pull advances the checkpoint | Retry with the original ID returns the original receipt without reapplying or rewinding |
| A successful pull's source data is later pruned | Receipt replay still returns its committed outcome without downloading again |
| A pull retry changes its declaration, target, or requested selector | Reject reuse with `REQUEST_MISMATCH` |
| Receipt metadata is missing in a previously initialized workspace | Report inconsistent metadata; do not infer rollback or invent an empty history |
| A declared not-null check fails in an otherwise unchanged table | Roll back the whole pull, including the checkpoint and attempt receipt |
| Adapter cancellation sees an unresolved destination commit | Preserve receipts and journal, and resolve the outcome; never report an assumed rollback |
| Another binding overlaps an owned replacement scope | Reject before mutation regardless of source selector |
| A replacement declaration changes its mapping members | Update the stable owner scope atomically and clear obsolete owned targets |
| Pull uses a fixed revision | Write the declared destination; create no generated historical schema |
| The same owned replacement scope switches between latest and fixed selectors | Permit the explicit request without changing ownership or target naming |
| Identity replacement gains a partition selector or SQL | Require a separate application scope; never retain build-input eligibility |

### Pull source selection and reading

| scenario | required result |
| ---------- | ----------------- |
| A Parquet file is below partition and `version=N` directories | Read only its logical file columns with Hive inference disabled |
| An older compatible version lacks appended columns | Add typed nulls after per-file validation; reject retyped or reordered schemas |
| SQL selects a partition column from one chosen revision | Read only its manifest-selected versions with Hive inference disabled |
| Partition-selected import excludes a pruned unrelated month | Acquire and validate only selected entries; unrelated data availability does not fail the import |
| Selected partition itself is pruned | Fail unavailable before destination mutation |
| An explicit empty partition list has no known source contract | Request `expect.columns`; never invent an older revision schema from the current baseline |
| A baseline appended a column after the selected old revision | Discover the schema from selected versions; do not assert the later column existed |
| A pull's selected source schema contains `int32`, `time`, or `list` | `INVALID_DECLARATION` naming the column, before destination mutation |
| A timestamp mapping would truncate nanoseconds or change UTC semantics | Reject before table mutation or version allocation |

### SQL imports

| scenario | required result |
| ---------- | ----------------- |
| DuckDB append SQL excludes an existing business ID | Preserve existing rows; insert only query-selected rows |
| Incoming rows duplicate an ID and SQL uses a window rule | Apply the declared rule; infer no extra business-key policy |
| SQL append runs again with the same successful attempt ID | Return its receipt without re-evaluating SQL or duplicating rows |
| SQL append runs with a fresh attempt against the same revision | Evaluate current local conditions; infer no snapshot-ledger no-op |
| Two output queries inspect destination rows | Both see the same pre-write state; no query sees another output's inserts |
| Append SQL fails while writing its second target | Roll back rows, schema and binding metadata, checks, and receipts together |
| Append produces zero rows | Commit a successful zero-row import receipt |
| Append targets an existing ordinary table with a different schema | Reject before inserting or altering existing application rows |
| SQL uses DML, multiple statements, or external-read functions | Reject before destination mutation |
| SQL output names, order, or types differ from the resolved output column contract | Reject without silent projection or lossy casts |
| A SQL file contains two statements or an external-scanning local view | Reject before writes, using the adapter parser and execution dependency contract |

### Engine ownership and locks

| scenario | required result |
| ---------- | ----------------- |
| Pull or engine inspection runs while the external engine owns DuckDB | Return `ENGINE_BUSY`, with no engine mutation or invented engine observation |
| Session renewal runs while the external engine owns DuckDB | Renew using only the context and backend, without opening DuckDB |
| A driver exits but its engine child still owns the database | Respect the remaining file owner; do not steal access or fence it with lease expiry |
| Different context copies mutate the same session | Serialize on the lock keyed by canonical engine path and run ID, not on the context-file spelling |

### Build sessions and completion

| scenario | required result |
| ---------- | ----------------- |
| A pull changes tracking inputs between preparation and invocation | The session reads its fixed private input generation |
| A tracking input table is edited after its pull | The build loads verified held GRV files; the edited rows never reach its outputs |
| A build tries to use append, partition-selected, or SQL-import targets as identity GRV inputs | Reject unsupported provenance; do not infer a complete source materialization |
| An input belongs to another GRV root | Reject preparation |
| A build input's dataset is the target dataset | `INVALID_DECLARATION` before run creation; direct the user to `self_input` |
| Input datasets reach the target through current states | `INVALID_DECLARATION` reporting the cycle path, before run creation |
| Source pruning before preparation confirms holds | Preparation fails; cached rows cannot authorize derived publication |
| A source GC runs after session hold confirmation | The held input revision remains complete |
| A model requests self-input | Seed from the prepared target base, with no self-hold |
| Self-input seeds an output that the invocation does not complete | Seeded table existence does not authorize export |
| Managed self-input query omits an old row | Replace private outputs with the complete result; do not retain or duplicate seeded rows |
| A successful partial invocation leaves a declared output untouched | Reject missing completion evidence before allocation; do not publish its prepared emptiness |
| A selected output genuinely completed with zero rows | Accept its completion evidence; publish an empty unpartitioned version or explicitly named empty partition |
| A completion record names another run, workspace, digest, or mapping | Reject before staging or allocation |
| Push retries with its accepted completion record and no result file | Reuse the immutable accepted record and fixed plan |
| A retry supplies a different completion record | Reject `REQUEST_MISMATCH`; preserve the accepted record |
| An omission-only session has no output tables | Require a bound omission-only completion record with an empty completion list |
| A driver loses ownership while its engine process continues | No new allocations under that owner; private outputs cannot affect another session |
| Managed build SQL fails on its second output | Accept no complete build result and publish no output from that invocation |
| Managed build owner is lost while execution remains active | Stop publication and allocation, cancel writers and wait for them, never adopt its tables |
| Managed build completes an empty output | Attest completion internally and apply normal empty output rules |

### Push selection and publication

| scenario | required result |
| ---------- | ----------------- |
| Explicit include covers an explicit empty pair | Treat include as eligibility; require completion and zero rows, without duplicate contribution |
| A held or wholly dropped output has a precreated table | Suppress automatic export; table existence causes no empty contribution |
| Another publication changes and then restores an output partition | The old session conflicts |
| A publication contains only an effective omission | A new revision is committed |
| An unchanged extraction reruns under `changed` | Staged canonical file hashes equal the base manifest; no new version, and no revision when the whole state is equal |
| A base version was written by another tool or `canonical_writer` | Publish a new version; unequal bytes are not an error |
| A crash occurs after GRV publication but before consumer result recording | Retry recovers the existing committed publication |
| Abort follows an ambiguous publication that actually committed | Report the committed outcome; do not mark it unpublished |
| Session inspection sees an ambiguous publication attempt | Report the attempt read-only; a mutating retry resolves it |
| A sealed run matches the durable export plan exactly | Resume publication without changing inputs, outputs, or run entries |
| A recovery seals the run without an expected output version | Require a new session and build |

### Extraction

| scenario | required result |
| ---------- | ----------------- |
| Salesforce receives a pull declaration | Reject `UNSUPPORTED_CAPABILITY` before source login or mutation |
| An extraction fails on the second object or page | Accept no capture receipt; publish no partial snapshot or omission |
| A successful source query genuinely selects zero rows | Record completion and publish the defined empty snapshot |
| A Salesforce case stops matching the declared filter | The next full snapshot removes it from dataset state |
| A source alias or adapter version changes on an unfinished retry | Reject `REQUEST_MISMATCH`; preserve the fixed source identity |
| A retry has a sealed complete capture | Verify and reuse it without querying the changing source again |
| A captured decimal cannot be represented exactly | Reject the whole extraction before allocation |
| Another revision commits during an extraction | Reject snapshot publication against the stale prepared base |
| A table or partition disappears from a successful extraction snapshot | Omit it with the prepared-base guard; failures never authorize omission |
| An extraction's base contains a table the declaration neither declares nor drops | `STATE_CONFLICT` naming the table, before source acquisition or run creation |
| An extraction lists a base table in `selection.drop` | Omit that table under the base guard; a dropped table already absent is a no-op |
| A declaration maps two source columns to one output name | Reject duplicate or incomplete mappings before source acquisition |
| An extraction derives `_month_` from a UTC timestamp | Values are `YYYY-MM` from the UTC date; a null source value fails the extraction |
| Source acknowledgement fails after confirmed publication | Preserve and report the committed outcome; retry acknowledgement idempotently without re-extraction |

### Declarations, results, and adapters

| scenario | required result |
| ---------- | ----------------- |
| A declaration has duplicate YAML keys, unknown fields, an invalid selector, or an unsupported Arrow type | Reject before engine or GRV mutation |
| A column or SQL file changes on an unfinished retry | Reject the changed effective request; preserve accepted capture and outcome evidence |
| A DuckDB `INTEGER` source column is declared `int64` | Accept the exact widening; a narrowing or lossy declaration fails |
| A command emits success or a partial error in JSON | Validate its command-specific schema, error-to-exit correspondence, precision, and token redaction |
| A namespaced adapter command returns JSON | Validate the common envelope and registered redacted adapter-result schema |
| An adapter adds unknown binding fields | Reject through its registered closed schema without modifying the common envelope |
| Built-in and third-party bindings use equivalent capabilities | Follow the same registration, validation, lifecycle, and result paths |
| A built-in runs in-process | Same registration, authority split, and logical conformance results as a process adapter |

### Roots, inspection, and administration

| scenario | required result |
| ---------- | ----------------- |
| Initialization finds dataset objects without `grv.json` | Report a damaged root; create no configuration |
| Initialization repeats against a valid root | Reuse its configuration; reject conflicting explicit parameters |
| Listing discovers orphan version directories | Do not include them in a committed revision's table and partition state |
| Historical manifests have been pruned | Log and revision-entry diff still work; unavailable schema and provenance details are labeled |
| Status inspects an unbound engine or a pending operation | Report it without binding, renewing, or repairing anything |
| Full verification encounters concurrent pruning | Report unavailable objects; create no automatic pin or repair |
| Two independent pins protect one revision | Releasing one leaves the other protection active |
| Pin creation retries with the same active ID and matching request | Return that pin without creating another protection |
| Pin creation attempts to reuse a released ID in the same revision | Reject reuse; preserve the release record |
| The same pin UUID exists in two revisions | Treat them independently; unpin addresses only the specified revision |
| Unpin's ID exists only in another revision | Return `NOT_FOUND`; perform no dataset-wide lookup or release |
| Unpin receives a table, partition, or version pin ID | Reject the unsupported mutation scope |
| GC dry run finds a missing supersession receipt | Report protection and write no receipt or other object |
| A new hold or pin appears after a GC preview | Applying GC rechecks protection and preserves protected versions |
| Physical deletion fails after a committed prune decision | Report tombstoned data and incomplete cleanup; retry preserves the decision |
| Recovery targets an unexpired run or claim | Respect its lease; report waiting or busy work rather than force takeover |
| Recovery resolves an expired run | Seal its authorized entries without publishing or changing its base or inputs |
