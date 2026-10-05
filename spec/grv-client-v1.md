# GRV Client v1

| | |
| --------- | ------------ |
| Status | draft; no CLI implemented |
| Version | 1 |
| Date | 2026-10-05 |
| Extends | GRV v2 |

## Purpose and reading guide

GRV moves declared datasets between GRV and adapter-backed systems.
**Push** means adapter → GRV; **pull** means GRV → adapter.
Users describe the source, selection, columns, and destination behavior.
The CLI and adapters handle authentication, acquisition, staging, transactions,
retries, and publication. A push succeeds when a GRV revision is committed;
a pull succeeds when its destination operation is known to have completed.

This is a design, not an implemented CLI. The version-1 formats may evolve
until the implementation contract is frozen. This document defines the user
surface and adapter architecture. The [execution companion](grv-client-v1-execution.md)
defines implementation and conformance requirements, including locks, holds,
leases, manifests, transactional receipts, recovery, retention, and advanced
external build sessions. Those requirements are performed by the implementation.

The [adapter process protocol](grv-adapter-protocol-v1.md) mechanizes the adapter
lifecycle obligations through a local process channel. See the
[specification guide](README.md) for document scopes and version relationships.

The client adds no GRV storage layout. [GRV v2](grv-storage-v2.md) remains authoritative.
Adapter authentication, captures, job state, contexts, and transfer receipts
are consumer state outside GRV. The GRV backend (filesystem/S3/GCS) is separate
from a data adapter (DuckDB/Salesforce/a website).

## Everyday workflow

```console
grv adapter salesforce login --org sample-org
grv push --decl spec/examples/salesforce-push-filter-mapping.yml --grv ./grv
grv pull --decl spec/examples/duckdb-pull-append-sql.yml --grv ./grv
grv pull --decl spec/examples/duckdb-pull-replace.yml --grv ./grv --revision 7
grv status credit_notes --grv ./grv
```

A declaration defines one directional binding to one dataset. Another
binding can pull a Salesforce-populated dataset into DuckDB. The dataset does
not belong permanently to its producing adapter.

| Adapter | Push | Pull | Initial scope |
| --------- | ------ | ------ | --------------- |
| `duckdb` | Yes | Yes | Local snapshot extraction; managed SQL builds; transactional replace/append pulls |
| `salesforce` | Yes | No | Filtered objects with mapped columns; full extractions |
| Additional installed adapter | Capability | Capability | Its registered source/destination binding and commands |

```console
grv push --decl <yaml> --grv <root>
grv pull --decl <yaml> --grv <root> [--revision <N|latest>]
grv adapter list
grv adapter <name> capabilities
grv adapter <name> <command> [adapter flags]
```

`--grv` selects one local path, `s3://bucket/prefix`, or `gs://bucket/prefix`.
`--decl` selects one YAML declaration. Relative connection, SQL, and column-file
paths resolve from the file that contains the reference. There are no generic
`--engine` or overloaded `--target` overrides; connection/destination settings
are declared explicitly. `--revision` overrides the declaration's pull selector
and changes only the source state. Every command supports `--json`; progress
uses stderr and stdout contains the versioned result envelope.

The CLI reports a transfer UUID before mutation. An advanced `--attempt <uuid>`
reuses that transfer request for inspection/retry; running a declaration again
without it starts new work. Successful retry returns the recorded result and
never inserts again. An unfinished retry preserves its identities and accepted
capture, or fails rather than silently starting a different acquisition.
For a failed, uncommitted pull of `latest`, a retry may resolve a newer latest
revision; a committed receipt always returns the original resolved revision.
Use a fixed revision when the source selection must remain constant.
The state directory has a platform default; advanced `--state <dir>` selects
push state outside GRV. Neither a state path nor a session file is needed for
ordinary transfers.

## The declaration

Every declaration has this common envelope:

| Field | Meaning |
| --------- | --------- |
| `declaration_version` | `1` |
| `kind` | `push` or `pull` |
| `dataset` | The GRV dataset name; declared once |
| `adapter` | A registered adapter name |
| `connection` | Adapter-specific connection identity; credentials stay in authentication stores |
| `tables` | Table names, data selection, output contracts, and optional mappings |
| `checks` | Optional output checks, initially `not_null` |
| `options` | Optional advanced adapter tuning; ordinary declarations omit it |

Push tables name their GRV outputs. Pull tables name their GRV sources.
Names and destination mappings must be unique within the declaration.
Unknown fields fail validation. Built-ins and plugins use the same envelope,
with their registered schemas validating the adapter-specific objects.
There is no duplicated `source.grv`/`target.grv`, explicit extraction mode,
or user-authored `derived_from: session` field.

The [common declaration schema](grv-client-v1-declaration.schema.json) validates
the envelope, table contracts, partitions, checks, and build controls.
[DuckDB](adapters/duckdb.schema.json) and
[Salesforce](adapters/salesforce.schema.json) binding schemas illustrate the
registration points used by every adapter. An extension does not add another
branch to the common schema or use a special `config_version/config` wrapper.

### Output columns and types

A push table's `columns` is an ordered array of `{name, type, source?}` entries.
`name` is the output column; `source` is the adapter's input field selector and
defaults to `name`. Its syntax is validated by the adapter. Mapping and type
are declared together, once. Core normalization strips the source selectors
and produces the ordered Arrow output contract.

A pull without SQL discovers its source schema and partition layout from GRV.
It does not require copying them into the declaration. A SQL pull requires
`columns` containing its ordered output `{name, type}` contract; source mapping
belongs in SQL. An optional non-SQL `columns` is an exact output assertion,
not an implicit projection or cast. Optional `expect` asserts source `columns`
and/or `partition_keys` before destination mutation.

`columns` and `expect.columns` may reference `{file: ./columns.yml}` containing
one YAML array of column entries, or `{ipc: ./schema.arrow}` containing one
serialized Arrow Schema message. References are local, expanded before hashing,
and cannot recursively include other files. A push column file may contain
source selectors; pull/expect files and IPC schemas contain pure Arrow fields.
IPC schemas use input names equal to output names on extraction; choose a YAML
column file when mapping names. Inline and file forms share one validation path.

Supported types are `bool`, `date32`, `int64`, `double`, `utf8`, `binary`,
`decimal128(p,s)` (precision 1–38, scale 0–precision), `timestamp(ms)`,
`timestamp(us)`, `timestamp(ns)`, `timestamp(ms,UTC)`, and `timestamp(us,UTC)`.
Unsupported structures or lossy conversions fail. Decimal values cannot pass
through floating point; timestamp precision and UTC semantics are preserved.
The client validates query/extraction results against the contract without
silently casting, truncating, or changing schema to make them pass.

Push tables optionally declare `partition_keys` (default `[]`). Their output
columns include the non-null string `_{key}_` columns required by GRV.
Values are canonical partition strings, not inferred from dates. Registered
`ext`, `extensions`, and `column_ext` retain their GRV contracts; unsupported
required extensions fail. Arrow nullability does not replace a `not_null` check.
Checks address the operation's output column names, including renamed SQL
outputs, and run over the completed resulting destination scope.

### Push selection

For extraction, each table has an adapter-specific `source` object and its
column contract. DuckDB uses `{table: schema.table, filter?: <predicate>}`;
Salesforce uses `{object: <API name>, filter?: <SOQL predicate>}`.
Filters select rows before projection, and may refer to unexported fields.
They are parsed as one source-language row predicate; SQL expressions are not
accepted as identifiers. Extraction predicates cannot read other relations.
Joins and aggregation use a managed build with declared inputs.

An extraction is a complete filtered snapshot of the declared dataset,
not a row-level delta. Every table must finish, including zero-row tables,
before publication. A record that stops matching the filter disappears from
the next snapshot. Previously published tables/partitions absent from the
successful new snapshot are omitted. A failed page or failed table can never
be interpreted as an omission or empty snapshot.

`selection.policy: changed` is the default: reuse a base version only when
complete content/schema equality is proved. `all` writes new versions even
when equal. Other publication selectors are advanced build controls in the
execution companion. An extraction fixes its whole dataset base; a concurrent
publication requires a new attempt rather than mixing snapshots.

### Pull selection and write behavior

`revision` is `latest` by default or a nonnegative integer selecting a committed
GRV dataset revision. It chooses the table/partition versions recorded in that
revision; users do not select arbitrary physical version directories.
Revision 0 is the unpublished empty initial state, available only to identity
replacement with a known empty source contract.

Each table may declare `partitions: [{month: '2026-09'}, ...]`. Each entry names
one complete canonical partition tuple, containing exactly the layout's keys.
Omitted `partitions` selects every partition of the table; `[]` explicitly
selects none. Tuples are an OR-list with no duplicates. Unknown keys and
selectors on unpartitioned tables fail. A valid tuple absent from the chosen
revision contributes no rows. The core selects revision entries **before**
checking/downloading their manifests/data. Unselected partitions are not fetched
or required to be available. Every selected file must verify; missing or pruned
selected data is an error, not an empty result.

The source relation's schema is the longest schema among its selected versions,
with older prefix schemas null-padded under GRV rules. The latest table-wide
baseline is not evidence of an older revision's schema. When no version is
selected, use `expect.columns` or a trustworthy previously recorded source
contract for an empty typed relation. Without either, return
`INVALID_DECLARATION` requesting an empty-source contract. `expect` checks
selected schemas/layouts; its column template does not fabricate historical
schema when there is no selected version.

`write: replace` is the default. It replaces the declared destination with the
selected source/query result. `write: append` inserts exactly those rows and
preserves existing rows. Neither invents a key, upsert, deletion, or business
increment rule. A new invocation runs the declared selection again; SQL decides
which business records qualify. Within one successful transfer, the adapter's
receipt prevents reapplication when the same attempt is retried.

Destination names are always explicit or adapter defaults from the table name.
Selecting revision 7 still writes the same declared destination. To keep a
separate historical copy, use another declaration with a distinct target
schema/table, as in the historical example below. Source revision selection is
not part of destination ownership;
the same owned replacement scope can intentionally alternate latest/fixed
revisions. A different binding cannot take over its targets.

## Worked declarations

The following files are complete declarations, not generated datasets.

| Case | Declaration |
| --------- | --------------- |
| Push a DuckDB table with a selection filter and column mapping | [duckdb-push-filter.yml](examples/duckdb-push-filter.yml) |
| Push selected Salesforce Cases with SOQL filtering and mapped columns | [salesforce-push-filter-mapping.yml](examples/salesforce-push-filter-mapping.yml) |
| Pull an identity replacement into DuckDB | [duckdb-pull-replace.yml](examples/duckdb-pull-replace.yml) |
| Keep revision 7 in an explicitly named historical destination | [duckdb-pull-replace-historical.yml](examples/duckdb-pull-replace-historical.yml) |
| Append rows using source filters, local lookups, target anti-join, and remapping | [duckdb-pull-append-sql.yml](examples/duckdb-pull-append-sql.yml) |
| Reuse a SQL file and human-readable output column contract | [duckdb-pull-append-sql-files.yml](examples/duckdb-pull-append-sql-files.yml) |
| Select September partitions before transfer, then append with SQL | [duckdb-pull-partitioned-append-sql.yml](examples/duckdb-pull-partitioned-append-sql.yml) |
| Replace with a SQL-selected/remapped result | [duckdb-pull-replace-sql.yml](examples/duckdb-pull-replace-sql.yml) |
| Run a managed SQL build over a held GRV input, then push its result | [duckdb-build-push.yml](examples/duckdb-build-push.yml) |

### Salesforce push

```yaml
declaration_version: 1
kind: push
dataset: credit_notes
adapter: salesforce
connection:
  org: sample-org
tables:
  - name: cases
    source:
      object: Case
      filter: Type = 'Credit Note' AND CreditAmount__c > 0
    columns:
      - {name: id, source: Id, type: utf8}
      - {name: case_number, source: CaseNumber, type: utf8}
      - {name: account_id, source: AccountId, type: utf8}
      - {name: credit_note_number, source: CreditNoteNumber__c, type: utf8}
      - {name: status, source: Status, type: utf8}
      - {name: credit_amount, source: CreditAmount__c, type: 'decimal128(38,6)'}
      - {name: currency_code, source: CurrencyIsoCode, type: utf8}
      - {name: modified_at, source: SystemModstamp, type: 'timestamp(us,UTC)'}
checks:
  - {table: cases, not_null: [id, credit_amount]}
```

This produces `credit_notes.cases`. The adapter generates the SOQL query,
selects a complete lossless extraction path, fetches every page/result file,
and maps values to the declared types. The core writes Parquet and commits the
complete dataset revision. No intermediate JSONL or user-managed staging tool
is required. Salesforce custom field names are examples and must match the org.

### DuckDB SQL append

```yaml
declaration_version: 1
kind: pull
dataset: credit_notes
adapter: duckdb
connection:
  database: ../../application.duckdb
write: append
target:
  schema: app
tables:
  - name: cases
    target:
      table: credit_notes
    select:
      sql: |
        SELECT
          s.id AS source_case_id,
          s.credit_note_number AS note_number,
          s.credit_amount AS amount,
          s.currency_code AS currency
        FROM grv_source.cases AS s
        WHERE s.credit_amount > 0
          AND s.currency_code = 'SEK'
          AND NOT EXISTS (
            SELECT 1 FROM app.blocked_accounts AS b
            WHERE b.account_id = s.account_id
          )
          AND NOT EXISTS (
            SELECT 1 FROM grv_target.credit_notes AS t
            WHERE t.source_case_id = s.id
          )
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY s.id
          ORDER BY s.modified_at DESC, s.credit_note_number DESC,
                   s.credit_amount DESC
        ) = 1
    columns:
      - {name: source_case_id, type: utf8}
      - {name: note_number, type: utf8}
      - {name: amount, type: 'decimal128(38,6)'}
      - {name: currency, type: utf8}
checks:
  - {table: cases, not_null: [source_case_id, amount]}
```

This appends into `app.credit_notes`. SQL excludes blocked accounts and
already-present business records, removes duplicates within the incoming batch,
and selects/remaps destination columns. The CLI does not infer those policies.
A subsequent new transfer sees the updated target; a retry of the same successful
attempt returns its receipt without evaluating SQL again.

The partitioned example adds `partitions: [{month: '2026-09'}]` to the source
table. Its SQL applies the September 15 cutoff, allowlist, anti-join, and mapping
after only September files have been acquired. SQL partition predicates can
still be used as row checks, but do not replace manifest-level selection.

The identity replacement example needs only its table name and destination
mapping. SQL replacement uses the same SQL/column form as append, with
`write: replace`. Both latest and fixed revisions use the declared destination.

## DuckDB adapter

The connection is `{database: <local native path>}`. It cannot be `:memory:`,
a network file, or a server in v1. The adapter owns database locking, private
relations, transactions, and `_grv` consumer metadata. Managed pull/build binds
the workspace to one canonical GRV root and durable UUID; read-only extraction
from unmanaged tables does not establish this binding. Metadata is never inferred
from the existence of ordinary tables.

Direct extraction reads unmanaged base tables in one snapshot transaction.
Views, managed imports, and already-built GRV-derived working tables are not
extraction sources: their dependencies cannot be reconstructed after the fact.
Use a managed build for GRV-derived SQL. Source mappings rename fields and
perform only the declared lossless representation conversion.

A pull declares `target: {schema: app}`. Each table defaults to its GRV name,
or overrides it with `target: {table: credit_notes}`. Advanced adapter `options`
are `materialization: local | s3-view` and `refresh: auto | full`.
The ordinary path defaults to local/auto. Identity replacement may update
changed partitions; SQL, append, and explicitly selected partition scopes
always evaluate their full selected scope. `refresh: full` forces a rebuild.
No user must choose a refresh algorithm to make SQL/append correct.
S3 views support complete identity replacement only, against S3 roots, with the
verification and availability rules in the execution companion.

### SQL bindings and transaction

Each table may provide `select: {sql: <query>}` and its output `columns`.
The query is one read-only `SELECT`, optionally with `WITH`. An alternative
`select: {file: ./import.sql}` reads exactly one statement from a UTF-8 file;
inline/file forms are mutually exclusive and expanded before hashing.
The DuckDB adapter binds these relations inside one transaction:

- `grv_source.<table.name>` contains that table's selected revision/partitions.
- `grv_target.<destination table>` contains its pre-write rows, or an empty
  typed relation if the destination does not exist.
- Local tables can be read by qualified name, for example `app.blocked_accounts`.
  Local views are supported only when their dependencies meet this execution contract.

All queries are fully evaluated/staged before any destination changes. They see
one fixed source selection, one local snapshot, and pre-write destinations;
results cannot depend on destination write order. Then every target write,
check, ownership/binding update, and successful attempt receipt commits together.
A second-table error rolls the entire transaction back. Row counts in receipts
mean completed selected/inserted rows; they do not imply business uniqueness.

Append can attach to an ordinary existing table with the exact ordered output
schema or create a missing table. Several append bindings may share such a table.
Replacement creates/exclusively owns a stable scope identified by the bound
root/workspace, dataset, adapter, and destination namespace (`target.schema`
for DuckDB). Mapping members and source revision are not part of that identity.
Declarations with the same scope update that binding; independent replacement
bindings for one dataset use different destination schemas. Replacement cannot
silently adopt and clear an unowned application table. Append cannot write into replacement-owned
or session/metadata tables. A mapping change clears obsolete owned replacement
targets in the same transaction; it does not abandon old managed data.

Complete identity replacement with no SQL/partition selector is an eligible
GRV build input. SQL replacement, append, and partition-limited replacement are
application imports; their receipts record source selection and output contracts,
without asserting that destination rows represent a complete raw snapshot.
`scope_mode` is `complete` or `selected`; `transform_mode` is `identity` or `sql`.
Any table SQL/partition selector classifies the whole invocation accordingly.
Changing a scope between identity and application roles requires a new target
schema. `SELECT *` remains a SQL import; a new business attempt remains new work.

### SQL execution boundary

Declarations and database definitions are trusted operator-authored code.
The adapter parses one query statement, rejects mutation/control statements,
and evaluates user queries with external access and extension autoload/install
disabled. It constrains dependencies/functions using its supported engine's
binding facilities and registered built-ins, including expansion of local views.
It does not promise a universal SQL safety classifier or sandbox untrusted SQL.
See [DuckDB execution hardening](https://duckdb.org/docs/current/operations_manual/securing_duckdb/overview).

For local pulls/builds, verified source/private input relations are materialized
before restricted query evaluation. Reads of `_grv`, session internals, engine
secret/catalog functions, external scanners, and side-effecting functions are
outside the user-query contract. Only the displayed source/target/input bindings
and supported local dependencies are accessible. SQL does not supply paths to
core staging files. An unsupported dependency fails before destination writes.
S3-view identity refresh has no user query and follows its separate reader path.

## Managed builds

An optional `build` block changes a push from extraction into a provenance-aware
build. The default `build.execution` is `managed`. A DuckDB managed build lists
completed identity input tables with aliases, and supplies one output query per
table. Users first pull the required identity inputs; the build runner selects
and freezes their completed generations during preparation.

```yaml
declaration_version: 1
kind: push
dataset: month_totals
adapter: duckdb
connection:
  database: ../../workspace.duckdb
build:
  inputs:
    - {table: raw.mt_month_stats, as: month_stats}
tables:
  - name: totals
    source:
      sql: |
        SELECT period, SUM(spend)::DECIMAL(38,4) AS total_spend, _period_
        FROM grv_input.month_stats
        GROUP BY period, _period_
    columns:
      - {name: period, type: date32}
      - {name: total_spend, type: 'decimal128(38,4)'}
      - {name: _period_, type: utf8}
    partition_keys:
      - period
checks:
  - {table: totals, not_null: [period, total_spend, _period_]}
```

Run this with ordinary `grv push --decl ... --grv ...`. The runner fixes
input revisions, confirms dependency holds, binds immutable private inputs as
`grv_input.<alias>`, executes each output query, verifies completion/schema,
captures outputs, and publishes. Queries read only declared private inputs and,
when `build.self_input: true`, locally materialized prepared-base relations as
`grv_self.<output table>`. They cannot read unlisted local working tables or
other output queries. All queries evaluate before output writes. Managed query results are
complete output relations and replace private output contents; they never append
to previous-state seeds. Self-input is the separate immutable read binding.

The runner owns locking, renewal, cancellation, stopped-writer checks, and the
completion receipt. It never treats abandoned output tables as completion.
Every derived output conservatively cites all confirmed external input revisions;
self-input is the prepared target base, not a self-hold. Source revision 0 and
SQL/application imports are not eligible external build inputs.
`build.code_fingerprints` optionally supplies external code identities.

Advanced external engines use `build.execution: external`, output aliases in
`source.table`, and the explicit session/completion API in the companion.
Their drivers must meet its fencing/attestation contract. Managed and external
build capabilities are separate; declaring a capability requires implementing
its lifecycle rather than merely accepting its flag.

## Adapter architecture and extension contract

### Modules and responsibilities

| Module | Owns |
| --------- | ------ |
| CLI | Commands, YAML/reference loading, result rendering; no transport or destination SQL implementation |
| Transfer core | Capability checks, normalized plan/identity, source revision/file selection, Arrow contracts, capture writer, GRV holds/runs/publication, outcome coordination |
| Adapter | Connection/auth, source dialect/encoding/jobs, destination execution/transactions/receipts, engine-specific locks, registered schemas/commands |
| Managed runner | Logical input/output lifecycle coordinated by core; engine execution and stopped-writer attestation supplied by its adapter |
| GRV backend | Immutable storage operations, conditional writes, validators, backend credentials |

One normalization path serves inline/file column contracts, all adapters, retries,
and advanced sessions. Adapter modules do not allocate GRV versions, edit
manifests, or write `LATEST`. Core code does not branch on adapter names to parse
SOQL, choose REST/Bulk, manage DuckDB catalogs, or operate a browser. Shared helpers
provide Arrow/Parquet validation and the common state/cancellation APIs.

### Registration and validation

Every adapter registers its name, package/interface version, binding schema
version, capability descriptor, validation-point schemas, result schemas, and
namespaced commands. Duplicate names and incompatible versions fail. The CLI
uses installed adapters; transfer execution does not install/download plugins.
Built-ins register through the same API and use the same binding shape.

Capabilities include `push`, `pull`, `managed_build`, `external_build`,
`source_consistency`, `resumable_extract`, supported `pull_write_modes`, and
command names. Additional pull adapters must declare transactional or journaled
completion/recovery before advertising support; the core does not pretend all
destinations commit atomically.

Validation points are `connection`, `options`, extraction/managed-build/external-
build table `source`, pull `target`, pull table `target`, pull table `select`,
column `source`, build input relation, and adapter result/command/session details.
Schemas are closed at each implemented point and selected for the requested
direction/build mode; DuckDB extraction/build tuning is empty in v1. Missing optional bindings receive
adapter defaults and are validated again. An adapter cannot add undeclared common
fields. Its configuration syntax can evolve under its registered schema version;
requests fix the resolved schema/package/interface versions before execution.

Validate YAML/common shape first, load the registry, and reject unsupported
direction/write/build capabilities before login, GRV runs, or mutation. Then
validate every implemented adapter point, expand defaults, bind connection
identity, and resolve source/schema expectations. Metadata reads may occur only
in this binding phase. Invalid configurations never leave partial destination
writes or GRV allocations.

### Lifecycle interface

These are language-neutral obligations for the future SDK, not implemented APIs.
Optional methods are required when the corresponding capability is advertised.

| Method | Inputs and required result |
| --------- | --------------------------- |
| `validate_binding` | Common declaration + adapter fragments → normalized config/defaults; pure validation, no authentication or storage I/O |
| `bind_connection` | Normalized config → stable system identity and authenticated handle; aliases are resolved, secrets remain private |
| `extract` | Fixed context/output contracts + persisted source checkpoint → named table batches and explicit table/source completion |
| `prepare_pull` | Fixed revision/selected files/contracts + target config → physical mappings, output contracts, ownership/write/recovery plan |
| `resolve_pull` | Attempt/request identity → committed receipt, trustworthy not-committed, busy, or unknown; checked before reading source files |
| `apply_pull` | Prepared plan + verified source provider → destination writes and durable receipt under the adapter's declared commit/recovery contract |
| `prepare_build` | Core-held logical input/base bindings + output contracts → private engine mappings and controlled invocation handle |
| `execute_build` | Prepared handle + declared queries/model integration → outputs, success/failure, stopped writers, and immutable completion facts |
| `inspect_connection` | Optional read-only connection state → registered inspection details, with no binding/repair/renewal |
| `cancel` / `close` | Signal cancellation, stop/await adapter work, release handles/locks; preserve uncertain commit evidence |
| `after_publish` | Confirmed GRV outcome → idempotent source acknowledgement/cursor update, if the adapter needs one |

Push streaming events identify the table and provide its schema, batches, and
one explicit `table_complete` (including zero rows), followed by `source_complete`.
Core rejects missing/duplicate completion, batches after completion, unexpected
outputs, schema mismatch, and partial success. Batching does not prove completeness.
The core stages Parquet and seals the capture only after all selected outputs
and all writers are complete. Resumption uses persisted source identity;
fragments from unrelated snapshots cannot be combined.

The core resolves committed GRV revision entries and verifies selected files.
The source provider can stage/materialize them without adapters guessing paths.
Pull adapters own destination atomicity and durable receipts. `resolve_pull`
cannot report not-committed merely because current destination rows differ;
it must fence the former writer and establish trustworthy receipt/journal state.
No source download/re-evaluation occurs when a committed receipt already proves
success. Lost metadata produces an explicit conflict/unknown outcome.

Core contexts own attempt/request identities, fixed adapter versions, capture
integrity, GRV holds/runs/allocations, and publication outcomes. Adapter state owns
source job/authentication state and destination commit evidence; both are durable
before advancing a phase. Core supervises cancellation/renewal; engine locks,
execution handles, and writer-stop evidence are implemented by the adapter.
Publication ownership loss prevents new allocations and publication, even if
an engine cannot immediately cancel. Source cursors advance only after confirmed
publication; acknowledgement failure preserves the committed outcome and a durable pending
acknowledgement. Same-attempt retry may retry that hook using its fixed state,
without reacquiring data or publishing again. Hooks must be idempotent and
monotone; they cannot roll back a later acknowledged cursor. Cleanup never discards evidence for an unresolved commit.

A managed runner operates on logical input/output names and explicit completion.
DuckDB private schemas and OS locks belong to DuckDB's implementation. Future
engine adapters use their own mappings without changing the publication core.
An external integration that bypasses the runner remains responsible for the
advanced lifecycle obligations; declaring inputs alone does not establish provenance.

### Salesforce binding and additional adapters

Salesforce uses `{org: <alias-or-username>, api_version?: <version>}` and the
existing `sf` authentication store. `grv adapter salesforce login`, `status`, and
`describe` delegate login/session/source inspection. Aliases resolve to the
actual org ID for a fixed attempt; credentials are not copied into declarations.

The default `options.transport: auto` chooses a supported complete, lossless
REST/Bulk extraction path. Decimal integrity is a requirement of either path,
not something the user must achieve by choosing a transport. Advanced `rest`
or `bulk` preferences fail if they cannot meet the declared semantics/types.
`options.all_rows` defaults to false. Fetch all pages/results, verify reported
counts where available, and reject successful-but-truncated CLI exports.
Salesforce consistency is a reported capture window, not a cross-object
transaction snapshot. V1 re-evaluates the whole filter; no hidden watermark/CDC.

A website adapter can register `source: {url, fields, pagination, ...}`, column
selectors, and login/browser-session commands under `grv adapter <name>`.
It uses the same table contracts and completion boundary. Failed login, changed
page structure, or unfinished pagination cannot masquerade as an empty capture.
Website extraction implementation is an extension example, not a built-in promise.

### Adapter conformance

Every adapter package runs the common contract suite plus its capability-specific
suite: closed configuration validation; unsupported direction before side effects;
stable identity/version retries; schema/type/check failure; zero-row completion;
missing pages/tables/completion; restart and cancellation; secret redaction;
no cursor advancement before publication; and confirmed-vs-unknown outcomes.
Pull adapters also test multi-output failure, receipt replay without source reads,
new-attempt re-evaluation, ownership/schema conflicts, and ambiguous commit recovery.
Build adapters test fixed inputs/holds, private mappings, cancelled/lost ownership,
stopped writers, completion integrity, and source provenance. DuckDB adds SQL
bindings, one-statement files, partition acquisition scope, pre-write target
snapshots, and transactional data/receipt rollback. Salesforce adds complete
pagination/counts and exact decimals. See the companion's concrete scenarios.

## Inspection, administration, and advanced integrations

```console
grv init --grv <root>
grv ls --grv <root> [<dataset>] [--table <table>] [--revision N]
grv show <dataset> --grv <root> [--revision N] [--retention]
grv status <dataset> --grv <root> [--decl <yaml>]
grv log <dataset> --grv <root> [--limit N]
grv diff <dataset> --grv <root> --from N --to <N|latest>
grv verify <dataset> --grv <root> [--revision N] [--full]
grv pin <dataset> --grv <root> --revision N --reason <text> [--pin <id>]
grv unpin <dataset> --grv <root> --revision N --pin <id>
grv gc <dataset> --grv <root> [--dry-run | --apply]
grv recover <dataset> --grv <root> [--run <run-id>] [--dry-run]
```

`status --decl` optionally inspects the matching dataset's adapter connection
without transferring data, using registered `inspect_connection` support;
unsupported adapters reject this optional inspection. Core inspection does not need a declaration or
adapter login. Read-only commands never establish bindings, create pins, renew
leases, or repair state. Listing objects does not prove their commitment.
Explicit pins protect retained revisions; merely choosing a revision does not.
GC/recovery use the complete existing GRV protocols and report partial effects.

Advanced root initialization accepts clock/lease/grace parameters defined in the
companion. External engines use `grv session prepare/show/renew/abort` and
`grv push --session ... --build-result ...`. Contexts are protected consumer state;
inspection redacts tokens. These are integration tools beneath the managed path.

Results conform to [the output schema](grv-client-v1-command-output.schema.json).
Status returns nullable `adapter_state: {adapter, details}`. Each session has
registered `adapter_context`; physical mapping identifiers are adapter-owned.
Every adapter uses the same common transfer result fields plus registered
`adapter_result` details; DuckDB details include write/transform/scope modes and
resolved source partitions. Version/revision/byte values use decimal strings
in JSON results. Exit statuses are 0 success, 2 invalid request, 3 conflict/busy/
ownership loss, 4 unavailable/not found, 5 integrity/protocol failure, and
6 adapter/backend/engine failure or unknown outcome. Error codes and full
command/result contracts are defined in the companion.

## Scope and later additions

V1 supports full filtered extractions, lossless typed mappings, DuckDB identity
replacement, SQL imports with append/replacement, and managed/external DuckDB
builds with fixed provenance. It supports one dataset/root per transfer, explicit
revision retention, inspection, GC, recovery, installed adapters, and adapter
commands. Native DuckDB database access is serialized across processes.

Scheduling, multi-dataset atomicity, automatic retention, cross-root derivation,
CDC/watermarks, inferred keys, generic merge/upsert, concurrent/server DuckDB,
provenance for arbitrary SQL application imports, and narrower output dependencies
need later contracts. V1 does not orchestrate arbitrary external model workflows;
it manages the lifecycle of declared SQL builds and advanced attested integrations.
There is no CLI implementation in this revision.
