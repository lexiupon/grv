# GRV Client v1

|         |                                    |
| ------- | ---------------------------------- |
| Status  | draft; partial Rust implementation |
| Version | 1                                  |
| Date    | 2026-10-05                         |
| Extends | GRV v2                             |

## Purpose and reading guide

GRV moves declared datasets between GRV and adapter-backed systems:

- **Push** moves data from an adapter into GRV (adapter → GRV). A push succeeds
  when a GRV revision is committed.
- **Pull** moves data from GRV into an adapter (GRV → adapter). A pull succeeds
  when its destination operation is known to have completed.

Users describe the source, selection, columns, and destination behavior. The
CLI and adapters handle authentication, acquisition, staging, transactions,
retries, and publication.

This is the full v1 contract. The Rust CLI implements a staged subset; see
[the repository README](../README.md) for implemented paths and remaining verification gates. The version-1 formats may evolve
until the implementation contract is frozen. The specification has three
documents:

- This document defines the user surface and the adapter architecture.
- The [execution companion](grv-client-v1-execution.md) defines implementation
  and conformance requirements, including locks, holds, leases, manifests,
  transactional receipts, recovery, retention, and advanced external build
  sessions. The implementation performs those requirements.
- The [adapter process protocol](grv-adapter-protocol-v1.md) mechanizes the
  adapter lifecycle obligations through a local process channel.

See the [specification guide](README.md) for document scopes and version
relationships.

The client adds no GRV storage layout; [GRV v2](grv-storage-v2.md) remains
authoritative. Adapter authentication, captures, job state, contexts, and
transfer receipts are consumer state outside GRV. The GRV backend (filesystem,
S3, or GCS) is separate from a data adapter (such as DuckDB, Salesforce, or a
website).

New readers should read _Terms_, _Everyday workflow_, and _The declaration_,
then the _Worked declarations_. _DuckDB adapter_ and _Managed builds_ cover
adapter-specific features. _Adapter architecture and extension contract_ is for
adapter authors. _Inspection, administration, and advanced integrations_ lists
the remaining commands.

## Terms

All three client documents use these terms with these meanings.

- **attempt** — one transfer request, identified by an attempt UUID (`--attempt`). Retrying with the same UUID resumes or replays that request; it never starts different work.
- **fixed request** — the canonical effective declaration plus adapter and connection identities that an attempt is bound to; hashed as `declaration_sha256` and, for pulls, `request_sha256`.
- **capture** — the staged Parquet output of a complete extraction, sealed by a **capture receipt** (schemas, counts, hashes, source identity). Completion evidence for an extraction push, not a GRV commit.
- **pull receipt** — the immutable record of a successful pull attempt, retained by the destination adapter. Proof that the attempt succeeded. A transactional adapter (such as DuckDB) commits it in the same transaction as the writes; a journaled adapter records it under its registered recovery contract.
- **pull checkpoint** — the destination's record of the revision and generation a complete identity materialization currently reflects. (Distinct from a **source checkpoint**, the adapter's durable record of a resumable extraction's source identity.)
- **materialization** — destination tables written by pulls of one GRV dataset into one replacement scope. An **identity materialization** comes from complete identity replacement (no SQL, no partition selector) and is the only kind eligible as a build input. Its tables are **tracking tables**.
- **generation** — one successful refresh of a materialization, identified by `generation_id`.
- **application import** — a pull that uses SQL, `write: append`, or a partition selector. It has receipts but is not an identity materialization.
- **replacement scope** — the destination exclusively owned by one replacement binding, identified by the bound root and workspace, the dataset, the adapter, and the destination namespace (DuckDB `target.schema`).
- **workspace** — one DuckDB database file bound to one GRV root, identified by `workspace_id`.
- **session** — one GRV run of a push plus its fixed declaration, base revision, inputs, and outcome state. A build session also owns private engine tables. Its **context file** is the protected, token-bearing file that describes it.
- **completion record** — the managed runner's or external driver's attestation that a build invocation succeeded, all writers stopped, and which outputs completed.
- **export plan** — the durable plan of versions to write, reuse, or omit, recorded before any version allocation.
- **outcome** — `published`, `no-op`, or `aborted`.

## Everyday workflow

A typical sequence logs in to a source, pushes a dataset, pulls it into DuckDB,
and inspects its status:

```console
grv adapter salesforce login --org sample-org
grv push --decl spec/examples/salesforce-push-filter-mapping.yml --grv ./grv
grv pull --decl spec/examples/duckdb-pull-append-sql.yml --grv ./grv
grv pull --decl spec/examples/duckdb-pull-replace.yml --grv ./grv --revision 7
grv status credit_notes --grv ./grv
```

A declaration defines one directional binding to one dataset. A dataset does
not belong permanently to the adapter that produced it: another binding can
pull a Salesforce-populated dataset into DuckDB.

| Adapter                      | Push       | Pull       | Initial scope                                                                         |
| ---------------------------- | ---------- | ---------- | ------------------------------------------------------------------------------------- |
| `duckdb`                     | Yes        | Yes        | Local snapshot extraction; managed SQL builds; transactional replace and append pulls |
| `salesforce`                 | Yes        | No         | Filtered objects with mapped columns; full extractions                                |
| Additional installed adapter | Capability | Capability | Its registered source or destination binding and commands                             |

```console
grv push --decl <yaml> --grv <root>
grv pull --decl <yaml> --grv <root> [--revision <N|latest>]
grv adapter list
grv adapter install <path|tarball> [--replace]
grv adapter <name> capabilities
grv adapter <name> <command> [adapter flags]
```

### Common flags

- `--grv` selects one GRV root: a local path, `s3://bucket/prefix`, or
  `gs://bucket/prefix`.
- `--decl` selects one YAML declaration. Relative connection, SQL, and
  column-file paths resolve from the file that contains the reference.
- `--revision` overrides the declaration's pull selector. It changes only the
  source state.
- `--json` is supported by every command. Progress goes to stderr, and stdout
  contains the versioned result envelope (execution companion, _Common command
  behavior_).

There are no generic `--engine` or overloaded `--target` overrides. Connection
and destination settings are declared explicitly.

### Attempts and retries

The CLI reports the attempt UUID before mutation.

- Running a declaration again without `--attempt` starts new work.
- The advanced `--attempt <uuid>` flag reuses that transfer request for
  inspection or retry.
- A successful retry returns the recorded result and never inserts again.
- An unfinished retry preserves its identities and accepted capture, or fails.
  It never silently starts a different acquisition.
- For a failed, uncommitted pull of `latest`, a retry may resolve a newer latest
  revision. A committed pull receipt always returns the original resolved
  revision. Use a fixed revision when the source selection must remain constant.

The execution companion defines retry behavior in full (_The pull algorithm_
and _Extraction session preparation and capture_).

### State directory

Push state lives in a state directory outside GRV. The state directory has a
platform default; the advanced `--state <dir>` flag selects another one.
Ordinary transfers need neither a state path nor a session file.

On ephemeral machines such as CI runners, the default state directory is lost
with the machine, and with it the ability to retry an attempt with `--attempt`.
GRV itself stays consistent: `grv recover` or GC recovers an interrupted run
once its lease expires, and the run's source holds stay until then. Such
deployments should do one of the following:

- place `--state` on durable storage; or
- treat every job as new work, and schedule `grv recover` and `grv gc` for the
  datasets they write.

## The declaration

A declaration is one YAML file that describes one push or one pull of one
dataset. Every declaration has this common envelope:

| Field                 | Meaning                                                                         |
| --------------------- | ------------------------------------------------------------------------------- |
| `declaration_version` | `1`                                                                             |
| `kind`                | `push` or `pull`                                                                |
| `dataset`             | The GRV dataset name; declared once                                             |
| `adapter`             | A registered adapter name                                                       |
| `connection`          | Adapter-specific connection identity; credentials stay in authentication stores |
| `tables`              | Table names, data selection, output contracts, and optional mappings            |
| `checks`              | Optional output checks, initially `not_null`                                    |
| `options`             | Optional advanced adapter tuning; ordinary declarations omit it                 |

Envelope rules:

- Push tables name their GRV outputs. Pull tables name their GRV sources.
- Names and destination mappings must be unique within the declaration.
- Unknown fields fail validation.
- Built-in and plugin adapters use the same envelope. Their registered schemas
  validate the adapter-specific objects.
- There is no duplicated `source.grv` or `target.grv`, no explicit extraction
  mode, and no user-authored `derived_from: session` field.

The [common declaration schema](grv-client-v1-declaration.schema.json) validates
the envelope, table contracts, partitions, checks, and build controls. The
[DuckDB](adapters/duckdb.schema.json) and
[Salesforce](adapters/salesforce.schema.json) binding schemas illustrate the
registration points that every adapter uses. An extension does not add another
branch to the common schema, and does not use a special `config_version/config`
wrapper.

YAML parsing, normalization, and request hashing follow the execution
companion, _Normalized plans and identity_.

### Output columns and types

This section defines how a declaration names its output columns, maps them to
source fields, and types them.

#### Push columns

A push table's `columns` is an ordered array of `{name, type, source?}` entries:

- `name` is the output column.
- `source` is the adapter's input field selector. It defaults to `name`. A
  selector is an identifier, never an expression. The adapter validates its
  syntax.

Mapping and type are declared together, once. Core normalization strips the
source selectors and produces the ordered Arrow output contract.

#### Pull columns

A pull without SQL discovers its source schema and partition layout from GRV.
It does not require copying them into the declaration. On a non-SQL pull,
optional `columns` is an exact output assertion, not an implicit projection or
cast.

A SQL pull requires `columns` containing its ordered output `{name, type}`
contract. Source mapping belongs in SQL.

Optional `expect` asserts source `columns` and/or `partition_keys` before
destination mutation.

#### Column files

`columns` and `expect.columns` may reference a file instead of listing entries
inline:

- `{file: ./columns.yml}` contains one YAML array of column entries.
- `{ipc: ./schema.arrow}` contains one serialized Arrow Schema message.

References are local, are expanded before hashing, and cannot recursively
include other files. A push column file may contain source selectors. Pull and
`expect` column files, and IPC schemas, contain pure Arrow fields. On
extraction, IPC schemas use input names equal to output names; choose a YAML
column file when mapping names. Inline and file forms share one validation path.

#### Types

Declarations name types with the Arrow-style spellings below. Each maps to
exactly one GRV logical type ([GRV v2 §4](grv-storage-v2.md)), which is what
schema baselines, revision schemas, and the adapter wire contract use:

| Declaration type                                  | GRV logical type                                          |
| ------------------------------------------------- | --------------------------------------------------------- |
| `bool`                                            | `"boolean"`                                               |
| `int64`                                           | `"int64"`                                                 |
| `double`                                          | `"float64"`                                               |
| `utf8`                                            | `"string"`                                                |
| `binary`                                          | `"binary"`                                                |
| `date32`                                          | `"date"`                                                  |
| `decimal128(p,s)` (precision 1–38, scale 0–p)     | `{"decimal": {"precision": p, "scale": s}}`               |
| `timestamp(ms)`, `timestamp(us)`, `timestamp(ns)` | `{"timestamp": {"unit": "ms"\|"us"\|"ns", "utc": false}}` |
| `timestamp(ms,UTC)`, `timestamp(us,UTC)`          | `{"timestamp": {"unit": "ms"\|"us", "utc": true}}`        |

Every other GRV logical type is unsupported in client v1: `int8`, `int16`,
`int32`, unsigned integers, `float32`, `json`, `uuid`, `fixed_binary`, `time`,
nanosecond UTC timestamps, `list`, `map`, and `struct`. If a pull's selected
source schema contains one of these types, the pull fails with
`INVALID_DECLARATION` naming the column, before destination mutation. Such a
column is never converted.

#### Exact widening

Extraction may convert a source value only by **exact widening** into its
declared type. The permitted conversions are:

- a narrower signed integer to `int64`;
- a 32-bit float to `double`;
- a decimal to a decimal with at least as many integer digits (`p − s`) and
  fractional digits (`s`);
- a timestamp to a finer unit with the same UTC flag;
- a source's textual encoding of a number, date, or timestamp that the adapter
  parses exactly (for example, Salesforce decimal strings).

Any other conversion fails, and so does any value that does not fit. Decimal
values never pass through floating point. The client validates query and
extraction results against the contract. It does not silently cast, truncate,
or change schema to make them pass.

#### Partition columns

Push tables optionally declare `partition_keys` (default `[]`). Their output
columns include the non-null `utf8` `_{key}_` columns that GRV requires. Values
of these columns must already be canonical partition strings; nothing is
inferred implicitly.

In an extraction, a `_{key}_` column may instead declare
`derive: {from: <output column>, format: year | month | day}` in place of
`source`:

- The core computes the value from the named `date32` or timestamp output
  column, as `2026`, `2026-09`, or `2026-09-15`.
- UTC timestamps use their UTC calendar date. Timestamps without a time zone use
  their wall-clock date.
- A null or out-of-range value (outside years 0001–9999) fails the extraction.

Builds compute partition columns in SQL instead.

#### Extensions and checks

Registered `ext`, `extensions`, and `column_ext` retain their GRV contracts.
Unsupported required extensions fail.

Arrow nullability does not replace a `not_null` check. Checks address the
operation's output column names, including renamed SQL outputs. For a pull,
they run over the completed resulting destination scope, inside the
destination transaction. For a push, they run over the complete staged output,
after capture or export and before any version allocation.

### Push selection

This section defines which rows an extraction reads and which tables and
versions a push publishes. Managed builds are described in _Managed builds_.

#### Source and filter

For extraction, each table has an adapter-specific `source` object and its
column contract:

- DuckDB uses `{table: schema.table, filter?: <predicate>}`.
- Salesforce uses `{object: <API name>, filter?: <SOQL predicate>}`.

Filters select rows before projection, and may refer to unexported fields. A
filter is parsed as one source-language row predicate. SQL expressions are not
accepted as identifiers. Extraction predicates cannot read other relations.
Joins and aggregation use a managed build with declared inputs.

#### Snapshots

An extraction is a complete filtered snapshot of the declared dataset, not a
row-level delta:

- Every table must finish before publication, including zero-row tables.
- A record that stops matching the filter disappears from the next snapshot.
- Partitions of declared tables that are absent from the successful new
  snapshot are omitted.
- A failed page or failed table can never be interpreted as an omission or an
  empty snapshot.

An extraction fixes its whole dataset base. A concurrent publication requires a
new attempt rather than mixing snapshots (execution companion, _Extraction
snapshot membership_).

#### Table membership

Whole tables are never removed implicitly. If the base revision contains a table
that the declaration does not list, the push fails with `STATE_CONFLICT` before
source acquisition, because that table may belong to another declaration.

To remove such a table intentionally, name it in
`selection.drop: [{table: <name>}]`. A listed table that is already absent is a
no-op. Independent sources that refresh on their own schedules belong in
separate datasets.

#### Version reuse

`selection.policy` decides whether unchanged content gets new versions:

- `changed` is the default. It reuses a base version only when complete equality
  of content and schema is proved. The core writes every output in a canonical
  sorted Parquet form, so it proves equality by comparing staged file hashes
  with the base manifest, without downloading base data (execution companion,
  _Canonical encoding and equality_).
- `all` writes new versions even when content is equal.

Apart from whole-table `drop`, publication selectors are advanced build controls
in the execution companion (_Build selection and no-op rules_).

### Pull selection and write behavior

A pull selects one committed revision, optionally narrows it to partitions, and
writes the result to a declared destination.

#### Revision

`revision` is `latest` by default, or a nonnegative integer selecting a
committed GRV dataset revision. It chooses the table and partition versions
recorded in that revision; users do not select arbitrary physical version
directories. Revision 0 is the unpublished empty initial state. It is available
only to identity replacement with a known empty source contract.

#### Partitions

Each table may declare `partitions: [{month: '2026-09'}, ...]`:

- Each entry names one complete canonical partition tuple, containing exactly
  the layout's keys.
- Tuples are an OR-list with no duplicates.
- Omitted `partitions` selects every partition of the table. `[]` explicitly
  selects none.
- Unknown keys fail. Selectors on unpartitioned tables fail.
- A valid tuple that is absent from the chosen revision contributes no rows.

The core selects revision entries **before** checking or downloading their
manifests and data. Unselected partitions are not fetched and are not required
to be available. Every selected file must verify. Missing or pruned selected
data is an error, not an empty result.

#### Source schema and the empty-source contract

The source relation's schema is the longest schema among its selected versions.
Older prefix schemas are null-padded under GRV rules. The latest table-wide
baseline is not evidence of an older revision's schema.

When no version is selected, the pull uses an empty-source contract for an empty
typed relation: either `expect.columns` or a trustworthy previously recorded
source contract. Without either, the pull returns `INVALID_DECLARATION`
requesting an empty-source contract. `expect` checks selected schemas and
layouts. Its column template does not fabricate historical schema when there is
no selected version.

#### Write mode

- `write: replace` is the default. It replaces the declared destination with the
  selected source or query result.
- `write: append` inserts exactly those rows and preserves existing rows.

Neither mode invents a key, upsert, deletion, or business increment rule. A new
invocation runs the declared selection again, and SQL decides which business
records qualify. Within one successful transfer, the adapter's pull receipt
prevents reapplication when the same attempt is retried.

#### Destinations

Destination names are always explicit, or are adapter defaults from the table
name. Selecting revision 7 still writes the same declared destination. To keep a
separate historical copy, use another declaration with a distinct target schema
or table, as in the historical example below.

Source revision selection is not part of destination ownership. The same owned
replacement scope can intentionally alternate between latest and fixed
revisions. A different binding cannot take over its targets.

## Worked declarations

The following files are complete declarations, not generated datasets.

| Case                                                                             | Declaration                                                                               |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Push a DuckDB table with a selection filter and column mapping                   | [duckdb-push-filter.yml](examples/duckdb-push-filter.yml)                                 |
| Push selected Salesforce Cases with SOQL filtering and mapped columns            | [salesforce-push-filter-mapping.yml](examples/salesforce-push-filter-mapping.yml)         |
| Pull an identity replacement into DuckDB                                         | [duckdb-pull-replace.yml](examples/duckdb-pull-replace.yml)                               |
| Keep revision 7 in an explicitly named historical destination                    | [duckdb-pull-replace-historical.yml](examples/duckdb-pull-replace-historical.yml)         |
| Append rows using source filters, local lookups, target anti-join, and remapping | [duckdb-pull-append-sql.yml](examples/duckdb-pull-append-sql.yml)                         |
| Reuse a SQL file and human-readable output column contract                       | [duckdb-pull-append-sql-files.yml](examples/duckdb-pull-append-sql-files.yml)             |
| Select September partitions before transfer, then append with SQL                | [duckdb-pull-partitioned-append-sql.yml](examples/duckdb-pull-partitioned-append-sql.yml) |
| Replace with a SQL-selected and remapped result                                  | [duckdb-pull-replace-sql.yml](examples/duckdb-pull-replace-sql.yml)                       |
| Run a managed SQL build over a held GRV input, then push its result              | [duckdb-build-push.yml](examples/duckdb-build-push.yml)                                   |

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
      - { name: id, source: Id, type: utf8 }
      - { name: case_number, source: CaseNumber, type: utf8 }
      - { name: account_id, source: AccountId, type: utf8 }
      - { name: credit_note_number, source: CreditNoteNumber__c, type: utf8 }
      - { name: status, source: Status, type: utf8 }
      - { name: credit_amount, source: CreditAmount__c, type: "decimal128(38,6)" }
      - { name: currency_code, source: CurrencyIsoCode, type: utf8 }
      - { name: modified_at, source: SystemModstamp, type: "timestamp(us,UTC)" }
checks:
  - { table: cases, not_null: [id, credit_amount] }
```

This produces `credit_notes.cases`. The adapter generates the SOQL query,
selects a complete lossless extraction path, fetches every page and result file,
and maps values to the declared types. The core writes Parquet and commits the
complete dataset revision. No intermediate JSONL or user-managed staging tool
is required. The Salesforce custom field names are examples and must match the
org.

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
      - { name: source_case_id, type: utf8 }
      - { name: note_number, type: utf8 }
      - { name: amount, type: "decimal128(38,6)" }
      - { name: currency, type: utf8 }
checks:
  - { table: cases, not_null: [source_case_id, amount] }
```

This appends into `app.credit_notes`. The SQL:

- excludes blocked accounts and already-present business records;
- removes duplicates within the incoming batch; and
- selects and remaps destination columns.

The CLI does not infer those policies. A subsequent new transfer sees the
updated target. A retry of the same successful attempt returns its pull receipt
without evaluating SQL again.

The partitioned example adds `partitions: [{month: '2026-09'}]` to the source
table. Its SQL applies the September 15 cutoff, allowlist, anti-join, and
mapping after only September files have been acquired. SQL partition predicates
can still be used as row checks, but they do not replace manifest-level
selection.

The identity replacement example needs only its table name and destination
mapping. SQL replacement uses the same SQL and column form as append, with
`write: replace`. Both latest and fixed revisions use the declared destination.

## DuckDB adapter

This section describes the DuckDB connection, extraction sources, pull
destinations, and options.

### Connection and workspace

The connection is `{database: <local native path>}`. In v1 it cannot be
`:memory:`, a network file, or a server. The adapter owns database locking,
private relations, transactions, and `_grv` consumer metadata (execution
companion, _DuckDB process ownership_ and _Consumer metadata_).

A managed pull or build binds the workspace to one canonical GRV root and a
durable UUID. Read-only extraction from unmanaged tables does not establish this
binding. Metadata is never inferred from the existence of ordinary tables.

### Extraction sources

Direct extraction reads unmanaged base tables in one snapshot transaction.
Views, managed imports, and already-built GRV-derived working tables are not
extraction sources, because their dependencies cannot be reconstructed after the
fact. Use a managed build for GRV-derived SQL. Source mappings rename fields and
perform only the exact widening conversions defined in
[Output columns and types](#output-columns-and-types). For example, a DuckDB
`INTEGER` column may be declared `int64`.

### Pull destinations and options

A pull declares `target: {schema: app}`. Each table defaults to its GRV name, or
overrides it with `target: {table: credit_notes}`.

Advanced adapter `options` are:

- `materialization: local | s3-view` (default `local`);
- `refresh: auto | full` (default `auto`).

Identity replacement may update changed partitions. SQL, append, and explicitly
selected partition scopes always evaluate their full selected scope.
`refresh: full` forces a rebuild. No user must choose a refresh algorithm to make
SQL or append correct.

S3 views support complete identity replacement only, against S3 roots, with the
verification and availability rules in the execution companion (_Backends and
S3 views_).

### SQL bindings and transaction

A DuckDB pull table can transform its selected source with one SQL query. This
section defines the query forms, the relations a query can read, how writes
commit, and how replacement and append own their destinations.

#### Query forms and bindings

Each table may provide `select: {sql: <query>}` and its output `columns`. The
query is one read-only `SELECT`, optionally with `WITH`. The alternative
`select: {file: ./import.sql}` reads exactly one statement from a UTF-8 file.
The inline and file forms are mutually exclusive, and both are expanded before
hashing.

The DuckDB adapter binds these relations inside one transaction:

- `grv_source.<table.name>` contains that table's selected revision and
  partitions.
- `grv_target.<destination table>` contains its pre-write rows, or an empty
  typed relation if the destination does not exist.
- Local tables can be read by qualified name, for example `app.blocked_accounts`.
  Local views are supported only when their dependencies meet this execution
  contract.

#### Evaluation and commit

1. All queries are fully evaluated and staged before any destination changes.
   They see one fixed source selection, one local snapshot, and pre-write
   destinations. Results cannot depend on destination write order.
2. Then every target write, check, ownership and binding update, and the pull
   receipt commit together. An error on a second table rolls the entire
   transaction back.

Row counts in receipts mean completed selected or inserted rows; they do not
imply business uniqueness. The execution companion's _The pull algorithm_
defines the transactional implementation.

#### Append and replacement ownership

Append can attach to an ordinary existing table with the exact ordered output
schema, or create a missing table. Several append bindings may share such a
table. Append cannot write into replacement-owned tables or into session or
metadata tables.

Replacement creates and exclusively owns a stable replacement scope. The scope
is identified by the bound root and workspace, the dataset, the adapter, and
the destination namespace (`target.schema` for DuckDB). Mapping members and
source revision are not part of that identity.

- Declarations with the same scope update that binding.
- Independent replacement bindings for one dataset use different destination
  schemas.
- Replacement cannot silently adopt and clear an unowned application table.
- A mapping change clears obsolete owned replacement targets in the same
  transaction. It does not abandon old managed data.

#### Identity materializations and application imports

Complete identity replacement, with no SQL and no partition selector, produces
an identity materialization, which is an eligible GRV build input. SQL
replacement, append, and partition-limited replacement are application imports.
Their receipts record source selection and output contracts, without asserting
that destination rows represent a complete raw snapshot.

Two modes describe this classification:

- `scope_mode` is `complete` or `selected`.
- `transform_mode` is `identity` or `sql`.

Any table SQL or partition selector classifies the whole invocation accordingly.
Changing a scope between identity and application roles requires a new target
schema. `SELECT *` remains a SQL import. A new business attempt remains new
work.

### SQL execution boundary

This section states what user SQL may access and what the adapter does and does
not guarantee.

Declarations and database definitions are trusted operator-authored code. The
adapter:

- parses one query statement and rejects mutation and control statements;
- evaluates user queries with external access and extension autoload and
  install disabled; and
- constrains dependencies and functions using its supported engine's binding
  facilities and registered built-ins, including expansion of local views.

It does not promise a universal SQL safety classifier, and it does not sandbox
untrusted SQL. See
[DuckDB execution hardening](https://duckdb.org/docs/current/operations_manual/securing_duckdb/overview).

For local pulls and builds, verified source and private input relations are
materialized before restricted query evaluation. Only the displayed source,
target, and input bindings and supported local dependencies are accessible.
These are outside the user-query contract: reads of `_grv`, session internals,
engine secret and catalog functions, external scanners, and side-effecting
functions. SQL does not supply paths to core staging files. An unsupported
dependency fails before destination writes. S3-view identity refresh has no user
query and follows its separate reader path.

## Managed builds

An optional `build` block changes a push from an extraction into a
provenance-aware build. The default `build.execution` is `managed`.

A DuckDB managed build lists completed identity input tables with aliases, and
supplies one output query per table. Users first pull the required identity
inputs. The build runner uses that completed pull only to choose each input's
revision and contract. It then loads private input copies from the held,
verified GRV files of that revision, not from the tracking tables (execution
companion, _DuckDB build session preparation and context_).

```yaml
declaration_version: 1
kind: push
dataset: month_totals
adapter: duckdb
connection:
  database: ../../workspace.duckdb
build:
  inputs:
    - { table: raw.mt_month_stats, as: month_stats }
tables:
  - name: totals
    source:
      sql: |
        SELECT period, SUM(spend)::DECIMAL(38,4) AS total_spend, _period_
        FROM grv_input.month_stats
        GROUP BY period, _period_
    columns:
      - { name: period, type: date32 }
      - { name: total_spend, type: "decimal128(38,4)" }
      - { name: _period_, type: utf8 }
    partition_keys:
      - period
checks:
  - { table: totals, not_null: [period, total_spend, _period_] }
```

Run this with ordinary `grv push --decl ... --grv ...`. The runner:

1. fixes input revisions and confirms dependency holds;
2. binds immutable private inputs as `grv_input.<alias>`;
3. executes each output query;
4. verifies completion and schema;
5. captures outputs and publishes.

### Query bindings

Build queries read only:

- declared private inputs, as `grv_input.<alias>`; and
- when `build.self_input: true`, locally materialized prepared-base relations,
  as `grv_self.<output table>`.

They cannot read unlisted local working tables or other output queries. All
queries evaluate before output writes. Managed query results are complete output
relations and replace private output contents; they never append to
previous-state seeds. Self-input is the separate immutable read binding.

### Completion and provenance

The runner owns locking, renewal, cancellation, stopped-writer checks, and the
completion record. It never treats abandoned output tables as completion
(execution companion, _Managed runner execution_ and _Build completion record_).

Every derived output conservatively cites all confirmed external input
revisions. Self-input is the prepared target base, not a self-hold (execution
companion, _Schemas and provenance_). Source revision 0 and application imports,
including SQL imports, are not eligible external build inputs.
`build.code_fingerprints` optionally supplies external code identities.

### External builds

Advanced external engines use `build.execution: external`, output aliases in
`source.table`, and the explicit session and completion API in the execution
companion (_`grv session`_). Their drivers must meet its fencing and attestation
contract. Managed and external build capabilities are separate. Declaring a
capability requires implementing its lifecycle, not merely accepting its flag.

## Adapter architecture and extension contract

This section is for adapter authors. It defines how responsibilities split
between the CLI, the transfer core, and adapters, and what every adapter
registers and implements.

### Modules and responsibilities

| Module         | Owns                                                                                                                                                                         |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CLI            | Commands, YAML and reference loading, result rendering; no transport or destination SQL implementation                                                                       |
| Transfer core  | Capability checks; normalized plan and identity; source revision and file selection; Arrow contracts; capture writer; GRV holds, runs, and publication; outcome coordination |
| Adapter        | Connection and authentication; source dialect, encoding, and jobs; destination execution, transactions, and receipts; engine-specific locks; registered schemas and commands |
| Managed runner | Logical input and output lifecycle coordinated by core; engine execution and stopped-writer attestation supplied by its adapter                                              |
| GRV backend    | Immutable storage operations, conditional writes, validators, backend credentials                                                                                            |

One normalization path serves inline and file column contracts, all adapters,
retries, and advanced sessions (execution companion, _Normalized plans and
identity_). The authority split is strict:

- Adapter modules do not allocate GRV versions, edit manifests, or write
  `LATEST`.
- Core code does not branch on adapter names to parse SOQL, choose REST or Bulk,
  manage DuckDB catalogs, or operate a browser.

Shared helpers provide Arrow and Parquet validation and the common state and
cancellation APIs.

### Registration and validation

#### Registration

Every adapter registers:

- its name, package version, and interface version;
- its binding schema version;
- its capability descriptor;
- its validation-point schemas and result schemas; and
- its namespaced commands.

Duplicate names and incompatible versions fail. The CLI uses installed adapters.
Transfer execution does not install or download plugins. Built-ins register
through the same API and use the same binding shape.

#### Process and linked adapters

The lifecycle interface below is the v1 adapter contract. Installed adapters
always run as supervised processes speaking the
[adapter process protocol](grv-adapter-protocol-v1.md). The v1 built-ins
(DuckDB and Salesforce) may instead be linked into the CLI behind this same
interface, provided they:

- keep the authority split (no GRV coordination objects, tokens, or mutations);
  and
- pass the same logical conformance suites.

The process protocol then becomes necessary only when the first installed
third-party adapter ships.

#### Capabilities

Capabilities include `push`, `pull`, `managed_build`, `external_build`,
`source_consistency`, `resumable_extract`, supported `pull_write_modes`, and
command names. An additional pull adapter must declare transactional or
journaled completion and recovery before advertising support. The core does not
pretend that all destinations commit atomically.

#### Validation points

The validation points are:

- `connection`;
- `options`;
- table `source` for extraction, managed builds, and external builds;
- pull `target`;
- pull table `target`;
- pull table `select`;
- column `source`;
- build input relation;
- adapter result, command, and session details.

Schemas are closed at each implemented point, and are selected for the requested
direction and build mode. DuckDB extraction and build tuning is empty in v1.
Missing optional bindings receive adapter defaults and are validated again. An
adapter cannot add undeclared common fields. Its configuration syntax can evolve
under its registered schema version. Requests fix the resolved schema, package,
and interface versions before execution.

#### Validation order

1. Validate the YAML and common shape.
2. Load the registry. Reject unsupported direction, write, or build capabilities
   before login, GRV runs, or mutation.
3. Validate every implemented adapter point, expand defaults, bind connection
   identity, and resolve source and schema expectations. Metadata reads may
   occur only in this binding phase.

Invalid configurations never leave partial destination writes or GRV
allocations.

### Lifecycle interface

These are language-neutral obligations; the Rust SDK implements their process
protocol equivalents.
An optional method is required when the corresponding capability is advertised.

| Method               | Inputs and required result                                                                                                                                                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `validate_binding`   | Common declaration and adapter fragments → normalized config and defaults; pure validation, with no authentication or storage I/O                                                                                                                            |
| `bind_connection`    | Normalized config → non-secret handle and, when knowable offline, the stable system identity; aliases are resolved, and secrets remain private. In the process protocol this is split into `locate_connection`, `bind_connection`, and `authenticate` (§4.2) |
| `authenticate`       | Bound handle → resolved stable system identity, compared with the recorded identity on retry; invoked only for new execution or a pending acknowledgement that needs it                                                                                      |
| `extract`            | Fixed context and output contracts, plus persisted source checkpoint → named table batches and explicit table and source completion                                                                                                                          |
| `prepare_pull`       | Fixed revision, selected files, and contracts, plus target config → physical mappings, output contracts, and an ownership, write, and recovery plan                                                                                                          |
| `resolve_pull`       | Attempt and request identity → committed receipt, trustworthy not-committed, busy, or unknown; checked before reading source files                                                                                                                           |
| `apply_pull`         | Prepared plan and verified source provider → destination writes and durable receipt under the adapter's declared commit and recovery contract                                                                                                                |
| `prepare_build`      | Core-held logical input and base bindings, plus output contracts → private engine mappings and controlled invocation handle                                                                                                                                  |
| `execute_build`      | Prepared handle, plus declared queries or model integration → outputs, success or failure, stopped writers, and immutable completion facts                                                                                                                   |
| `inspect_connection` | Optional read-only connection state → registered inspection details, with no binding, repair, or renewal                                                                                                                                                     |
| `cancel`, `close`    | Signal cancellation, stop adapter work and wait for it, and release handles and locks; preserve uncertain commit evidence                                                                                                                                    |
| `after_publish`      | Confirmed GRV outcome → idempotent source acknowledgement or cursor update, if the adapter needs one                                                                                                                                                         |

#### Extraction obligations

Push streaming events identify the table and provide its schema, its batches,
and one explicit `table_complete` (including for zero rows), followed by
`source_complete`. The core rejects:

- missing or duplicate completion;
- batches after completion;
- unexpected outputs;
- schema mismatch; and
- partial success.

Batching does not prove completeness. The core stages Parquet and seals the
capture only after all declared tables and all writers are complete (execution
companion, _Extraction session preparation and capture_). Resumption uses
persisted source identity. Fragments from unrelated snapshots cannot be
combined.

#### Pull obligations

The core resolves committed GRV revision entries and verifies selected files.
The source provider can stage or materialize them without adapters guessing
paths. Pull adapters own destination atomicity and durable receipts.

`resolve_pull` cannot report not-committed merely because current destination
rows differ. It must fence the former writer and establish trustworthy receipt
or journal state. When a committed receipt already proves success, no source
download or re-evaluation occurs. Lost metadata produces an explicit
`OUTCOME_UNKNOWN` or `PROTOCOL_FAILURE`, never an assumed outcome. Receipt lookup and ambiguous-commit resolution follow the
execution companion, _The pull algorithm_.

#### State ownership and post-publication hooks

Core contexts own attempt and request identities, fixed adapter versions,
capture integrity, GRV holds, runs, and allocations, and publication outcomes.
Adapter state owns source job and authentication state and destination commit
evidence. Both are durable before advancing a phase. The core supervises
cancellation and renewal. The adapter implements engine locks, execution
handles, and writer-stop evidence. Loss of publication ownership prevents new
allocations and publication, even if an engine cannot immediately cancel.

Source cursors advance only after confirmed publication. Hooks must be
idempotent and monotone; they cannot roll back a later acknowledged cursor. An
acknowledgement failure preserves the committed outcome and a durable pending
acknowledgement. A same-attempt retry may retry that hook using its fixed state,
without reacquiring data or publishing again (execution companion, _Extraction
session preparation and capture_). Cleanup never discards evidence for an
unresolved commit.

#### Managed runner

A managed runner operates on logical input and output names and explicit
completion. DuckDB private schemas and OS locks belong to DuckDB's
implementation. Future engine adapters use their own mappings without changing
the publication core. An external integration that bypasses the runner remains
responsible for the advanced lifecycle obligations. Declaring inputs alone does
not establish provenance.

### Salesforce binding and additional adapters

#### Salesforce

Salesforce uses `{org: <alias-or-username>, api_version?: <version>}` and the
existing `sf` authentication store. `grv adapter salesforce login`, `status`,
and `describe` delegate login, session, and source inspection. Aliases resolve
to the actual org ID for a fixed attempt. Credentials are not copied into
declarations.

- The default API version is `v66.0`. The default `options.transport: auto`
  uses deterministic REST-first selection among proven complete, lossless
  transports. Decimal integrity is a requirement of either path. Preserve an
  explicitly authored alias and `transport: auto`; record canonical connection
  coordinates and resolved transport separately.
- Empty `job_ids` are permitted when REST returns no locator or Bulk creation
  remains unresolved. Persist ambiguous Bulk creation and return
  `OUTCOME_UNKNOWN` without repeating its POST. Private REST bootstrap rows
  may establish acquisition evidence, but no rows or table completion are
  emitted before durable checkpoint acknowledgement.
- Advanced `rest` or `bulk` preferences fail if they cannot meet the declared
  semantics and types.
- `options.all_rows` defaults to false. When true, the extraction uses
  `queryAll` semantics, like `sf data query --all-rows`: records that are
  deleted (in the Recycle Bin, `IsDeleted = true`) or archived are included if
  they match the filter. When false, they are excluded. The value is part of
  the fixed request.
- The adapter fetches all pages and results, verifies reported counts where
  available, and rejects successful-but-truncated CLI exports.

Salesforce consistency is a reported capture window, not a cross-object
transaction snapshot. V1 re-evaluates the whole filter; there is no hidden
watermark or CDC.

#### Additional adapters

A website adapter can register `source: {url, fields, pagination, ...}`, column
selectors, and login and browser-session commands under `grv adapter <name>`.
It uses the same table contracts and completion boundary. Failed login, changed
page structure, or unfinished pagination cannot masquerade as an empty capture.
Website extraction implementation is an extension example, not a built-in
promise.

### Adapter conformance

Every adapter package runs the common contract suite plus its
capability-specific suite. The concrete scenarios are in the execution
companion (_Required conformance scenarios_) and the adapter process protocol
(§9).

| Suite                  | Covers                                                                                                                                                                                                                                                                                                                                           |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Common (every adapter) | Closed configuration validation; unsupported direction rejected before side effects; stable identity and version on retries; schema, type, and check failure; zero-row completion; missing pages, tables, or completion; restart and cancellation; secret redaction; no cursor advancement before publication; confirmed versus unknown outcomes |
| Pull adapters          | Multi-output failure; receipt replay without source reads; re-evaluation on a new attempt; ownership and schema conflicts; ambiguous commit recovery                                                                                                                                                                                             |
| Build adapters         | Fixed inputs and holds; private mappings; cancelled or lost ownership; stopped writers; completion integrity; source provenance                                                                                                                                                                                                                  |
| DuckDB                 | SQL bindings; one-statement files; partition acquisition scope; pre-write target snapshots; transactional rollback of data and receipt                                                                                                                                                                                                           |
| Salesforce             | Complete pagination and counts; exact decimals                                                                                                                                                                                                                                                                                                   |

## Inspection, administration, and advanced integrations

These commands inspect a GRV root, manage retention, and recover interrupted
work. The execution companion defines their behavior.

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
without transferring data, using registered `inspect_connection` support.
Adapters without that support reject this optional inspection. Core inspection
needs neither a declaration nor an adapter login.

Read-only commands never establish bindings, create pins, renew leases, or
repair state. Listing objects does not prove their commitment (execution
companion, _Common command behavior_). Explicit pins protect retained revisions;
merely choosing a revision does not. GC and recovery use the complete existing
GRV protocols and report partial effects (execution companion, _`grv gc`_ and
_`grv recover`_).

Advanced root initialization accepts clock, lease, and grace parameters defined
in the execution companion (_`grv init`_). External engines use
`grv session prepare/show/renew/abort` and
`grv push --session ... --build-result ...`. Contexts are protected consumer
state; inspection redacts tokens. These are integration tools beneath the
managed path.

### Results and exit statuses

Results conform to [the output schema](grv-client-v1-command-output.schema.json).
Every adapter uses the same common result fields plus registered adapter details:
`adapter_state` in status, `adapter_context` for sessions, and `adapter_result`
for transfers. Physical mapping identifiers are adapter-owned. DuckDB transfer
details include write, transform, and scope modes and resolved source
partitions. Version, revision, and byte values use decimal strings in JSON
results.

In summary, exit statuses are:

| Exit | Meaning                                                 |
| ---- | ------------------------------------------------------- |
| 0    | Success                                                 |
| 2    | Invalid request                                         |
| 3    | Conflict, busy, or ownership loss                       |
| 4    | Unavailable or not found                                |
| 5    | Integrity or protocol failure                           |
| 6    | Adapter, backend, or engine failure, or unknown outcome |

Error codes and full command and result contracts are defined in the execution
companion (_Common command behavior_ and _JSON result contracts_).

## Scope and later additions

V1 supports:

- full filtered extractions and lossless typed mappings;
- DuckDB identity replacement, and SQL imports with append or replacement;
- managed and external DuckDB builds with fixed provenance;
- one dataset and one root per transfer;
- explicit revision retention, inspection, GC, and recovery;
- installed adapters and adapter commands.

Native DuckDB database access is serialized across processes.

These need later contracts: scheduling, multi-dataset atomicity, automatic
retention, cross-root derivation, CDC and watermarks, inferred keys, generic
merge or upsert, concurrent or server DuckDB, provenance for arbitrary SQL
application imports, and narrower output dependencies. V1 does not orchestrate
arbitrary external model workflows; it manages the lifecycle of declared SQL
builds and advanced attested integrations.
