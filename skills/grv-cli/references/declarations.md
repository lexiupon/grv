# Declarations

A declaration is one YAML file describing one push or one pull of one
dataset. Unknown fields fail validation, so do not add fields that are not
documented here or in the GRV spec.

## Envelope

| Field                 | Required | Meaning                                                        |
| --------------------- | -------- | -------------------------------------------------------------- |
| `declaration_version` | yes      | Always `1`                                                     |
| `kind`                | yes      | `push` or `pull`                                               |
| `dataset`             | yes      | Dataset name: `^[a-z0-9][a-z0-9_-]{0,63}$`                     |
| `adapter`             | yes      | `duckdb`, `salesforce`, or another installed adapter           |
| `connection`          | yes      | DuckDB: `{database: <path>}`. Salesforce: `{org: <cli alias>}` |
| `tables`              | yes      | Tables to push (outputs) or pull (sources)                     |
| `checks`              | no       | e.g. `- { table: cases, not_null: [id, amount] }`              |
| `options`             | no       | Advanced adapter tuning; normally omitted                      |

Table and column names follow the same pattern as dataset names. Paths in
`connection.database`, `select.file` and `columns.file` resolve from the
declaration file's directory. DuckDB databases must be local files (not
`:memory:` or a server).

## Column types

Use these spellings exactly. Quote any type that contains parentheses.

| Type                                                    | Meaning                                          |
| ------------------------------------------------------- | ------------------------------------------------ |
| `bool`, `int64`, `double`, `utf8`, `binary`             | boolean, 64-bit int, 64-bit float, string, bytes |
| `date32`                                                | date                                             |
| `"decimal128(p,s)"`                                     | decimal, precision 1–38, scale 0–p               |
| `"timestamp(ms)"`, `"timestamp(us)"`, `"timestamp(ns)"` | timestamp without time zone                      |
| `"timestamp(ms,UTC)"`, `"timestamp(us,UTC)"`            | UTC timestamp                                    |

Not supported: `int8/16/32`, unsigned ints, `float32`, `json`, `uuid`,
`time`, nested types, and nanosecond UTC timestamps. Sources may only be
**widened exactly** into the declared type (e.g. DuckDB `INTEGER` → `int64`,
`DECIMAL(10,2)` → `decimal128(38,6)`). Anything else fails; nothing is
truncated or cast.

## Push: extraction

Each push table has a `source` and an ordered `columns` contract. `source` on a
column maps it from a differently named source field (identifiers only, no
expressions). `filter` is one row predicate in the source's language.

```yaml
declaration_version: 1
kind: push
dataset: credit_notes
adapter: duckdb
connection:
  database: ../data/source.duckdb
tables:
  - name: cases
    source:
      table: billing.crm_cases # Salesforce: { object: Case, filter: <SOQL> }
      filter: case_type = 'Credit Note' AND amount > 0
    columns:
      - { name: id, source: case_id, type: utf8 }
      - { name: credit_amount, source: amount, type: "decimal128(38,6)" }
      - { name: modified_at, type: "timestamp(us,UTC)" }
checks:
  - { table: cases, not_null: [id, credit_amount] }
```

Facts to remember:

- A push is a **complete snapshot** of each table, not a delta. Rows that no
  longer match the filter disappear in the new revision.
- If the dataset already has a table this declaration does not list, the push
  fails with `STATE_CONFLICT`. Remove a table on purpose with
  `selection: { drop: [{ table: <name> }] }`, or use a separate dataset.
- Unchanged tables reuse their previous version (`selection.policy: changed`,
  the default).
- Filters cannot join other tables. For joins and aggregation, use a managed
  build (below).

### Partitions

Declare `partition_keys: [month]` on the table and include a non-null `utf8`
column named `_month_`. Either supply canonical values (`2026`, `2026-09`,
`2026-09-15`) or derive them:

```yaml
- { name: _month_, type: utf8, derive: { from: posted_date, format: month } }
```

`format` is `year`, `month` or `day`; `from` must be a `date32` or timestamp
column.

## Push: managed build (DuckDB)

A `build` block turns a push into SQL over **GRV datasets already pulled**
into the DuckDB workspace. Inputs are available as `grv_input.<alias>`.

```yaml
declaration_version: 1
kind: push
dataset: month_totals
adapter: duckdb
connection:
  database: ../workspace.duckdb
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
    partition_keys: [period]
checks:
  - { table: totals, not_null: [period, total_spend, _period_] }
```

The revision records which input revisions it was derived from.

## Pull

```yaml
declaration_version: 1
kind: pull
dataset: credit_notes
adapter: duckdb
connection:
  database: ../app.duckdb
revision: latest # or a revision number; CLI --revision overrides this
write: replace # default; or append
target:
  schema: crm
tables:
  - name: cases # the GRV table to read
    target:
      table: credit_notes # defaults to the GRV table name
checks:
  - { table: cases, not_null: [id, credit_amount] }
```

- `write: replace` replaces the destination table. `write: append` inserts
  rows and keeps existing ones. Neither mode invents keys or upserts; write
  SQL that skips rows already in the target.
- To read only some partitions of a partitioned table, add
  `partitions: [{ month: "2026-09" }]` to the table (omit it for all
  partitions; `[]` selects none). Selectors on unpartitioned tables fail.
- Without `select`, the GRV table is copied as is; optional `columns` is an
  exact assertion, not a projection.
- With `select`, `columns` (the output contract) is required:

```yaml
select:
  sql: |
    SELECT s.id AS source_case_id, s.credit_amount AS amount
    FROM grv_source.cases AS s
    WHERE NOT EXISTS (
      SELECT 1 FROM grv_target.credit_notes AS t
      WHERE t.source_case_id = s.id)
columns:
  - { name: source_case_id, type: utf8 }
  - { name: amount, type: "decimal128(38,6)" }
```

The query is one read-only `SELECT`/`WITH`. It can read
`grv_source.<grv table>`, `grv_target.<destination table>` (the rows before
the write) and qualified local tables such as `app.blocked_accounts`. Use
`select: { file: ./query.sql }` to keep SQL in a file.

- All tables in one pull commit in one transaction, or none do.
- To keep a historical copy next to the latest one, use a second declaration
  with a different `target.schema` and a fixed `revision`.

More complete examples are in `spec/examples/` in the GRV repository.
