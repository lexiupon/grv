# GRV (Golden Record Vault)

GRV keeps your important datasets in a **versioned, auditable Parquet store**
and moves them in and out of other systems with short YAML declarations.

- **Every push is a revision.** Past states stay readable, can be pinned
  against cleanup, and record where they came from.
- **One store, three backends.** The same layout works on a local directory,
  S3 or GCS.
- **Adapters do the I/O.** DuckDB (push and pull) and Salesforce (push) ship
  today. Transfers are transactional and safe to retry.

> **Status:** pre-release (0.1.0, macOS ARM64 first). See [Status](#status).

## A quick example

Snapshot credit notes from a DuckDB database into GRV. A declaration says
what to read and the exact column contract:

```yaml
# credit-notes-push.yml
declaration_version: 1
kind: push
dataset: credit_notes
adapter: duckdb
connection:
  database: ./source.duckdb
tables:
  - name: cases
    source:
      table: billing.crm_cases
      filter: case_type = 'Credit Note' AND amount > 0
    columns:
      - { name: id, source: case_id, type: utf8 }
      - { name: credit_amount, source: amount, type: "decimal128(38,6)" }
      - { name: modified_at, type: "timestamp(us,UTC)" }
checks:
  - { table: cases, not_null: [id, credit_amount] }
```

```console
grv init --grv ./store                                  # create a store
grv push --decl credit-notes-push.yml --grv ./store     # → revision 1
# ...source data changes...
grv push --decl credit-notes-push.yml --grv ./store     # → revision 2
grv log credit_notes --grv ./store                      # history and provenance
grv diff credit_notes --grv ./store --from 1 --to latest
grv pin credit_notes --grv ./store --revision 1 --reason "Q3 audit"
grv pull --decl credit-notes-pull.yml --grv ./store --revision 1   # write rev 1 out (see pull examples below)
```

Each push stores a complete snapshot. Unchanged tables reuse their previous
version, and nothing is overwritten in place. A pull writes a chosen revision
into a destination such as a DuckDB schema, either replacing it or appending
to it, in a single transaction.

## Install

### Homebrew

```console
brew install lexiupon/tap/grv
grv --version
```

On Apple silicon Macs this installs the `grv` CLI, the DuckDB and Salesforce
adapters that `push` and `pull` use, and the AI agent skill. The CLI builds
from source; the adapters (with DuckDB's pinned native library and signed
extensions) come prebuilt from the GitHub release. `grv adapter list` shows
them. On other platforms Homebrew installs the CLI only; build adapters with
[docs/building.md](docs/building.md).

Adapters you install yourself (`grv adapter install`) go into
`~/.config/grv/adapters` and take precedence over the Homebrew ones.

### From source

To build the CLI and adapters yourself, see
[docs/building.md](docs/building.md).

## Use with AI agents

`grv` ships an [agent skill](skills/grv-cli/SKILL.md) that teaches coding
agents (Claude Code and any agent that reads `.agents/skills`) how to write
declarations and use the CLI safely:

```console
grv skills install -g                    # ~/.claude/skills/grv-cli
grv skills install --agent agents        # ./.agents/skills/grv-cli
grv skills install --dir <path>          # any other agent's skills directory
grv skills list                          # what is installed, and whether it is current
```

The skill is built into the binary, so it always matches the installed
version. `grv skills install` never overwrites a copy you edited unless you
pass `--force`. The skill uses the standard `skills/<name>/SKILL.md` layout, so
repository-based installers such as `npx skills add` can also pick it up:
`npx skills add lexiupon/grv --skill grv-cli`.

## How it works

- **Dataset**: a set of tables, versioned together. Tables may be
  partitioned (for example by month).
- **Revision**: a complete, numbered snapshot of a dataset. Revisions only
  advance through an explicit publish step. Two publications that change the
  same table partition are detected, never silently merged.
- **Run and provenance**: every version records the run that produced it and
  the input revisions it was built from. All of this can be audited from the
  store alone.
- **Pin**: explicit protection for a revision. `grv gc` never removes
  pinned revisions. Pulling a revision does not pin it.
- **Attempt**: one transfer request, identified by a UUID. Retrying with
  `--attempt <uuid>` resumes or replays that request and never starts
  different work.
- **Adapter**: a separate process that talks to one kind of system. The CLI
  supervises it and owns all writes to the store.
- **Protected directory**: a path whose every parent is owned by you or
  root, is not group- or world-writable, and contains no symlinks. Adapters
  are only installed and loaded from such directories.

Full definitions are in the [specification](spec/README.md).

## Capabilities

### Push: from a source into GRV

- **Extraction**: a filtered, complete snapshot of DuckDB tables or
  Salesforce objects, with column renaming and exact type widening. Nothing
  is cast or truncated silently.
  Examples: [DuckDB](spec/examples/duckdb-push-filter.yml),
  [Salesforce](spec/examples/salesforce-push-filter-mapping.yml).
- **Managed builds**: SQL over GRV datasets that were pulled into DuckDB.
  The new revision records exactly which input revisions it was derived from
  ([example](spec/examples/duckdb-build-push.yml)).
- **Partitions**: declared partition keys, given directly or derived from a
  date column (`year`, `month`, `day`).
- **Checks**: `not_null` checks run before anything is published.

### Pull: from GRV into a destination

- **Replace or append** a DuckDB table, from `latest` or a fixed revision
  ([replace](spec/examples/duckdb-pull-replace.yml),
  [historical copy](spec/examples/duckdb-pull-replace-historical.yml)).
- **SQL transforms** that can read the selected source, the destination's
  current rows and local tables, for example to append only new records
  ([example](spec/examples/duckdb-pull-append-sql.yml)).
- **Partition selection**: pull only the partitions you name
  ([example](spec/examples/duckdb-pull-partitioned-append-sql.yml)).
- All tables in one pull commit together, or not at all.

### Inspect, retain and recover

| Command               | Purpose                                                    |
| --------------------- | ---------------------------------------------------------- |
| `grv ls` / `show`     | Datasets, tables, partitions and revision detail           |
| `grv log` / `diff`    | Revision history and changes between revisions             |
| `grv status`          | Dataset state, optionally checked against a declaration    |
| `grv verify [--full]` | Integrity check, optionally hashing every data file        |
| `grv pin` / `unpin`   | Protect revisions from garbage collection                  |
| `grv gc [--apply]`    | Remove unprotected old versions (dry run unless `--apply`) |
| `grv recover`         | Finish or roll back interrupted runs                       |
| `grv adapter ...`     | List, install and query adapters                           |
| `grv skills ...`      | Install the AI agent skill                                 |

Every command accepts `--json` as its first argument. Stdout then carries a
versioned result envelope
([schema](spec/grv-client-v1-command-output.schema.json)) and progress goes
to stderr. Exit statuses: `0` success, `2` invalid request, `3` conflict or
busy, `4` not found or unavailable, `5` integrity or protocol failure, `6`
adapter, backend or engine failure, or unknown outcome.

The complete command reference is in the
[client v1 spec](spec/grv-client-v1.md) and its
[execution companion](spec/grv-client-v1-execution.md).

### Storage backends

| Backend          | Root                 | Notes                                                             |
| ---------------- | -------------------- | ----------------------------------------------------------------- |
| Local filesystem | a path               | Reference backend                                                 |
| S3               | `s3://bucket/prefix` | Multipart by default; a limited-permission single-PUT mode exists |
| GCS              | `gs://bucket/prefix` | Full-object reads; GCS views are not yet advertised               |

Settings for cloud storage (single-PUT mode, buffer limits, timeouts and
credentials) are in [docs/cloud-storage.md](docs/cloud-storage.md).

### Environment variables

| Variable                                           | Purpose                                                   |
| -------------------------------------------------- | --------------------------------------------------------- |
| `GRV_ADAPTERS_DIR`                                 | Adapter discovery root (default `~/.config/grv/adapters`) |
| `GRV_S3_UPLOAD_MODE`                               | `single-put` for writers without multipart permissions    |
| `GRV_S3_SINGLE_PUT_MAX_BYTES`                      | Single-PUT buffer limit (default 1 GiB)                   |
| `GRV_CLOUD_READ_TIMEOUT_SECONDS`                   | Cloud download timeout, 1–3600 (default 600)              |
| `GRV_GCS_ACCOUNT`, `GRV_GCS_PROJECT`               | Explicit GCS account and project                          |
| `AWS_PROFILE`, `AWS_REGION` / `AWS_DEFAULT_REGION` | Standard AWS settings for S3                              |

## Status

GRV 0.1.0 is a scoped first release for macOS ARM64 (macOS 26.0 or newer).
Full client v1 delivery is still in progress: bundles record
`complete_client_v1: false`, and adapters only advertise capabilities whose
conformance gates pass. Other platforms are built in CI but not yet
qualified. Details, gates and known limits are in
[spec/release-validation/STATUS.md](spec/release-validation/STATUS.md).

## Contributing

See [docs/development.md](docs/development.md) for the architecture, crate
layout, toolchain and tests.

## License

[Apache License 2.0](LICENSE).
