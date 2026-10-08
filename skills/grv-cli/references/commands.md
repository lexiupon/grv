# grv command reference

Put the global `--json` flag first: `grv --json <command> ...`.
`--grv <root>` accepts a local path, `s3://bucket/prefix` or `gs://bucket/prefix`.

## Data movement

| Command                                                        | Purpose                                                                                       |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `grv push --decl <yaml> --grv <root>`                          | Extract from the adapter source (or run a managed build) and commit a new revision            |
| `grv pull --decl <yaml> --grv <root> [--revision <N\|latest>]` | Write a committed revision to the adapter destination; `--revision` overrides the declaration |
| `... --attempt <uuid>`                                         | Retry or inspect an earlier transfer instead of starting new work                             |
| `... --state <dir>`                                            | Use a non-default local state directory (needed to retry on ephemeral machines)               |

## Inspection (read-only)

| Command                                                        | Purpose                                                      |
| -------------------------------------------------------------- | ------------------------------------------------------------ |
| `grv ls --grv <root> [<dataset>] [--table <t>] [--revision N]` | List datasets, or the tables/partitions of one dataset       |
| `grv show <dataset> --grv <root> [--revision N] [--retention]` | Revision detail; `--retention` adds pins, holds and GC state |
| `grv status <dataset> --grv <root> [--decl <yaml>]`            | Dataset status; with `--decl`, also checks the adapter side  |
| `grv log <dataset> --grv <root> [--limit N]`                   | Revision history with run provenance                         |
| `grv diff <dataset> --grv <root> --from N --to <N\|latest>`    | Table and partition changes between revisions                |
| `grv verify <dataset> --grv <root> [--revision N] [--full]`    | Check integrity; `--full` hashes every data file             |

Read-only commands never create pins, take leases or repair anything.

## Store administration

| Command                                                                    | Purpose                                                                                        |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `grv init --grv <root>`                                                    | Create a store (advanced: `--max-clock-skew`, `--max-lease-ttl`, `--pending-grace` in seconds) |
| `grv pin <dataset> --grv <root> --revision N --reason <text> [--pin <id>]` | Protect a revision from GC; prints the pin id                                                  |
| `grv unpin <dataset> --grv <root> --revision N --pin <id>`                 | Release one pin                                                                                |
| `grv gc <dataset> --grv <root> [--dry-run \| --apply]`                     | Collect unprotected old versions; **dry run unless `--apply`**                                 |
| `grv recover <dataset> --grv <root> [--run <run-id>] [--dry-run]`          | Finish or roll back interrupted runs after their lease expires                                 |

## Adapters

| Command                                           | Purpose                                                                      |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| `grv adapter list`                                | Search roots and installed adapters (no adapter is started)                  |
| `grv adapter install <path\|tarball> [--replace]` | Install an adapter package                                                   |
| `grv adapter <name> capabilities`                 | Show what the adapter supports                                               |
| `grv adapter <name> <command> [args]`             | Adapter-specific commands, e.g. `grv adapter salesforce login --org <alias>` |

## Agent skills

| Command                                                                                             | Purpose                                     |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| `grv skills list`                                                                                   | Bundled skills and where they are installed |
| `grv skills show <name>`                                                                            | Print a skill's `SKILL.md`                  |
| `grv skills install [<name>...] [--agent claude\|agents] [-g] [--dir <path>] [--force] [--dry-run]` | Install skills for an AI agent              |
| `grv skills uninstall <name> [--agent ...] [-g] [--dir <path>]`                                     | Remove an installed skill                   |

## Exit statuses and error codes

| Exit | Meaning                                                 | Error codes                                                                                                 |
| ---- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| 0    | Success                                                 |                                                                                                             |
| 2    | Invalid request: fix the input                          | `INVALID_ARGUMENT`, `INVALID_DECLARATION`, `REQUEST_MISMATCH`, `BUILD_INCOMPLETE`, `UNSUPPORTED_CAPABILITY` |
| 3    | Conflict or busy: usually retry later                   | `ENGINE_BUSY`, `STATE_CONFLICT`, `OWNERSHIP_LOST`                                                           |
| 4    | Missing or unavailable                                  | `NOT_FOUND`, `UNAVAILABLE`                                                                                  |
| 5    | Integrity or protocol failure: stop, report to the user | `INTEGRITY_FAILURE`, `PROTOCOL_FAILURE`                                                                     |
| 6    | Adapter, backend or engine failure, or unknown outcome  | `BACKEND_FAILURE`, `ENGINE_FAILURE`, `OUTCOME_UNKNOWN`, `EXTRACTION_INCOMPLETE`, `ADAPTER_FAILURE`          |

After `OUTCOME_UNKNOWN`, retry with the same `--attempt <uuid>` rather than
starting new work; GRV then either returns the recorded result or finishes it.

## Environment variables

| Variable                             | Purpose                                                    |
| ------------------------------------ | ---------------------------------------------------------- |
| `GRV_ADAPTERS_DIR`                   | Adapter discovery root                                     |
| `GRV_S3_UPLOAD_MODE=single-put`      | S3 mode for writers without multipart permissions          |
| `GRV_S3_SINGLE_PUT_MAX_BYTES`        | Per-object buffer limit in single-put mode (default 1 GiB) |
| `GRV_CLOUD_READ_TIMEOUT_SECONDS`     | Cloud download timeout, 1–3600 (default 600)               |
| `GRV_GCS_ACCOUNT`, `GRV_GCS_PROJECT` | Explicit GCS account/project                               |
| `AWS_PROFILE`, `AWS_REGION`          | Standard AWS settings for S3                               |

The full contract is in `spec/grv-client-v1.md` and
`spec/grv-client-v1-execution.md` in the GRV repository.
