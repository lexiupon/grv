---
name: grv-cli
description: >-
  Use when working with GRV (Golden Record Vault) or the `grv` command: writing
  push/pull YAML declarations, moving data between DuckDB or Salesforce and a
  versioned Parquet store, inspecting datasets and revisions (ls, show, log,
  diff, verify, status), pinning revisions, running gc or recover, or
  interpreting grv errors and exit codes.
---

# Using the grv CLI

GRV keeps **datasets** in a versioned Parquet store (a local directory,
`s3://bucket/prefix` or `gs://bucket/prefix`). Data moves in and out through
**adapters** (`duckdb`, `salesforce`), described by YAML **declarations**:

- `push`: source system → GRV. It succeeds when a new **revision** is committed.
- `pull`: GRV → destination. It succeeds when the destination write completes.

Each revision is an immutable snapshot of a dataset. Older revisions stay
readable until garbage collection, which never removes **pinned** revisions.

## Core workflow

```console
grv init --grv ./store                               # once per store
grv adapter list                                     # check adapters are installed
grv push --decl push.yml --grv ./store               # creates a revision
grv --json ls --grv ./store                          # datasets
grv --json show <dataset> --grv ./store              # latest revision detail
grv --json log <dataset> --grv ./store --limit 10    # revision history
grv pull --decl pull.yml --grv ./store [--revision N|latest]
```

## Rules for agents

1. **Use `--json` when you will read the output.** It must be the _first_
   argument (`grv --json ls ...`). Stdout is then one JSON envelope
   `{ok, exit_status, command, root, result, errors}`; progress goes to stderr.
   Check `ok` and `errors[].code`; do not parse the human output.
2. **Validate declarations against the examples** in
   [references/declarations.md](references/declarations.md) before running
   them. Types use exact spellings such as `utf8`, `int64`, `date32`,
   `"decimal128(38,6)"` and `"timestamp(us,UTC)"`. GRV never casts silently.
3. **Relative paths in a declaration** (database, SQL files, column files)
   resolve from the declaration file's directory, not the shell's directory.
4. **Retries:** running a declaration again starts _new_ work. To retry or
   inspect the _same_ transfer, pass the attempt UUID the CLI printed:
   `--attempt <uuid>`. Never invent an attempt UUID.
5. **Destructive operations need confirmation from the user:**
   - `grv gc` is a dry run by default. Show the dry-run result and ask before
     running `grv gc <dataset> --grv <root> --apply`.
   - `pull` with `write: replace` (the default) replaces the destination
     table. Confirm the target before running it on real data.
   - `grv unpin` removes protection from a revision.
6. **Protect revisions people rely on:**
   `grv pin <dataset> --grv <root> --revision N --reason "<why>"`. Choosing a
   revision in a pull does not protect it.
7. **Do not work around protected-directory errors** by loosening
   permissions broadly or adding symlinks. Explain the requirement to the user
   (see [references/troubleshooting.md](references/troubleshooting.md)).
8. **Never put credentials in declarations.** Salesforce uses the existing
   local CLI login (`connection: {org: <alias>}`); cloud stores use the
   standard AWS/GCP environment.

## References

- [references/commands.md](references/commands.md): every command, its flags
  and exit codes.
- [references/declarations.md](references/declarations.md): declaration
  format, types, partitions, checks, write modes, with full examples.
- [references/troubleshooting.md](references/troubleshooting.md): common
  errors and what to do about them.
