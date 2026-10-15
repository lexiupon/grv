# Superseded dedicated GCS native-owner implementation

Historical, nonshipping source retained after the approved return to in-process
DuckDB execution. See `spec/gcs-in-process-execution-decision.md` and the original
isolated-owner/held-lifecycle checkpoints for evidence and limitations.

These files were moved from the adapter source tree, not rewritten into current
in-process tests. They are not Cargo targets, exported APIs, runtime dependencies
or package contents. Their relative imports and old guard-revision assumptions
belong to the historical implementation; this directory is not a standalone
buildable or supported helper. Do not use a previously built helper as part of
the current adapter/native pair.

The helper experiment qualified useful IPC/descriptor/process primitives but
introduced additional supervision, ownership, IPC, packaging and target costs.
Its stronger in-flight termination design was superseded explicitly, not marked
as a passed hard-deadline test. Current GCS execution is within the adapter, with
cooperative cancellation and documented stock-transport/engine-memory limits.
No prototype allocator policy is introduced into production by this transition.
