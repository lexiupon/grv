# GCS source-buffer ownership follow-up

Status: scoped source inspection/targeted regression; incorporated into agreed
GCS memory acceptance in [completion checkpoint](gcs-memory-qualification-completion.md).
Not binary-to-source or universal owner/RSS proof. No cloud execution,
allocator quota, helper owner or native-library rebuild in this stage.

## What the scoped reviews established

`adf5a93f-6948-4f1` inspected metadata-result closure: `GCSMetadata` holds its
SendQuery result locally, FetchRaw chunks are loop-local, and partial/refused
streams interrupt/fetch/Close under live scanner admission with a positive
QueryEnd witness. This does not itself prove destruction of every HTTPFS handle.
Collector64KiB is soft;4MiB adoption check occurs after chunk allocation.

`77d0e021-27c0-4dd` inspected DuckDB1.5.6 ownership:

- `extension/parquet/include/parquet_reader.hpp:255`,
  `extension/parquet/parquet_reader.cpp:871–875,956–957`: ParquetReader owns a
  unique CachingFileHandle; member destruction releases it.
- `src/include/duckdb/storage/caching_file_system.hpp:100–103`: underlying
  FileHandle is unique, separately from shared cached blocks.
- `src/include/duckdb/common/multi_file/multi_file_states.hpp:77–88,190`:
  initial/union readers and local scan readers hold references. Automatic
  multi-file statement caching is unsupported.
- `src/include/duckdb/common/multi_file/multi_file_function.hpp:513–516,447–448`:
  bind-time initial reader moves into execution; finished global readers become
  weak and the strong reference clears.
- `extension/parquet/include/parquet_reader.hpp:65–67`,
  `extension/parquet/parquet_reader.cpp:1299–1321`: local scan also owns a second
  file handle. Bind-reader destruction alone is not sufficient.
- `src/main/client_context.cpp:80–82,265,304–310`: active query owns prepared data/
  executor and is destroyed before QueryEnd callbacks. Independently retained
  PreparedStatementData may still own an unexecuted physical plan.
- `src/common/multi_file/multi_file_reader.cpp:55–70`: copied bind data does not
  copy initial/union readers; view catalog holds parsed SelectStatement rather
  than a live reader (`view_catalog_entry.hpp:42`).

HTTPFS review source `httpfs.cpp:555–576,614–618` has handle-local request stats
and adaptive read-buffer growth. Neither reviewer proved pinned signed binary
correspondence or actual surviving owners in GRV. Their searches are bounded,
not an exhaustive negative proof. Shared buffer-manager cached blocks must not
be conflated with these private HTTP-handle allocations.

## Actual GRV export ownership boundary

Parent inspected `BuildStore::start_export` in `src/build.rs`: after exact durable
accepted-output validation, it invokes `NativeEngine::close_invocation` before
`acquire_build_outputs` and subsequent `fetch_export`. Native `pull.inc` destroys
old Owner.engine synchronously on its owner thread, then opens a fresh Engine.
The interrupt object keeps a non-owning Engine pointer that is nulled under its
mutex during destruction; Rust retains the workspace lock. `Engine` has no
persistent PreparedStatement field. Preparations in stage_build_query/pull_query
are function-local and metadata SendQuery results are locally RAII-owned.

Thus this actual path has a stronger potential ownership boundary than merely
QueryEnd or clearing scanner authority. It must not be sold as rollback/immutable
outcome proof, hard deadline, zeroization or universal handle-cache/RSS proof.
Focused independent source review `435b1a3a-046f-4cb` completed at its tool/turn
bound, reporting no concrete defect in the inspected edges. It confirms the
export destruction/reopen barrier for Engine-owned state and that failure marks
the store broken before acquisition. No GRV prepared-owner escape was shown;
metadata results return bytes/status, and user build/pull SQL must be exactly one
SELECT before preparation. Internal metadata admits one generated statement,
not necessarily SELECT: the reviewer did not finish inspecting every internal
caller/guard and does not prove arbitrary independently escaped owners absent.
Signed HTTPFS source/binary correspondence remains outside that review.

The same review inspected the final SDK cancellation-refusal ordering: admission
precedes successful-result caches; refusal sets pending_failure, sends Error,
then returns Err before either cancellation acknowledgement, followed by
stop_and_wait/socket shutdown/reader join. This preserves recovery-required
known outcome, not a no-effects claim. The regression test was inspected but
not independently rerun. This scoped review is not whole-memory approval.

## Small implementation correction and tests

`NativeEngine::relation_schema` now chooses the existing bounded decoded metadata
allowance when GCS mode is active and checks reported native length before
adoption. Ordinary local/S3 decoding remains unchanged. Targeted regression checks
stored schema in GCS metadata mode, invocation close/reopen removes temporary
state and clears native GCS mode/deadline, and durable table schema remains.
This test is not a synthetic HTTPFS destructor counter or allocation measurement.

Latest offline rerun: native102passed/12ignored, build21, extraction9, wire5library/
20protocol, SDK18library/3runtime. Strict affected native Clippy, fmt/diff pass.
Logs: `/tmp/grv-gcs-final-memory-native.txt`,
`/tmp/grv-gcs-final-memory-wire-sdk.txt`, targeted initial
`/tmp/grv-gcs-final-memory-regressions.txt`. Earlier staged-source, stale-peer,
live and held evidence are not overwritten or promoted to this artifact stage.

Final inventory/session recovery and current-artifact validation now complete
the agreed GCS logical admission/transfer scope; see
[gcs-memory-qualification-completion.md](gcs-memory-qualification-completion.md).
Internal arbitrary escaped-owner/source correspondence beyond the bounded review
remain limitations, not universal proof or a blocking demand for global quota.
Actual SDK/native recovery passes (build22total); final actual held
`y_2_qyrt` passes1870.18seconds with five artifact hashes unchanged and owned
cleanup404. Pause now before unrelated qualification.
