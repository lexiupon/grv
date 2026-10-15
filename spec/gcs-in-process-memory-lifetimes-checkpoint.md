# In-process GCS memory lifetimes: historical findings and disposition

Current status: agreed GCS-specific memory scope complete; paused. See
[completion checkpoint](gcs-memory-qualification-completion.md) for final
inventory, session/refusal tests and current-artifact held evidence. Historical
open findings below retain their stage-specific scope; not universal RSS proof.
See [execution decision](gcs-in-process-execution-decision.md) and
[transition checkpoint](gcs-in-process-transition-checkpoint.md).

## Separate budgets, not a single32MiB/RSS promise

Protocol5.5 source/scratch bounds govern retained transfer buffers. Caller
metadata, protocol documents, engine caches/staging, runtime state, allocators
and source-library overhead remain separately accounted. The512MiB engine
memory_limit and16GiB spill quota are partial controls. The approved decision
explicitly declines a global allocator/helper-quota policy; this checkpoint does
not resurrect one or automatically increase budgets.

Current direct Rust/native bridge lifetimes:

| Phase                  | Simultaneously owned logical content                                                                       | Separate/excluded costs                                                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| internal dispatch      | admitted command CString≤2MiB; encoded output allocation2MiB                                               | original borrowed/caller SQL; parser/engine/catalog state; headers/runtime                                                    |
| remote dispatch        | derived URL/command CString content aggregate≤2MiB,≤4096 files; pointers/vector headers; output2MiB        | original verified files/SQL; native URL vector, admission set and temporary allowed paths; engine parser/HTTPFS/Parquet state |
| metadata production    | output2MiB; current fetched result with4MiB **post-allocation** refusal; collector64KiB **soft** threshold | oversized source/expression/chunk already allocated upstream; no pre-engine cap                                               |
| Rust metadata decoding | encoded allocation still2MiB plus decoded row/cell/string logical allocation admitted≤8MiB                 | command/locations/caller documents still live; allocator excess/headers/engine caches                                         |

The decode phase does **not** also keep the finished native result chunk alive:
FFI has returned and its query temporaries have been destroyed. Conversely SQL
and original caller documents do not disappear when a2MiB encoded result is
returned. No active helper send/receive/JSON response copies exist now. Logical
capacity/request accounting is not malloc overhead or total RSS.

Source export uses `staging.inc` immutable ordinal windows and length preflight
before requesting variable-width data. It resets length metadata before data
fetch, admits standard-vector capacity and per-cell copies, then serializes a
bounded native window. Rust retains that encoded window during Arrow conversion;
`ipc.rs` accounts arrays/conversion/IPC output separately against scratch and
`native_allowance` leaves room before fetch. Workers use one synchronous command/
reply slot and demand-driven credit; all outputs stage in engine storage, not an
adapter queue. Remaining source accounting must verify encoded+decoded overlap,
length-metadata/container overhead and exact supported-target capacity—not infer
it solely from logical row bytes. Engine storage allocation to evaluate length
is the accepted partial-control limitation, not hidden transfer buffering.

## Concrete remaining blockers

Independent read-only reviewer `12f8db92-b74d-4ef` identified:

1. **SQL construction precedes bridge admission.** `pull::file_projection`,
   `remote_view_projection` and `BuildStore::import_remote_view` allocate per-file
   projections, joined SQL and final formatted SQL before `remote_query` rejects
   aggregate command>2MiB. Repeating target expressions per file can amplify a
   small contract/file document. Bound aggregate generated SQL before those
   copies/joins; scope GCS changes deliberately to preserve local/S3 semantics.
   Empty projections and assertions need corresponding accounting, not just the
   final UNION. Check generated command+locations together and wrapper overhead.
2. **Caller/queued documents are a separate budget.** Public process wire caps
   are1MiB frame/16MiB identification/64MiB document/512KiB decoded chunk. JSON
   parse/canonical validation/resolution clones and typed conversion coexist.
   `pull_runtime` clones contracts and verified files while its original plan
   remains live; build retains reservation/discovery and preparation clones.
   Embedded callers lack a demonstrated equivalent aggregate pre-copy admission.
   Serialized-document size alone is not a decoded-Value/container allocation cap.
   A caller's arbitrary already-owned String capacity is outside library ownership
   but never excuses unbounded newly owned clones made by the library.
3. **Counts are not byte budgets.** SDK channel4 and native worker channel1 bound
   counts; queued frame/document closures still need byte accounting. Wire
   `document_ids` lifetime tracking and runtime connection/session maps require
   an independent cardinality/lifetime budget. Reject safely before effect; no
   automatic migration, dropping malformed evidence, or wire-version invention.
4. **Source-library retention is not universally capped.** Reviewed stockHTTPFS
   grows read buffering to its own maximum and appends per-request statistics for
   nonparallel handles (`httpfs.cpp:555–576,613–618`). Native threads1/cache-off/
   force-download-off do not by themselves prove aggregate per-handle/source
   library allocation. Classify retained transfer buffers vs engine private
   allocations honestly under the approved partial-engine contract; do not treat
   all source-library memory as outside protocol5.5 or assert total32MiB safety.

## Executable evidence and next action

Revision6 live pair `~/grv-shipping-gcs-296qgt4m` passed guarded pull/refresh/
source-free replay and nonempty self-preparation, with one owned generation
removed, empty inventory and bucket404. This qualifies those behavioral slices,
not the accounting gaps above.

During the active prebuilt held lifecycle, no sources/artifacts were rebuilt or
changed. Existing standalone allocation/stream/transaction proof binaries were
executed without rebuilding, logs `/tmp/grv-inprocess-{allocation,stream,
transaction}-proof.txt`. Counterexamples still show a16MiB engine setting does
not prevent a67,108,864-byte expression allocation; research quota trials remain
research only. Stream cleanup and actual WAL/refused invalidated-read-only
rollback proofs pass with their prior scoped limitations; no abrupt-crash or
universal mutating rollback inference.

Actual held run `dw_ypv61` passed1test/1962.69s, all five pre/post artifact hashes
match and owned inventory/bucket404 verified. Only after completion were Rust
code/build changes made. Added borrowed pre-copy conservative GCS sizing for
repeated target-column casts, null padding, coordinate expansion/escaping, scanner
arguments and CREATE VIEW wrapper in pull/build. Validating GCS native plans and
runtime conversions now checks that size before per-file clones; build external
preparation checks before reservation/session clones and import authorization.
Local/S3 construction remains unchanged; mixed local self-base is excluded from
the GCS sizing gate. This closes a narrow generated-view construction gap, not
all assertions, caller/queued documents or source-library retention accounting.
Latest94native/12ignored, mixed-local signed synthetic test and strictClippy pass.
New Rust stage live pair `_6in5234` passes both tests/owned404 (before final mixed-
local gating cleanup); final-stage live pair `2mfmdbmq` also passes both tests with
owned empty inventory/bucket404. Offline mixed-local signed regression,5MiB default
budget extraction and stale-registry CLI refusal pass. Narrow pre-copy review is
pending; other memory acceptance remains open.

Follow-up narrow borrowed caller metadata admission is now implemented in
[gcs-caller-metadata-admission-checkpoint.md](gcs-caller-metadata-admission-checkpoint.md):
99native/12ignored, strict native/conformance/non-native Clippy and latest live
pair `ywo05jpl` pass/owned404. Reported review multipliers (files/partition, stored
base contracts, selected partitions and fifth discovery-derived instance) are
fixed with explicit charges and boundary regression. This logical pre-copy
reserve does not close SDK/wire decoded document, queue/cardinality or source-
library bounds. Next finish
those separate budgets and remaining source/assertion accounting. Task49 offline
slices now fix the oversized frame probe and explicitly reserve GCS encoded+
decoded staged-export source capacity;101native/12ignored and wire/SDK/extraction
slices pass. User approved GCS-only metadata/lifetime policy; exact internal
caps and bootstrap/activation/refusal boundary are in
[gcs-metadata-policy.md](gcs-metadata-policy.md). Shared logical reserve used
by wire/SDK/caller accounting, selected8MiB documents/1024IDs/depth64 plus runtime
cardinality and pre-copy output/expansion guards implemented and offline-tested.
[gcs-queue-source-memory-checkpoint.md](gcs-queue-source-memory-checkpoint.md)
retains the prior decision boundary and review defects/fixes. Aggregate lifetime/
source acceptance remains open. No generic limits, rotation, eviction or ID resets.
Requalify changed
artifacts as needed; a successful held run cannot silently qualify later edits.
Final GCS-only aggregate accounting is in
[gcs-aggregate-metadata-checkpoint.md](gcs-aggregate-metadata-checkpoint.md):
scoped review supports finite logical copy/cardinality model, not an aggregate
8MiB/RSS ceiling; actual SDK/native session-recovery regression passes. GCS
durable JSON now has preparse structural admission with no-repair history
regressions.104native/12ignored22build9extraction pass; final durable review and
stable artifact live/held qualification remain open.
Tasks37/43/45 remain open. Do not proceed to unrelated release/cloud qualification
or mark memory complete simply because hard native/RSS guarantees were relaxed.
