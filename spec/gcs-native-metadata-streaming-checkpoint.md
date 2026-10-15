# Native GCS metadata streaming checkpoint

Status: implemented, passing synthetic/local and live slices; full task37
pair-wide resource acceptance remains open. Current native revision5; public ABI
unchanged. Historical streaming evidence below retains its actual earlier stage.

## Superseding execution decision

The [in-process execution decision](gcs-in-process-execution-decision.md) accepts
partial engine memory limits and declines helper-only allocation policy adoption.
Current native revision6 code is in-process and helper source is archived; see
[gcs-in-process-transition-checkpoint.md](gcs-in-process-transition-checkpoint.md).
These streaming/allocator experiments remain scoped evidence, not a hard engine/
RSS cap or live/held qualification of the replacement.
Retain bounded result/decoded transfer buffers and honest cleanup/recovery.

## Retention change

`native/pull.inc` uses `SendQuery`/`FetchRaw` for configured GCS metadata instead
of a complete `MaterializedQueryResult`. Local/S3 retain the original path.
Only one fetched chunk is adopted at a time. The stock streaming buffer is set
temporarily to64KiB and restored, but this is a **soft collector threshold**:
one engine chunk may exceed it before GRV sees it. GRV refuses fetched chunk
allocation above4MiB before adapter decoding. Native operators/upstream source/
expression allocation still needs its own accounting; these checks are not RSS
or universal32MiB proof.

VARCHAR lengths/data are inspected directly through UnifiedVectorFormat before
copying into the bounded output. Oversize strings are not first copied into
Value/ToString. Other metadata is restricted to bounded scalar types; nested
values/BLOB renderings refuse rather than amplifying ToString output. No lazy
result leaves the synchronous call or its exact scanner admission scope.

## Cleanup semantics and review corrections

Pinned `StreamQueryResult::Close()` only resets buffered context; it does NOT
execute EndQuery/rollback. The initial Close-only version was corrected before
claiming cleanup acceptance. On partial output refusal, RAII interrupts the
active stream and fetches once to trigger pinned execution cleanup, never drains
the remaining rows or retries the statement. Explicit transaction rollback
remains caller responsibility.

A per-query registered QueryBegin/QueryEnd witness resets on each query; duplicate
registration is refused. Missing QueryEnd or unexpected cleanup error revokes
exactGCS authority. Expected interruption during bounded-result refusal is not
mistaken for an unexpected cleanup failure. The witness spans SendQuery exceptions
and materialized/streamed paths, not just stream destruction. QueryEnd errors
are tracked; normal EOF checks HasError before returning any result.

A dedicated source-including `metadata_stream_proof.cpp` now injects a
QueryEnd-listener exception into the actual GCS metadata path: bounded refusal
revokes authority and removes the listener; normal refusal preserves rollback
and subsequent scalar results. It uses synthetic configured state, no token,
transport or network. This passing exception slice alone is not universal
sign-off or immutable rollback evidence. Additional actual transaction fault
slices are recorded below; the complete engine failure matrix remains open.

## Passing evidence

- Synthetic configured-owner test: billion-row SELECT refused at bounded prefix
  within5s, BEGIN/create/ROLLBACK leaves no table;3MiB and6MiB strings refuse;
  subsequent scalar SELECT works; execution-time cast error followed by a fresh
  transaction/rollback works;4097 rows across chunks, scalar decimal/double/null
  round-trip correctly; nested values refuse. No cloud in this test.
- Native112passed/22ignored; five isolated admission/descriptor/self-base and seven
  recovery/setup slices passed. Normal failure rollback/commit/reopen test passes.
- Ten native ctest proofs passed including injected QueryEnd failure; affected
  strict native/conformance Clippy passes.
- Live pull/self-preparation latest cleanup-aware library:
  `~/grv-shipping-gcs-a00qrq7y`: both tests passed; owned generation/bucket removed,
  empty all-version inventory and404. Earlier streaming slices at
  `m3l1v1a9`/`9ldcslff`/`d9labe3i` retained, likewise cleaned.
- Latest shipping scanner ZSTD/dictionary/no-statistics65536-row slice:
  `~/grv-shipping-matrix-_ymm6gwa`, projection99.0238%, selective-wide95.9295%
  accepted-content savings; ten oracles/denials and owned cleanup passed. Savings
  do not waive full preparation verification or imply statistics pruning.

The prior full parent-held1839.11s pass predates this native streaming change.
Latest supported/helper pair full held rerun is not inferred from these slices.

## Actual transaction error slices

`native/transaction_fault_proof.cpp` includes actual shim/pull source, but installs
fault authority only in its private disposable test databases. It does not add a
production fault option or patch upstream DuckDB.

- A LocalFileSystem subtype injects WAL write or sync IOException during actual
  explicit COMMIT. Both record a reached fault, return failure, revoke exactGCS
  authority and remove the metadata witness. An independent connection observes
  only42 before orderly close with fault authority still armed. Owner is then
  destroyed; fresh DuckDB
  reopens and observes only previously committed42, not attempted99. Neither
  INSERT nor COMMIT is repeated. This is native row-history evidence, not full
  GRV immutable-receipt or crash-at-every-WAL-boundary acceptance.
- While an actual autocommit streamed result is active, test invalidates its
  attached database before interrupted cleanup. Pinned MetaTransaction::Rollback
  rejects the invalidated database: a nonthrowing observer verifies the exact
  invalidation reason in the error delivered by pinned EndQueryInternal after
  rollback; QueryEnd witness records cleanup_error,
  authority is revoked, active transaction clears, witness removes and fresh
  source-free database opens. This exercises an actual rollback rejection, not a
  throwing QueryEnd callback. It does not inject every undo/catalog/I/O failure.
- Commit cases currently resolve after orderly owner destruction with faults
  disabled; this is not an abrupt-crash WAL-recovery matrix.

## Upstream allocation counterexample

`native/metadata_allocation_proof.cpp` replaces the public stock default allocator
reference ONLY in a standalone single-owner proof process. It records sizes
forwarded to actual allocator functions; no shipping/global instrumentation or
memory-layout hack. Allocation tracing omits C++ STL/other allocators/malloc
headers and RSS; these are lower-bound slices, not universal high-water maxima.

With64KiB reply allowance and soft64KiB streaming buffer, all trials refuse:

| Closed proof SQL                                 | Tracked baseline | Tracked peak | Largest request |
| ------------------------------------------------ | ---------------: | -----------: | --------------: |
| billion-row BIGINT prefix                        |          253,952 |      516,096 |         262,144 |
| 6,000,000-byte repeat string                     |          253,952 |   17,063,952 |       8,388,608 |
| 40,000,000-byte repeat string                    |          253,952 |  134,504,464 |      67,108,864 |
| 40,000,000-byte malformed stored receipt, LIMIT2 |          253,952 |   67,411,968 |      67,108,864 |

These are concrete counterexamples to treating adopted-chunk/output limits as
preallocation limits. Repeat proof SQL is a trusted-native seam test, NOT an
assertion that shipping user declarations enter metadata SQL directly. The final
trial uses the actual metadata-authorized `_grv.pull_attempts` table with an
oversized malformed existing row created only for this proof. Even LIMIT2 cannot
prevent its native collector from allocating before field rejection. Legitimate
writer receipt size checks do not bound arbitrary corrupted existing workspaces.
This is a native malformed-record read slice, not a full GRV discovery/auth route.
Production generated SQL/source/durable field maxima and independent engine
budget still require analysis. No universal32MiB total heap claim is valid here.

Independent read-only transaction-proof review confirmed reached WAL faults
and authority checks, while flagging orderly-close and rollback-provenance
limits. Pre-close independent visibility and nonthrowing error-provenance
assertions now pass; abrupt crash/mutating rollback matrices remain open. A
separate fault-seam research agent ended with no result; it is not evidence.

Both new targets are ctest entries:12native proofs passed. Their binaries/log
are hashed in `release-validation/gcs-native-resource-fault-checkpoint.json`.
No shipping library/helper rebuilt for the transaction proof additions; latest
live/held records therefore retain their prior association for that stage.
Subsequent Rust durable-read changes and allocator instrumentation below have
separate evidence. Full upstream allocator accounting remains open.

## Durable admission and expanded allocator coverage

The first table above tracked only the default allocator. This missed database
storage allocations. The proof now also supplies DBConfig::allocator with shared
counters, forwarding both allocator paths to stock functions. No production
allocator substitution; ordinary malloc/STL/headers/RSS remain outside coverage.
The fixture is persisted in a private disposable database and each reopened
candidate has a fresh owner (not an OS-cold cache claim).

| Current combined-allocator trial                   |   Baseline |        Peak | Largest request |
| -------------------------------------------------- | ---------: | ----------: | --------------: |
| 40million expression,512MiB engine limit           | 40,294,400 | 174,589,968 |      67,108,864 |
| raw40million stored receipt                        | 40,294,400 | 107,522,048 |      67,108,864 |
| reopened byte-length scalar                        |    520,192 |  81,027,080 |      40,001,536 |
| reopened conditional small projection              |    520,192 |  81,094,688 |      40,001,536 |
| reopened size-filter candidate                     |    520,192 |  81,010,708 |      40,001,536 |
| reopened scalar with16MiB engine limit, refused    |    520,192 |   1,163,264 |         262,144 |
| 40million expression with16MiB limit, refused late |    520,192 | 134,815,760 |      67,108,864 |

These results refute a SQL-length-check-only total-memory fix. Engine memory_limit
prevents the measured reopened storage request, but not the expression allocator
request. Candidate filtering is NOT adopted: it could hide malformed history as
absent. Single-statement CASE/error has separate measured trials; unexpected
query error can revoke authority, so recovery always fences/reopens when needed.

Rust `NativeEngine::durable_metadata_query` now guards selected GCS isolated
receipt/binding/session reads with complete ordered VARCHAR CASE projections.
Aggregate per-row UTF-8 byte length uses bit_length/8, NULL contributes0. Oversize
raises fixed error; original inner WHERE/LIMIT/order selection remains. No
truncation, casting, missing-row reinterpretation or check/fetch snapshot gap.
Local/S3 ordinary metadata path is unchanged. This is result-copy admission only,
not a bound before storage allocation. Two individually admitted rows can still
exceed total reply capacity and refuse at existing framing/decoder checks.

Tests pass for small/empty/NULL/duplicate rows, absent WHERE, exact aggregate UTF8
boundary, excluded oversized row beyond LIMIT1, multi-field aggregate overflow,
and fence/reopen original lengths. Caller-level malformed receipt resolves
OutcomeUnknown, never NotCommitted, with DB/ownership witness bytes unchanged.
Initial live test exposed rejection of legitimate `request_sha256` by overly
strict name validation; digit support/regression test fixed it. Failed fixture
`~/grv-shipping-gcs-s2gqhiii` retained/cleaned404. Both live pull/self-preparation
then passed at `~/grv-shipping-gcs-73ki_c3w`, empty all-version inventory/bucket404.
Native114passed/24ignored,7recovery and2explicit durable slices,12ctest and strict
native/conformance Clippy pass. Fullheld latest guarded helper is still unproved.

## Research-only preallocation request quota

The standalone allocation proof now also exercises a shared request-byte quota
before forwarding allocation to stock functions. It conservatively accounts
old+new storage during realloc and rolls reservation back on allocator failure.
This exists ONLY in the disposable proof executable, not shipping native/helper,
parent, local or S3 consumers.16MiB is a deliberately small test budget, not a
proposed production limit or32MiB total-memory claim.

| 16MiB request quota case       | Baseline | Peak admitted requests | Largest rejected request |
| ------------------------------ | -------: | ---------------------: | -----------------------: |
| reopened stored40million value |  520,192 |              1,163,264 |               40,001,536 |
| 40million expression           |  520,192 |                786,432 |               67,108,864 |

Both fail before forwarding the oversized request; fresh source-free owner
SELECT42 then succeeds. This demonstrates a viable admission seam for the two
measured allocator paths, not exhaustive recovery or total memory safety.
STL/runtime allocations, signed-library independent allocators, malloc headers,
parent retained documents and RSS are still unbounded by this quota. It must
not be installed as a process-global allocator in a parent with independently
configured DuckDB consumers.

Production adoption would require a reviewed dedicated-helper allocation
policy, independent engine/source/expression coverage, separate parent/bridge
accounting, supported-target validation and an explicitly chosen engine budget.
That is a material allocation-handling decision; no production change was made
by this experiment. User requested pause once memory-related acceptance
completes. Task37 remains open, not silently marked complete at this decision
boundary.
