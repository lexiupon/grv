# Queue/source memory scope: implemented slices and policy boundary

Current status: task49 and agreed GCS-specific memory scope complete; paused.
See [completion checkpoint](gcs-memory-qualification-completion.md). The initial
slices below were primarily offline; final stable live/held current-artifact
evidence is separately recorded there, not inferred from old stages. Native
revision6 library unchanged; no allocator quota/version/advertised capability.

## Implemented without changing public wire limits

`grv_adapter_wire::serialized_len` uses a counting serde writer with checked
addition, no serialized Vec or Value. `Codec::write` measures frame+LF before
creating its Value or bounded serialization copy. SDK `Outbound::new` likewise
measures its frame before the old full frame-Vec probe. Existing1MiB frame/
16MiB identified limit, successful encoded bytes and errors remain unchanged.
This fixes oversized preliminary frame-copy construction, not all subsequent
SDK document/canonical/Value allocations or arbitrary Serialize implementations.

Source accounting review `f3ea07f8-4efe-41a` found a real overlap: Rust's full native
encoded allocation is live during length metadata/native decoded-window fetch.
The scratch batch reservation numerically covered it, but protocol5.5 requires
encoded+decoded source together fit the source-unit allowance. Added GCS-only
`gcs_transfer_allowances`: encoded capacity=min(native allowance,source/2), decoded
native allowance=source-encoded, fail before allocation if no viable unit. Build
GCS export passes the reduced native source allowance and original scratch budget.
Local/S3 path unchanged; unchanged behavior is compatibility, not shared memory
qualification. This subtracts full capacity, not truncated written bytes.

Native staging releases length metadata before variable-value fetch; the encoded
allocation overlaps either phase, never both phases simultaneously. Rust window
contains encoded bytes and type descriptors, not a second decoded row copy. Arrow
conversion uses existing preallocation `native_allowance`/four-copy scratch checks;
held slots prevent uncredited fetch. Current GCS source-free integration exercises
a2MiB source-unit export through BuildStore, and native fixture scans5000actual
staged rows with partitioned allowances. These are executable accounting slices,
not measured allocator/RSS maxima or prevention of upstream storage allocation.

HTTPFS request read buffers/statistics belong to cloud-query execution before
local staged export, not the local window source unit. Their aggregate retention
and engine/Parquet intermediates have only the accepted partial engine controls;
no total source-library/RSS cap is inferred. Protocol5.5 cannot exclude genuinely
retained extraction buffers just by renaming them; further reachability review of
remote scans that return metadata remains open.

## Validation

- Native library101passed/12ignored;9extraction tests including5MiB defaultbudget
  row,>256fields, credit cancellation and shared snapshot pass.
- Wire2library+18protocol tests pass; SDK14library+3runtime tests pass.
- New wire test checks byte-count equality on escapes/multibyte/containers and
  oversize write rejection before any transport bytes.
- New GCS BuildStore source-free reduced2MiB-source export passes.
- Strict Clippy for wire/SDK/native adapter/conformance, fmt/diff checks pass.
- Source review read-only8calls, no artifact changes. Independent slice review
  `8bc02d61-152c-426` completed six inspections without identifying a blocking
  defect in frame-size probing or GCS staged-export budget partition. Scope only:
  canonical allocations, persistent IDs/maps and other GCS source paths remain
  open. Reviewer did not rerun tests; no overall memory approval.

## Historical boundary that required an explicit policy decision

Remaining closure is not just arithmetic or another long cloud test:

1. **Channel-lifetime document IDs.** Protocol2.4 requires uniqueness per channel.
   `State.document_ids` retains every ID, even after the payload is consumed.
   Channels allow repeated ordinary operations; no total document/operation count
   cap exists. Clearing IDs silently permits reuse and changes authority semantics.
   A new hard count/byte cap or required channel rotation is a new generic refusal/
   lifetime policy, not an implementation of the existing64MiB per-document limit.
2. **Generic decoded/canonical document capacity.**64MiB raw document permits many
   tiny JSON nodes and substantially larger decoded Value/container storage.
   Upload bytes, raw-object keys/sets, decoded Values, canonical temporary Value/
   output and typed resolution clones overlap. SDK input channel4 bounds count,
   not decoded byte admission. Raw/frame caps give a finite worst-case per-item
   shape but not the claimed independently usable metadata reserve. A decoded
   logical byte/node cap would refuse currently size-valid messages unless a
   deliberately opt-in GCS policy is added and propagated before parsing.
3. **Embedded runtime cardinalities.** SDK served lifecycle already permits one
   bound handle and one build session; this helps process-path maps. Direct public
   Runtime callers can retain additional connections and sessions until stop;
   these cardinalities are not universally bounded. Silent session eviction would
   alter identity/recovery authority. New embedding limits need an explicit policy
   with fail-before-effects admission and stopped ownership on refusal.
4. **SDK outbound document conversion.** The preliminary frame Vec is gone, but
   conversion to Value and canonical JSON still creates full document copies.
   Caller pre-copy admission in DuckDB doesn't authorize claiming all generic
   SDK output budgeted, especially before historically malformed receipts are
   interpreted. Refuse safely, preserve known outcomes, no automatic repair/drop.

User now explicitly approved an internal **GCS-only selected policy**, not a
custom allocator. Concrete caps, activation boundary and refusal are recorded in
[`gcs-metadata-policy.md`](gcs-metadata-policy.md). Implementation adds one shared
internal lexical reserve in wire used by SDK and DuckDB caller-copy accounting;
generic/local/S3 defaults remain unchanged. No reset/eviction/rotation/version.
Tests/review are progressing; policy implementation is not whole-memory closure.

Policy review `327be8f3-095c-4ac` found two concrete defects: recursive document
reference expansion could clone many admitted documents before aggregate charge;
SDK lifecycle result caches were updated before outgoing admission. Added borrowed
aggregate expansion charge before all clones (including arbitrary JSON reference
shapes), and completed-result admission before SDK successful state transitions.
Regression repeated ten1MiB references refuses without consuming the document.
Follow-up `bbf792d4-7a24-4ea` confirms expansion and normal-result guards,
but found refused known terminal results could become Stopped under pending
cancellation. Fixed by error then joined channel close requiring recovery; new
regression proves no stopped acknowledgement or effects retry. Review predates
that final fix. Subsequent scoped source review `435b1a3a-046f-4cb` confirms
final cancellation-refusal ordering and actual export Engine destruction/reopen
before acquisition, reporting no concrete defect within its turn-limited scope.
None of these reviews reran tests. `adf5a93f-6948-4f1` supports native result/
chunk closure; `77d0e021-27c0-4dd` traced reader/local handle owners. See
[gcs-source-lifetime-checkpoint.md](gcs-source-lifetime-checkpoint.md) for actual
export barrier and remaining ownership/correspondence qualifications. Whole
source/memory acceptance stays open.

Latest policy-stage offline passes: native101/12ignored, wire5library20protocol,
SDK18library3runtime, build21 and extraction9, staleCLI1. Strict affected Clippy,
fmt/diff pass at that stage. Direct runtime test proves second GCS/mixed binding
refused before new database creation, first ownership retained; two ordinary local
connections still succeed. Outgoing lifecycle/cancellation tests pass; final copy/state/source
accounting remains under work; no cloud test or held run requested yet.
Development failures preserved: test fixture incorrectly used `result` instead
of `details` for CommandResult; Clippy tests-before-items; caller shared-counter
migration changed error wording expected by a regression (restored adapter error
context, not weakened admission). No publication/capability advertisement.

All earlier failures and stages remain in their checkpoints. `dw_ypv61` held pass
has unchanged pre/post artifacts for its earlier stage; it does not qualify later
Rust/wire/SDK edits. User's pause after actual memory completion still stands;
the former policy-boundary stop is resolved by approval, but memory qualification
remains ongoing rather than the promised completion pause. No cloud test, held
process or helper is running; scoped policy review may remain.
