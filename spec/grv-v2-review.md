# GRV v2 concurrency and recovery review

Reviewed 2026-09-28 against the seven reported issues. This checkout has no
`rfc/` directory; the matching document is [grv-v2.md](grv-v2.md). All seven
issues were present, and the specification now defines protocols addressing
them. These are specification changes; there is no implementation in this
checkout to validate against the revised contract.

## Changes and decisions

| Issue | Decision in the revised specification | Why |
|---|---|---|
| 1. Stale publisher survives GC takeover | Put the dataset lease, revision pointer, allocation counter, and pending operation in the same CAS-protected `LATEST` JSON record (§8). Change its content on every transition. | GC takeover now invalidates a delayed publication CAS even when the current revision number stays unchanged. |
| 2. Pin race and shortened grace | Serialize pin and irrevocable prune decisions through `LATEST`; recover pending markers before accepting another decision (§10). Start supersession grace from a receipt sampled after successful publication, with clock-skew allowance (§8). | A pin cannot succeed after pruning is committed. Revision preparation time cannot consume the predecessor's grace period. |
| 3. Allocation reuse after repeated crashes | Persist `high_water` in every version claim and in `LATEST`; advance it atomically with reservation and preserve it through all transitions (§5, §8). | No sequence of takeovers can erase a reservation, even when no data object exists. |
| 4. Recovery races a live run | Add a leased run control record and entry journal; CAS-transition `open` to `recovering` or `sealed`; materialize only the sealed payload (§6). | Recovery cannot infer completion from idle partition claims, and a delayed owner cannot commit a competing run file. |
| 5. Missing or partition-local schema baseline | Add permanent, table-wide `.schema.json` with CAS-registered append-only schemas (§4). Validate selected versions against it. | Pruning and unfinished versions cannot erase compatibility history; incompatible extensions across disjoint partitions compete on one record. |
| 6. Sync retry leaves extra rows | Journal dirty tables before mutation. On retry, rebuild their full contents from the new target or empty them; atomically commit the watermark and clear the journal (§11). | A failed attempt's additions remain discoverable even when absent from both the previous watermark and the replacement target. |
| 7. Concurrent case collisions | Restrict dataset/table identifiers, partition keys, and partition values to lowercase ASCII; reject uppercase without normalization (§3). | Two valid identifiers cannot differ only by case, so no separate atomic name registry is required. |

Two closely related races also needed explicit treatment:

- A source revision can be pruned after a downstream publisher checks it.
  Derived versions now register durable holds under each source dataset's
  lease before writing their manifests (§9). A hold is released only after
  its target version is permanently tombstoned. Grace affects the chance
  of successful acquisition, while holds provide retention safety.
- An expired GC worker can resume a deletion after another operation takes
  over. GC now commits an exact prune batch before writing tombstones or
  deleting data. Takeover completes those irreversible tombstones first;
  a late delete can only affect a version that can never be selected again.

## Crash and interleaving review

The revised text was checked against these schedules:

1. **Publisher pauses before pointer CAS; GC takes over and prunes.** The
   takeover rewrites `LATEST` with a new mutation id. The old publication
   CAS fails; its revision remains an orphan.
2. **Pin and prune contend.** If pin commits first, takeover materializes
   its marker before GC evaluates protection. If prune commits first,
   takeover materializes its tombstones before pin validation. A successful
   specific-version/revision pin cannot overlap committed pruning.
3. **GC commits, then crashes before any tombstone.** The next holder finds
   the pending immutable plan and writes all tombstones before allowing
   publication or pinning. A plan written without a successful commit is
   inert.
4. **Revision preparation exceeds grace; publication occurs later.** The
   predecessor's deadline uses a post-publication observation. A crash
   before writing that receipt extends retention; it does not shorten it.
5. **A reserves 7 and crashes; B takes over and crashes before allocation;
   C takes over.** Both takeovers preserve `high_water = 7`; C reserves at
   least 8. The same rule applies to revision numbers.
6. **A run is between partition writes when recovery starts.** Its live
   run lease prevents takeover. If it expires, recovery and the owner race
   on the same control record. Only one can freeze the entry set, and both
   must materialize that exact sealed result.
7. **Two partitions extend `[id]` to `[id, x]` and `[id, y]`.** One schema
   CAS wins; the loser rereads and rejects the incompatible extension.
   Neither pruning nor writing disjoint revisions removes that baseline.
8. **Watermark R0 has no P; failed R1 adds P; retry R2 also has no P.** The
   table was journaled before inserting P. The retry rebuilds it from R2,
   removing P even though an R0-to-R2 diff alone would miss it.
9. **Concurrent `Orders` and `orders` creation.** `Orders` fails lexical
   validation before any layout write; only `orders` can be created.
10. **Upstream GC races a new downstream dependency.** Hold registration
    and source pruning commit through the same source control object. The
    winner either preserves the complete source state or causes hold
    acquisition to fail. A target publication cannot release its own hold;
    only its irreversible pruning permits release.

These are manual protocol/interleaving checks, not an executable model or
an implementation test suite. `git diff --check` also passed.

## Tradeoffs adopted

No blocking human decision is needed to update this draft. The following
choices are explicit so that implementation can follow one coherent contract:

- `LATEST` is now JSON and several durable metadata records are required.
  This is incompatible with earlier draft writers; migration and mixed
  writer deployment are not implicitly supported.
- Lowercase-only validation also applies to partition values. Callers must
  explicitly map source values that need another spelling.
- Explicit `.keep` pins are permanent in this draft. Deleting markers is
  not a safe unpin protocol; reversible pins would require an additional
  protocol. Automatic dependency holds do have a terminal release path.
- Schema registration reserves appended nullable columns even if a writer
  later fails. Existing older versions remain selectable for rollback.
- Incoming dependency holds may keep sources longer than the minimum
  closure requires. Prune targets before releasing their source holds;
  uncertain crash cleanup favors retention.
- Recovery journals and operation descriptions add storage and CAS traffic.
  Sync retries may rebuild full tables. Prune batches should be bounded
  because their marker recovery blocks new decisions for that dataset.
