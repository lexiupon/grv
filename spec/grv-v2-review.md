# GRV v2 review log

- [Round 3 (2026-09-29): holds without upstream writes, publish cost, erasure](#round-3-2026-09-29-holds-without-upstream-writes-publish-cost-erasure)
- [Round 2 (2026-09-29): readiness review](#round-2-2026-09-29-readiness-review)
- [Round 1 (2026-09-28): concurrency and recovery review](#round-1-2026-09-28-concurrency-and-recovery-review)

## Round 3 (2026-09-29): holds without upstream writes, publish cost, erasure

### Context

Follow-up questions on round 2's remaining issues led to three changes and
several clarifications.

- **Reads were never the reason for holds.** A reader of a superseded
  revision is protected for `pending_grace` after its receipt, with no
  writes of its own. Holds protect *lineage*: they keep a source revision
  resolvable for as long as a product version derived from it is kept.
- **A mistaken schema column** can be worked around in the same table:
  append a correctly typed column under a new name, and write nulls to the
  old one. Only reusing the original name requires a new table.
- **GC cost** grows with committed revisions × state size. Revisions are
  immutable, so GC should keep an incremental index and read only new
  revisions each pass; at small-team scale that is not a concern.

### Changes

| Topic | Decision | Where |
|---|---|---|
| Consumer writes to upstream `LATEST` | Consumers no longer take a source's lease or write its `LATEST`. A hold is a create-only record in `<source>/.holds/<consumer-dataset>/`, checked by re-reading the source; the run records `holds_confirmed`. The source's GC commits a `prune_intent`, re-lists holds, and then commits the `prune` decision, so either the consumer sees the prune or GC sees the hold. Release markers move to `.states/released-holds/` and are written only by the source's GC. | §2, §5, §6, §9, §10 |
| Publish cost | Two `LATEST` writes per publish: acquire and reserve, then commit and release. The supersession receipt is written afterwards without a lease; any process that finds it missing writes one with a later time, which only extends grace. The lease is renewed only when it could expire before the commit, since the commit is fenced by compare-and-swap either way. | §7, §8, §10 |
| Data erasure | Companion RFC [grv-crypto-shredding.md](grv-crypto-shredding.md): per-subject keys, KMS envelope encryption, an erasure procedure, residual risks, and DPO checklists. The core gains two hooks: `extensions` in `.layout.json`, which writers must implement or refuse to write, and a per-column `ext` in `.schema.json`, set in the same CAS that adds the column. | RFC; §3, §4 |

### Self-review

An adversarial review of the new hold and publish protocol found one safety
bug, now fixed. Release markers were keyed by `retention_id` alone, so a
faulty consumer could create a record in its own subfolder reusing another
consumer's id and get that id released. Release markers are now keyed by
the full hold path, and the publisher checks that a cited hold's body
matches its path and names the citing run (§9).

It also found two liveness limits, now documented in §9:

- A crashed GC's `prune_intent` blocks hold confirmation for the
  revisions it touches until the next publisher or GC clears it. It never
  blocks runs reading the current `LATEST`. Letting consumers ignore an
  intent whose lease has expired was rejected as unsafe: consumers never
  write `LATEST`, so nothing would fence the stale GC's decision.
- The source's GC cannot recover crashed consumer runs without write access
  to the consumer's dataset. Their holds stay until the consumer's own
  recovery seals the run.

Checked and holding:

- Every ordering of hold creation, the consumer's `LATEST` read, intent,
  re-list, decision, and clear.
- Stale GC workers, which the decision CAS fences out.
- Unknown outcomes of intents, decisions, and publish commits.
- The hold fast path: the current `LATEST` of a non-retired dataset, or an
  active revision pin.
- Irreversibility of the release conditions.
- Lazy receipts, which never shorten grace.
- The two-write publish, which the compare-and-swap fences.
- Pins, unpins, and retirement, which serialize with prune intents.
- Inherited validity.

These are manual checks. The crypto-shredding RFC has had only the author's
review so far.

### Remaining issues, updated

Round 2's list, with these changes:

- **2. Data erasure** — addressed by the companion RFC, a draft pending DPO
  sign-off. Its open questions matter most, above all whether erasure is
  per account or per individual.
- **3. Cross-dataset write access** — resolved: consumers only create
  records in their own `.holds/` subfolder of each source.
- **4. Coordination throughput** — downgraded to a documented limit. A
  publish writes `LATEST` twice, consumers add no `LATEST` traffic, and GC
  adds one write per prune batch. This is fine for batch pipelines.
- **5. Schema mistakes** — a workaround exists within the same table (see
  Context).
- **7. Growth** — acceptable with an incremental GC index (see Context).

Items 1, 6, and 8–10 are unchanged. Item 1, the lack of an implementation
and model check, remains the largest risk.

## Round 2 (2026-09-29): readiness review

### Context

**GRV v2 is for a new store. There is no v1 data to migrate, and no
earlier-draft writers will share the store.** The specification now says so
(Overview, Trade-offs), the migration and mixed-writer caveats are gone,
and `GRV_DIR/grv.json` records `format_version: 2` so that a later format
can detect this one and this one refuses a later format.

The review asked whether [grv-v2.md](grv-v2.md) was ready to roll out for
a small team. Verdict before this round: the core layout (§1–4, §7) was
strong; claims, runs, and publish (§5, §6, §8) were sound in intent but
under-specified; the retention subsystem (§9–10: holds, pins, GC) was not
ready. It had five blockers, ten significant issues, and a list of smaller
fixes, all addressed below.

### Findings and resolutions

Blockers:

| # | Finding | Resolution |
|---|---|---|
| B1 | Holds were taken per derived *version*, each checking the entire source state and running a durable operation on the source `LATEST`: about 1,000 serialized operations for a 500-partition, two-source run, well over an hour on GCS. | One hold per (run, source revision), taken at run start (§6, §9). The completeness check is immediate when the source revision is the current `LATEST` or already pinned or held. A hold is released once its run is sealed and every entry citing it is tombstoned, so abandoned allocations no longer pin sources. GC runs per dataset: holds replace the cross-dataset `derived_from` closure (§10). |
| B2 | "Pending" counted orphan revisions, so a publisher that lost its lease to GC left an orphan revision that made its new versions unprotected at once. | Only committed revisions count (§7, §10); orphans are ignored. |
| B3 | No conflict rule: the last publisher won even when built from older inputs, and a regression looked like a rollback. | Runs record `base_revision` (§6). A publish step applies a change set of runs, omissions, and selections; first committer wins per (table, partition); selections are the explicit override for rollback and repair (§8). |
| B4 | Schema equality compared raw Parquet types, so writers that encode the same column differently (INT96 vs TIMESTAMP_MICROS, decimal width, list naming, REQUIRED vs OPTIONAL) collided, permanently. | Canonical logical schema with a Parquet-to-GRV type mapping; repetition, wrapper names, and field ids are ignored; `INT96` and `INTERVAL` are rejected (§4). |
| B5 | Record formats missing: `.schema.json`, run control and journal, operation payloads, hold and marker bodies. | Specified: `.schema.json`; run control with a transition table; per-allocation records; the operation description and a payload per kind; `change_set`; hold, pin, release, receipt, `.pruned`, and `.retired` bodies; replay "same fact" rules (§4, §6, §8–§10). |

Significant:

| # | Finding | Resolution |
|---|---|---|
| S6 | Every publish revalidated every entry of the state. | Inherited validity: carried-over entries stay publishable; only changed entries are validated (§7, §8). |
| S7 | Pins were permanent; one mistaken table pin meant unbounded growth. No erasure path. | Pins have ids and an `unpin` operation (§10). Data erasure is listed as out of scope (remaining issue). |
| S8 | Hot single objects: every task CAS-merged one run journal; GCS throttles one object to about one write per second. | Per-allocation records replace the shared journal (§6); consecutive `LATEST` transitions may share a CAS (§8); limits documented (Trade-offs). |
| S9 | No upper bound on lease TTLs; no break-glass. | `max_lease_ttl`, plus expiry after observing an unchanged validator for that long (§2, §5). Runbooks remain out of scope. |
| S10 | Consumers must write to upstream `LATEST` and `.states/`. | Documented as a trade-off (remaining issue). |
| S11 | Grace ran from manifest creation, so a run longer than `pending_grace` could lose its first outputs before sealing. | Versions of unsealed runs are protected; grace runs from the run's `sealed_at`; any process may recover an expired run (§6, §10). |
| S12 | `derived_from` required one reference per (table, partition). | `table` and `partition` are optional: whole-revision or table-level references (§4, §9). |
| S13 | The GC closure over `derived_from` duplicated the holds and forced a global fixpoint. | Removed; holds are the only cross-dataset protection (§10). |
| S14 | No format version or shared parameters. | `grv.json`: format version, `max_clock_skew`, `max_lease_ttl`, `pending_grace` (§2). |
| S15 | The local validator (inode, mtime, size) allowed an ABA compare-and-swap after inode reuse. | The local validator is the SHA-256 of the content (§1). |

Smaller fixes:

- "Publish" is reserved for revisions; versions are **committed** (§4–§6).
- Ambiguous outcomes (timeouts that actually committed) are resolved by
  rereading content or `mutation_id` (§1); §5 step 3 no longer assumes a
  failed create means a foreign writer.
- Creating the publish operation description is an explicit step (§8
  step 5); revisions carry `grv.operation_id` (§7).
- Removed: a leftover sentence about the old schema baseline, and the
  unused `put` backend operation.
- `version` and `revision` are reserved partition key names (§3).
- An absent `LATEST` alongside existing revisions is a protocol violation
  (§8).
- Self-derivation is disallowed, with `base_revision` providing
  self-provenance (§9). Deriving from orphan revisions is disallowed
  (§9 step 2).
- Revision numbers may have gaps (§7).
- The layout trees show `.pins/`, `.pruned`, `.holds/`, the release
  markers, and allocation records (§2, §3).
- Conventions section covering RFC 2119, identifiers, and JSON equality;
  state and transition tables for claims and run controls (§5, §6).
- "Tombstone" now means only `.pruned`; empty versions are called empty
  versions (§4).
- Local temporary-file naming and cleanup (§1); claim takeover clears
  `version`, `released_at`, and `outcome` (§5); the GC never-delete rule is
  stated positively (§10).

### Decisions taken in this round

These choices were needed to write the fixes. The team can revisit them;
each would be a local change.

- **First-committer-wins per (table, partition)** (B3). The alternative,
  last-writer-wins with version-number monotonicity, would not catch stale
  inputs.
- **Holds per (run, source revision), revision-wide, and transitive.** One
  pinned product revision keeps every upstream revision its versions were
  derived from, through intermediate datasets. This matches round 1's
  retention intent.
- **Nullability is not part of the logical schema.** Non-null rules,
  including for `_{k}_` partition columns, are data rules.
- **No self-derivation.** Holding one's own revisions would keep a
  dataset's whole history; `base_revision` records the prior state instead.
- **Pins are reversible; there is still no purge.**
- **Allocation records are separate objects**, and the sealed control's
  `entries` are authoritative: late records are orphans.

### Self-review

Method: a full reread by the author, plus two independent reviews of the
revised text — one checking every finding and the internal consistency, one
adversarial search for interleavings that break safety or liveness. Both
reviews were repeated after the fixes below. `git diff --check` passes.

The reviews found these problems in the first revision, all now fixed:

| Problem found | Fix |
|---|---|
| The hold fast path trusted "superseded less than `pending_grace` ago", which is unsafe once an administrator changes `pending_grace`. | Removed. The fast path now requires the current `LATEST`, or an active revision pin or hold. Only GC reads `pending_grace`, and it does so under the lease (§2, §9). |
| Using the receipt as the committed test hid a lost receipt, so the revision looked like an orphan. | GC walks the chain; a chain member without a receipt is kept and reported. The receipt remains a shortcut for holds and pins, where a false negative only fails closed (§7, §10). |
| A dataset-level cycle (A → B → A) retains both histories forever, just like self-derivation. | The dataset derivation graph MUST be acyclic. It is documented, not detected (§9, Trade-offs). |
| An ambiguous `conditional-put` was treated as proof of failure even when the write had landed and was then overwritten. That could orphan a finalized version, or make a publisher recompute over its own revision. | Success is proven only by the caller's own `mutation_id`; otherwise the outcome is unknown (§1). Claims keep `last_release`, and acquirers update the releasing holder's allocation record (§5). Unknown operation and publication outcomes are resolved after reacquiring the lease (§8). |
| A run adding a new partition could resurrect a table that another publication had omitted. | That is now a conflict (§8). |
| A helper without the lease could clear `pending` over a newer operation, letting a pruned version be published. | Only the lease holder clears `pending`, by a CAS on a read that names its own operation (§8). |
| Smaller gaps: no `change_set` shape; operation description fields only in an example; marker timestamps outside the replay rules; claim takeover left stale fields; a legacy repeated-field mapping that also caught maps; unsafe temp-file cleanup; an incomplete never-delete list; the change-set example violating its own rule. | Specified or corrected (§1, §4, §5, §8, §10). |

Schedules checked against the final text, besides round 1's ten:

1. **A publisher loses its lease to GC after writing revision n.** The orphan
   n is ignored when computing pending versions, so the run's versions keep
   their protection and a retry can still publish them.
2. **Runs R1 and R2 share base b and both write t/p.** The first to publish
   wins; the other conflicts and is rebuilt.
3. **Source GC races a run's hold on S@s.** Both commit through S's
   `LATEST`: either the completeness check sees the tombstones, or GC keeps
   the whole of s.
4. **The target GC prunes a derived entry while its hold is being
   released.** Release needs the run sealed and every citing entry
   tombstoned. Both are irreversible, so no target lease is needed.
5. **A run takes longer than `pending_grace`.** Its versions are protected
   until it seals, and grace runs from `sealed_at`.
6. **A task's claim release lands but the reply is lost, and another run
   takes over the claim.** `last_release`, or the acquirer's
   allocation-record update, proves the outcome.
7. **Two local CASes after inode reuse, within one timestamp tick.** The
   content-hash validator differs because `mutation_id` differs.
8. **A client writes `expires_at` a year ahead.** Observers treat the claim
   or lease as expired once the validator has been unchanged for
   `max_lease_ttl + max_clock_skew`.
9. **Writers A (pyarrow) and B (Spark with `TIMESTAMP_MICROS`) write the same
   table.** Their logical schemas are equal.
10. **Pin and prune contend, then the pin is released.** The prune decision
    or the pin commits first, as in round 1. After an unpin, the next GC pass
    may prune.

The adversarial review also confirmed these properties hold:

- Version and revision numbers are never reused.
- A stale claim holder never reaches a run file.
- There is exactly one sealed payload per run.
- Every run entry was finalized under its own claim token.
- Nothing can prune, pin or hold between validation and commit.
- Inherited validity holds.
- Hold-release conditions are irreversible and never depend on a deleted
  manifest.
- Pending protection shrinks only through irreversible events.
- A pin cannot override a committed prune.
- Each revision gets at most one receipt.
- Races on allocation records settle on a single terminal state.

All of these are manual checks: no executable model or implementation
exists yet.

### Remaining issues

Every round 2 finding is resolved in the text, except for the items below,
which the specification now lists as trade-offs or out of scope.

1. **Nothing is implemented or model-checked.** This is the largest
   remaining risk. The protocol has six interacting state machines: claims,
   run control and allocation records, the `LATEST` lease, durable
   operations, holds, and GC. Before enabling GC, build an in-memory backend
   that injects faults (committed-but-timed-out requests, pauses past TTL,
   clock skew), a conformance suite, and ideally a TLA+ or P model of
   `LATEST`, operations, holds, and GC.
2. **Data erasure.** There is no purge path for legal deletion requests
   (out of scope). It is required before golden records hold personal data.
3. **Cross-dataset write access.** Runs write holds into their sources'
   `.states/` and CAS their `LATEST`, so producer and consumer datasets
   cannot be separated by least-privilege permissions.
4. **Coordination throughput.** Every coordination record is a single
   object, and GCS throttles one object to about one write per second. A
   publish costs at least three `LATEST` writes, and a run start costs about
   three writes per input source. That is fine for batch use, not for
   high-frequency publishing.
5. **Schema mistakes are permanent.** A wrongly registered column can only
   be fixed with a new table. Writers should register declared schemas
   only.
6. **An acyclic dataset graph is required but not enforced.** Enforce it in
   deployment configuration, for example with dataset layers.
7. **Growth.** GC scans every committed revision, and allocation listings and
   durable records grow without bound (compaction is out of scope). This is
   acceptable at small-team scale, but monitor it.
8. **Runbooks and backup/restore** are out of scope: protocol violations,
   stuck pending operations, and restoring a whole root while writers are
   stopped.
9. **Accepted liveness limits:**
   - A finalized version can still end up `unproven` after repeated claim
     takeovers if the acquirer skips the SHOULD update.
   - An abandoned tail version without a manifest stays until a newer version
     is published.
   - Revision-wide, transitive holds over-retain inputs.
10. **Size.** The specification grew to about 1,780 lines, mostly record
    formats and state tables. Moving record formats into an appendix would
    help readers.

**Rollout recommendation for a small team.** Implement and roll out phase 1
now: the layout, claims, runs, revisions, publish with conflicts, and sync.
Write holds from day one, since they are one cheap operation per input per
run and avoid a backfill later. Keep GC disabled, retaining everything,
until item 1's fault-injection tests (and ideally the model) pass. Then
enable GC, pins, and retirement. Decide item 2 before any personal data is
stored.

## Round 1 (2026-09-28): concurrency and recovery review

Reviewed 2026-09-28 against the seven reported issues. This checkout has no
`rfc/` directory; the matching document is [grv-v2.md](grv-v2.md). All seven
issues were present, and the specification now defines protocols addressing
them. These are specification changes; there is no implementation in this
checkout to validate against the revised contract.

Round 2 later revised several of these mechanisms: holds are taken per run
rather than per derived version, pins are reversible, runs use
per-allocation records instead of a shared journal, and schemas are
compared in logical form. The text below records round 1 as it was.

### Changes and decisions

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

### Crash and interleaving review

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

### Tradeoffs adopted

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
