# Register refresh review: the ten stale source pins

Task #64 prepares the parent review. It proposes one action for each stale pin line. It
changes nothing. The register, the historical checkpoints, and the plan stay as they are
until the parent gives dispositions.

Rules honoured here:

- byte-identical reproducibility is not an acceptance item (decision of 2026-10-16);
- a shared-path gate closes once, and the closure is credited to both gates;
- historical checkpoints are not rewritten;
- a single-occurrence failure stands as a failure;
- a refusal code belongs to one call site, so it cannot be split per mechanism;
- the guard digest moved from `2709cc61…` to `be5ce03b…` by a reviewed rebuild from
  committed source, and the suite passed 15 of 15 after it.

## 1. Checker output (verbatim)

```text
source changed; review and refresh the register: spec/grv-v1-release-validation.md
exit=1
```

The checker stops at the first pin mismatch. It names one file. The full stale set below
came from a direct digest comparison of all eleven pins.

## 2. Pin state

| Line | Cited file                                        | Pinned digest | Current digest | Status |
| ---- | ------------------------------------------------- | ------------- | -------------- | ------ |
| 7    | `spec/grv-v1-release-validation.md`               | `daee718ed18` | `a6dc39512e2`  | STALE  |
| 8    | `spec/grv-client-v1.md`                           | `fda37dad8e5` | `c6070ffe88d`  | STALE  |
| 9    | `spec/grv-client-v1-execution.md`                 | `ed0356cfaa9` | `399dcf16318`  | STALE  |
| 10   | `spec/grv-adapter-protocol-v1.md`                 | `c9ac36e6d16` | `520bbcb530f`  | STALE  |
| 11   | `spec/grv-storage-v2.md`                          | `6e1389a74a5` | `06654e0dba7`  | STALE  |
| 12   | `spec/grv-client-v1-declaration.schema.json`      | `f12eac416a6` | `c2ac33e9d0e`  | STALE  |
| 13   | `spec/grv-client-v1-command-output.schema.json`   | `407ffad4cb1` | `d19b7354e2b`  | STALE  |
| 14   | `spec/grv-client-v1-build-completion.schema.json` | `2757b3d0a4f` | `a98d9aae4b5`  | STALE  |
| 15   | `spec/adapters/duckdb.schema.json`                | `d4672070def` | `5a6845b55d5`  | STALE  |
| 16   | `spec/adapters/salesforce.schema.json`            | `250e77fbe9f` | `2437532136c`  | STALE  |
| 17   | `spec/fixtures/identity-v1.json`                  | `76bf7ad5d96` | `76bf7ad5d96`  | ok     |

All eleven stored digests match the tree at `52ee088`. So every change below happened after
the pins were taken. Two commits did the work:

- `9900dff` — "Adopt KB tooling standard: prettier, markdownlint-cli2, downlint via prek".
  This is a formatting commit. It touched all ten files.
- `8c5a773` and `4a17671` — the coordinated `gcs-view` record variants. These are content
  commits. They touched five of the ten files.

## 3. How each line was classified

Two methods, run independently on every file.

1. Parsed comparison. For each JSON schema, the pinned bytes were parsed and compared with
   the current bytes. Three files are parse-identical: lines 12, 14, 16. Their only change
   is formatting.
2. Token comparison. For each Markdown file, the two versions were reduced to content tokens
   after Markdown syntax was normalised (heading form, emphasis form, table widths, fenced
   block language). What is left is the normative change. Lines 7 and 11 have none. Lines 8,
   9, 10, 13, 15 have the `gcs-view` additions only.

A line whose delta is formatting only needs no new evidence. A line with a normative delta
needs evidence produced after the change, at the layer where that claim lives.

## 4. Per-line review proposals

| Line | Cited file                                        | What changed since the pin                                                                                                                                                       | Post-change evidence                                                                                                                                                                                                                                                                                  | Recommendation | Basis                                                                                                                                                                                                                                |
| ---- | ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 7    | `spec/grv-v1-release-validation.md`               | Formatting only. Table column widths.                                                                                                                                            | None needed. The 51 release cases in the register still match the current text word for word.                                                                                                                                                                                                         | RE-PIN         | No normative change. The tooling commit `9900dff` is the whole delta.                                                                                                                                                                |
| 8    | `spec/grv-client-v1.md`                           | Formatting, plus `gcs-view` added to the advanced adapter option list (line 579).                                                                                                | The value reaches a runtime today only through conformance fixtures (`crates/grv-conformance/src/bin/fixture-duckdb-build.rs`, `fixture-stale-pull.rs`, `fixture-duckdb-gcs-build.rs`). Production advertisement does not emit it.                                                                    | DEMOTE         | The claim here is what a shipping user declaration may author today. Its basis is the advertisement record, and the advertisement excludes the value until capability activation (#29). The formatting remainder needs no re-review. |
| 9    | `spec/grv-client-v1-execution.md`                 | Formatting, plus `gcs-view` added to the `materialization_mode` list (line 942, prose above the scenario region).                                                                | The offline closure round of 2026-10-17, 15 of 15, credited once to both gates. The four-platform CI runs `38863068396` and `38887075813`, on bytes identical to the current file. The pinned-path refusals seen live: mixed shapes `211/206`, contradiction `212/206` in `grv-s3-shipping-madrrsjm`. | RE-PIN         | The added value is the reviewed frozen change set (`b3c5f6e`). The 108 derived rows are text-identical. Only the anchors moved.                                                                                                      |
| 10   | `spec/grv-adapter-protocol-v1.md`                 | Formatting, plus `gcs-view` at three grammar sites (the capability array, `File.access`, `InputBinding.materialization`) and a new block on GCS-view provenance and preparation. | The preparation and traffic-accounting decision (`7d56a6e`). The aggregate-metadata and per-shape rows recorded as evidenced, with scope stated. The OAuth isolation proof. The live refusals above. The live GCS leg `grv-shipping-matrix-yl5kcolh`. The four-platform CI runs on current bytes.     | RE-PIN         | The added text is the frozen reviewed change set. Each claim it adds already sits with a post-change record at its own layer, and each limitation is named there. The 61 derived rows are text-identical. Only the anchors moved.    |
| 11   | `spec/grv-storage-v2.md`                          | Formatting only. Emphasis form, heading form, table widths, fenced-JSON reflow, fence language.                                                                                  | None needed. No register row derives from this file.                                                                                                                                                                                                                                                  | RE-PIN         | No normative change. Token comparison after syntax normalisation shows nothing.                                                                                                                                                      |
| 12   | `spec/grv-client-v1-declaration.schema.json`      | Formatting only.                                                                                                                                                                 | Parse-identical to the pinned bytes. The compiled-in copy at `crates/grv-core/src/declaration.rs:161` was exercised by the post-change runs.                                                                                                                                                          | RE-PIN         | No normative change.                                                                                                                                                                                                                 |
| 13   | `spec/grv-client-v1-command-output.schema.json`   | Formatting, plus `gcs-view` added to `$defs/materialization/properties/materialization_mode/enum`.                                                                               | Post-change runs reach the new value through fixtures and offline rounds only. No post-change run produced the record through a shipping user declaration.                                                                                                                                            | NEEDS-RERUN    | Offline mirror tests and successful builds cannot qualify native grammar. The accepted residual states that shipping-declaration reachability needs a new method or a live round. See section 5.                                     |
| 14   | `spec/grv-client-v1-build-completion.schema.json` | Formatting only.                                                                                                                                                                 | Parse-identical to the pinned bytes.                                                                                                                                                                                                                                                                  | RE-PIN         | No normative change.                                                                                                                                                                                                                 |
| 15   | `spec/adapters/duckdb.schema.json`                | Formatting, plus `gcs-view` added to three enums: `$defs/options/properties/materialization`, `$defs/engine_materialization`, `$defs/pull_result`.                               | Post-change runs reach the new value through fixtures and offline rounds only. Production advertisement for this adapter emits `s3-view` (`crates/grv-adapter-duckdb/src/process.rs:65`). The four-platform CI runs are green on bytes identical to the current file.                                 | NEEDS-RERUN    | The shipping-declaration path for the added value has never run, and offline evidence cannot close it. See section 5.                                                                                                                |
| 16   | `spec/adapters/salesforce.schema.json`            | Formatting only.                                                                                                                                                                 | Parse-identical to the pinned bytes.                                                                                                                                                                                                                                                                  | RE-PIN         | No normative change. The live Salesforce oracle stays at the platform gate and is not pulled forward.                                                                                                                                |

Counts: RE-PIN 7, NEEDS-RERUN 2, DEMOTE 1.

## 5. The one run that would close lines 13 and 15

One live round on the current guard bytes (`be5ce03b…`):

- author one shipping user declaration that carries `materialization: gcs-view`;
- run it through the production CLI and the DuckDB adapter, not a fixture binary;
- record the declaration digest, the guard digest, the command output record, and cleanup.

This round cannot pass on today's advertisement, because the production adapter does not
advertise the value. It needs capability activation (#29) or a new agreed method. Until one
of them exists, the fixture-level coverage stays scoped, and lines 13 and 15 stay open.

## 6. Mechanical consequences when the register is refreshed

- All 169 row anchors moved (108 execution rows, 61 protocol rows). The refresh must
  re-derive every line number. Row text is unchanged for all 169.
- The register stays `status: draft`. All 51 release cases stay `planned` with no evidence.
- Six historical checkpoints cite the superseded guard digest `2709cc61…`. They are not
  rewritten and they do not attest the current bytes. Only
  `spec/release-validation/s3-shipping-scanner-checkpoint.json` cites `be5ce03b…`.
- `grv-s3-shipping-madrrsjm` and `grv-s3-shipping-meqesy0l` stay recorded as failures.
  They are not relabelled as closure evidence.

## 7. Parent review

Dispositions were given on 2026-10-17 and executed in the register. Six recommended re-pins ran
(lines 9, 10, 11, 12, 14, 16) and all 169 anchors were re-derived; no row text changed. Line 8
is demoted, and its pin now matches the current bytes. Line 7 keeps the pre-review reviewed
digest for the reason in its row. Lines 13 and 15 stay open.

| Line | Recommendation | Parent disposition                                                                                                                                                                                                                                                                                                                                                                   | Date       |
| ---- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| 7    | RE-PIN         | RE-PIN recorded, pin left unchanged. Self-pinning is impossible by construction: a refresh changes the bytes this line would have to match. The pin stays on the pre-review reviewed state, and this document is the authority for this refresh batch. No normative change; the delta is tooling commit `9900dff`.                                                                   | 2026-10-17 |
| 8    | DEMOTE         | DEMOTE executed. Pin refreshed to the current bytes. The claim is what a shipping user declaration may author today, and it belongs with capability activation (#29), where the advertisement record excludes the value. The formatting remainder needed no re-review.                                                                                                               | 2026-10-17 |
| 9    | RE-PIN         | RE-PIN executed. Pin and the 108 anchors refreshed; row text unchanged. Basis: the offline closure round of 2026-10-17, 15 of 15, credited once to both gates; the four-platform CI runs `38863068396` and `38887075813`, on bytes identical to the current file; the live refusals `211/206` and `212/206` in `grv-s3-shipping-madrrsjm`; the frozen reviewed change set `b3c5f6e`. | 2026-10-17 |
| 10   | RE-PIN         | RE-PIN executed. Pin and the 61 anchors refreshed; row text unchanged. Basis: the preparation and traffic-accounting decision `7d56a6e`; the aggregate-metadata and per-shape rows recorded as evidenced, with scope stated; the OAuth isolation proof; the live refusals above; the live GCS leg `grv-shipping-matrix-yl5kcolh`; the four-platform CI runs on current bytes.        | 2026-10-17 |
| 11   | RE-PIN         | RE-PIN executed. Pin refreshed. No normative change: token comparison after syntax normalisation shows nothing. The delta is tooling commit `9900dff`. No register row derives from this file.                                                                                                                                                                                       | 2026-10-17 |
| 12   | RE-PIN         | RE-PIN executed. Pin refreshed. Parse-identical to the pinned bytes; the delta is tooling commit `9900dff`. The compiled-in copy was exercised by the post-change runs.                                                                                                                                                                                                              | 2026-10-17 |
| 13   | NEEDS-RERUN    | Open. Pin left unchanged, pending gcs-view activation (section 5). Offline mirror tests and successful builds cannot qualify native grammar.                                                                                                                                                                                                                                         | 2026-10-17 |
| 14   | RE-PIN         | RE-PIN executed. Pin refreshed. Parse-identical to the pinned bytes; the delta is tooling commit `9900dff`. No normative change.                                                                                                                                                                                                                                                     | 2026-10-17 |
| 15   | NEEDS-RERUN    | Open. Pin left unchanged, pending gcs-view activation (section 5). The shipping-declaration path for the added value has never run, and offline evidence cannot close it.                                                                                                                                                                                                            | 2026-10-17 |
| 16   | RE-PIN         | RE-PIN executed. Pin refreshed. Parse-identical to the pinned bytes; the delta is tooling commit `9900dff`. No normative change. The live Salesforce oracle stays at the platform gate and is not pulled forward.                                                                                                                                                                    | 2026-10-17 |

The checker stops at the first mismatch and names one file, so it reports line 7 only. Lines
13 and 15 stay unmatched behind it. Three unmatched lines are the expected end state of this
batch.
