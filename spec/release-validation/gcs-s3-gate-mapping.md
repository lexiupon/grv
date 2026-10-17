# G1-G6 gate mapping for the cloud-views package

Parent-side mapping of each gate requirement to the evidence that exists, with deferrals named.
This document does not change any checkpoint status and does not constitute release readiness.
Gate closure happens against the frozen candidate, not here.

Frozen source for this work: `b3c5f6e` plus `7d56a6e`. Nothing is pushed. `gcs-view` remains
unadvertised.

Status words used: **evidenced (scoped)** — a named run or reviewed source inspection supports it;
**partial** — recorded as partial by its own checkpoint; **not run**; **deferral** — explicitly
deferred for review.

## G1 Candidate

| Requirement                                    | Evidence                                                                                                                                                                                | Status                         |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| Reviewed source, lockfile and contracts frozen | Commit `b3c5f6e`, tree clean, nothing pushed                                                                                                                                            | evidenced                      |
| Full automated run, scripts and lint           | CI run `38863068396`, commit `2996b83`, **four platforms** (osx_arm64, osx_amd64, linux_arm64, linux_amd64), native features. Local offline chunk `~/grv-evidence/grv-task28-gate-run/` | **evidenced**                  |
| Release-profile build of the release profile   | CI built the native guard and ran tests; the Rust release profile itself was not built on this path. Byte-identical rebuild still pending — see note below                              | not run                        |
| Declared MSRV proved or explicitly revised     | CI installs and uses declared `rust-version = 1.89` on all four platforms, including full native tests and strict lint                                                                  | **evidenced (four platforms)** |
| Binary hashes bound independently              | Parent-side re-hashes only so far                                                                                                                                                       | deferral (this gate)           |

## G2 Installation

| Requirement                                                                                        | Evidence                                                                                                                                                               | Status                                                 |
| -------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Relocated release bundle; protected directory and tarball installs                                 | CI run `38863068396` verifies **relocation** on four platforms (artifacts uploaded from each). Protected directory and tarball _installs_ are not covered by that step | **partially evidenced** — dev profile, relocation only |
| CLI and both production adapters and native dependencies load without build tree, cache or network | Packaging work                                                                                                                                                         | not run                                                |
| Representative production workflow on the installed form                                           | Packaging work                                                                                                                                                         | not run                                                |

## G3 Distribution — no preview exemption

| Requirement                                               | Evidence       | Status  |
| --------------------------------------------------------- | -------------- | ------- |
| Exact shipped dependency notices, licences and provenance | Packaging work | not run |
| Checksums                                                 | Packaging work | not run |
| Artifact, secret and fixture scan                         | Packaging work | not run |

## G4 Safety

| Requirement                                                   | Evidence                                                                                                                                               | Status                      |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| Exact omission and complete-snapshot behaviour                | Per-shape verification on native ground truth; single-file shapes, three-file aggregate, typed all-null shape                                          | evidenced (scoped)          |
| Refusal shapes                                                | Refusal shape 212 / HTTP 206; all four denials refused including after a warm read                                                                     | evidenced (scoped)          |
| Cross-file evolution on the native path                       | Not reachable on this path                                                                                                                             | **deferral**                |
| Type contradiction on the cloud path                          | Not reachable on this path                                                                                                                             | **deferral**                |
| Destination commit/receipt and publication ambiguity          | Refresh, source-fault and nonempty-COMMIT work; durable recovery                                                                                       | evidenced (scoped)          |
| Accepted/terminal source-free replay and mismatch refusal     | Stale-peer and history work; terminal slices; fail-closed history rules                                                                                | evidenced (scoped)          |
| Cancellation and stopped writers                              | Cooperative cancellation and finite recovery, closed **once** on the shared path and credited to both this gate and the in-process transition          | evidenced (shared)          |
| Preparation and traffic accounting                            | Counters cover accepted protected-reader GET bodies and HEAD/GET attempts. Application-layer estimate method stated; TLS excluded and named unmeasured | evidenced (stated coverage) |
| Live Salesforce mutation, omission, reset and restored oracle | Platform gate, not this package                                                                                                                        | **deferral**                |

## G5 Integrations

Nothing in this package is **advertised**. Per the gate, primitives and fixture CLI do not qualify a
production integration, so this gate is closed by an explicit reviewed qualification limitation
rather than by a green run:

- Ground truth for multi-file reads is computed independently in Python from local fixtures, because
  the verified local seam admits one descriptor per call. This difference is disclosed in the plan and
  the code.
- Offline mirror tests and successful builds do not qualify native manifest grammar.
- Modelled faults are a real durable write plus an authored refusal, not a real lost HTTPS response.

| Requirement                                                                  | Status                                         |
| ---------------------------------------------------------------------------- | ---------------------------------------------- |
| Production candidate lifecycle per advertised provider and transport         | Not applicable while nothing is advertised     |
| Independent exact values, schema, bytes, no-op and replay                    | evidenced (scoped), with the limitations above |
| Reviewed qualification limitation recorded instead of a full integration run | **this document, pending review**              |

## G6 Review

| Requirement                                                  | Evidence                                                                                                                                                                                    | Status                                                 |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Exact critical scenario mappings                             | `spec/s3-shipping-security-signoff.md`, 15 claims against recorded evidence, 3 resting on source inspection plus tests                                                                      | evidenced (parent review, not independent third-party) |
| Explicit noncritical deferrals                               | The deferral rows above                                                                                                                                                                     | this document, pending review                          |
| Capability, platform and recovery limits                     | Security signoff gap list; no universal hard stop; no finite HTTP overrun; no heap-erasure, escaped-owner, allocator or RSS claims; logical admission budget is not aggregate memory or RSS | evidenced (stated)                                     |
| Immutable evidence archive                                   | `~/grv-evidence/` consolidation with `MAPPING.md` as resolver; historical checkpoints not rewritten                                                                                         | evidenced                                              |
| Compatibility baseline                                       | Packaging work                                                                                                                                                                              | not run                                                |
| Separate approval for final tag, destination and publication | Awaiting explicit approval                                                                                                                                                                  | open                                                   |

## Recorded partials

These stay partial. They are not folded into a pass by this mapping:

- `gcs-native-resource-fault-checkpoint` — partial native resource and fault checkpoint.
- `gcs-isolated-owner-checkpoint` — partial implementation and supplemental proofs.

## Single-occurrence failures retained

Both remain recorded as they happened. Neither is called a pattern:

- Multi-file aggregate failed once at code 212 with status 0; it passed on the two rounds after.
- `zstd-dictionary-no-statistics` failed once at phase 32 with status 0; it was green before and after.
- An observation round for the refusal shape ran with the wrong expected code and is kept as a failure;
  the expectation was corrected to 212 on the strength of the observed round.

## What this run changed

- `build.py` previously built five targets while `CMakeLists.txt` registered tests for fourteen
  executables. The four unbuilt targets were exactly the tests reported `Not Run` on the first run of
  this work. Fixed in `3d4ffaa`; the guard suite now passes on all four platforms.
- The guard was rebuilt from committed source and **its digest changed**: `2709cc619eb125d1…` →
  `be5ce03b192047f14e74…`. The previous binary had been built from sources other than the committed
  ones, so `notices/native-build.json` was refreshed as the reviewed source-change the register was
  holding. **Historical checkpoints citing the old digest are not rewritten.**

**Standing caveat, stated plainly:** the refreshed record was produced by a build **on this machine**.
**Decision (2026-10-16): byte-identical reproducibility across locations is NOT claimed for this
project on this toolchain, and is dropped as an acceptance item.** A clean run of the declared
procedure (`build.py`, fresh extraction from the pinned tarball, archive and member verification
passing, guard suite 13/13) produced `a5ccfa88d53170c5907d6f39dc0f0c025b25775455a142c95680fa054fb757d8`
— different bytes from the recorded `be5ce03b…`, as expected once path embedding is understood.
Path remapping (`-ffile-prefix-map`) stays an optional enhancement for the #30 window.

Provenance for the native build therefore rests on five things, all of which are in force: pinned
inputs verified at fetch; the declared procedure verifying every archive member except explicit patch
sites; digests recorded from clean runs; **custody — the artifact that produced a cited digest stays
on disk** (both `be5ce03b…` and `a5ccfa88…` are retained; the dead `35f92611…` was overwritten during
the reviewed rebuild and its historical checkpoints are paper-only, not rewritten); and behavioural
tests against the recorded binary (15/15 original tree, 13/13 fresh tree, four platforms green on CI,
reproduced on a second full run `38887075813`). The record is Mach-O specific
(`mach_o_architecture`, `mach_o_minimum_os`), so it can only attest the macOS build identity; the
Linux platforms have no equivalent binding record. CI packaging runs `--profile dev` **without**
`--require-distribution-ready`, so on that path the native-library binding check is **not enforced**
and a non-matching guard binary is accepted.

## What closes where

This document is an input to review. It closes nothing by itself. Gate closure happens when the runs
against the frozen candidate complete, and publication stops for explicit approval.
