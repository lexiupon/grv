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

| Requirement                                    | Evidence                                                              | Status                     |
| ---------------------------------------------- | --------------------------------------------------------------------- | -------------------------- |
| Reviewed source, lockfile and contracts frozen | Commit `b3c5f6e`, tree clean, nothing pushed                          | evidenced                  |
| Full automated run, scripts and lint           | `~/grv-evidence/grv-task28-gate-run/`                                 | running at time of writing |
| Release-profile build of the release profile   | Native rebuild window                                                 | not run                    |
| Declared MSRV proved or explicitly revised     | Declared `rust-version = 1.89`; proof runs on the available toolchain | not run                    |
| Binary hashes bound independently              | Parent-side re-hashes only so far                                     | deferral (this gate)       |

## G2 Installation

| Requirement                                                                                        | Evidence       | Status  |
| -------------------------------------------------------------------------------------------------- | -------------- | ------- |
| Relocated release bundle; protected directory and tarball installs                                 | Packaging work | not run |
| CLI and both production adapters and native dependencies load without build tree, cache or network | Packaging work | not run |
| Representative production workflow on the installed form                                           | Packaging work | not run |

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

## What closes where

This document is an input to review. It closes nothing by itself. Gate closure happens when the runs
against the frozen candidate complete, and publication stops for explicit approval.
