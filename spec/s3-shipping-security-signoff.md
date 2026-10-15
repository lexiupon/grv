# S3 shipping security signoff

Method: a review of claims against evidence already recorded — live rounds, native proof binaries,
offline tests, and source inspection. This is the **parent review required by the working rules**. It
is not an independent third-party security review, and it does not constitute release readiness or
publication approval.

Each claim below states the kind of evidence behind it. Where evidence is a mirror test or a code
review, that is said. Nothing is promoted above what supports it.

## Claims and their evidence

| #   | Claim                                                                                                                                                                                                      | Evidence                                                                                                                                                                                                                           | Status                 |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| 1   | Only the selected identity is used. No ambient fallback, no ADC substitution, no impersonation                                                                                                             | Live: every round records the selected profile, region, account and ARN with `fallback: false`. Offline: env-stripping test refuses a missing profile and drops ambient credentials and proxy                                      | Closed                 |
| 2   | Exactly the authority given: read-only, fixed host, TLS, no redirects, proxy cleared, finite timeouts, pinned retry                                                                                        | Source inspection (`gcs_http_policy.hpp`: proxy fields cleared, zero/unbounded timeouts forced down to a finite cap, fresh params copy carries them) plus the native proof binary and the supplemental signed-header process fence | Closed within evidence |
| 3   | Tokens never appear in argv, environment, logs, receipts, views, durable JSON, profiling or digests                                                                                                        | By construction — the token travels on stdin. Offline: a test asserts provider text does not land in reconstructed evidence                                                                                                        | Closed                 |
| 4   | Mandatory full SHA256, size, schema and row preparation cannot be bypassed                                                                                                                                 | Live: observed on every shipping round; the verified reader runs the full SHA256 verification over the object in bounded chunks before use                                                                                         | Closed                 |
| 5   | Malformed, unreadable or dangling history fails closed. It is not reported as NotCommitted or Uninitialized                                                                                                | Source inspection plus targeted offline tests from the stale-peer and history work                                                                                                                                                 | Closed within evidence |
| 6   | Only a checked ENOENT counts as absence. Links, non-regular files, inventory and read errors refuse                                                                                                        | Source inspection: `lstat`, which never follows symlinks; inventories must be empty, not assumed                                                                                                                                   | Closed                 |
| 7   | Ownership and admission are protected. Unique synthetic resources, atomic intents, absence before creation, exact-generation cleanup, verified 404 afterwards, no wildcard delete, nothing unknown deleted | Live: every round's cleanup record. Refusals observed live, not just asserted — an unexpected inventory entry and a generation mismatch both stop before any delete                                                                | Closed                 |
| 8   | No production reads, no broader permissions, no IAM, ACL, policy or versioning change to existing resources                                                                                                | Live: cleanup reports `iam_changes: false`, `credential_creation: false`, `existing_data_changes: false` on every authorized round. Offline: a versioning change during cleanup refuses                                            | Closed                 |
| 9   | Cancellation is cooperative only. Ownership is retained until native work stops. A failed owner is not reused. No detach, no premature Stopped                                                             | Live: finite cancellation, recovery and terminal slices; the harness reaps the whole process group before returning, including on interruption — that is containment, not a shipping guarantee                                     | Closed as stated       |
| 10  | A known terminal state is not the same as Stopped, even when a cancellation arrives at the same moment                                                                                                     | Live: terminal slices, including the cancellation-ack race                                                                                                                                                                         | Closed                 |
| 11  | Setup failure returns Err with proved restoration or proved absence, adds no Rust poison, and native cloud authority fails closed                                                                          | Live and offline from the setup and admission work                                                                                                                                                                                 | Closed within evidence |
| 12  | The metadata budget is a logical admission (bytes, depth, IDs, per-family finite copies, one retained connection-session), not an RSS claim                                                                | Source inspection of the admission function; no memory claim is made anywhere in this package                                                                                                                                      | Closed as stated       |
| 13  | Holds and preparation give no per-range generation pinning, and no protection against a privileged same-key or same-generation replacement                                                                 | Stated limitation, tested as such. The refusal shape for a missing projected column was observed at 212 with HTTP 206                                                                                                              | Closed as stated       |
| 14  | The export boundary destroys and reopens before staged access                                                                                                                                              | Prior reviewed work on lifetimes and export boundaries                                                                                                                                                                             | Closed within evidence |
| 15  | Corrected tests use fresh artifacts. Nothing is retroactively passed, and no prior qualification is inherited by a corrected run                                                                           | Every corrected run in this package is a new round with its own evidence directory and its own freeze                                                                                                                              | Closed                 |

## Gaps, named rather than smoothed over

- No universal hard stop, and no finite HTTP overrun proof. Cooperative cancellation is what is
  claimed and what is evidenced.
- No heap-erasure proof, no arbitrary-escaped-owner proof, no global allocator or RSS claim.
- No source-to-binary build attestation. Recorded hashes identify _attempted_ artifacts and sources.
- Counters cover accepted protected-reader GET bodies and HEAD/GET attempts only. Headers, rejected
  bodies, TLS and total wire are not measured, and neither is this harness's own object traffic.
  That item is closed as a definition, not as a measurement.
- Cross-file evolution in general is not reachable on the current manifest path, and a declared type
  that contradicts a trial's SQL could not be attributed to a mechanism. Neither is claimed.
- Type variety covered on the local proof path is not cloud coverage. The two are kept separate.
- Retained failures are retained: three multi-file failure rounds, one refusal-shape round that
  failed its then-current expectation, and one round where a settings pair failed at an unclassified
  status. Corrected runs passed afterwards; the failures were not relabeled, and single occurrences
  are not called patterns.
- The offline suite runs on the system interpreter. The pyarrow fixture path is exercised only on
  live rounds, and is checked by source-text assertions offline.

## Conclusion

Security signoff for the S3 shipping package is **closed within the evidence above**, with the gaps
listed as gaps. Three claims rest on source inspection plus tests rather than a dedicated live
observation — they are marked as such. No claim here reaches beyond its evidence.

This does not make the package release-ready. The release register still requires a reviewed source
change before refresh, and publication remains a separate approval.
