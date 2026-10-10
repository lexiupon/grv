# Native cloud-view experiment: findings and limitations

This is evidence for the [main RFC](../../../spec/grv-cloud-views-pushdown-v1-rfc.md),
not a separate proposal. This directory is an isolated feasibility experiment;
no production implementation or capability is added by these files.

## Question and evidence

Can the existing GRV per-file cast/typed-NULL `UNION ALL` SQL remain intact while
the pinned native Parquet scanner reduces range payload bytes? **Yes, for the
identity casts and fixtures tested here.** The recorded simulated transport run
shows projection, ordered row-group predicates, and typed-null branch pruning.
This is stronger evidence than EXPLAIN alone, but not cloud integration evidence.
See README and `results.json` for the executable experiment and counters.

## Proposed direction, subject to follow-up validation

Keep SQL schema evolution separate from file authority. A verified exact object
set should be immutable for a query lifecycle. The native FS should accept only
those exact identities, expose seekable read-only handles, and split scanner
reads into bounded ranges. Scanner pushdown can then avoid irrelevant columns
and row groups without broadening authorization to prefixes, listings or HTTPFS.

A real transport callback would need to enforce conditional reads and response
identity/size/range validation before exposing bytes, with pinned generation or
version plus ETag as appropriate to each backend. The simulator models those
checks with immutable bytes and mutable validators; it does not implement them
against an actual service.

Predicate safety and authorization must not depend on optimizer decisions. Even
an eliminated branch may bind/read its footer. No-statistics files and functions
without usable statistics must remain correct; resource budgets must tolerate
full reads. Identity casts can disappear during optimization; lossy or semantic
casts must retain the engine's semantics, never be rewritten just for savings.

## Wrinkles found

1. Footer/bind traffic survives branch elimination; "zero bytes" is the wrong
   acceptance criterion. Compare data-scale traffic against cold wide scans.
2. Filters lacking min/max pruning can still save bytes via native late
   materialization. The no-stats selective scan was only 40% larger than the
   stats scan, not the assumed >2x. Full-match function filtering proves that
   savings are not guaranteed.
3. With the native external cache disabled, mutations between callbacks fail.
   A cache-enabled engine could return previously validated bytes without a new
   transport check. Production must choose an explicit immutable-version cache
   key/lifecycle or cache policy; this experiment makes no guarantee there.
4. This engine reports v1.5.6 but an empty source ID. Archive/build attestation and
   matching-header ABI checks remain necessary; README records the tested dylib
   checksum.
5. Both scheme names use the same simulated implementation. This demonstrates
   URI identity/API compatibility, not independently validated S3/GCS protocols.
6. Exact string allowlisting succeeds for awkward URI characters, but production
   canonicalization and decoded filename aliases require separate adversarial
   tests before adoption.

## Follow-up gates (not implemented here)

- Real S3 version/If-Match and GCS generation preconditions: response status,
  content-range, response validator, redirects, truncation and retry semantics.
- Production filesystem/guard end-to-end tests; HTTPFS is neither required nor
  validated by this prototype.
- Cache-policy mutation tests during binding, streaming and prepared reuse;
  cancellation, multi-threaded reads and bounded memory/concurrency accounting.
- Schema evolution with many branches, widening/rescaling/timezone casts,
  nullable source columns, nested data and compressed/dictionary-encoded files.
- Exact canonical URI parsing and alias-collision rejection, no fallback or
  allowlist bypass through scanner APIs.
- Snapshot acquisition/verification cost accounting: full content verification
  may dominate the savings unless valid pinned immutable identities can be reused.
- Range request economics on real latency/pricing: fewer bytes need not mean
  fewer requests (the full-match predicate increases callback count here).

## Acceptance boundary

The current experiment is complete when its oracle, relative-byte and refusal
assertions pass using the existing pinned build, and output clearly identifies
simulated transport. Adoption requires the follow-up gates, separate review and
production changes outside this directory. No production cloud-reader claim is
made by the current PASS.
