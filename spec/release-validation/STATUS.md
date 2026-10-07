# Initial macOS ARM64 release status

Scope: macOS ARM64 0.1.0 initial scoped release, recorded in 0.1.0-SCOPE.md.
Local reviewed candidate commit/freeze and current staging S3 writes are
user-authorized. Exact prior S3 worksheet is being recovered before effects.
User approved pinned upstream release licenses plus conservative bundled
third-party notices; exact upstream CI/SBOM reconstruction is not mandatory.
Actual44engine/55extension noticepayloads reviewed and strictpolicy packaging,
artifactaudit and native/extension/localflow verification passed on historical
releasebytes. No current native notice-text blocker; finalcandidate check pending.
No publication, tag or broad/full-v1 qualification is approved.

Candidate15ec092 builtfromcleansource:strictnoticeverify,actualnative/extension
localbuildpullreplay,protected directory/tarinstall,freshHOME,artifactaudit,
compatibilitybaseline replay PASS. Fullnativeworkspace510PASS22ignored (600s
firsttimeoutretained/ownedorphanreview;1800sretryPASS),strictClippy/scripts36,
Rust1.89alltargets/nativeCTest8 PASS. ProductioncandidateGCSDuckDB+SF900exact
publication/noop/pull/offlinereplay/ownedUUIDcleanup PASS. Newcfgtest-only
SDKcommitcancel/EOF+partialcapture safety tests now PASS;candidate respinneeded.
Runnerprocessgrouptimeoutcleanup improved;privatearchivehelperadded(42offline
scripts currentlyPASS). Task21scopedsafetyclosed:helperinheritableFDisolation
negativecontrolPASS;GCcorruptcoordination zeroeffectassertionsPASS;operational
localattester exactepoch/stoppedacceptedproofandcloudrefusal reviewed.
S3worksheet: s3://private-scope-placeholder/private-scope-placeholder/
profile private-scope-placeholder, region eu-west-1 userauthorized. Productionrecipe
firstpreflight REFUSED ListBucketMultipartUploads AccessDenied beforewrites;
objectlistingAPI passed,parentempty. No multipartcleanupqualification or IAM
changeclaim;permission/explicitlimitationdecision pending.

## Gate board

| Gate / task | Current evidence | Remaining closure |
|---|---|---|
| G1 / #18 | Development workspace 510 passed; followup strict Clippy green. Fresh Rust1.89 all-target check and 298 library tests passed (6 ignored); not frozen-candidate qualification. | Clean reviewed commit; release-profile build; source/binary/native/extension provenance, Rust 1.89 and candidate rerun. |
| G2 / #19 | Fresh release-profile baseline built/relocated; packaged native library/extensions and build/pull/replay loaded. Build-path rpath fallback removed for bundle builds. Dirty/unqualified baseline, not clean-host approval. | Actual release bundle clean-environment relocation/install/load and representative production workflow. |
| G3 / #20 | Rust notice-file gaps zero; 44engine+55extension conservative noticepayloads reviewed under approved policy; historicalstrictpolicy package/audit/load+localflow PASS. Package/verify checks reviewed payload hashes; exact linked graph completeness not claimed. Baseline hash/fixture/heuristic credential scan passed;36offline scripts tests and8nativeCTest pass. | Finish actual reviewed notice payloads; qualify frozen bundle notices/integrity/secret canaries. |
| G4 / #21, #9 | Core/local faults/native kill; SF loss plus scaled live exact mutation/partition move/complete-empty/reset/restored oracle passed; GCS primitive races/fixture replay. | Other critical boundary review and frozen-candidate faults/replay/stopped-work proof. |
| G5 / #22, #9 | Scaled SF rich/eligible Bulk gates pass; actual production release-profile GCS DuckDB managed populated+empty publication/no-op/full verify/pull independent schema/rows/offline replay and900-row SF publication/pull exact oracle/no-op/offline replay passed. | Frozen-candidate production workflows, S3/GCS scope and SDK/full fault qualification. |
| G6 / #23 | Nine exact register mappings(160gaps);12additional safety selectors reviewed. Extended native commit-crash test now proves recovered originalreceipt after newerpull with no rewind/source; passed. No full release case claimed. | Critical mapping review, explicit deferrals/claims, compatibility baseline, durable evidence archive and final review/approval. |

## Evidence already available

`evidence/first-pass.json` preserves 31 log-backed historical executions,
including failures and retries, with log SHA-256 and observed test selectors.
It does **not** invent executed source/binary hashes or exact UTC intervals.
Counts overlap; do not add them into a unique coverage total. Existing native
CTest/scripts counts without dedicated retained logs remain transcript-level
references, not candidate passes.

Highlights: workspace 510/0/19; SF regressions 56/0/2; cloud 25/0/4;
rich SF REST/auto acquisition and replay; empty/relationships/wrong identity;
precise Bulk refusal (not successful rich Bulk acquisition); real Bulk POST
then synthetic loss/reopen/no repost; GCS conditional/streaming/80 ordered
races/source-failure; fixture CLI publication/no-op/offline replay and owned
UUID cleanup. Original failed logs and corrected assumptions are preserved.
See ignored `artifacts/release-validation-macos/RESUME.md` for the old narrative.

## Current next actions

1. New execution runner records exact command/UTC/pre-post source snapshots and
   marks source changes;30offline script tests pass. Latest native workspace
   rerun510passed22ignored; only newGCS Python harness changed during suite,
   so still pre-freeze evidence. Final full candidate rerun remains.
2. Inventory packaging/notices/MSRV and advertised capabilities; resolve scope.
   Packaging now distinguishes candidate-unqualified release builds, records
   provenance, and has an explicit strict distribution gate for incomplete
   native inventories. Pinned signed aws/httpfs project licenses still do not
   close bundled third-party inventory. Bundle builds now omit the original
   native build-path rpath; observed loader-relative-only rpath and relocation
   proof passed, but clean-host/source freeze still pending. Root
   LICENSE is Apache-2.0; added missing inherited license metadata to
   grv-types/grv-adapter-api. Capability/native documentation still needs
   reconciliation; root README no longer claims GCS validation is read-only.
3. Scaled SF and actual production GCS DuckDB/SF gates now pass;7GCS attempts
   preserve earlier harness failures and prove allownedUUIDprefixes absent.
   Successful pass includes independent native pulled values/schema plus
   exact900rowSF generator/rawREST oracle. S3 writes reconfirmed authorized;
   exact old staging root/profile/region worksheet must be recovered before effects. FullGCS fault/build matrix
   is not inferred from this representative lifecycle.
4. CRITICAL-SAFETY.md records inspected selectors and remaining cancel/GC/
   production secret checks. Commit-crash->newercheckpoint->originalreplay
   conjunction now tested/passed;cancel/EOF not implied. Persistent local build
   terminal+pull receipt baseline created and self-replayed for later candidates;
   acceptednonterminal fixtures and durablearchive still pending. Native notice
   collector gathered31files bound to archive/library/184compiled sources,
   complete=false; HistoricalThrift/header/extension noticegaps addressedby actualconservative payload44+55texts reviewed, exactgraphnotclaimed;finalcandidatequalification pending.
5. Local candidate commit/freeze now authorized. Explicit source/path inventory
   prepared, not blanket-staged. Added offline directory/tarball installation
   recipe with fresh HOME/environment, replacement/traversal refusal and prior
   installation usability; historical bundle execution PASSED with explicit
   --allow-unqualified. Run against frozen release bytes after notices.
   Baseline artifact audit passed with zero unlisted fixture/auth artifacts;
   PEM parser markers distinguished from actual encoded keys, failures retained.
6. Archive sanitized evidence and qualify actual distributed bytes; publish
   only after reviewing limitations, destination/tag and separate approval.

## Salesforce scale correction — storage blocker resolved

User approved a tenfold correctness-fixture reduction: manifest v3 contains
**1,000 main + 10 related**, 900 Active and nine Active lookup rows. Scalar
boundary rows/null classes/three partitions remain; no performance claim.
Earlier 800-row STORAGE_LIMIT_EXCEEDED failures and incomplete fixture evidence
remain in operations-followup.json; they are not reclassified as passes.

Clean scaled provisioning passed with zero strict mismatches and limits 5 MB
Max/3 MB Remaining. Live mutation moves one row August->September and excludes
another, independently checks changed revision, then complete-empty omission,
clean reset and full pristine reacquisition. The whole gate now passed.
Supported REST batchSize=200 observed [200,200,200,200,100] Active pages;
eligible Bulk test asks maxRecords=100, observed nine real pages/900 exact rows,
empty completion, durable checkpoint and exact-job cleanup. Responses use the
unchanged production parser; defaults/budgets are not changed. This test-side
acquisition driver does not establish SDK wire/credit integration.

REST/auto rich base, empty, relationships, wrong identity, rich Bulk refusals,
Bulk loss and auth smoke rerun passed. Production Salesforce adapter came from
the relocated release-profile baseline; the CLI harness is conformance CLI,
not full frozen production-bundle qualification. Scope limits stay explicit.
No permission/global org settings changed; old SOAP purge absence was never
proved and remains failed historical cleanup, not a new claim.
Logs: artifacts/release-validation/sf-scaled; reviewed index: evidence/sf-scaled.json.

## Evidence retention and reruns

Additional task #24 retains storage/receipt/accepted-terminal compatibility
fixtures and the durable reviewed dossier. Operations-tooling checks now pass:
17 offline scripts tests, catalog/history digest checks and new append-only
record roundtrip verified; these are tooling evidence, not candidate passes.

Versioned catalog/policy/status/index live here. Raw run logs and binaries live
under ignored artifacts during work, then must be copied to reviewed durable
storage before release approval. Archive destination is **not yet selected**;
local files alone are not sufficient long-term incident evidence.
For patches, always run baseline candidate/package/compatibility checks and
change-triggered safety/provider gates; reuse earlier provider results only as
an explicitly reviewed baseline with the diff and provenance recorded.
