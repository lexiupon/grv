# Native distribution notice blocker

Status: conservative engineering notice payload assembled and reviewed; old
exact-CI/SBOM prerequisite superseded by user-approved policy. User approved pinned
upstream release licenses + conservative bundled third-party notices for the
macOS ARM64 0.1.0 engineering distribution gate. No legal certification or
exact linked dependency/SBOM completeness claim. See 0.1.0-SCOPE.md.

Pinned official osx_arm64 DuckDB v1.5.6 extensions:
- httpfs commit 4bc690dba4496c765777a0269d48fdbaff7cdc11,
  sha256 0cde1f5acd1970bfd414b11f5e945a1eddd8425e96e273f6411538d8fe3d951d
- aws commit 28c853c084a6e3acd36d7b8018c42438bb8c5a33,
  sha256 911b68461fdd989e0c9e74926b197c8a05727bfafc05df38baa47ece42174f1c

Signature/footer/load proof authenticates bytes, not dependency/license closure.
Top-level MIT licenses plus observed version strings are insufficient. Upstream
CI defaults and moving refs do not establish the exact signed release run.
No complete installed dependency notice/SBOM asset was found in the bounded
upstream review. This is not proof none exists. Required license/attribution
texts are now assembled conservatively from pinned source/releases; extras
are acceptable and documented. Known missing required texts still block.

Actual payload: 44 engine source/license/embedded-attribution files and 55
extension-third-party license/NOTICE/embedded-attribution files, including
Thrift/Parquet/Catch recovered texts, AWS SDK tinyxml2/cJSON headers and AWS C
Common's third-party notices. Both review JSONs anchor paths/SHA/source refs.
Conservative tests/platform extras included, no IMDb data or implementation
code copies. Existing signed binary pins remain unchanged. Strict policy
packaging of the historical release binaries PASSED; bundle audit and actual
native/extension/local lifecycle verification PASSED. Clean candidate build
qualification is still separate. Review claims engineering policy satisfaction,
not legal certification/perfect graph.

The following stronger provenance would improve exact-SBOM confidence, but is
NOT an absolute release prerequisite under the user-reviewed policy:
1. Signed digest -> deployment/signing record -> producing run/job/attempt and
   unsigned digest (signing changes bytes).
2. Exact resolved engine/template/CI-tools SHAs, workflow inputs, merged vcpkg
   manifests, triplets/overlays and dependency cache ABI records.
3. Installed vcpkg status plus share/<port>/copyright, required NOTICE/SPDX and
   source/patch provenance for linked packages (not only build tools).
4. Vendor/generated dependencies outside vcpkg and linker/CMake inclusion proof.

Alternatively a controlled rebuild from reviewed pinned sources can establish
its own bytes/inventory; it cannot retroactively qualify these signed bytes.
Do not weaken trusted-loading/signature rules or remove advertised capabilities
silently. Rebuilding/re-scoping is a reviewed decision.

The guarded engine's own embedded third-party notices also need inventory;
its source archive and compile_commands are available locally but top-level
DuckDB LICENSE alone does not close that gate. Rust dependency notice extraction
has no missing top-level files in the new release-profile baseline bundle;
this does not certify every vendor's transitive native code.

Packaging now labels release-profile output candidate-unqualified and records
source/toolchain/dirty state. --require-distribution-ready in package.py and
verify-bundle.py now fails closed on unreviewed policy/missing or mismatched
notice payloads, not inability to reconstruct upstream CI. Native exact graph
completeness flags remain false. No upstream request has been sent, and no
external publishing/upload is authorized by this note.

Reference investigation:
crates/grv-adapter-duckdb/native/EXTENSIONS.txt and notices/native-extensions.json.
