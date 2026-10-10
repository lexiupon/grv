# Stock HTTPFS GCS validation status

**Keyless OAuth HTTPS now passes live GCS validation.** See
[live evidence and cleanup](httpfs/oauth-live-evidence/README.md): ten oracle scans
and one default production-owner authority probe, using signed pinned HTTPFS
with temporary `TYPE http` bearer secrets. Query-only projection saved **96.96%**
and selective wide filtering **93.59%** of HTTP payload bytes. No keys, new
service accounts, IAM grants or policy changes were required.

HMAC remains blocked by production policy. The RFC and implementation/release
plan now use the proven keyless stock HTTPS direction; production credential
lifecycle/positive guards still require implementation and qualification. Neither prototype is a shipped `gcs-view` capability.

## Prerequisites checked

- Existing patched DuckDB v1.5.6 native engine/source are available.
- Official osx_arm64 HTTPFS/AWS extensions fetched using the repository's pinned
  hash/footer/signature verifier. Local output is a protected directory outside
  the checkout; no runtime extension install or capability change occurred.
- Explicit GCP management account `jiacao@on.sinch.com` can describe the authorized
  `entit-finance-prod` project (`828148855924`).
- A metadata-only `gcloud storage hmac list` with that explicit account/project
  returned an empty list. No HMAC-named environment variables or explicit local
  GCS HMAC configuration reference was found. OAuth tokens do not replace HMAC.

## Historical HMAC authentication gate: blocked by organization policy

After explicit user approval, created an isolated disposable service account
and empty private throwaway bucket in `entit-finance-prod`, granting the new
account object-viewer access only on that bucket. GCS HMAC creation was refused
with **HTTP 412**, identifying **`constraints/iam.disableServiceAccountKeyCreation`**.
The effective policy is enforced. No HMAC secret/key was created.

No organization policy was changed or bypassed. The bucket-scoped grant, empty
bucket and disposable service account were removed; bucket 404, account absence
and empty owned-HMAC inventory were verified. See [blocker](httpfs/live-blocker.json)
and [cleanup evidence](httpfs/cleanup.json).

No real GCS HTTPFS/HMAC success is claimed. Proceeding now requires one of:

1. An existing approved HMAC credential in a private file/provider, with explicit
   associated service-account/project and allowed bucket scope; or
2. A policy-owner-approved test project/exception where disposable HMAC creation
   is allowed, plus explicit fixture/identity scope. Do not disable a production
   organization's key-creation control merely to validate this prototype.
   Capture secret material privately; revoke/delete it and owned resources after
   the test.

These options are required only to pursue HMAC specifically, not the proven
OAuth HTTPS alternative. Do not choose an existing production service account by convenience. Previous
throwaway bucket tests used OAuth. Subsequent explicit approval authorized
isolated provisioning, but did not authorize changing organization policy or
project-wide IAM. All created test resources have now been cleaned up.

## Offline stock-reader validation

A separate experimental harness under `httpfs/` exercises the actual pinned
HTTPFS implementation against a local S3-compatible mock with synthetic HMAC
credentials. Its results establish reader/SQL/guard feasibility only, not Google
Cloud identity/IAM or service-specific API compatibility. Refer to that harness's
[recorded results](httpfs/README.md): 11 offline trials passed on the actual
pinned HTTPFS extension, with ten oracle scans and one authority probe. Narrow
projection reduced loopback HTTP body bytes 96.62%, selective wide filtering
93.26%. Existing production-owner negative guard probes passed, but positive
trusted GCS scanner integration is unimplemented. Do not substitute these mock
results or historical custom-reader live reductions for real HTTPFS/GCS proof.
The new OAuth HTTPS live evidence is separately recorded and does establish
real stock-reader results. Remaining gates include token expiry/renewal,
redirect containment, positive production guard integration, and exhaustive URI
canonicalization. Requester Pays with explicit `x-goog-user-project` succeeded,
but no-header also succeeded for this identity, so mandatory billing-project
enforcement/billing attribution is not proven.
