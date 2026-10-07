# Salesforce adapter implementation

The executable advertises extraction with capture-window consistency after
process, conversion, metadata, recovery and core publication conformance gates.
Build, pull, commands and resumable acquisition remain unadvertised. The
conformance harness uses this same registration and replaces only helper
programs, then publishes and replays accepted captures without those helpers.

Pure configuration validation and offline alias lookup do not authenticate or
make HTTP calls. Authenticated resolution uses supervised Salesforce CLI and
curl helpers. Their output is private, bounded, and never included in errors.
CLI file logging and Node preloads/debug output are disabled. The CLI token is
requested into a private pipe; it is not passed through arguments or environment
variables.

Each helper inherits a process-creation restriction before it executes. On
macOS the native keychain credential is read by a separate contained
`/usr/bin/security` helper. A private-stdin Node bootstrap supplies only the
existing `sfdx`/`local` credential getter, and refuses every setter. This bridge
pins Salesforce CLI 2.152.14, core 9.2.2, their canonical module locations, and
the entry/keychain module hashes. A version or hash change needs another bridge
compatibility review. The selected generic Unix keychain retains its ordinary
CLI path. Linux containment and native secret-tool support require platform
validation before release.

Extraction defaults to API v66.0 and REST for authored `transport: auto`.
Both transports use Describe metadata to prove source domains and decimal
widening before querying or creating a job. Bulk additionally proves exact CSV
representations, including empty-cell semantics. Formulas and functions are not categorically forbidden. A
projection whose nullable text cannot distinguish null from empty in CSV must
use REST. Numeric, timestamp, and other primitive decoders reject rounding or
precision loss.

Acquisition journals precede source requests, checkpoint acknowledgements
precede row emission, and each attempt is nonresumable. Ambiguous Bulk creation
is durably unknown and never repeats its POST. REST pages and Bulk result pages
share bounded encoded/decoded source accounting; credited Arrow batches use
separate scratch and output bounds. Accepted-capture and terminal-publication
replay belong to the core.

Run the offline suite in a context that allows installing the helper's native
containment:

```console
cargo test -p grv-adapter-salesforce --offline
cargo clippy -p grv-adapter-salesforce --all-targets --offline -- -D warnings
```

The ignored live test requires both an explicit org alias/username and exact
expected `salesforce:<org-id>` identity. It only authenticates, describes
Organization, and queries Organization.Id; it creates no Bulk job or records.

```console
GRV_SALESFORCE_TEST_ORG=<alias> GRV_SALESFORCE_TEST_IDENTITY=salesforce:<org-id> \
  cargo test -p grv-adapter-salesforce --test live_service --offline -- --ignored
```
