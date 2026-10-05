# GRV specifications

GRV (Golden Record Vault) moves data between source/destination systems and
versioned Parquet datasets. This directory contains the draft contracts for
its storage format, client behavior, adapter processes, and optional extensions.
The client CLI is not yet implemented.

## Documents and reading order

Read the first four documents in the order below. Crypto-shredding is an
optional extension RFC.

| Document | Status | Scope and authority |
|----------|--------|---------------------|
| [Storage v2](grv-storage-v2.md) | Draft | Authoritative GRV storage layout and backend contract: tables, versions, claims, runs, revisions, publication, holds, pins, and GC. |
| [Client v1](grv-client-v1.md) | Draft; no CLI implemented | Commands, declarations, supported data types, adapter registration, and language-neutral lifecycle obligations. |
| [Client v1 execution semantics](grv-client-v1-execution.md) | Draft; normative companion | Normalized identity, command results, orchestration, transactions, locks, build completion, recovery, and conformance requirements. |
| [Adapter process protocol v1](grv-adapter-protocol-v1.md) | Draft; normative companion | Installation, process supervision, handshake, frame grammar, shared-memory data plane, and adapter operation ordering. Implements the client contracts across a process boundary. |
| [Crypto-shredding v1 RFC](grv-crypto-shredding-v1-rfc.md) | Draft; DPO review pending | Proposed `crypto-shredding/1` extension for encrypted personal data and subject-key erasure; includes unresolved decisions and sign-off requirements. |

Each document owns the contract stated above. The execution companion and
adapter protocol must satisfy the client obligations; none adds or overrides
the storage layout. Conflicts require correction of the dependent document,
not an implicit override. The crypto-shredding RFC does not make its proposed
extension a mandatory storage feature.

## Versions and compatibility

Storage v2, client v1, adapter interface v1, and `crypto-shredding/1` are
independent version domains. The current client v1 targets storage v2, and the
adapter process protocol v1 implements the client v1 adapter lifecycle. The
crypto-shredding RFC proposes an optional extension to storage v2.

The versions in filenames identify those contracts, not document edit counts.
Schema fields such as `output_version` and `result_version` retain their own
defined meanings. A future adapter interface version need not change the client
or storage version; compatibility must be specified explicitly.

## Schemas and examples

| File or directory | Purpose |
|-------------------|---------|
| [Declaration schema](grv-client-v1-declaration.schema.json) | Common push/pull authoring envelope; adapter fragments require additional registered validation. |
| [Command output schema](grv-client-v1-command-output.schema.json) | Public command result envelope, errors, and command-specific result shapes. |
| [Build completion schema](grv-client-v1-build-completion.schema.json) | Successful invocation and stopped-writer attestation supplied before build finalization. |
| [Adapter binding schemas](adapters/) | DuckDB and Salesforce validation-point schemas. |
| [Examples](examples/) | Client declarations and their local SQL/column-contract files. |

Schemas validate structural shapes; the specifications also define required
cross-field, identity, ordering, and recovery checks. Examples illustrate the
contracts and do not override their normative rules. Relative SQL and column
file references resolve from their declaration file as specified by the client.
