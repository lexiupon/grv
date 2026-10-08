# GRV crypto-shredding v1 (companion RFC)

|              |                                                                |
| ------------ | -------------------------------------------------------------- |
| Status       | draft — for review by the Data Protection Officer (DPO)        |
| Extension id | `crypto-shredding/1`                                           |
| Date         | 2026-09-29                                                     |
| Companion to | [grv-storage-v2.md](grv-storage-v2.md) (GRV v2 storage layout) |
| Sign-off     | see [§13](#13-sign-off)                                        |

## 1. Summary

GRV stores data as immutable versions and keeps history, so personal data
cannot be deleted in place. This RFC erases personal data by
**crypto-shredding**:

- every value in a column that holds personal data is stored encrypted;
- the key that encrypts it belongs to exactly one **subject** (for example,
  one account) and one GRV partition;
- erasing a subject destroys that subject's keys. The encrypted values stay
  in GRV but can never be decrypted again, by anyone.

The GRV storage protocol does not change. A table opts in when it is
created, GRV records which columns are protected and which column
identifies the subject, and the keys live outside GRV in a key store
protected by a cloud key management service (KMS).

## 2. What erasure means here

For a DPO, the guarantees and their limits in plain terms:

**Guaranteed.** Once a subject is shredded:

- the subject's protected values in every GRV dataset, table, partition,
  version, and revision — including pinned and historical ones — cannot be
  decrypted, because the only keys that decrypt them no longer exist;
- no new key is ever created for that subject, so reprocessing old inputs
  cannot bring the data back (§10, suppression);
- the erasure is logged without recording personal data (§10.4).

**Completed after a bounded delay.** Destroyed keys can survive for a
limited time in places this RFC controls. Erasure is complete when the last
of them expires:

| where                                     | bounded by                   | parameter |
| ----------------------------------------- | ---------------------------- | --------- |
| key-store backups                         | backup retention             | `B`       |
| in-memory key caches                      | cache time-to-live           | `T`       |
| warehouse copies (time travel, fail-safe) | warehouse retention settings | `W`       |

The erasure completion date is `shred time + max(B, T, W)`. Each value is
recorded at sign-off (§13) and must fit within the legal response deadline:
one month under GDPR Art. 12(3), extendable by two further months.

**Not covered by encryption alone.**

- Columns that are not declared protected stay readable. After shredding,
  rows remain with their subject id and unprotected columns (§11, R5).
- Copies outside GRV — warehouse tables, exports, BI extracts, logs — need
  their own erasure. This RFC requires an inventory of them (§9).
- Data written before a table opted in, or by a non-conforming writer, is
  plaintext and cannot be shredded (§11, R9).

## 3. Terms

| term                  | meaning                                                                                             |
| --------------------- | --------------------------------------------------------------------------------------------------- |
| **subject**           | the person or account whose erasure requests are honored                                            |
| **subject id**        | a surrogate identifier of the subject, such as an internal account id; not personal data on its own |
| **protected column**  | a column whose values are personal data; stored only encrypted                                      |
| **key scope**         | the data one key covers: one subject within one GRV partition (default), or within one table        |
| **key id (`kid`)**    | the canonical name of a key scope (§6.1)                                                            |
| **DEK**               | data encryption key: one per key id and generation                                                  |
| **wrapping key (WK)** | a key-store key that encrypts ("wraps") DEKs; itself wrapped by the KEK                             |
| **KEK**               | key encryption key held in the KMS; never leaves the KMS                                            |
| **key store**         | the mutable database holding wrapped DEKs and wrapping keys                                         |
| **suppression list**  | subjects that have been shredded; no key is ever created for them again                             |
| **shredding**         | irreversibly deleting all of a subject's DEKs                                                       |

## 4. Design overview

```text
KMS key encryption key (KEK)              never leaves the KMS
  │ wraps (KMS encrypt/decrypt)
wrapping keys (WK)                         few; in the key store
  │ wrap (AES-256-GCM)
DEK per (subject, dataset, table, partition)   in the key store; DELETED on erasure
  │ encrypts (AES-256-GCM or AES-256-SIV)
protected values in GRV data files         binary envelopes (§7)
```

- **Write.** A writer finds each row's subject id, derives the row's key id,
  fetches or creates that DEK, and stores each protected value as an
  encrypted envelope.
- **Read.** A reader derives the same key id, fetches the DEK, and decrypts.
  If the DEK has been shredded, the value reads as null.
- **Erase.** An operator adds the subject to the suppression list and
  deletes all of the subject's DEKs (§10). Nothing in GRV is rewritten or
  pruned.

The KMS is called only to unwrap the few wrapping keys, never per subject,
so a partition with 100,000 subjects needs no extra KMS calls.

## 5. What GRV records

GRV records the protection in two places defined by the core
specification. Both are written with the table's normal coordination, so
every writer sees them before it can write data.

### 5.1 Table configuration: `.layout.json`

A protected table is created with the extension in `.layout.json`
(core §3). This is fixed for the table's lifetime, and every writer that
does not implement this RFC must refuse to write to the table:

```json
{
  "table": "customers",
  "partition_keys": ["region"],
  "extensions": {
    "crypto-shredding/1": {
      "subject_column": "account_id",
      "key_scope": "partition",
      "on_suppressed": "null",
      "dpia": "DPIA-2026-014"
    }
  }
}
```

| field            | required | notes                                                                                                                        |
| ---------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `subject_column` | yes      | the column whose value selects the key: a column of GRV type `string` or `int64`, never protected, non-null in every row     |
| `key_scope`      | yes      | `partition` — one key per (subject, dataset, table, partition); or `table` — one key per (subject, dataset, table)           |
| `on_suppressed`  | yes      | what writers do with rows of a shredded subject: `null` (write the row with protected columns null) or `drop` (omit the row) |
| `dpia`           | no       | reference to the data protection impact assessment or record of processing that covers the table                             |

Rules:

- Subject id values must match `^[A-Za-z0-9_-]{1,128}$`; an `int64` subject
  id is written in canonical decimal. A row with a null or invalid subject
  id is rejected by the writer.
- The subject column may be a partition column (`_{k}_`, core §3) when a
  table is partitioned by subject.
- `key_scope: partition` allows erasing one partition of a subject (for
  example, one region) and limits what one key exposes, at the cost of
  more keys (subjects × partitions). Tables partitioned by date with many
  subjects SHOULD use `key_scope: table` unless per-period erasure is
  required.

### 5.2 Protected columns: `.schema.json`

Each protected column is declared in the table's schema baseline (core §4)
with an `ext` entry, in the same compare-and-swap that adds the column:

```json
{ "name": "phone_number", "type": "binary", "ext": { "crypto-shredding/1": { "plaintext_type": "string", "mode": "randomized" } } }
```

| field            | notes                                                                                                                                                                                                     |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `plaintext_type` | the value's type before encryption, in GRV type notation (core §4); one of `string`, `json`, `binary`, `boolean`, `int32`, `int64`, `float64`, `date`, `timestamp`, `decimal`                             |
| `mode`           | `randomized` (AES-256-GCM; equal values encrypt differently) or `deterministic` (AES-256-SIV; equal values of one subject in one key scope and column encrypt identically, which allows equality lookups) |

Rules:

- A protected column's GRV type is `binary`; its `ext` never changes.
- A column already in the baseline without protection can never become
  protected, because its existing values are plaintext. Protect data by
  adding a new protected column (or a new table) and stop writing the old
  one.
- Nested values are not protected directly; serialize them to `json`
  first.
- A tool can list every protected column in the store by reading each
  table's `.layout.json` and `.schema.json`. This listing is the inventory
  of protected fields for the DPIA (checklist A1).

## 6. Keys

### 6.1 Key identity

A row's **key id** is derived from its subject id and GRV coordinates:

```text
key_scope = partition:  cs1:<subject_id>:<dataset>/<table>/<partition path>
key_scope = table:      cs1:<subject_id>:<dataset>/<table>
```

For a non-partitioned table the two forms are identical. Example:
`cs1:acct_000123:crm/customers/region=eu`. Key ids contain no personal
data: subject ids are surrogates, and GRV forbids personal data in names
and partition values (checklist A4).

Rows copied into a new version of the same partition keep the same key id,
so their ciphertext can be copied unchanged. Rows moved to another
partition, table, or dataset must be decrypted and re-encrypted under the
destination's key id.

### 6.2 Key store

The key store is a transactional database outside GRV — never GRV itself,
whose data is immutable. It holds:

| table                 | columns                                                                                                                                                                           |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `deks`                | `kid`, `generation`, `subject_id`, `dataset`, `table`, `scope_path`, `wrapped_dek`, `wk_id`, `created_at`, `created_by`; unique on (`kid`, `generation`); indexed by `subject_id` |
| `wrapping_keys`       | `wk_id`, `wrapped_wk` (by the KEK), `kek_version`, `created_at`, `state` (`active` or `decrypt_only`)                                                                             |
| `suppressed_subjects` | `subject_id`, `request_id`, `suppressed_at`                                                                                                                                       |
| `shredded_keys`       | `kid`, `generation`, `request_id`, `shredded_at`; no key material                                                                                                                 |

The **erasure log** (§10.4) is kept in a separate append-only store, not in
the key store, so that it survives a key-store restore.

Requirements:

- **One DEK per key id and generation.** A writer creates a DEK with an
  insert that fails if the (`kid`, `generation`) exists, and in the same
  transaction checks that the subject is not suppressed. On conflict it
  reads and uses the winner's DEK. A writer never encrypts with a DEK whose
  insert has not committed.
- **Real deletion.** Shredding deletes DEK rows. There is no soft delete,
  and deletions propagate to every replica. The database must also remove
  deleted rows physically (for example, PostgreSQL `VACUUM` of dead tuples)
  within `B` days, and write-ahead-log archives count as backups.
- **Bounded backups.** Backups, point-in-time recovery, and log archives
  keep at most `B` days of history. Restoring a backup must first reapply
  every erasure in the erasure log since the backup's time — suppressing
  the subjects and deleting their DEKs again — before any reader or writer
  connects.
- **Generations.** A key id starts at `generation: 1`. Shredding records
  each deleted (`kid`, `generation`) in `shredded_keys`, and a writer that
  later needs a DEK for that key id (after a partial or retention-based
  erasure, which does not suppress the subject) creates the next
  generation. A shredded generation is never recreated, so its envelopes
  keep reading as unavailable rather than failing to decrypt under a new
  key.

### 6.3 KMS integration

- **KEK.** One symmetric KMS key per environment (and per region, if data
  residency requires), with automatic rotation enabled. Older key versions
  stay available for decryption. HSM protection is recommended.
- **Wrapping keys.** A WK is 32 random bytes generated by the key service,
  encrypted with the KEK, and stored in `wrapping_keys`. The KMS call
  binds the WK to its id: `additionalAuthenticatedData = wk_id` on Google
  Cloud KMS, or `EncryptionContext = {"grv-wk": wk_id}` on AWS KMS. New DEKs
  use the newest `active` WK. Old WKs are marked `decrypt_only`, never
  deleted while DEKs reference them.
- **DEK wrapping.** A DEK is 32 random bytes from a cryptographically secure
  generator, encrypted under its WK with AES-256-GCM and associated data
  `kid ‖ 0x00 ‖ generation` (decimal).
- **Access.**

  | principal                    | KMS permission on the KEK | key store permission                                                                       |
  | ---------------------------- | ------------------------- | ------------------------------------------------------------------------------------------ |
  | key service                  | encrypt and decrypt       | manage wrapping keys                                                                       |
  | writers (ingest, publishers) | decrypt                   | read and insert `deks`; read `suppressed_subjects`                                         |
  | readers and pull clients     | decrypt                   | read `deks`                                                                                |
  | erasure operators            | none                      | delete `deks`; insert `suppressed_subjects` and `shredded_keys`; append to the erasure log |
  | database administrators      | none                      | administer the database                                                                    |

  Nobody who administers the key store database also holds KMS decrypt, so
  no single role can read protected data from the key store alone.

- **Audit.** KMS calls are logged: on Google Cloud, enable Data Access audit
  logs for Cloud KMS; on AWS, make sure CloudTrail does not exclude KMS
  events. Logs are retained per the security policy.
- **Caching.** Unwrapped WKs and DEKs are kept only in process memory, for
  at most `T` minutes, and are never written to disk, logs, or metrics.
- **Development.** A local key file may replace the KMS only with synthetic
  data, never with personal data.

## 7. Encryption format

Each non-null protected value is stored as a binary **envelope**:

| offset | size | field                                                                                                                   |
| ------ | ---- | ----------------------------------------------------------------------------------------------------------------------- |
| 0      | 1    | format version: `0x01`                                                                                                  |
| 1      | 1    | algorithm: `0x01` = AES-256-GCM (`randomized`), `0x02` = AES-256-SIV (`deterministic`)                                  |
| 2      | 4    | DEK generation, unsigned big-endian                                                                                     |
| 6      | …    | AES-256-GCM: 12-byte random nonce ‖ ciphertext ‖ 16-byte tag. AES-256-SIV (RFC 5297): 16-byte synthetic IV ‖ ciphertext |

- **Keys per algorithm.** The algorithm key is derived from the DEK with
  HKDF-SHA256 (RFC 5869): `info = "grv-cs/1 gcm"`, 32 bytes, for AES-256-GCM;
  `info = "grv-cs/1 siv"`, 64 bytes, for AES-256-SIV.
- **Associated data.** Both algorithms authenticate
  `bytes 0–5 of the envelope ‖ kid ‖ 0x00 ‖ column name` (UTF-8). A value
  therefore fails to decrypt if it is moved to another subject, partition,
  table, or column, or if its header is altered.
- **Plaintext encoding.**

  | `plaintext_type` | bytes encrypted                                              |
  | ---------------- | ------------------------------------------------------------ |
  | `string`, `json` | UTF-8                                                        |
  | `binary`         | as is                                                        |
  | `boolean`        | one byte, `0x00` or `0x01`                                   |
  | `int32`, `int64` | two's complement, big-endian, 4 or 8 bytes                   |
  | `float64`        | IEEE 754 binary64, big-endian                                |
  | `date`           | days since 1970-01-01 as `int32`                             |
  | `timestamp`      | `int64` in the type's unit                                   |
  | `decimal`        | unscaled value, two's complement, big-endian, minimal length |

- **Nulls** are stored as null, unencrypted (§11, R6).
- **Libraries.** Implementations use a vetted library for AES-GCM, AES-SIV,
  and HKDF — for example Google Tink with raw output prefix — never
  hand-written cryptography. The reference implementation publishes test
  vectors for this format.

## 8. Writing, reading, and derived data

**Writers** (ingest jobs and the publisher wrapper, core §11):

1. Refuse to write to a table whose `.layout.json` lists an extension the
   writer does not implement (core §3).
2. For each row, validate the subject id and look up the subject in the
   suppression list. For a suppressed subject, apply `on_suppressed`, and
   never create or use a DEK.
3. Encrypt every protected value under the row's key id (§6, §7). Copy
   ciphertext unchanged only within the same key id.
4. Disable Parquet column statistics, dictionary encoding, and bloom
   filters for protected columns. Statistics over ciphertext are useless,
   and dictionaries over deterministic ciphertext reveal repetition.
5. Before committing a version (core §5 step 4), check that every non-null
   protected value parses as an envelope with the column's algorithm, and
   decrypt a sample.
6. Never put personal data in partition values, table or dataset names,
   GRV `metadata` or `reason` fields, logs, error messages, or metric
   labels.

**Readers** that need plaintext derive each row's key id, fetch the DEKs of
the distinct subjects in a file in batches, and decrypt. A missing DEK
means the subject was shredded: the value reads as null, and the reader
reports how many values were unavailable. A value that fails authentication
is an integrity error, not a shredded value. Readers that do not need
plaintext read the envelopes as `binary`.

**Derived data.** A column derived from personal data — a copy, a
normalized form, a substring, or a hash of a phone number — is itself
personal data. A GRV product table that contains one must protect it, with
the same subject id, under the product table's own key ids. Aggregates over
many subjects that cannot be attributed to one subject need no protection;
the DPO confirms the threshold (checklist D4).

## 9. Copies outside GRV

Decrypted data that leaves GRV is outside the reach of crypto-shredding.
Each such copy needs its own erasure path, and all of them are listed in a
**plaintext inventory** that the DPO reviews (checklist D1):

- **Warehouse tables** loaded by the pull job (core §11). After a
  shredding, the pull rebuilds every managed table loaded from a GRV table
  in which the subject had keys (the dry-run list, §10.2). Rows then carry
  nulls in protected columns, or are dropped per `on_suppressed`. The pull
  adapter supports full rebuilds through either transactional refreshes or
  its dirty-table journal (core §11). The warehouse's own time travel and
  fail-safe retention is `W`.
- **Exports, BI extracts, caches, and downstream systems** fed from GRV or
  the warehouse: each needs a named owner, an erasure mechanism, and a
  maximum delay.
- **Logs and error reports** must not contain protected values at all
  (§8, writer rule 6).

## 10. Erasure procedure

### 10.1 Roles

| role             | responsibility                                                                    |
| ---------------- | --------------------------------------------------------------------------------- |
| requester intake | receives and verifies requests (outside this RFC); maps the person to subject ids |
| erasure operator | runs the procedure below                                                          |
| approver         | a second person who approves each shredding before it runs                        |
| DPO              | owns the procedure, reviews the erasure log, and signs off on this RFC            |

### 10.2 Steps

1. **Record** the request in the erasure tracker with a `request_id`. Record
   the subject ids, never the requester's personal data.
2. **Check legal holds.** A subject under a legal hold is deferred and the
   requester is informed; the procedure resumes when the hold ends.
3. **Dry run.** List the DEKs to be deleted (by `subject_id`, or by key id
   for a partial erasure) with counts per dataset and table. The approver
   reviews the list; shredding cannot be undone.
4. **Log.** Append the erasure record (§10.4) to the erasure log, before
   any key is deleted, so that a key-store restore can repeat the erasure.
5. **Suppress and shred.** In one key-store transaction, insert the subject
   into `suppressed_subjects`, record each of its DEKs in `shredded_keys`,
   and delete the DEK rows. From then on, no writer can create or use a DEK
   for the subject. Update the erasure record with the deleted count.
6. **Wait out caches.** After `T` minutes no process holds the keys.
7. **Purge copies.** Rebuild the warehouse tables loaded from the tables in
   the dry-run list, and trigger the erasure path of every
   plaintext-inventory entry (§9). Record each completion time.
8. **Verify.** Decrypting a sample of the subject's envelopes from GRV
   fails with "key not found", and the subject's rows in warehouse tables
   have null protected columns (or are gone).
9. **Complete.** When `B` and `W` have also elapsed, mark the request
   complete and respond to the requester within the legal deadline.

A **partial erasure** (one key scope, for example one region) runs the same
steps for the listed key ids only, and does not suppress the subject.
**Account closure** at the end of a contract uses the same procedure.
Optional **retention-based shredding** — deleting DEKs of partitions older
than a retention period — uses the same steps without suppression.

### 10.3 Interaction with GRV

Shredding needs no GRV operation. Versions stay immutable, revisions stay
resolvable, and envelopes whose keys are gone read as null. GRV pins and
dependency holds (core §9, §10) never delay erasure: they keep the
ciphertext, not the keys. Normal GC may later prune the ciphertext;
erasure does not depend on it.

### 10.4 Erasure record

Kept in the erasure log, a separate append-only store (§6.2). It contains
no personal data beyond the surrogate subject id:

```json
{
  "request_id": "ER-2026-0042",
  "subject_id": "acct_000123",
  "scope": "subject",
  "approved_by": "operator-17",
  "executed_by": "operator-04",
  "suppressed_at": "2026-10-02T09:14:00Z",
  "shredded_at": "2026-10-02T09:15:12Z",
  "deks_deleted": 412,
  "copies_purged_at": { "warehouse": "2026-10-02T11:40:00Z", "bi-extracts": "2026-10-03T08:00:00Z" },
  "verified_at": "2026-10-03T09:00:00Z",
  "complete_after": "2026-10-16T09:15:12Z"
}
```

## 11. Residual risks

| id  | risk                                                                                                                                                                                                                                | bound or mitigation                                                                                                                                                                           |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R1  | Deleted DEKs survive in key-store backups, log archives, or not yet vacuumed storage                                                                                                                                                | all bounded by `B` days; restores reapply the erasure log first (§6.2)                                                                                                                        |
| R2  | Plaintext survives in warehouse time travel or fail-safe                                                                                                                                                                            | at most `W` days (§9)                                                                                                                                                                         |
| R3  | Processes hold unwrapped keys in memory                                                                                                                                                                                             | at most `T` minutes (§6.3)                                                                                                                                                                    |
| R4  | Plaintext copies outside GRV and the warehouse                                                                                                                                                                                      | plaintext inventory with owners and erasure paths (§9)                                                                                                                                        |
| R5  | Unprotected columns remain, linked to the subject id; they are pseudonymous personal data for as long as the subject id can be linked to the person                                                                                 | the DPO decides per table whether the remaining columns are acceptable, or protects them (checklist A8); erasing the subject-id mapping in the system of record makes the remainder anonymous |
| R6  | Metadata is visible without keys: whether a value is null, ciphertext length (about the plaintext length), row counts per subject and partition, and, in deterministic mode, which values are equal within one key scope and column | accepted by the DPO (checklist F2); deterministic mode only where justified (A6)                                                                                                              |
| R7  | Anyone holding KMS decrypt and key-store read access can read protected data                                                                                                                                                        | separation of duties (§6.3), audit logs                                                                                                                                                       |
| R8  | A mistaken shredding is irreversible                                                                                                                                                                                                | dry run and second approver (§10.2)                                                                                                                                                           |
| R9  | Plaintext written before a table opted in, or by a non-conforming writer                                                                                                                                                            | tables are created with the extension; writers validate before commit (§8); DLP scans (checklist C7)                                                                                          |

## 12. Checklists

Each item needs attached evidence, such as a link, test report, or
configuration export. Item ids are stable so that sign-off can refer to
them.

### A. Table onboarding — data owner, per protected table

- [ ] **A1** Personal-data columns are identified and recorded in the DPIA
      or record of processing, whose reference is in the table's `dpia` field.
      _Evidence:_ DPIA entry, and the protected-column inventory listing (§5.2).
- [ ] **A2** Every personal-data column is declared protected, including
      derived forms (normalized values, hashes, free text that may contain
      personal data). _Evidence:_ the table's `.schema.json`.
- [ ] **A3** The subject column is a surrogate id that is not personal data
      on its own, and the system of record that maps it to a person has its own
      erasure procedure. _Evidence:_ data model and the system of record's
      procedure.
- [ ] **A4** No personal data in dataset names, table names, partition
      keys, or partition values. _Evidence:_ `.layout.json` and a sample of
      partition paths.
- [ ] **A5** Key scope chosen, with the expected key count (subjects ×
      partitions for `partition` scope). _Evidence:_ sizing note.
- [ ] **A6** Deterministic mode is used only where equality lookups require
      it, with the reason recorded. _Evidence:_ column list with reasons.
- [ ] **A7** `on_suppressed` behavior is chosen and agreed with the DPO.
- [ ] **A8** The unprotected columns that remain after shredding are
      reviewed and accepted (R5). _Evidence:_ DPO note.

### B. Key management — platform team

- [ ] **B1** The KEK is in the KMS, with automatic rotation enabled and
      access restricted per §6.3. _Evidence:_ KMS key configuration and IAM
      policy export.
- [ ] **B2** The key store performs real deletes, removes deleted rows
      physically within `B` days, keeps backups, point-in-time recovery, and
      log archives for at most `B` days, and deletions reach every replica.
      _Evidence:_ database, vacuum, and backup configuration.
- [ ] **B3** The restore procedure reapplies the erasure log before any
      client connects. _Evidence:_ runbook and a restore drill.
- [ ] **B4** A unique constraint enforces one DEK per (key id, generation),
      and DEK creation checks the suppression list in the same transaction.
      _Evidence:_ schema and a concurrency test.
- [ ] **B5** Keys are cached only in memory, for at most `T` minutes.
      _Evidence:_ configuration and code reference.
- [ ] **B6** Database administrators hold no KMS decrypt permission, and
      erasure operators hold no KMS permission. _Evidence:_ IAM export.
- [ ] **B7** KMS audit logging is enabled and retained. _Evidence:_ logging
      configuration.

### C. Writers and readers — engineering

- [ ] **C1** Writers refuse tables with unknown extensions. _Evidence:_
      automated test.
- [ ] **C2** The envelope format, key derivation, and associated data follow
      §7, using a vetted library; the test vectors pass. _Evidence:_ test
      report.
- [ ] **C3** Parquet statistics, dictionaries, and bloom filters are
      disabled for protected columns. _Evidence:_ writer configuration and a
      file inspection.
- [ ] **C4** Writers validate envelopes before committing a version.
      _Evidence:_ test that plaintext is rejected.
- [ ] **C5** No personal data in logs, error messages, metrics, or GRV
      metadata. _Evidence:_ log review and a test with synthetic personal data.
- [ ] **C6** Readers return null for shredded values and report counts; an
      authentication failure raises an error. _Evidence:_ automated test.
- [ ] **C7** A data loss prevention (DLP) scan checks new versions for
      plaintext personal data in unprotected columns, with alerts.
      _Evidence:_ scan configuration and a sample report.

### D. Copies and derived data — data owners and platform team

- [ ] **D1** The plaintext inventory lists every place where decrypted data
      lands, each with an owner. _Evidence:_ the inventory.
- [ ] **D2** Each inventory entry has an erasure mechanism and a maximum
      delay within the deadline. _Evidence:_ the inventory.
- [ ] **D3** Warehouse time-travel and fail-safe retention is at most `W`
      days. _Evidence:_ warehouse configuration.
- [ ] **D4** Derived GRV product tables protect derived personal data under
      their own key ids, and the aggregation threshold is agreed. _Evidence:_
      product schemas and the DPO note.

### E. Erasure operations — operations team

- [ ] **E1** The §10 procedure is adopted, with named operators and
      approvers. _Evidence:_ runbook and roster.
- [ ] **E2** Every shredding has a reviewed dry run and a second approver.
      _Evidence:_ erasure log fields.
- [ ] **E3** Legal holds are checked before shredding. _Evidence:_ runbook
      step and tooling.
- [ ] **E4** Erasure records contain no personal data beyond the surrogate
      subject id. _Evidence:_ sample record.
- [ ] **E5** An end-to-end erasure drill on a synthetic subject passed, and
      is repeated at least yearly. _Evidence:_ drill report, including step 8
      verification.
- [ ] **E6** `max(B, T, W)` plus operating time fits within the legal
      deadline. _Evidence:_ the values from §13.

### F. DPO decisions

- [ ] **F1** Crypto-shredding is accepted as the erasure method for data in
      GRV.
- [ ] **F2** The residual risks R1–R9 (§11) are accepted, with the values
      of `B`, `T`, and `W` below.
- [ ] **F3** The treatment of remaining pseudonymous data (R5) is accepted.
- [ ] **F4** The subject granularity is confirmed (open question 1).
- [ ] **F5** The `on_suppressed` default and the permanence of suppression
      are confirmed (open question 2).

## 13. Sign-off

| parameter                                      | value | set by   |
| ---------------------------------------------- | ----- | -------- |
| `B` — key-store backup retention (days)        |       | platform |
| `T` — key cache time-to-live (minutes)         |       | platform |
| `W` — warehouse time travel + fail-safe (days) |       | platform |
| erasure response target (days)                 |       | DPO      |

| role                    | name | date | decision |
| ----------------------- | ---- | ---- | -------- |
| Data Protection Officer |      |      |          |
| Security                |      |      |          |
| Platform owner          |      |      |          |

## 14. Open questions

1. **Subject granularity.** This draft keys data by account. If an erasure
   request can come from an individual whose data appears under many
   accounts — for example, a message recipient identified by a phone
   number — dropping account keys cannot erase that individual without
   erasing whole accounts. Such tables would need the individual as the
   subject (a surrogate id per individual), possibly alongside the account.
2. **Suppression.** Is suppression permanent, and does it also apply to new
   data about the subject arriving after the request, or only to data from
   before it?
3. **Retention values.** Choose `B`, `T`, and `W`, and the erasure response
   target.
4. **Acceptance.** Does the DPO (and legal) accept crypto-shredding as
   erasure, given the bounded delays in §2?
5. **Default key scope.** `partition` (finer erasure, more keys) or
   `table` (fewer keys)?
6. **Retention-based shredding.** Should the store use key deletion to
   enforce storage limitation for old partitions (§10.2)?
