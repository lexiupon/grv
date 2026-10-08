# Cloud storage operations

Operational reference for GRV's S3 and GCS backends: the limited-permission
S3 upload mode, its memory behavior, and cloud download timeouts. The storage
layout itself is defined by [Storage v2](../spec/grv-storage-v2.md) and is
identical across local, S3, and GCS backends.

## S3 upload modes

Normal S3 publication uses multipart streaming. That is the default, and its
abort/list cleanup permissions are separate from the write permissions a
normal writer needs.

For S3 writers that only have `s3:ListBucket`, `s3:GetObject`,
`s3:PutObject`, and `s3:DeleteObject`, set the GRV CLI environment variable:

```bash
export GRV_S3_UPLOAD_MODE=single-put
```

Production adapters send Arrow data to the host; the CLI owns GRV publication
writes and reads these settings directly. In single-put mode each object is
written with one atomic conditional PUT (`If-Match` / `If-None-Match`).
Multipart APIs are never used.

### Single-put buffer limit

Single-put mode buffers each object in memory, up to a configurable limit:

| Setting                       | Default                     | Valid range                                                                                   |
| ----------------------------- | --------------------------- | --------------------------------------------------------------------------------------------- |
| `GRV_S3_SINGLE_PUT_MAX_BYTES` | 1 GiB (1,073,741,824 bytes) | positive ASCII decimal, at most 5,000,000,000, and within the platform's address-space bounds |

The limit applies to each object, not the entire dataset. Set it on the CLI
process that owns publication:

```bash
export GRV_S3_SINGLE_PUT_MAX_BYTES=67108864  # 64 MiB per object
```

Memory guidance:

- The 1 GiB default targets deployments with at least 64 GiB of RAM.
- Higher limits allow correspondingly higher per-upload memory usage, and
  concurrent uploads multiply that usage. Choose a limit appropriate for
  available memory and concurrency.
- GRV does not detect RAM or reduce the limit automatically; smaller hosts
  should set a lower value explicitly.
- Buffers grow on demand rather than allocating the configured maximum up
  front. Larger objects or buffer-allocation failures are refused before a
  write request. This remains a buffered mode, not an unbounded streaming
  mode.

### What single-put mode does not do

- There is no automatic retry or fallback after a failed or ambiguous
  multipart request.
- It does not inventory or reclaim preexisting multipart uploads.
- Bucket policies, KMS encryption, and other service restrictions can still
  require additional permissions.

## Cloud read timeouts

Cloud downloads (S3/GCS full-object reads and S3 range reads) default to a
**600-second total request timeout**, which allows larger files on slower
links. Override with an integer from 1 to 3600:

```bash
export GRV_CLOUD_READ_TIMEOUT_SECONDS=1800  # 30 minutes
```

This is a total deadline, not a throughput promise or idle timeout. Upload
timeouts remain 120 seconds; no write-retry or conditional-write safety rule
changes.
