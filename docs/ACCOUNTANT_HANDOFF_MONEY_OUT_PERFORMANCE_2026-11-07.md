# Accountant Handoff Money-Out Performance Check

Date: 2026-11-07
Status: **Local synthetic evidence only. Cluster-hardware verification remains a deployment gate.**

The canonical preview was exercised with server-owned receipt metadata for both upload sources. The benchmark did not allocate source image bytes because the production preview reads `original_metadata` rather than document content. It did account for the exact file counts and byte totals used by the 5,000-file and 1 GiB export limits.

| Receipts | Source | Bytes represented | Local preview time |
|---:|---|---:|---:|
| 500 | App scan, 324 KiB each | 165,888,000 | 0.007509 s |
| 1,000 | App scan, 324 KiB each | 331,776,000 | 0.015203 s |
| 500 | Computer upload, 12 MiB each | 6,291,456,000 | 0.007517 s |
| 1,000 | Computer upload, 12 MiB each | 12,582,912,000 | 0.015212 s |

The maximum-size computer cases correctly represent packages above the existing 1 GiB export limit. They measure builder work, not a permitted ZIP size.

## Interpretation

- Canonical JSON and reconciliation work is linear and small in the in-memory fixture.
- Source byte size does not affect preview CPU time because preview reads metadata only.
- This does not measure PostgreSQL latency. The route advisory lock is held for the preview transaction, so the actual cluster measurement must include database round trips and report total lock hold time.
- No batching, chunked ZIP, new queue, or additional cache was added from this synthetic result.

## Cluster gate

Before deployment, run the same 500 and 1,000 receipt cases against the candidate image and cluster PostgreSQL. Record preview duration, SQL statement count, and route-lock hold time. A result near the five-second competing-write lock timeout requires a measured query reduction before release. The cluster image build and deployment still require separate approval.
