# Accounting receipt integrity contract

Date: 2026-11-06

## Capture and import boundary

The app currently records each retained page's byte count, but does not compute a
phone-side SHA-256 digest. Firebase Storage supplies a server-computed MD5 value
for each exact object generation. The Firebase completion function seals that
generation, size, content type and MD5 in `storageManifestHash`.

The cluster importer then:

1. pins the exact Firebase Storage generation;
2. revalidates its identity, byte count, content type and server MD5;
3. downloads and parses the complete JPEG;
4. computes SHA-256 from the downloaded bytes;
5. commits the originals and receipt manifest to PostgreSQL; and
6. reads every committed original back through the normal SHA-256 verification
   path before setting `clusterImportStatus: imported`.

The phone may remove its retained copy only after the cluster returns `imported`.
If database read-back fails, Firebase remains pending and the phone retains its
copy. The importer retries transient failures and never marks that packet
imported without a successful read-back.

No new app field is required for this contract. A future contract version may
add a client-computed digest for additional end-to-end evidence, but that is not
the boundary that previously allowed an unverified database write to be called
imported.

## Phone source-status contract

After import, the original authenticated uploader may poll:

`GET /api/business/accounting-purchase-source-status?route={route}&packetId={packetId}`

The endpoint rechecks the upload identity, the uploader's current owner or
verified-team-member assignment, both canonical route ownership anchors, and
the cluster import owner. It returns no receipt content or route-wide data.

Possible responses are:

```json
{"route":"989262","packetId":"...","status":"processing"}
{"route":"989262","packetId":"...","status":"verified"}
{"route":"989262","packetId":"...","status":"needs_rescan","reason":"stored_original_hash_mismatch"}
```

`processing` includes `Retry-After: 5`. Every response is private and
non-cacheable. The app should keep its retained photo and small upload record
while processing, remove both after `verified`, and keep the photo while showing
the rescan alert after `needs_rescan`. A revoked team member and any other user
receive no status.

## Receipt inbox integrity status

Before OCR claims a receipt, the cluster reads and verifies every saved source.
If a source is unsafe, OCR is blocked and the receipt inbox item contains:

```json
{
  "processingStatus": "needs_attention",
  "sourceIntegrity": {
    "schemaVersion": 1,
    "status": "needs_rescan",
    "reason": "stored_original_hash_mismatch",
    "detectedAt": "2026-11-06T19:00:00+00:00"
  }
}
```

Allowed `reason` values are:

- `stored_original_missing`
- `stored_original_hash_mismatch`
- `stored_original_metadata_mismatch`

The public object deliberately contains no file ID, file name, storage path,
database key or digest. Portal copy should use one action-oriented message for
all three reasons: `We could not verify this saved receipt image. Scan it again
while you still have the receipt.`

## Exact-file replacement

The generic upload endpoint must not simply accept a duplicate digest when the
existing row is damaged. `accounting_originals` has a tenant-scoped unique
digest constraint and is intentionally immutable to the runtime role. A safe
repair needs a targeted replacement command that:

- names the affected receipt under the authenticated tenant scope;
- accepts bytes whose computed digest exactly matches that receipt's manifest;
- proves the retained bytes fail verification;
- replaces only that one content-addressed original; and
- records an audit event.

Until that command exists, the supported immediate recovery is a new scan (new
bytes), followed by archiving the damaged receipt. This repair work is separate
from the import read-back change and must not be implemented by weakening the
duplicate or unique constraints.

## Periodic audit and backup recovery

Encrypted backups are whole PostgreSQL backups, not an online per-file object
store. A periodic integrity audit can be added independently. Automatic repair
must wait for an isolated restore-and-verify workflow that can recover one exact
tenant-scoped original without allowing a live database to be overwritten from
an unverified backup. The periodic audit is not part of the immediate import
contract and does not block the scanner OTA.

