# Accountant Handoff API Contract - 2026-11-06

Status: **Backend contract approved for implementation. No endpoint is deployed.**

This contract is the boundary between the cluster and the Accounting portal for
the Accountant handoff. One canonical, read-only builder supplies both the JSON
preview and the ZIP export. The builder reports facts and evidence; it does not
compute taxable income, profit, or a tax return.

## Public endpoints

Both endpoints require the existing verified Firebase bearer token and canonical
route ownership enforced by `require_business_access`. Team membership is not
enough. Every database query remains scoped by the verified Firebase UID and
route.

```text
GET /api/business/accounting/handoff/preview?route=989262&year=2026
GET /api/business/accounting/handoff/export?route=989262&year=2026
```

`route` remains six decimal digits. `year` is a four-digit calendar year from
2000 through 2100. No endpoint accepts record IDs, filenames, labels, storage
paths, or arbitrary date ranges from the client.

Both responses use:

```text
Cache-Control: private, no-store
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
```

The preview is limited to 30 authenticated requests per minute under the
existing Accounting read policy. Export is limited to two requests per 15
minutes per authenticated user on each API replica. A PostgreSQL session lock
additionally permits only one active export for the same owner and route across
all replicas.

## Canonical snapshot rules

The builder reads one consistent server-side snapshot and returns a
`snapshotId`. It reads:

- active paid expenses and their category splits;
- the canonical confirmed settlement row for each billing period;
- the exact immutable statement attempt selected by that row's
  `statementReview` pointer;
- the exact immutable statement PDF whose SHA-256 matches that attempt;
- recorded settlement payments;
- active cash-account collections;
- active receipt reviews and the immutable supporting documents they select.

The builder fails closed with `503 HANDOFF_EVIDENCE_UNVERIFIED` if a confirmed
row, immutable attempt, manifest, original, byte count, or SHA-256 does not
reconcile. It never falls back to a newer OCR attempt, a similarly named file,
or another billing period.

Archived paid expenses, archived cash-account collections, and deleted receipts
are excluded. A supporting document is exportable only when its active purchase
is marked for the Accountant handoff and linked to an active paid expense placed
in the requested year.

Statement facts are placed by billing-period end date. Settlement payments and
cash-account collections are placed by received date. Paid expenses are placed
by payment date. All relevant dates remain in the output, and a billing period
whose start and end have different years is flagged.

## Preview response

Content type: `application/json`.

The response shape is versioned independently of user-facing labels. Money is
always integer US cents. An unread fact is `null`, never zero. Every summarized
statement field includes known and unknown counts so a partial total cannot look
complete.

```json
{
  "schemaVersion": 1,
  "route": "989262",
  "year": 2026,
  "generatedAt": "2026-11-06T22:30:00Z",
  "snapshotId": "sha256:0123456789abcdef...",
  "revisions": {
    "expenses": 12,
    "moneyIn": 18,
    "cashAccounts": 7,
    "receiptReviewsDigest": "sha256:abcdef0123456789..."
  },
  "placement": {
    "statementFacts": "billing_period_end",
    "settlementPayments": "received_date",
    "cashAccountCollections": "received_date",
    "paidExpenses": "payment_date"
  },
  "statementPeriods": [
    {
      "periodStart": "2026-09-03",
      "periodEnd": "2026-09-09",
      "crossesCalendarYear": false,
      "version": "final",
      "source": {
        "uploadId": "00000000-0000-4000-8000-000000000001",
        "fileId": "00000000-0000-4000-8000-000000000002",
        "attemptId": "00000000-0000-4000-8000-000000000003",
        "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "mediaType": "application/pdf"
      },
      "facts": {
        "grossSalesCents": 514225,
        "marginCents": 125025,
        "inventoryCents": 0,
        "totalAdjustmentsCents": -5000,
        "newSettlementCents": 120025,
        "previousBalanceCents": 0,
        "statementPaymentCents": 0,
        "amountToPayCents": 120025,
        "handheldRentalCents": 2000,
        "cashAccountSalesCents": 30000,
        "adjustments": [
          {"key": "netCashSales", "sourceLabel": "Net Cash Sales", "cents": -30000},
          {"key": "hhcRentalFee", "sourceLabel": "HHC Rental Fee", "cents": -2000}
        ]
      },
      "unknownFields": []
    }
  ],
  "settlementPayments": [
    {
      "id": "00000000-0000-4000-8000-000000000010",
      "periodEnd": "2026-09-09",
      "receivedDate": "2026-09-14",
      "amountCents": 120025,
      "method": "bank",
      "note": ""
    }
  ],
  "cashAccountCollections": [
    {
      "id": "00000000-0000-4000-8000-000000000020",
      "periodEnd": "2026-09-09",
      "receivedDate": "2026-09-15",
      "amountCents": 30000,
      "method": "cash",
      "customer": "Example Store",
      "storeId": "store-123",
      "note": ""
    }
  ],
  "paidExpenses": [
    {
      "id": "00000000-0000-4000-8000-000000000030",
      "date": "2026-09-12",
      "vendor": "Example Fuel Stop",
      "category": "fuel",
      "amountCents": 17500,
      "paymentMethod": "card",
      "note": "",
      "splits": [],
      "supportingPurchaseIds": ["00000000-0000-4000-8000-000000000040"]
    }
  ],
  "supportingPurchases": [
    {
      "purchaseId": "00000000-0000-4000-8000-000000000040",
      "expenseId": "00000000-0000-4000-8000-000000000030",
      "createdAt": "2026-09-12T18:00:00Z",
      "files": [
        {
          "fileId": "00000000-0000-4000-8000-000000000041",
          "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          "mediaType": "image/jpeg",
          "bytes": 331776
        }
      ]
    }
  ],
  "summaries": {
    "statementFacts": {
      "grossSalesCents": {"valueCents": 514225, "knownCount": 1, "unknownCount": 0},
      "marginCents": {"valueCents": 125025, "knownCount": 1, "unknownCount": 0},
      "amountToPayCents": {"valueCents": 120025, "knownCount": 1, "unknownCount": 0}
    },
    "settlementPayments": {"valueCents": 120025, "count": 1},
    "cashAccountCollections": {"valueCents": 30000, "count": 1},
    "paidExpenses": {"valueCents": 17500, "count": 1}
  },
  "gaps": [],
  "export": {
    "available": true,
    "sourceFileCount": 2,
    "sourceBytes": 645120,
    "maximumSourceFileCount": 5000,
    "maximumSourceBytes": 1073741824,
    "reasonCode": null
  }
}
```

`sourceLabel` preserves a label printed in the verified settlement extraction;
it is evidence, not RouteSpark interface copy. Claude supplies the presentation
labels for stable machine keys, PDF headings, and CSV columns before X2 is
released.

### Gap entries

Each gap has a stable code, record type, record ID, and relevant dates. It does
not contain document bytes or extracted PDF text.

```json
{
  "code": "MARKED_PURCHASE_WITHOUT_PAID_EXPENSE",
  "recordType": "purchase",
  "recordId": "00000000-0000-4000-8000-000000000050",
  "dates": {"createdAt": "2026-09-20T17:00:00Z"},
  "fields": []
}
```

Stable v1 gap codes:

- `MARKED_PURCHASE_WITHOUT_PAID_EXPENSE`
- `PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS`
- `STATEMENT_FACTS_UNKNOWN`
- `PRELIMINARY_STATEMENT_ONLY`
- `BILLING_PERIOD_CROSSES_CALENDAR_YEAR`

An unverified or missing source is an endpoint failure, not a gap.

## ZIP export

Content type: `application/zip`.

```text
Content-Disposition: attachment; filename="routespark-accountant-handoff-2026.zip"
```

The export endpoint runs the same builder itself. It does not trust or accept a
preview response, `snapshotId`, amount, document reference, or hash from the
client. `manifest.json` repeats the exact preview facts and snapshot identity
used for the export and adds a SHA-256 and byte count for every other ZIP entry.
The response header `X-RouteSpark-Manifest-SHA256` authenticates the manifest
itself without creating an impossible self-referential hash.

The archive contains:

```text
summary.pdf
income-by-billing-period.csv
paid-expenses.csv
items-to-check.csv
manifest.json
settlement-statements/YYYY-MM-DD_to_YYYY-MM-DD.pdf
supporting-documents/YYYY-MM-DD/<expense-id>/<purchase-id>/<sequence>.<jpg|png|pdf>
```

The server generates every archive path. Original filenames appear only as
escaped metadata in `manifest.json`; they never become paths. ZIP entries cannot
be absolute, contain `..`, use backslashes, or be symlinks.

CSV text is encoded as UTF-8 with a BOM for common spreadsheet programs. String
cells beginning with `=`, `+`, `-`, `@`, tab, carriage return, or newline are
escaped to prevent spreadsheet formula injection. Money remains integer cents
in the machine-readable manifest and uses Claude's approved headings and display
format in the human-facing files.

The v1 export refuses more than 5,000 immutable source files or 1 GiB of verified
source bytes with `413 HANDOFF_EXPORT_TOO_LARGE`. The preview remains available
and reports the measured counts. The server uses bounded temporary storage,
permits one export at a time per owner and route, and deletes its exact temporary
file in `finally` after completion or disconnect. It does not retain generated
exports or create database records.

## Errors

Errors use a stable code plus a non-sensitive plain message. The portal owns the
final on-screen wording.

| HTTP | Code | Meaning |
|---:|---|---|
| 400 | `HANDOFF_YEAR_INVALID` | Year is outside the supported range or malformed |
| 401 | existing authentication code | Firebase authentication failed |
| 403 | existing Business access code | Caller is not the canonical route owner |
| 409 | `HANDOFF_EXPORT_BUSY` | An export for the same owner and route is active |
| 413 | `HANDOFF_EXPORT_TOO_LARGE` | Verified source count or bytes exceed v1 limits |
| 429 | `HANDOFF_EXPORT_RATE_LIMITED` | Export rate limit exceeded |
| 503 | `HANDOFF_EVIDENCE_UNVERIFIED` | Canonical stored evidence failed reconciliation |
| 503 | `HANDOFF_PREVIEW_UNAVAILABLE` | Preview storage or generation was unavailable |
| 503 | `HANDOFF_EXPORT_UNAVAILABLE` | Bounded export generation failed without exposing internals |

No error names a database table, storage path, private email address, original
filename, extracted document text, or another account.

## Snapshot identity

`snapshotId` is a server-generated SHA-256 over canonical JSON containing:

- route and year;
- the three ledger revisions;
- each included settlement source pointer and SHA-256;
- each active receipt review revision, archive state, link, selected file IDs,
  and immutable file SHA-256;
- each included expense, settlement payment, and collection ID plus its fields.

The digest excludes `generatedAt`, display labels, and document bytes. Preview
and export tests must prove that identical source state produces the same
snapshot ID and totals, while any included fact or evidence selection changes
the ID.

## Required verification before deployment

1. Preliminary and final uploads for one billing period produce exactly one
   canonical period and export only the final pointer.
2. A preliminary-only period is included once and produces the corresponding
   gap.
3. Missing gross sales remains `null`, increments `unknownCount`, and never
   contributes zero to a complete-looking total.
4. Payments, collections, expenses, and cross-year periods follow the approved
   placement rules while retaining every relevant date.
5. A marked unlinked purchase and an expense without documents both appear in
   `items-to-check.csv`.
6. Archived expenses, archived collections, and deleted receipts are absent.
7. Any source-pointer, manifest, byte-count, or hash mismatch fails closed.
8. Owner, assigned team member, unrelated owner, and anonymous HTTP cases prove
   owner-only isolation.
9. Preview and ZIP are built independently from the same canonical builder and
   have matching `snapshotId`, totals, gaps, and manifest hashes.
10. Filenames, CSV cells, ZIP paths, disconnect cleanup, rate limits, concurrent
    requests, and maximum sizes are exercised with hostile boundary inputs.
11. Export generation is benchmarked on the cluster hardware without holding an
    Accounting write lock during PDF/image compression or response transfer.
12. The endpoint writes no Accounting record and logs no financial values,
    document bytes, original filenames, or recovery material.
