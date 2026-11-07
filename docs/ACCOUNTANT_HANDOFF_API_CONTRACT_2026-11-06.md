# Accountant Handoff API Contract

Date: 2026-11-06, revised 2026-11-07
Status: **Implemented and verified locally. Not deployed.**

This is the boundary between the cluster and the Accounting portal. One canonical, read-only builder supplies both the JSON preview and ZIP export. The builder reports recorded facts and evidence. It does not calculate profit, taxable income, or a tax return.

## Authorization and tenancy

All endpoints require a verified Firebase bearer token and canonical route ownership through `require_business_access`. Team membership is not enough. Every PostgreSQL read and write is scoped by the verified Firebase UID and six-digit route. Responses use `Cache-Control: private, no-store`, `X-Content-Type-Options: nosniff`, and `Referrer-Policy: no-referrer`.

## Endpoints

```text
GET /api/business/accounting/handoff/preview?route=989262&year=2026
GET /api/business/accounting/handoff/export?route=989262&year=2026
POST /api/business/accounting/receipt-reviews/{packetId}/record-expense?route=989262
```

`year` is a calendar year from 2000 through 2100. Handoff endpoints accept no client-supplied facts, identifiers, filenames, hashes, storage paths, or date ranges beyond the route and year query parameters.

Preview uses the existing Accounting read limit of 30 authenticated requests per minute. Export permits two requests per 15 minutes per authenticated user on each API replica. A PostgreSQL session lock permits one active export per owner and route across replicas.

## Canonical preview

The response remains schema version 1. New fields are additive. Money is always integer US cents. Unknown statement facts are `null`, never zero.

The builder reads one route-scoped snapshot containing:

- confirmed settlement statement facts and exact selected source evidence;
- settlement payments and cash-account collections;
- active paid expenses, including category splits;
- the current bill list and recorded payments linked to each bill;
- active receipt reviews and their selected immutable source files.

Placement rules are fixed:

- statement facts by billing-period end date;
- settlement payments and cash-account collections by received date;
- paid expenses by payment date;
- an unrecorded receipt by its server-set upload year;
- after a receipt is linked, by the paid expense's payment year.

Archived paid expenses, archived collections, and deleted receipts are absent. Current bills are excluded only when created after the selected year and they have no recorded payment in that year. A current bill row never claims that a payment was owed or missed.

### Additive response fields

```json
{
  "schemaVersion": 1,
  "route": "989262",
  "year": 2026,
  "generatedAt": "2026-11-07T18:00:00Z",
  "snapshotId": "sha256:...",
  "revisions": {
    "expenses": 12,
    "moneyIn": 18,
    "cashAccounts": 7,
    "bills": 4,
    "receiptReviewsDigest": "sha256:..."
  },
  "moneyIn": {
    "verifiedStatementCount": 0,
    "unverifiedStatementCount": 0,
    "paymentCount": 0,
    "collectionCount": 0
  },
  "bills": [
    {
      "billId": "server-id",
      "createdAt": "2026-02-03T18:00:00Z",
      "vendor": "Example Storage",
      "category": "storage",
      "note": "",
      "status": "active",
      "frequency": "monthly",
      "dueDate": "2026-11-20",
      "anchorDay": 20,
      "amountCents": 28400,
      "paymentCount": 9,
      "paymentTotalCents": 255600
    }
  ],
  "receiptsNotRecorded": [
    {
      "purchaseId": "server-id",
      "createdAt": "2026-09-12T18:00:00Z",
      "documentCount": 2,
      "fileStatus": "included",
      "folder": "receipts-not-recorded/2026-09-12-001",
      "files": [
        {
          "fileId": "server-id",
          "sha256": "...",
          "mediaType": "image/jpeg",
          "bytes": 331776
        }
      ]
    }
  ],
  "receiptsLeftOutCount": 2,
  "summaries": {
    "paidExpensesByCategory": [
      {"category": "fuel", "valueCents": 15000},
      {"category": "route_supplies", "valueCents": 2500}
    ],
    "bills": {"count": 1, "paymentCount": 9, "paymentTotalCents": 255600}
  },
  "export": {
    "available": true,
    "sourceFileCount": 1,
    "sourceBytes": 331776,
    "statementFileCount": 0,
    "supportingDocumentFileCount": 0,
    "unrecordedReceiptFileCount": 1,
    "maximumSourceFileCount": 5000,
    "maximumSourceBytes": 1073741824,
    "reasonCode": null
  }
}
```

Existing fields remain: `placement`, `statementPeriods`, `settlementPayments`, `cashAccountCollections`, `paidExpenses`, `supportingPurchases`, statement and payment summaries, and `gaps`.

Internal IDs remain in authenticated JSON where the portal needs to link an action. They never become ZIP paths or error details. Handoff facts never use reader-proposed receipt payee, date, amount, classification, or reading status.

### Receipt cases

- Checked and linked in the selected year: supporting document for the expense.
- Unchecked and linked: file omitted and `LINKED_PURCHASE_NOT_MARKED` added.
- Checked and unlinked in the upload year: included under `receiptsNotRecorded`, outside all financial totals.
- Unchecked and unlinked in the upload year: omitted and counted only in `receiptsLeftOutCount`.
- Known damaged and checked: no file is exported, `fileStatus` is `unverified` for an unlinked receipt, and `SUPPORTING_DOCUMENT_EVIDENCE_UNVERIFIED` is added. The rest of the handoff remains available.
- Unexpected source corruption while building the ZIP: the entire temporary archive is removed and export fails closed with safe dated details.

New portal and phone receipts persist `includeInAccountantHandoff: true` in the same transaction that creates the receipt. The fallback for an older receipt with no saved review remains `false`. An import replay does not overwrite a choice the owner later changed.

## Atomic receipt recording

Request:

```json
{
  "reviewRevision": 0,
  "expenseRevision": 3,
  "groupedFileIds": ["server-file-id"],
  "includeInAccountantHandoff": true,
  "expense": {
    "date": "2026-11-07",
    "vendor": "Example Fuel Stop",
    "category": "fuel",
    "amountCents": 7500,
    "paymentMethod": "card",
    "note": ""
  }
}
```

The request body is exact. The server validates the route-scoped receipt, review revision, expense revision, selected source IDs, handoff choice, and expense input. One database transaction creates exactly one paid expense and links the receipt. Remembering a payee is deliberately outside this operation.

Response:

```json
{
  "schemaVersion": 1,
  "route": "989262",
  "expenseLedger": {"schemaVersion": 1, "route": "989262", "revision": 4, "items": [], "bills": []},
  "receiptInbox": {"schemaVersion": 1, "route": "989262", "items": [], "archivedItems": []}
}
```

`expenseLedger.bills` is the existing current-bill convenience list returned by
expense writes. It is not a second ledger revision and does not change the
atomic boundary.

The normal `Idempotency-Key` replay contract applies. If the response is lost after commit and the portal retries with a new key, the committed `recorded` state or stale revision returns `409` before a second expense is created. The portal reloads the receipt inbox after that conflict.

## Gaps and evidence safety

Stable gap codes are:

- `MARKED_PURCHASE_WITHOUT_PAID_EXPENSE`
- `PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS`
- `LINKED_PURCHASE_NOT_MARKED`
- `STATEMENT_EVIDENCE_UNVERIFIED`
- `SUPPORTING_DOCUMENT_EVIDENCE_UNVERIFIED`
- `STATEMENT_FACTS_UNKNOWN`
- `PRELIMINARY_STATEMENT_ONLY`
- `BILLING_PERIOD_CROSSES_CALENDAR_YEAR`

The builder isolates an attributable damaged statement or receipt at preview time and omits its unverified facts and bytes. Corruption discovered only while reading source bytes for export fails the ZIP with `503 HANDOFF_EVIDENCE_UNVERIFIED`. Its optional `details` value is allowlisted to exactly one of these shapes:

```json
{"recordType":"statement","periodStart":"2026-09-03","periodEnd":"2026-09-09"}
```

```json
{"recordType":"receipt","createdDate":"2026-09-12"}
```

No error exposes a database ID, upload ID, file ID, filename, digest, storage path, private address, extracted text, or exception text. If the identifying dates cannot themselves be trusted, `details` is omitted.

## ZIP export

The export endpoint rebuilds the canonical snapshot. It never accepts a preview response, `snapshotId`, amount, source reference, or hash from the client. `manifest.json` contains the exact snapshot used and the SHA-256 and byte count of every other ZIP entry.

Only non-empty CSVs and evidence folders are written:

```text
summary.pdf
income-by-billing-period.csv
paid-expenses.csv
bills.csv
receipts-not-recorded.csv
items-to-check.csv
settlement-statements/YYYY-MM-DD_to_YYYY-MM-DD.pdf
supporting-documents/YYYY-MM-DD-NNN/NN.ext
receipts-not-recorded/YYYY-MM-DD-NNN/NN.ext
manifest.json
```

All archive paths are generated by the server. They contain no internal IDs or original filenames and cannot be absolute, contain `..`, use backslashes, or be symlinks. CSV is UTF-8 with a BOM and formula-leading text is escaped.

The v1 export limit remains 5,000 source files or 1 GiB of verified source bytes. A failed or disconnected export removes its exact temporary file. The server retains no generated handoff archive and writes no Accounting record.

## Stable handoff errors

| HTTP | Code |
|---:|---|
| 400 | `HANDOFF_YEAR_INVALID` |
| 403 | existing Business access code |
| 409 | `HANDOFF_EXPORT_BUSY` |
| 413 | `HANDOFF_EXPORT_TOO_LARGE` |
| 429 | `HANDOFF_EXPORT_RATE_LIMITED` |
| 503 | `HANDOFF_EVIDENCE_UNVERIFIED` |
| 503 | `HANDOFF_PREVIEW_UNAVAILABLE` |
| 503 | `HANDOFF_EXPORT_UNAVAILABLE` |

The atomic receipt endpoint uses the existing Accounting validation and revision errors, including `409` for a stale revision or already-recorded receipt.

## Snapshot identity

`snapshotId` is a server-generated SHA-256 over canonical JSON containing the route, year, four ledger revisions, receipt review digest, every included fact, current bill row, evidence descriptor, unrecorded receipt, left-out count, gap, and section count. It excludes `generatedAt`, user-facing labels, and source bytes. A bill edit or receipt selection changes the snapshot. Identical source state produces the same ID.

## Verification gates

Before deployment:

1. Receipt-only, bill-only, expense-only, payment-only, mixed, and empty years produce the exact conditional sections and files defined above.
2. Category totals follow expense splits and never count an unrecorded receipt.
3. Current bill status, schedule, payment count, archived-payment exclusion, and selected-year filtering are correct without inventing missed payments.
4. New portal and phone receipts start checked; old fallback and saved choices remain unchanged; phone replay does not overwrite a choice.
5. Atomic recording proves success, failure rollback, same-key replay, lost-response/new-key refusal, owner-only access, and tenant isolation.
6. Known linked and unlinked damage is isolated. Real-store hash failure for a receipt or statement returns only safe dates and leaves no partial ZIP.
7. Preview and ZIP use the same canonical facts, counts, gaps, snapshot ID, and manifest entry hashes.
8. ZIP paths, CSV cells, disconnect cleanup, concurrency, rate limits, and size limits pass hostile boundary tests.
9. Canonical preview is measured with 500 and 1,000 receipts on representative hardware before adding batching or chunked exports.
10. A cluster image build, deployment, portal promotion, and OTA each require separate approval.
