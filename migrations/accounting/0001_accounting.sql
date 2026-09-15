-- Additive hosted pilot storage. No prototype import, deletes, or seeded money.
-- Ledger documents preserve the existing UI contract; bill items have independent
-- UUIDs referenced by payments. A workspace lock serializes cross-ledger changes.
CREATE TABLE IF NOT EXISTS accounting_documents (
    firebase_uid text NOT NULL CHECK (length(firebase_uid) BETWEEN 1 AND 128),
    route_number text NOT NULL CHECK (route_number ~ '^[0-9]{6}$'),
    document_key text NOT NULL CHECK (length(document_key) BETWEEN 1 AND 200),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (firebase_uid, route_number, document_key)
);

CREATE TABLE IF NOT EXISTS accounting_originals (
    firebase_uid text NOT NULL,
    route_number text NOT NULL CHECK (route_number ~ '^[0-9]{6}$'),
    kind text NOT NULL CHECK (kind IN ('receipt', 'statement')),
    upload_id uuid NOT NULL,
    file_id uuid NOT NULL,
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    media_type text NOT NULL CHECK (media_type IN ('image/jpeg','image/png','application/pdf')),
    original_name text NOT NULL CHECK (length(original_name) BETWEEN 1 AND 200),
    content bytea NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 15728640),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (firebase_uid, route_number, kind, upload_id, file_id),
    UNIQUE (firebase_uid, route_number, kind, sha256)
);

CREATE TABLE IF NOT EXISTS accounting_history (
    event_id uuid PRIMARY KEY,
    firebase_uid text NOT NULL,
    route_number text NOT NULL,
    document_key text NOT NULL,
    payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS accounting_history_scope_idx
    ON accounting_history (firebase_uid, route_number, document_key, recorded_at DESC);

-- Attempts are immutable evidence. Only a separate document selects the latest.
CREATE TABLE IF NOT EXISTS accounting_attempts (
    firebase_uid text NOT NULL,
    route_number text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('receipt', 'statement')),
    upload_id uuid NOT NULL,
    file_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (firebase_uid, route_number, kind, upload_id, file_id, attempt_id)
);

REVOKE ALL ON accounting_documents, accounting_originals, accounting_history,
    accounting_attempts FROM PUBLIC;
-- The migration runner grants SELECT/INSERT/UPDATE on accounting_documents and
-- SELECT/INSERT only on originals/history/attempts to the exact runtime role.

CREATE TABLE IF NOT EXISTS accounting_receipt_jobs (
    job_id uuid PRIMARY KEY,
    firebase_uid text NOT NULL,
    route_number text NOT NULL CHECK (route_number ~ '^[0-9]{6}$'),
    upload_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','complete','blocked')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    lease_token uuid,
    lease_expires_at timestamptz,
    completed_attempt_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (firebase_uid, route_number, upload_id)
);
CREATE INDEX IF NOT EXISTS accounting_receipt_jobs_pending_idx ON accounting_receipt_jobs (created_at)
    WHERE status IN ('queued','running') AND attempts < 3;
REVOKE ALL ON accounting_receipt_jobs FROM PUBLIC;
