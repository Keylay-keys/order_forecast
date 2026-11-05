-- Additive receipt-agent queue. OCR evidence and agent review remain separate.
-- The model has no database credential and can only return a candidate through
-- the leased worker API. User confirmation remains required to record expense data.
CREATE TABLE IF NOT EXISTS accounting_receipt_agent_jobs (
    job_id uuid PRIMARY KEY,
    firebase_uid text NOT NULL,
    route_number text NOT NULL CHECK (route_number ~ '^[0-9]{6}$'),
    upload_id uuid NOT NULL,
    ocr_attempt_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','complete','blocked')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    lease_token uuid,
    lease_expires_at timestamptz,
    completed_review_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (firebase_uid, route_number, upload_id, ocr_attempt_id)
);
CREATE INDEX IF NOT EXISTS accounting_receipt_agent_jobs_pending_idx
    ON accounting_receipt_agent_jobs (created_at)
    WHERE status IN ('queued','running') AND attempts < 3;
REVOKE ALL ON accounting_receipt_agent_jobs FROM PUBLIC;
