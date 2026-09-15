-- Additive email intake. Originals are immutable; jobs contain only processing state.
CREATE TABLE IF NOT EXISTS accounting_email_addresses (
    address text PRIMARY KEY CHECK (address ~ '^s-[0-9a-f]{32}@[a-z0-9.-]+$'),
    firebase_uid text NOT NULL CHECK (length(firebase_uid) BETWEEN 1 AND 128),
    route_number text NOT NULL CHECK (route_number ~ '^[0-9]{6}$'),
    forwarder_email text NOT NULL CHECK (length(forwarder_email) BETWEEN 3 AND 254),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (firebase_uid, route_number)
);
CREATE TABLE IF NOT EXISTS accounting_email_originals (
    message_id uuid PRIMARY KEY,
    address text NOT NULL REFERENCES accounting_email_addresses(address),
    firebase_uid text NOT NULL,
    route_number text NOT NULL CHECK (route_number ~ '^[0-9]{6}$'),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    envelope_from text NOT NULL CHECK (length(envelope_from) <= 254),
    content bytea NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 26214400),
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (address, sha256)
);
CREATE INDEX IF NOT EXISTS accounting_email_originals_scope_idx
    ON accounting_email_originals (firebase_uid, route_number, received_at DESC);
CREATE TABLE IF NOT EXISTS accounting_email_jobs (
    message_id uuid PRIMARY KEY REFERENCES accounting_email_originals(message_id),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','reading','ready','duplicate','verification','needs_attention','blocked')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    lease_token uuid,
    lease_expires_at timestamptz,
    result jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(result) = 'object'),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS accounting_email_jobs_pending_idx
    ON accounting_email_jobs (updated_at) WHERE status IN ('queued','reading') AND attempts < 3;
REVOKE ALL ON accounting_email_addresses, accounting_email_originals, accounting_email_jobs FROM PUBLIC;
