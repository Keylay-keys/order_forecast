-- RouteSpark durable PCF work and append-only event ledger.
--
-- This migration creates storage only. It does not enable internal ingress,
-- claims, OCR, Firebase writes, notifications, cleanup, or any production
-- effect.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'routespark_pcf_owner') THEN
        CREATE ROLE routespark_pcf_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'routespark_pcf_app') THEN
        CREATE ROLE routespark_pcf_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END
$$;

-- Idempotently repair role attributes as well as role presence.  The runtime
-- pool authenticates as routespark_pcf_app; the table owner must never login.
ALTER ROLE routespark_pcf_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
ALTER ROLE routespark_pcf_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;

GRANT USAGE, CREATE ON SCHEMA public TO routespark_pcf_owner;

SET ROLE routespark_pcf_owner;

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM PUBLIC;

CREATE TABLE IF NOT EXISTS pcf_work_items (
    work_id UUID PRIMARY KEY,
    route_number TEXT NOT NULL,
    delivery_number TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    input_hash_version TEXT NOT NULL,
    status TEXT NOT NULL,
    required_capability TEXT NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 100,
    manifest JSONB NOT NULL,
    ocr_context JSONB NOT NULL,
    result JSONB,
    source_queue_path TEXT NOT NULL,
    not_before TIMESTAMPTZ NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    worker_heartbeat_at TIMESTAMPTZ,
    claim_token_hash TEXT,
    hard_deadline_at TIMESTAMPTZ,
    effect_step TEXT,
    effect_attempt_count INTEGER NOT NULL DEFAULT 0,
    effect_not_before TIMESTAMPTZ,
    confidence SMALLINT,
    last_error_code TEXT,
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (route_number, delivery_number, input_hash_version, input_hash),
    UNIQUE (route_number, delivery_number, source_revision),
    CONSTRAINT pcf_work_route_valid CHECK (route_number ~ '^[0-9]{1,10}$'),
    CONSTRAINT pcf_work_delivery_valid CHECK (
        delivery_number ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CONSTRAINT pcf_work_source_revision_valid CHECK (
        source_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|0$'
    ),
    CONSTRAINT pcf_work_input_hash_valid CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT pcf_work_input_hash_version_valid CHECK (
        input_hash_version = 'pcf-work-input-v1'
    ),
    CONSTRAINT pcf_work_status_valid CHECK (
        status IN (
            'queued', 'leased', 'processing', 'effects_pending', 'complete',
            'review_scheduled', 'blocked', 'dead_letter', 'superseded',
            'cancelled'
        )
    ),
    CONSTRAINT pcf_work_capability_valid CHECK (
        required_capability = 'macos_vision'
    ),
    CONSTRAINT pcf_work_manifest_object CHECK (jsonb_typeof(manifest) = 'object'),
    CONSTRAINT pcf_work_ocr_context_object CHECK (jsonb_typeof(ocr_context) = 'object'),
    CONSTRAINT pcf_work_result_object CHECK (
        result IS NULL OR jsonb_typeof(result) = 'object'
    ),
    CONSTRAINT pcf_work_source_queue_path_valid CHECK (
        source_queue_path = 'pcf_processing_queue/' || route_number
            || '/queue/' || delivery_number
    ),
    CONSTRAINT pcf_work_attempts_valid CHECK (
        attempt_count >= 0 AND max_attempts BETWEEN 1 AND 100
    ),
    CONSTRAINT pcf_work_claim_hash_valid CHECK (
        claim_token_hash IS NULL OR claim_token_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT pcf_work_effect_step_valid CHECK (
        effect_step IS NULL
        OR effect_step IN ('projecting', 'verifying', 'cleaning', 'notifying', 'summarizing')
    ),
    CONSTRAINT pcf_work_effect_attempt_valid CHECK (effect_attempt_count >= 0),
    CONSTRAINT pcf_work_confidence_valid CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 100
    ),
    CONSTRAINT pcf_work_claim_state_consistent CHECK (
        (
            status IN ('leased', 'processing')
            AND claimed_by IS NOT NULL
            AND claimed_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND hard_deadline_at IS NOT NULL
            AND claim_token_hash IS NOT NULL
            AND lease_expires_at > claimed_at
            AND hard_deadline_at > claimed_at
        )
        OR (
            status NOT IN ('leased', 'processing')
            AND claimed_by IS NULL
            AND claimed_at IS NULL
            AND lease_expires_at IS NULL
            AND worker_heartbeat_at IS NULL
            AND claim_token_hash IS NULL
            AND hard_deadline_at IS NULL
        )
    ),
    CONSTRAINT pcf_work_effect_state_consistent CHECK (
        (
            status = 'effects_pending'
            AND effect_step IS NOT NULL
            AND effect_not_before IS NOT NULL
        )
        OR (
            status <> 'effects_pending'
            AND effect_step IS NULL
            AND effect_not_before IS NULL
            AND effect_attempt_count = 0
        )
    ),
    CONSTRAINT pcf_work_terminal_time_consistent CHECK (
        (
            status IN ('complete', 'review_scheduled', 'dead_letter', 'superseded', 'cancelled')
            AND completed_at IS NOT NULL
        )
        OR (
            status NOT IN ('complete', 'review_scheduled', 'dead_letter', 'superseded', 'cancelled')
            AND completed_at IS NULL
        )
    )
);

-- This file is still pre-production. Keep repeated isolated-sandbox applies
-- fail-closed while the frozen v1 shape is finalized: an older non-empty test
-- table must be explicitly rebuilt instead of receiving invented OCR context.
ALTER TABLE pcf_work_items ADD COLUMN IF NOT EXISTS ocr_context JSONB;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pcf_work_items WHERE ocr_context IS NULL) THEN
        RAISE EXCEPTION 'existing PCF work rows lack frozen OCR context';
    END IF;
END
$$;
ALTER TABLE pcf_work_items ALTER COLUMN ocr_context SET NOT NULL;
ALTER TABLE pcf_work_items DROP CONSTRAINT IF EXISTS pcf_work_ocr_context_object;
ALTER TABLE pcf_work_items ADD CONSTRAINT pcf_work_ocr_context_object
    CHECK (jsonb_typeof(ocr_context) = 'object');

CREATE INDEX IF NOT EXISTS pcf_work_claim_ready_idx
    ON pcf_work_items (
        required_capability,
        priority DESC,
        not_before,
        created_at
    )
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS pcf_work_effect_due_idx
    ON pcf_work_items (effect_not_before, updated_at)
    WHERE status = 'effects_pending';

CREATE UNIQUE INDEX IF NOT EXISTS pcf_work_one_effect_authority_idx
    ON pcf_work_items (route_number, delivery_number)
    WHERE status IN ('leased', 'processing', 'effects_pending');

-- LISTEN/NOTIFY is only a wake-up optimization. The durable queued row remains
-- authoritative, and the API closes the LISTEN registration race by querying
-- again after registration.
CREATE OR REPLACE FUNCTION pcf_notify_work_available()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('pcf_work_available', NEW.work_id::TEXT);
    RETURN NEW;
END
$$;

REVOKE ALL ON FUNCTION pcf_notify_work_available() FROM PUBLIC;

DROP TRIGGER IF EXISTS pcf_work_available_notify ON pcf_work_items;
CREATE TRIGGER pcf_work_available_notify
AFTER INSERT OR UPDATE OF status, not_before, effect_not_before ON pcf_work_items
FOR EACH ROW
WHEN (NEW.status IN ('queued', 'effects_pending'))
EXECUTE FUNCTION pcf_notify_work_available();

CREATE TABLE IF NOT EXISTS pcf_work_events (
    event_id UUID PRIMARY KEY,
    source_event_id TEXT,
    work_id UUID REFERENCES pcf_work_items(work_id) ON DELETE RESTRICT,
    attempt_number INTEGER,
    event_type TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pcf_event_source_id_valid CHECK (
        source_event_id IS NULL
        OR source_event_id ~ '^[A-Za-z0-9][A-Za-z0-9._:/+\-]{7,255}$'
    ),
    CONSTRAINT pcf_event_attempt_valid CHECK (
        attempt_number IS NULL OR attempt_number >= 0
    ),
    CONSTRAINT pcf_event_work_attempt_consistent CHECK (
        (work_id IS NULL AND attempt_number IS NULL)
        OR (work_id IS NOT NULL AND attempt_number IS NOT NULL)
    ),
    CONSTRAINT pcf_event_type_valid CHECK (
        event_type ~ '^[a-z][a-z0-9_.]{2,95}$'
    ),
    CONSTRAINT pcf_event_actor_type_valid CHECK (
        actor_type IN ('cloud_tasks', 'machine', 'cluster', 'operator', 'system')
    ),
    CONSTRAINT pcf_event_actor_id_valid CHECK (
        char_length(actor_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT pcf_event_payload_object CHECK (jsonb_typeof(payload) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS pcf_work_events_source_event_id_idx
    ON pcf_work_events (source_event_id)
    WHERE source_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS pcf_work_events_work_created_idx
    ON pcf_work_events (work_id, created_at, event_id)
    WHERE work_id IS NOT NULL;

-- Queue TTL arming reads only recent authenticated off-box receipts. Keep that
-- global operational lookup off the append-only event table's historical body.
CREATE INDEX IF NOT EXISTS pcf_work_events_offbox_backup_idx
    ON pcf_work_events (created_at DESC, event_id DESC)
    WHERE event_type = 'offbox_backup_verified';

RESET ROLE;

ALTER TABLE pcf_work_items OWNER TO routespark_pcf_owner;
ALTER TABLE pcf_work_events OWNER TO routespark_pcf_owner;

REVOKE ALL ON TABLE pcf_work_items, pcf_work_events FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO routespark_pcf_app;
GRANT SELECT, INSERT, UPDATE ON TABLE pcf_work_items TO routespark_pcf_app;
GRANT SELECT, INSERT ON TABLE pcf_work_events TO routespark_pcf_app;

-- The runtime role is intentionally denied ownership, role membership,
-- DELETE/TRUNCATE on both tables, and UPDATE on append-only event receipts.
