-- RouteSpark PCF shared projection and bounded due-work schema.
--
-- This migration creates storage only.  It does not enable ingress, claims,
-- Firebase writes, notifications, archive copies, or deletes.

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

CREATE TABLE IF NOT EXISTS items_current (
    route_number TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    container_id TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    loading_date DATE,
    page_numbers JSONB NOT NULL DEFAULT '[]'::jsonb,
    items JSONB NOT NULL,
    item_count INTEGER NOT NULL,
    valid_expiry_count INTEGER NOT NULL,
    earliest_expiry_date DATE,
    latest_expiry_date DATE,
    source_path TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    source_business_hash TEXT NOT NULL,
    source_mutation_kind TEXT NOT NULL,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (route_number, delivery_id, container_id),
    CONSTRAINT items_current_route_valid CHECK (route_number ~ '^[0-9]{1,10}$'),
    CONSTRAINT items_current_delivery_valid CHECK (
        delivery_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CONSTRAINT items_current_container_valid CHECK (
        container_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CONSTRAINT items_current_items_array CHECK (jsonb_typeof(items) = 'array'),
    CONSTRAINT items_current_page_numbers_array CHECK (
        jsonb_typeof(page_numbers) = 'array'
    ),
    CONSTRAINT items_current_item_counts_valid CHECK (
        item_count >= 0
        AND valid_expiry_count >= 0
        AND valid_expiry_count <= item_count
        AND item_count = jsonb_array_length(items)
    ),
    CONSTRAINT items_current_expiry_range_valid CHECK (
        (earliest_expiry_date IS NULL AND latest_expiry_date IS NULL)
        OR (
            earliest_expiry_date IS NOT NULL
            AND latest_expiry_date IS NOT NULL
            AND earliest_expiry_date <= latest_expiry_date
        )
    ),
    CONSTRAINT items_current_complete_expiry_valid CHECK (
        valid_expiry_count <> item_count
        OR item_count = 0
        OR (earliest_expiry_date IS NOT NULL AND latest_expiry_date IS NOT NULL)
    ),
    CONSTRAINT items_current_source_path_valid CHECK (
        source_path = 'routes/' || route_number || '/pcfs/' || delivery_id
            || '/containers/' || container_id
    ),
    CONSTRAINT items_current_source_revision_valid CHECK (
        source_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    ),
    CONSTRAINT items_current_business_hash_valid CHECK (
        source_business_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT items_current_mutation_kind_valid CHECK (
        source_mutation_kind IN ('upsert', 'delete')
    ),
    CONSTRAINT items_current_tombstone_consistent CHECK (
        (
            source_mutation_kind = 'upsert'
            AND deleted_at IS NULL
        )
        OR (
            source_mutation_kind = 'delete'
            AND deleted_at IS NOT NULL
            AND active = FALSE
            AND page_numbers = '[]'::jsonb
            AND items = '[]'::jsonb
            AND item_count = 0
            AND valid_expiry_count = 0
            AND earliest_expiry_date IS NULL
            AND latest_expiry_date IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS items_current_active_route_delivery_idx
    ON items_current (route_number, delivery_id, container_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS items_current_active_serving_idx
    ON items_current (
        route_number,
        loading_date DESC NULLS LAST,
        delivery_id DESC,
        container_id
    )
    WHERE deleted_at IS NULL AND active = TRUE;

CREATE INDEX IF NOT EXISTS items_current_tombstone_prune_idx
    ON items_current (deleted_at, route_number, delivery_id, container_id)
    WHERE deleted_at IS NOT NULL;

-- One compact control row per live route. This is the eligible-route registry
-- and request-independent projection health gate; it is not PCF history and it
-- never contains item payloads. Serving is fail-closed until reconciliation
-- proves parity and the ingress-health controller supplies a bounded window.
CREATE TABLE IF NOT EXISTS pcf_projection_routes (
    route_number TEXT PRIMARY KEY,
    eligible BOOLEAN NOT NULL DEFAULT FALSE,
    serving_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    reconciliation_status TEXT NOT NULL DEFAULT 'unknown',
    last_projected_revision TEXT,
    last_projected_at TIMESTAMPTZ,
    parity_source_revision TEXT,
    parity_projection_revision TEXT,
    source_entity_count INTEGER,
    projection_entity_count INTEGER,
    source_business_hash TEXT,
    projection_business_hash TEXT,
    last_reconciliation_attempt_at TIMESTAMPTZ,
    last_reconciled_at TIMESTAMPTZ,
    healthy_until TIMESTAMPTZ,
    lifecycle_policy_active BOOLEAN NOT NULL DEFAULT FALSE,
    lifecycle_retention_days SMALLINT,
    lifecycle_policy_revision TEXT,
    lifecycle_policy_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pcf_projection_route_valid CHECK (
        route_number ~ '^[0-9]{1,10}$'
    ),
    CONSTRAINT pcf_projection_serving_eligible CHECK (
        NOT serving_enabled OR eligible
    ),
    CONSTRAINT pcf_projection_reconciliation_status_valid CHECK (
        reconciliation_status IN ('unknown', 'matched', 'mismatched')
    ),
    CONSTRAINT pcf_projection_last_revision_valid CHECK (
        last_projected_revision IS NULL
        OR last_projected_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    ),
    CONSTRAINT pcf_projection_parity_source_revision_valid CHECK (
        parity_source_revision IS NULL
        OR parity_source_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    ),
    CONSTRAINT pcf_projection_parity_revision_valid CHECK (
        parity_projection_revision IS NULL
        OR parity_projection_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    ),
    CONSTRAINT pcf_projection_counts_valid CHECK (
        (source_entity_count IS NULL AND projection_entity_count IS NULL)
        OR (
            source_entity_count IS NOT NULL
            AND projection_entity_count IS NOT NULL
            AND source_entity_count >= 0
            AND projection_entity_count >= 0
        )
    ),
    CONSTRAINT pcf_projection_source_hash_valid CHECK (
        source_business_hash IS NULL
        OR source_business_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT pcf_projection_business_hash_valid CHECK (
        projection_business_hash IS NULL
        OR projection_business_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT pcf_projection_lifecycle_policy_complete CHECK (
        (
            lifecycle_policy_active = FALSE
            AND
            lifecycle_retention_days IS NULL
            AND lifecycle_policy_revision IS NULL
            AND lifecycle_policy_synced_at IS NULL
        )
        OR (
            lifecycle_policy_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
            AND lifecycle_policy_synced_at IS NOT NULL
            AND (
                (
                    lifecycle_policy_active = TRUE
                    AND lifecycle_retention_days BETWEEN 1 AND 3650
                    AND lifecycle_policy_revision LIKE '%|0'
                )
                OR (
                    lifecycle_policy_active = FALSE
                    AND lifecycle_retention_days IS NULL
                    AND lifecycle_policy_revision LIKE '%|1'
                )
            )
        )
    ),
    CONSTRAINT pcf_projection_matched_proof_complete CHECK (
        reconciliation_status <> 'matched'
        OR (
            parity_source_revision IS NOT NULL
            AND parity_projection_revision IS NOT NULL
            AND parity_projection_revision = parity_source_revision
            AND source_entity_count IS NOT NULL
            AND projection_entity_count IS NOT NULL
            AND source_entity_count = projection_entity_count
            AND source_business_hash IS NOT NULL
            AND projection_business_hash IS NOT NULL
            AND source_business_hash = projection_business_hash
            AND last_reconciled_at IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS pcf_projection_routes_reconcile_idx
    ON pcf_projection_routes (last_reconciliation_attempt_at, route_number)
    WHERE eligible = TRUE;

-- Existing compact dashboard mirror, now owned by the versioned PCF schema.
-- Runtime code may update a route row but cannot create or delete the table.
CREATE TABLE IF NOT EXISTS route_dashboard_summaries (
    route_number TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    source_revision TEXT,
    source_updated_at TIMESTAMPTZ,
    mirrored_revision TEXT,
    mirrored_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT route_dashboard_summaries_route_valid CHECK (
        route_number ~ '^[0-9]{1,10}$'
    ),
    CONSTRAINT route_dashboard_summaries_payload_object CHECK (
        jsonb_typeof(payload) = 'object'
    ),
    CONSTRAINT route_dashboard_summaries_source_revision_valid CHECK (
        source_revision IS NULL
        OR source_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    ),
    CONSTRAINT route_dashboard_summaries_mirrored_revision_valid CHECK (
        mirrored_revision IS NULL
        OR mirrored_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    )
);

CREATE TABLE IF NOT EXISTS due_work (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    route_number TEXT,
    delivery_id TEXT,
    container_id TEXT,
    recipient_id TEXT,
    transition_kind TEXT,
    effective_business_date DATE NOT NULL,
    source_revision TEXT NOT NULL,
    source_business_hash TEXT NOT NULL,
    decision_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
    due_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    deadline_at TIMESTAMPTZ NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'scheduled',
    block_reason TEXT,
    claim_token_hash TEXT,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    dispatch_deadline_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    terminal_outcome TEXT,
    terminal_at TIMESTAMPTZ,
    retain_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kind, semantic_key),
    CONSTRAINT due_work_kind_valid CHECK (kind IN ('expiring', 'lifecycle')),
    CONSTRAINT due_work_semantic_key_valid CHECK (
        char_length(semantic_key) BETWEEN 2 AND 2048
    ),
    CONSTRAINT due_work_route_valid CHECK (
        route_number IS NULL OR route_number ~ '^[0-9]{1,10}$'
    ),
    CONSTRAINT due_work_delivery_valid CHECK (
        delivery_id IS NULL
        OR delivery_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CONSTRAINT due_work_container_valid CHECK (
        container_id IS NULL
        OR container_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
    ),
    CONSTRAINT due_work_source_revision_valid CHECK (
        source_revision ~ '^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z\|[01]$'
    ),
    CONSTRAINT due_work_business_hash_valid CHECK (
        source_business_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT due_work_watermark_object CHECK (
        jsonb_typeof(decision_watermark) = 'object'
    ),
    CONSTRAINT due_work_time_order_valid CHECK (
        due_at <= deadline_at AND next_attempt_at <= deadline_at
    ),
    CONSTRAINT due_work_priority_valid CHECK (priority BETWEEN -32768 AND 32767),
    CONSTRAINT due_work_attempts_valid CHECK (
        attempt_count >= 0 AND max_attempts BETWEEN 1 AND 100
    ),
    CONSTRAINT due_work_status_valid CHECK (
        status IN (
            'scheduled', 'retryable', 'blocked', 'leased', 'dispatching',
            'complete', 'cancelled', 'failed', 'outcome_ambiguous', 'dead_letter'
        )
    ),
    CONSTRAINT due_work_claim_hash_valid CHECK (
        claim_token_hash IS NULL OR claim_token_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT due_work_kind_identity_valid CHECK (
        (
            kind = 'lifecycle'
            AND route_number IS NOT NULL
            AND delivery_id IS NOT NULL
            AND container_id IS NOT NULL
            AND transition_kind ~ '^[a-z][a-z0-9_]{2,63}$'
            AND recipient_id IS NULL
        )
        OR (
            kind = 'expiring'
            AND recipient_id IS NOT NULL
            AND route_number IS NULL
            AND delivery_id IS NULL
            AND container_id IS NULL
            AND transition_kind IS NULL
        )
    ),
    CONSTRAINT due_work_claim_state_consistent CHECK (
        (
            status = 'leased'
            AND claim_token_hash IS NOT NULL
            AND claimed_by IS NOT NULL
            AND claimed_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at > claimed_at
            AND dispatch_deadline_at IS NULL
        )
        OR (
            status = 'dispatching'
            AND claim_token_hash IS NOT NULL
            AND claimed_by IS NOT NULL
            AND claimed_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND dispatch_deadline_at IS NOT NULL
            AND dispatch_deadline_at > claimed_at
        )
        OR (
            status NOT IN ('leased', 'dispatching')
            AND claim_token_hash IS NULL
            AND claimed_by IS NULL
            AND claimed_at IS NULL
            AND lease_expires_at IS NULL
            AND dispatch_deadline_at IS NULL
        )
    ),
    CONSTRAINT due_work_terminal_state_consistent CHECK (
        (
            status IN ('complete', 'cancelled', 'failed', 'outcome_ambiguous')
            AND terminal_outcome IS NOT NULL
            AND terminal_at IS NOT NULL
            AND retain_until IS NOT NULL
            AND retain_until >= terminal_at
        )
        OR (
            status NOT IN ('complete', 'cancelled', 'failed', 'outcome_ambiguous')
            AND terminal_outcome IS NULL
            AND terminal_at IS NULL
            AND retain_until IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS due_work_claim_ready_idx
    ON due_work (kind, next_attempt_at, priority DESC, created_at, id)
    WHERE status IN ('scheduled', 'retryable', 'blocked');

CREATE INDEX IF NOT EXISTS due_work_lifecycle_active_entity_idx
    ON due_work (route_number, delivery_id, transition_kind, source_revision, id)
    WHERE kind = 'lifecycle'
      AND status IN ('scheduled', 'retryable', 'blocked', 'leased', 'dead_letter');

CREATE INDEX IF NOT EXISTS due_work_terminal_prune_idx
    ON due_work (retain_until, id)
    WHERE status IN ('complete', 'cancelled', 'failed', 'outcome_ambiguous');

RESET ROLE;

ALTER TABLE items_current OWNER TO routespark_pcf_owner;
ALTER TABLE pcf_projection_routes OWNER TO routespark_pcf_owner;
ALTER TABLE route_dashboard_summaries OWNER TO routespark_pcf_owner;
ALTER TABLE due_work OWNER TO routespark_pcf_owner;
ALTER SEQUENCE IF EXISTS due_work_id_seq OWNER TO routespark_pcf_owner;

REVOKE ALL ON TABLE items_current, pcf_projection_routes,
    route_dashboard_summaries, due_work FROM PUBLIC;
REVOKE ALL ON SEQUENCE due_work_id_seq FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO routespark_pcf_app;
GRANT SELECT ON TABLE items_current, pcf_projection_routes,
    route_dashboard_summaries, due_work
    TO routespark_pcf_app;
GRANT INSERT, UPDATE ON TABLE items_current, route_dashboard_summaries, due_work
    TO routespark_pcf_app;
REVOKE INSERT, UPDATE ON TABLE pcf_projection_routes FROM routespark_pcf_app;
REVOKE INSERT (eligible, serving_enabled),
       UPDATE (eligible, serving_enabled)
    ON pcf_projection_routes FROM routespark_pcf_app;
GRANT INSERT (
        route_number,
        last_projected_revision,
        last_projected_at,
        lifecycle_policy_active,
        lifecycle_retention_days,
        lifecycle_policy_revision,
        lifecycle_policy_synced_at,
        created_at,
        updated_at
    ) ON pcf_projection_routes TO routespark_pcf_app;
GRANT UPDATE (
        reconciliation_status,
        last_projected_revision,
        last_projected_at,
        parity_source_revision,
        parity_projection_revision,
        source_entity_count,
        projection_entity_count,
        source_business_hash,
        projection_business_hash,
        last_reconciliation_attempt_at,
        last_reconciled_at,
        healthy_until,
        lifecycle_policy_active,
        lifecycle_retention_days,
        lifecycle_policy_revision,
        lifecycle_policy_synced_at,
        updated_at
    ) ON pcf_projection_routes TO routespark_pcf_app;
GRANT USAGE, SELECT ON SEQUENCE due_work_id_seq TO routespark_pcf_app;

-- The runtime role is intentionally denied table ownership, DELETE, TRUNCATE,
-- role administration, membership in routespark_pcf_owner, and both INSERT and
-- UPDATE authority over eligible/serving_enabled.
