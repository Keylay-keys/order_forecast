-- Local candidate only. Preserve evidence; do not enable any cleanup.
-- Requires a separate runtime ACL/ownership review before production application.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';
CREATE TABLE IF NOT EXISTS order_archive_send_evidence (
    order_id TEXT PRIMARY KEY REFERENCES orders_historical(order_id),
    route_number TEXT NOT NULL,
    evidence JSONB NOT NULL,
    CHECK ((jsonb_typeof(evidence) = 'object'
       AND order_id ~ '^order-[0-9]{1,10}-send-[a-f0-9]{64}$'
       AND evidence->'version' = '1'::jsonb
       AND evidence->>'orderId' = order_id
       AND evidence->>'policy' = 'send_next_day_v1'
       AND evidence->'scheduledOrder'->>'sourceRouteNumber' = route_number) IS TRUE)
);
CREATE OR REPLACE FUNCTION protect_archive_send_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Immutable archived Send evidence' USING ERRCODE = '23514';
END;
$$;
CREATE OR REPLACE TRIGGER archive_send_evidence_guard
BEFORE UPDATE OR DELETE ON order_archive_send_evidence
FOR EACH ROW EXECUTE FUNCTION protect_archive_send_evidence();

CREATE OR REPLACE FUNCTION protect_archive_send_header() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.order_id ~ '^order-[0-9]{1,10}-send-[a-f0-9]{64}$' THEN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'Send archive retention not enabled' USING ERRCODE = '23514';
        END IF;
        IF NEW.order_id IS DISTINCT FROM OLD.order_id
           OR NEW.route_number IS DISTINCT FROM OLD.route_number
           OR NEW.user_id IS DISTINCT FROM OLD.user_id
           OR NEW.order_date IS DISTINCT FROM OLD.order_date
           OR NEW.scheduled_order IS DISTINCT FROM OLD.scheduled_order
           OR NEW.expected_load_date IS DISTINCT FROM OLD.expected_load_date
           OR NEW.delivery_date IS DISTINCT FROM OLD.delivery_date
           OR NEW.schedule_key IS DISTINCT FROM OLD.schedule_key
           OR NEW.adjustment_anchor IS DISTINCT FROM OLD.adjustment_anchor
           OR NEW.finalized_at IS DISTINCT FROM OLD.finalized_at THEN
            RAISE EXCEPTION 'Immutable archived Send header' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE OR REPLACE TRIGGER archive_send_header_guard
BEFORE UPDATE OR DELETE ON orders_historical
FOR EACH ROW EXECUTE FUNCTION protect_archive_send_header();
COMMIT;
