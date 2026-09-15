-- Additive reader-first migration. No backfill, deletion or cleanup activation.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';
ALTER TABLE orders_historical ADD COLUMN IF NOT EXISTS expected_load_date DATE;
ALTER TABLE orders_historical ADD COLUMN IF NOT EXISTS adjustment_anchor JSONB;

CREATE OR REPLACE FUNCTION protect_order_archive_schedule_v3() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.scheduled_order->>'version' = '3' AND (
        NEW.scheduled_order IS DISTINCT FROM OLD.scheduled_order OR
        NEW.expected_load_date IS DISTINCT FROM OLD.expected_load_date OR
        NEW.adjustment_anchor IS DISTINCT FROM OLD.adjustment_anchor OR
        NEW.finalized_at IS DISTINCT FROM OLD.finalized_at OR
        NEW.route_number IS DISTINCT FROM OLD.route_number OR
        NEW.schedule_key IS DISTINCT FROM OLD.schedule_key OR
        NEW.delivery_date IS DISTINCT FROM OLD.delivery_date
    ) THEN
        RAISE EXCEPTION 'Immutable archived schedule evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE OR REPLACE TRIGGER orders_historical_schedule_v3_guard
BEFORE UPDATE ON orders_historical
FOR EACH ROW EXECUTE FUNCTION protect_order_archive_schedule_v3();
COMMIT;
