-- Candidate additive migration. Apply only after explicit role/backup preflight.
-- Required NOLOGIN privilege roles must be provisioned separately; never grant
-- projector membership to an old worker. No role creation or live execution here.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';
ALTER TABLE public.user_schedules ADD COLUMN IF NOT EXISTS send_offset_days integer;
CREATE TABLE public.order_schedule_revisions (
  route_number text PRIMARY KEY,
  owner_uid text NOT NULL,
  revision bigint NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
  source_payload jsonb NOT NULL,
  projected_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.user_schedules ADD CONSTRAINT schedule_v3_offsets_valid CHECK (
  COALESCE(schedule_version, 2) <> 3 OR (
    send_offset_days IS NOT NULL AND load_offset_days IS NOT NULL AND delivery_offset_days IS NOT NULL
    AND needs_schedule_review IS FALSE AND is_active IS TRUE
    AND order_day BETWEEN 1 AND 7
    AND 0 <= send_offset_days AND send_offset_days <= load_offset_days
    AND load_offset_days <= delivery_offset_days AND delivery_offset_days <= 60
    AND load_day = (order_day - 1 + load_offset_days) % 7 + 1
    AND delivery_day = (order_day - 1 + delivery_offset_days) % 7 + 1
    AND schedule_key = (ARRAY['monday','tuesday','wednesday','thursday','friday','saturday','sunday'])[order_day]
  )
) NOT VALID;

CREATE FUNCTION public.guard_schedule_projection() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
  old_route text;
  new_route text;
  target text;
BEGIN
  IF TG_OP <> 'INSERT' THEN old_route := OLD.route_number; END IF;
  IF TG_OP <> 'DELETE' THEN new_route := NEW.route_number; END IF;
  FOR target IN SELECT DISTINCT r FROM unnest(ARRAY[old_route, new_route]) r WHERE r IS NOT NULL ORDER BY r LOOP
    PERFORM pg_advisory_xact_lock(hashtextextended(target, 307113));
    IF NOT pg_has_role(current_user, 'routespark_schedule_projector', 'MEMBER') AND
       (EXISTS (SELECT 1 FROM public.order_schedule_revisions WHERE route_number = target)
        OR (TG_OP <> 'DELETE' AND NEW.schedule_version = 3)) THEN
      RAISE EXCEPTION 'protected schedule requires projector role' USING ERRCODE = '42501';
    END IF;
  END LOOP;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER guard_schedule_projection BEFORE INSERT OR UPDATE OR DELETE ON public.user_schedules
  FOR EACH ROW EXECUTE FUNCTION public.guard_schedule_projection();
REVOKE ALL ON public.order_schedule_revisions FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_schedule_projection() FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO routespark_schedule_projector, routespark_schedule_legacy, routespark_schedule_reader;
GRANT SELECT, INSERT, UPDATE ON public.order_schedule_revisions TO routespark_schedule_projector;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_schedules TO routespark_schedule_projector;
GRANT SELECT ON public.order_schedule_revisions TO routespark_schedule_legacy, routespark_schedule_reader;
GRANT SELECT ON public.user_schedules TO routespark_schedule_reader;
-- Existing legacy table privileges remain unchanged. The role must not own the
-- table or have TRUNCATE, trigger-disable, projector membership, or superuser.
COMMIT;
