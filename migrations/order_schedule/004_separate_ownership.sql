-- Apply only AFTER compatible startup/readers and migrations 000-003.
-- Administrative session only. Do not remove shared membership database-wide.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='routespark_schedule_owner'
      AND NOT (rolcanlogin OR rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls))
     OR EXISTS (SELECT 1 FROM pg_auth_members WHERE roleid=
       (SELECT oid FROM pg_roles WHERE rolname='routespark_schedule_owner')) THEN
    RAISE EXCEPTION 'Dedicated non-inherited schedule owner required' USING ERRCODE='42501';
  END IF;
END $$;
ALTER TABLE public.user_schedules OWNER TO routespark_schedule_owner;
ALTER TABLE public.orders_historical OWNER TO routespark_schedule_owner;
ALTER TABLE public.order_schedule_revisions OWNER TO routespark_schedule_owner;
ALTER FUNCTION public.guard_schedule_projection() OWNER TO routespark_schedule_owner;
ALTER FUNCTION public.protect_order_archive_schedule_v3() OWNER TO routespark_schedule_owner;
ALTER FUNCTION public.protect_order_archive_schedule_v3() SET search_path = pg_catalog, public;

REVOKE ALL ON public.user_schedules, public.orders_historical, public.order_schedule_revisions
  FROM PUBLIC, routespark, routespark_events, routespark_forecast, routespark_web_api;
REVOKE ALL ON FUNCTION public.guard_schedule_projection(), public.protect_order_archive_schedule_v3()
  FROM PUBLIC, routespark, routespark_events, routespark_forecast, routespark_web_api;
GRANT USAGE ON SCHEMA public TO routespark_schedule_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_schedules, public.orders_historical TO routespark;
GRANT SELECT ON public.order_schedule_revisions TO routespark;

-- Check effective privileges, not just the direct ACL: unexpected inherited
-- grants or projector membership abort and roll back the ownership change.
DO $$
DECLARE runtime text; target text;
BEGIN
  FOREACH runtime IN ARRAY ARRAY['routespark','routespark_events','routespark_forecast','routespark_web_api'] LOOP
    IF pg_has_role(runtime,'routespark_schedule_projector','MEMBER') THEN
      RAISE EXCEPTION 'Runtime unexpectedly inherits projector' USING ERRCODE='42501';
    END IF;
    FOREACH target IN ARRAY ARRAY['public.user_schedules','public.orders_historical','public.order_schedule_revisions'] LOOP
      IF has_table_privilege(runtime,target,'TRUNCATE') OR has_table_privilege(runtime,target,'TRIGGER') THEN
        RAISE EXCEPTION 'Unexpected effective protected-table privileges' USING ERRCODE='42501';
      END IF;
    END LOOP;
    IF has_table_privilege(runtime,'public.order_schedule_revisions','INSERT') OR
       has_table_privilege(runtime,'public.order_schedule_revisions','UPDATE') OR
       has_table_privilege(runtime,'public.order_schedule_revisions','DELETE') THEN
      RAISE EXCEPTION 'Unexpected revision metadata write privileges' USING ERRCODE='42501';
    END IF;
  END LOOP;
END $$;
COMMIT;
