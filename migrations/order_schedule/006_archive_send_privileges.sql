-- Local candidate. Apply only after 005, before any Send creation activation.
-- No credentials, role creation, historical rewrites or retention changes.
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
  IF (SELECT pg_get_userbyid(relowner) FROM pg_class
      WHERE oid='public.orders_historical'::regclass) <> 'routespark_schedule_owner' THEN
    RAISE EXCEPTION 'Protected history owner required' USING ERRCODE='42501';
  END IF;
END $$;

ALTER TABLE public.order_archive_send_evidence OWNER TO routespark_schedule_owner;
ALTER FUNCTION public.protect_archive_send_evidence() OWNER TO routespark_schedule_owner;
ALTER FUNCTION public.protect_archive_send_header() OWNER TO routespark_schedule_owner;
ALTER FUNCTION public.protect_archive_send_evidence() SET search_path = pg_catalog, public;
ALTER FUNCTION public.protect_archive_send_header() SET search_path = pg_catalog, public;
REVOKE ALL ON public.order_archive_send_evidence
  FROM PUBLIC, routespark, routespark_events, routespark_forecast, routespark_web_api;
REVOKE ALL ON FUNCTION public.protect_archive_send_evidence(), public.protect_archive_send_header()
  FROM PUBLIC, routespark, routespark_events, routespark_forecast, routespark_web_api;
GRANT SELECT, INSERT ON public.order_archive_send_evidence TO routespark_events;
GRANT SELECT ON public.order_archive_send_evidence TO routespark_web_api;

-- Inspect effective grants including inheritance, not just the direct ACL.
DO $$
DECLARE runtime text; permission text; allowed boolean;
BEGIN
  FOREACH runtime IN ARRAY ARRAY['routespark','routespark_events','routespark_forecast','routespark_web_api'] LOOP
    IF pg_has_role(runtime,'routespark_schedule_owner','MEMBER') THEN
      RAISE EXCEPTION 'Runtime inherits archive owner' USING ERRCODE='42501';
    END IF;
    FOREACH permission IN ARRAY ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER'] LOOP
      allowed := (runtime='routespark_events' AND permission IN ('SELECT','INSERT'))
              OR (runtime='routespark_web_api' AND permission='SELECT');
      IF has_table_privilege(runtime,'public.order_archive_send_evidence',permission) <> allowed THEN
        RAISE EXCEPTION 'Unexpected effective Send archive privileges' USING ERRCODE='42501';
      END IF;
    END LOOP;
    IF has_function_privilege(runtime,'public.protect_archive_send_evidence()','EXECUTE')
       OR has_function_privilege(runtime,'public.protect_archive_send_header()','EXECUTE') THEN
      RAISE EXCEPTION 'Unexpected guard function privileges' USING ERRCODE='42501';
    END IF;
  END LOOP;
END $$;
COMMIT;
