-- Administrative, additive role provisioning. No login credentials or grants
-- to application identities. Existing unexpected role attributes fail closed.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';
DO $$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['routespark_schedule_owner',
    'routespark_schedule_projector','routespark_schedule_legacy','routespark_schedule_reader'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', role_name);
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname=role_name AND
      (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) THEN
      RAISE EXCEPTION 'Unexpected schedule privilege role attributes' USING ERRCODE='42501';
    END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE roleid=
    (SELECT oid FROM pg_roles WHERE rolname='routespark_schedule_owner')) THEN
    RAISE EXCEPTION 'Schedule owner must have no runtime members' USING ERRCODE='42501';
  END IF;
END $$;
-- The legacy trigger needs metadata SELECT immediately when 001 commits,
-- before any image replacement or ownership transfer. All current application
-- roles inherit routespark; this adds no projector/owner membership.
GRANT routespark_schedule_legacy, routespark_schedule_reader TO routespark;
COMMIT;
