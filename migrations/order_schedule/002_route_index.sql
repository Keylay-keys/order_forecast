-- Separate, non-transactional candidate step. Review existing indexes first.
-- On timeout/failure, inspect an invalid leftover index before any retry.
SET lock_timeout = '2s';
SET statement_timeout = '30s';
CREATE INDEX CONCURRENTLY schedule_projection_route_rows
  ON public.user_schedules (route_number, id);
RESET statement_timeout;
RESET lock_timeout;
