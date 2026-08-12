# RouteSpark Runtime Supervisors

RouteSpark production order and forecast services use PostgreSQL. There is no
database-manager process, Firebase `dbRequests` bridge, or local order-analytics
database in the active runtime.

## Production

Kubernetes starts the split runtime through `deploy/runtime/docker-entrypoint.sh`
and `runtime_supervisor.py`:

- `web-api`
- `order-forecast`
- `listeners`
- `workers`

Inspect production through the deployment manifests and `kubectl`; do not start
the historical monolithic stack on a production node.

## Local development

`supervisor_docker.py` and `supervisor.py` launch PostgreSQL-backed services only.
They require the normal `POSTGRES_*` environment variables and a Firebase service
account. No `--db` or `--duckdb` option exists.

```sh
python supervisor.py status
python supervisor.py start --service-account /path/to/service-account.json
python supervisor.py stop
```

The macOS-only helpers remain separate because they manage workstation services,
not the production order database.

## DuckDB boundary

DuckDB is permitted only in isolated offline audit warehouses such as the promo
and mailbox archive analysis tools. Those files are not an order source of truth,
do not receive Firebase order sync, do not produce live correction evidence, and
must never be wired into an active supervisor or API dependency.

The retired order/forecast DuckDB manager, client, schema, sync, feedback, share,
and retraining entrypoints were removed after PostgreSQL cutover.
