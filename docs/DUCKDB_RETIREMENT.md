# DuckDB retirement boundary

RouteSpark order and forecast runtime services use PostgreSQL directly. DuckDB
is not a runtime database, synchronization target, correction store, or model
training source.

The retired operational path included the DuckDB manager/client, Firebase sync,
schema bootstrap, feedback collector, share computation, retraining scheduler,
JSON loader, query tester, migration helper, and their supervisor wiring. Those
files and command-line switches have been removed.

DuckDB remains intentionally available only for isolated, offline analysis in:

- `scripts/build_mailbox_archive_warehouse.py`
- `scripts/build_promo_audit_warehouse.py`
- `scripts/deep_promo_anomaly_report.py`

`scripts/promo_audit_paths.py` may name the offline warehouse file, but it does
not import or connect to DuckDB. Production containers install
`requirements_docker.txt`, which does not include DuckDB. The broader local
`requirements.txt` retains DuckDB solely so the approved offline tools work.

`scripts/test_no_legacy_duckdb_runtime.py` enforces this boundary and should be
updated deliberately if an offline audit tool is added or removed.
