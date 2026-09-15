#!/usr/bin/env python3
"""Manage the external-drive RouteSpark forecast backtesting lab.

The lab separates cluster access from model evaluation:

* ``init`` creates the SSD directory and a forecast-only APFS sparse image.
* ``refresh`` performs one repeatable-read, read-only export through the pod.
* ``run`` restores the selected immutable snapshot into local PostgreSQL and
  runs the walk-forward harness without contacting the cluster.
* ``doctor`` verifies the drive, snapshot, and required local tools.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
from typing import Iterator


REPO_ROOT = Path(__file__).resolve().parents[2]
ORDER_FORECAST_ROOT = REPO_ROOT / "order_forecast"
EXPORTER = Path(__file__).with_name("forecast_lab_cluster_export.py")
LAB_GUIDE = ORDER_FORECAST_ROOT / "FORECAST_LAB.md"

EXTERNAL_VOLUME = Path("/Volumes/Extreme SSD")
EXPECTED_EXTERNAL_UUID = "28F3460F-72E4-3C91-8982-F9660B840E16"
DEFAULT_LAB_ROOT = EXTERNAL_VOLUME / "routespark-harnesses" / "forecast-engine"
MOUNT_POINT = Path("/Volumes/RouteSparkForecastLab")
POSTGRES_BIN = Path("/opt/homebrew/opt/postgresql@16/bin")
LAB_ROLE = "routespark_forecast_lab"
LAB_DATABASE = "routespark_forecast_lab"
CONFIG_NAME = "lab.json"
IMAGE_RELATIVE_PATH = Path("database/forecast-lab-postgres.sparseimage")

IMPORT_ORDER = (
    "orders_historical",
    "order_line_items",
    "forecast_corrections",
    "product_catalog",
    "user_schedules",
    "stores",
    "store_items",
    "forecast_band_calibration",
    "forecast_band_center_calibration",
    "forecast_band_source_calibration",
    "forecast_finalize_events",
    "forecast_generation_jobs",
    "prediction_log",
)


class LabError(RuntimeError):
    pass


def _run(command: list[str], **kwargs) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(command, check=True, **kwargs)
    except FileNotFoundError as exc:
        raise LabError(f"required command is missing: {command[0]}") from exc
    except subprocess.CalledProcessError as exc:
        rendered = " ".join(command)
        raise LabError(f"command failed ({exc.returncode}): {rendered}") from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _disk_info(path: Path) -> dict:
    result = _run(["diskutil", "info", "-plist", str(path)], stdout=subprocess.PIPE)
    return plistlib.loads(result.stdout)


def _verify_external_volume() -> None:
    if not EXTERNAL_VOLUME.is_dir():
        raise LabError(f"external volume is not mounted: {EXTERNAL_VOLUME}")
    actual_uuid = str(_disk_info(EXTERNAL_VOLUME).get("VolumeUUID") or "")
    if actual_uuid != EXPECTED_EXTERNAL_UUID:
        raise LabError(
            f"external volume UUID mismatch: expected {EXPECTED_EXTERNAL_UUID}, got {actual_uuid or 'missing'}"
        )


def _lab_root(value: str | None = None) -> Path:
    configured = Path(value or os.environ.get("FORECAST_LAB_ROOT", str(DEFAULT_LAB_ROOT)))
    external_resolved = EXTERNAL_VOLUME.resolve()
    resolved = configured.resolve(strict=False)
    if resolved != external_resolved and external_resolved not in resolved.parents:
        raise LabError(f"forecast lab must remain under {EXTERNAL_VOLUME}: {resolved}")
    return resolved


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temp.write_text(value, encoding="utf-8")
    os.replace(temp, path)


def _load_config(root: Path) -> dict:
    path = root / CONFIG_NAME
    if not path.is_file():
        raise LabError(f"lab is not initialized; run init first: {path}")
    config = json.loads(path.read_text(encoding="utf-8"))
    if config.get("external_volume_uuid") != EXPECTED_EXTERNAL_UUID:
        raise LabError("lab configuration belongs to a different external volume")
    return config


def _attach_image(root: Path, config: dict) -> None:
    image = root / config["postgres_image"]
    if not image.is_file() or image.is_symlink():
        raise LabError(f"forecast PostgreSQL image is missing or unsafe: {image}")
    if MOUNT_POINT.exists():
        raise LabError(f"forecast lab mount point is already in use: {MOUNT_POINT}")
    _run(["hdiutil", "attach", "-nobrowse", "-owners", "on", str(image)])
    try:
        info = _disk_info(MOUNT_POINT)
        if str(info.get("FilesystemType") or "").lower() != "apfs":
            raise LabError("forecast database image did not mount as APFS")
        if str(info.get("VolumeUUID") or "") != config.get("image_volume_uuid"):
            raise LabError("forecast database image UUID mismatch")
    except Exception:
        _run(["hdiutil", "detach", str(MOUNT_POINT)])
        raise


def _detach_image() -> None:
    if MOUNT_POINT.exists():
        _run(["hdiutil", "detach", str(MOUNT_POINT)])


def _compact_image(root: Path, config: dict) -> None:
    image = root / config["postgres_image"]
    _run(["hdiutil", "compact", str(image)], stdout=subprocess.DEVNULL)


def init_lab(root: Path, image_size: str) -> None:
    _verify_external_volume()
    root.mkdir(parents=True, exist_ok=True)
    for child in ("database", "snapshots", "runs"):
        (root / child).mkdir(exist_ok=True)

    config_path = root / CONFIG_NAME
    image_path = root / IMAGE_RELATIVE_PATH
    if config_path.exists():
        config = _load_config(root)
        if not (root / config["postgres_image"]).is_file():
            raise LabError("lab config exists but its PostgreSQL image is missing")
        print(f"[forecast-lab] Already initialized: {root}")
    else:
        if image_path.exists():
            raise LabError(f"unmanaged image already exists; refusing to adopt it: {image_path}")
        _run(
            [
                "hdiutil", "create", "-size", image_size, "-type", "SPARSE", "-fs", "APFS",
                "-volname", MOUNT_POINT.name, str(image_path),
            ]
        )
        if MOUNT_POINT.exists():
            raise LabError(f"mount point already exists after image creation: {MOUNT_POINT}")
        _run(["hdiutil", "attach", "-nobrowse", "-owners", "on", str(image_path)])
        try:
            info = _disk_info(MOUNT_POINT)
            image_uuid = str(info.get("VolumeUUID") or "")
            if not image_uuid:
                raise LabError("new forecast database image has no volume UUID")
            config = {
                "schema_version": "routespark-forecast-lab-config-v1",
                "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "external_volume": str(EXTERNAL_VOLUME),
                "external_volume_uuid": EXPECTED_EXTERNAL_UUID,
                "postgres_image": str(IMAGE_RELATIVE_PATH),
                "image_volume_uuid": image_uuid,
                "image_filesystem": "APFS",
                "network_listener": "disabled; Unix socket only",
            }
            _atomic_json(config_path, config)
        finally:
            _detach_image()
        print(f"[forecast-lab] Created forecast-only PostgreSQL image: {image_path}")

    if LAB_GUIDE.is_file():
        shutil.copy2(LAB_GUIDE, root / "README.md")
    print(f"[forecast-lab] Lab root: {root}")


def _safe_archive_members(archive: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members = archive.getmembers()
    for member in members:
        member_path = Path(member.name)
        if member_path.is_absolute() or ".." in member_path.parts or member.issym() or member.islnk():
            raise LabError(f"unsafe snapshot archive member: {member.name}")
    return members


def _read_snapshot_manifest(archive_path: Path) -> dict:
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            _safe_archive_members(archive)
            handle = archive.extractfile("manifest.json")
            if handle is None:
                raise LabError("snapshot archive has no manifest.json")
            manifest = json.load(handle)
    except (tarfile.TarError, json.JSONDecodeError) as exc:
        raise LabError(f"invalid snapshot archive: {archive_path}") from exc
    if manifest.get("schema_version") != "routespark-forecast-lab-snapshot-v1":
        raise LabError("unsupported forecast snapshot schema")
    for table in IMPORT_ORDER:
        if table not in manifest.get("tables", {}):
            raise LabError(f"snapshot manifest is missing required table: {table}")
    return manifest


def _extract_and_verify_snapshot(archive_path: Path, destination: Path) -> dict:
    manifest = _read_snapshot_manifest(archive_path)
    with tarfile.open(archive_path, "r:gz") as archive:
        members = _safe_archive_members(archive)
        archive.extractall(destination, members=members, filter="data")
    for table, metadata in manifest["tables"].items():
        table_path = destination / "tables" / f"{table}.csv"
        if not table_path.is_file():
            raise LabError(f"snapshot table file is missing: {table_path.name}")
        if _sha256(table_path) != metadata["sha256"]:
            raise LabError(f"snapshot table checksum mismatch: {table}")
    return manifest


def refresh_snapshot(root: Path, routes: str | None) -> None:
    _verify_external_volume()
    _load_config(root)
    if not EXPORTER.is_file():
        raise LabError(f"cluster exporter is missing: {EXPORTER}")

    with tempfile.TemporaryDirectory(prefix="routespark-forecast-refresh-") as temp_name:
        temp_root = Path(temp_name)
        archive_path = temp_root / "forecast_snapshot.tar.gz"
        command = [
            "kubectl", "-n", "routespark-forecast", "exec", "-i", "deploy/order-forecast",
            "--", "python3", "-",
        ]
        if routes:
            command.extend(["--routes", routes])
        print("[forecast-lab] Reading one repeatable snapshot from the cluster...")
        exporter_bytes = EXPORTER.read_bytes()
        with archive_path.open("wb") as output:
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=output, stderr=subprocess.PIPE)
            _, stderr = process.communicate(input=exporter_bytes)
        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace").strip()
            raise LabError(f"cluster snapshot export failed ({process.returncode}): {detail}")

        manifest = _read_snapshot_manifest(archive_path)
        stamp = manifest["exported_at_utc"].replace("-", "").replace(":", "")
        stamp = stamp.replace("+0000", "Z").replace("+00:00", "Z")
        snapshot_id = stamp
        destination = root / "snapshots" / snapshot_id
        if destination.exists():
            raise LabError(f"immutable snapshot already exists: {destination}")
        destination.mkdir(parents=True)
        shutil.copy2(archive_path, destination / archive_path.name)
        _atomic_json(destination / "manifest.json", manifest)
        _atomic_text(destination / "SHA256", f"{_sha256(destination / archive_path.name)}  {archive_path.name}\n")
        _atomic_text(root / "CURRENT", snapshot_id + "\n")
        if LAB_GUIDE.is_file():
            shutil.copy2(LAB_GUIDE, root / "README.md")

    print(f"[forecast-lab] Snapshot: {snapshot_id}")
    print(f"[forecast-lab] Routes: {manifest['route_count']}; orders: {manifest['tables']['orders_historical']['rows']}")
    print(f"[forecast-lab] No further cluster access is needed for local runs.")


def _resolve_snapshot(root: Path, requested: str | None) -> tuple[str, Path, dict]:
    snapshot_id = requested
    if not snapshot_id:
        current = root / "CURRENT"
        if not current.is_file():
            raise LabError("no CURRENT snapshot; run refresh first")
        snapshot_id = current.read_text(encoding="utf-8").strip()
    if not snapshot_id or Path(snapshot_id).name != snapshot_id:
        raise LabError(f"invalid snapshot ID: {snapshot_id!r}")
    archive_path = root / "snapshots" / snapshot_id / "forecast_snapshot.tar.gz"
    if not archive_path.is_file():
        raise LabError(f"snapshot archive is missing: {archive_path}")
    return snapshot_id, archive_path, _read_snapshot_manifest(archive_path)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _postgres_program(name: str) -> str:
    path = POSTGRES_BIN / name
    if not path.is_file() or not os.access(path, os.X_OK):
        raise LabError(f"required PostgreSQL 16 program is missing: {path}")
    return str(path)


def _initialize_database(data_dir: Path, socket_dir: Path, port: int) -> None:
    _run([
        _postgres_program("initdb"), "-D", str(data_dir), "-A", "trust", "-U", LAB_ROLE,
        "--no-locale", "--encoding=UTF8",
    ], stdout=subprocess.DEVNULL)
    _run([
        _postgres_program("pg_ctl"), "-D", str(data_dir),
        "-o", f"-F -k {socket_dir} -h '' -p {port}", "-w", "start",
    ], stdout=subprocess.DEVNULL)
    _run([
        _postgres_program("createdb"), "-h", str(socket_dir), "-p", str(port),
        "-U", LAB_ROLE, LAB_DATABASE,
    ])


def _stop_database(data_dir: Path) -> None:
    _run([
        _postgres_program("pg_ctl"), "-D", str(data_dir), "-m", "fast", "-w", "stop",
    ], stdout=subprocess.DEVNULL)


def _import_snapshot(extracted: Path, manifest: dict, socket_dir: Path, port: int) -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import psycopg2
    from psycopg2 import sql
    import pg_schema

    connection = pg_schema.get_connection(
        host=str(socket_dir), port=port, database=LAB_DATABASE, user=LAB_ROLE, password=""
    )
    try:
        pg_schema.create_schema(connection, manage_protected_tables=True)
        with connection.cursor() as cursor:
            identifiers = sql.SQL(", ").join(sql.Identifier(table) for table in reversed(IMPORT_ORDER))
            cursor.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(identifiers))
            for table in IMPORT_ORDER:
                metadata = manifest["tables"][table]
                columns = metadata["columns"]
                table_path = extracted / "tables" / f"{table}.csv"
                statement = sql.SQL(
                    "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '\\N')"
                ).format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                )
                with table_path.open("r", encoding="utf-8", newline="") as handle:
                    cursor.copy_expert(statement.as_string(connection), handle)
        connection.commit()
    finally:
        connection.close()


def _git_state(repository: Path) -> dict:
    revision = _run(
        ["git", "rev-parse", "HEAD"], cwd=repository, stdout=subprocess.PIPE, text=True
    ).stdout.strip()
    status = _run(
        ["git", "status", "--porcelain"], cwd=repository, stdout=subprocess.PIPE, text=True
    ).stdout
    return {"revision": revision, "dirty": bool(status.strip())}


def run_backtest(root: Path, snapshot: str | None, backtest_args: list[str]) -> None:
    _verify_external_volume()
    config = _load_config(root)
    snapshot_id, archive_path, snapshot_manifest = _resolve_snapshot(root, snapshot)
    if "--outputDir" in backtest_args:
        raise LabError("forecast-lab owns --outputDir so every run remains on the external drive")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = root / "runs" / run_id
    if run_dir.exists():
        raise LabError(f"run directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)

    database_root: Path | None = None
    socket_root: Path | None = None
    postgres_started = False
    image_attached = False
    exit_code = 1
    started_at = datetime.now(timezone.utc)
    try:
        _attach_image(root, config)
        image_attached = True
        database_root = Path(tempfile.mkdtemp(prefix="forecast-pg-", dir=MOUNT_POINT))
        data_dir = database_root / "data"
        socket_root = Path(tempfile.mkdtemp(prefix="routespark-forecast-pg-socket-", dir="/private/tmp"))
        socket_root.chmod(0o700)
        port = _free_port()
        _initialize_database(data_dir, socket_root, port)
        postgres_started = True

        extracted = database_root / "snapshot"
        extracted.mkdir()
        manifest = _extract_and_verify_snapshot(archive_path, extracted)
        print(f"[forecast-lab] Restoring immutable snapshot {snapshot_id}...")
        _import_snapshot(extracted, manifest, socket_root, port)

        args = list(backtest_args)
        if args and args[0] == "--":
            args = args[1:]
        if "--asOfDate" not in args:
            args.extend(["--asOfDate", manifest["snapshot_as_of_date"]])
        command = [
            str(ORDER_FORECAST_ROOT / "venv/bin/python"),
            str(Path(__file__).with_name("walk_forward_backtest.py")),
            *args,
            "--outputDir", str(run_dir),
        ]
        environment = os.environ.copy()
        environment.update({
            "POSTGRES_HOST": str(socket_root),
            "POSTGRES_PORT": str(port),
            "POSTGRES_DB": LAB_DATABASE,
            "POSTGRES_USER": LAB_ROLE,
            "POSTGRES_PASSWORD": "",
            "POSTGRES_APPLICATION_NAME": "routespark-forecast-lab-backtest",
            "FORECAST_LAB_SNAPSHOT_ID": snapshot_id,
        })
        print(f"[forecast-lab] Running locally; cluster access is not used. Results: {run_dir}")
        completed = subprocess.run(command, cwd=ORDER_FORECAST_ROOT, env=environment)
        exit_code = int(completed.returncode)

        run_manifest = {
            "schema_version": "routespark-forecast-lab-run-v1",
            "started_at_utc": started_at.isoformat(timespec="seconds"),
            "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "snapshot_id": snapshot_id,
            "snapshot_archive_sha256": _sha256(archive_path),
            "snapshot_exported_at_utc": snapshot_manifest["exported_at_utc"],
            "snapshot_as_of_date": snapshot_manifest["snapshot_as_of_date"],
            "git": {
                "workspace": _git_state(REPO_ROOT),
                "order_forecast": _git_state(ORDER_FORECAST_ROOT),
            },
            "code_sha256": {
                path.name: _sha256(path)
                for path in (
                    Path(__file__),
                    EXPORTER,
                    Path(__file__).with_name("walk_forward_backtest.py"),
                    Path(__file__).with_name("experimental_hierarchical_hurdle.py"),
                    Path(__file__).with_name("experimental_settlement_truth.py"),
                    Path(__file__).with_name("forecast_engine.py"),
                    Path(__file__).with_name("firebase_loader.py"),
                    Path(__file__).with_name("pg_schema.py"),
                )
            },
            "backtest_arguments": args,
            "output_directory": str(run_dir),
            "database_location": "forecast APFS sparse image; disposable per run",
            "network_listener": "disabled; Unix socket only",
            "cluster_access_during_run": False,
            "exit_code": exit_code,
        }
        _atomic_json(run_dir / "lab_run_manifest.json", run_manifest)
    finally:
        cleanup_error: Exception | None = None
        if postgres_started and database_root is not None:
            try:
                _stop_database(database_root / "data")
                postgres_started = False
            except Exception as exc:
                cleanup_error = exc
        if not postgres_started and database_root is not None:
            shutil.rmtree(database_root, ignore_errors=False)
        if socket_root is not None:
            shutil.rmtree(socket_root, ignore_errors=True)
        if image_attached and not postgres_started:
            try:
                _detach_image()
                image_attached = False
                _compact_image(root, config)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
        if cleanup_error:
            raise LabError(f"forecast lab cleanup failed; resources were preserved where possible: {cleanup_error}")

    if exit_code != 0:
        raise LabError(f"walk-forward backtest exited with status {exit_code}; see {run_dir}")
    print(f"[forecast-lab] Completed: {run_dir}")


def doctor(root: Path) -> None:
    _verify_external_volume()
    config = _load_config(root)
    for program in ("initdb", "pg_ctl", "createdb"):
        _postgres_program(program)
    for command in ("hdiutil", "diskutil", "kubectl"):
        if shutil.which(command) is None:
            raise LabError(f"required command is missing: {command}")
    if not EXPORTER.is_file() or not LAB_GUIDE.is_file():
        raise LabError("forecast lab source files are incomplete")
    image = root / config["postgres_image"]
    if not image.is_file():
        raise LabError(f"forecast database image is missing: {image}")
    current = "none"
    if (root / "CURRENT").is_file():
        current, archive_path, manifest = _resolve_snapshot(root, None)
        current = f"{current} ({manifest['route_count']} routes, {manifest['tables']['orders_historical']['rows']} orders)"
    print(f"[forecast-lab] OK")
    print(f"[forecast-lab] Root: {root}")
    print(f"[forecast-lab] Current snapshot: {current}")
    print("[forecast-lab] Local database networking: disabled")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RouteSpark external-drive forecast testing lab")
    parser.add_argument("--root", help=f"Lab root (default: {DEFAULT_LAB_ROOT})")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create the SSD lab and APFS database image")
    init_parser.add_argument("--imageSize", default="12g", help="Sparse image maximum size")

    refresh_parser = subparsers.add_parser("refresh", help="Read one fresh immutable cluster snapshot")
    refresh_parser.add_argument("--routes", help="Optional comma-separated route allowlist")

    run_parser = subparsers.add_parser("run", help="Restore locally and run walk-forward backtesting")
    run_parser.add_argument("--snapshot", help="Snapshot ID (default: CURRENT)")
    run_parser.add_argument("backtest_args", nargs=argparse.REMAINDER)

    subparsers.add_parser("doctor", help="Validate the lab without contacting the cluster")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        root = _lab_root(args.root)
        if args.command == "init":
            init_lab(root, args.imageSize)
        elif args.command == "refresh":
            refresh_snapshot(root, args.routes)
        elif args.command == "run":
            run_backtest(root, args.snapshot, args.backtest_args)
        elif args.command == "doctor":
            doctor(root)
    except LabError as exc:
        raise SystemExit(f"[forecast-lab] ERROR: {exc}") from exc


if __name__ == "__main__":
    main()
