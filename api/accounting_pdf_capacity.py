"""Bound PDF process trees across every API process in the same pod.

Locks are local scratch, never financial records. Kernel locks release after
errors or process death. Waiting holds no database connection or workspace lock.
"""
import contextlib
import fcntl
import hashlib
import os
from pathlib import Path
import re
import time
import threading
from fastapi import HTTPException

ROOT = Path("/tmp/routespark-pdf-capacity-v1")
ACTIVE_SLOTS = 2
WAITING_SLOTS = 8
ROUTE_WAITING_SLOTS = 2
REQUEST_SLOTS = ACTIVE_SLOTS + WAITING_SLOTS
ROUTE_REQUEST_SLOTS = 1 + ROUTE_WAITING_SLOTS
ORIGINAL_LOCK_STRIPES = 4096
WAIT_SECONDS = 45
_held = threading.local()


def _try_lock(name):
    fd = os.open(ROOT / name, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise


def _available(prefix, count):
    for index in range(count):
        fd = _try_lock(f"{prefix}-{index}")
        if fd is not None:
            return fd
    return None


def _busy():
    return HTTPException(503, "Statement reading is busy. Your original is saved. Try again shortly.",
                         headers={"Retry-After": "5"})


@contextlib.contextmanager
def _schedule(deadline):
    """Serialize only short lock/metadata operations, never database or parsing."""
    fd = None
    try:
        while fd is None:
            fd = _try_lock('scheduling')
            if fd is None:
                if time.monotonic() >= deadline:
                    raise _busy()
                time.sleep(.001)
        yield
    finally:
        if fd is not None:
            os.close(fd)


def _oldest_waiter():
    """Oldest live ticket whose route can run; stale/unlocked tickets are ignored."""
    oldest = None
    for index in range(WAITING_SLOTS):
        name = f'waiting-{index}'
        fd = _try_lock(name)
        if fd is not None:
            os.close(fd)
            continue
        fd = os.open(ROOT / name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            fields = os.read(fd, 80).split()
            # Registration is under _schedule. Empty metadata fails closed if
            # a process is stopped/interrupted before publishing its ticket.
            ticket = int(fields[0]) if fields and fields[0].isdigit() else 0
            if len(fields) == 2 and re.fullmatch(b'[0-9a-f]{24}', fields[1]):
                available = _try_lock('route-' + fields[1].decode('ascii'))
                if available is None:
                    continue
                os.close(available)
            candidate = (ticket, os.fstat(fd).st_ino)
            if oldest is None or candidate < oldest:
                oldest = candidate
        finally:
            os.close(fd)
    return oldest[1] if oldest else None


@contextlib.contextmanager
def request_capacity(route, original_key):
    """Bound retained sources and deduplicate one original before reading it.

    Database waits consume request admission, never an active reader slot.
    Kernel locks and admission tokens release if a worker dies.
    """
    if not isinstance(route, str) or not re.fullmatch(r"[0-9]{6}", route):
        raise ValueError("Invalid PDF route")
    if not isinstance(original_key, str) or not 1 <= len(original_key) <= 160:
        raise ValueError("Invalid PDF original key")
    ROOT.mkdir(mode=0o700, parents=False, exist_ok=True)
    route_key = hashlib.sha256(route.encode()).hexdigest()[:24]
    # A nonexistent/random original must not create unbounded scratch inodes.
    # A hash collision only serializes two requests; identity checks stay exact.
    source_digest = hashlib.sha256((route + ':' + original_key).encode()).digest()
    source_key = str(int.from_bytes(source_digest[:8], 'big') % ORIGINAL_LOCK_STRIPES)
    route_slot = request_slot = original_lock = None
    try:
        route_slot = _available('request-route-' + route_key, ROUTE_REQUEST_SLOTS)
        if route_slot is None:
            raise _busy()
        request_slot = _available('request', REQUEST_SLOTS)
        if request_slot is None:
            raise _busy()
        deadline = time.monotonic() + WAIT_SECONDS
        while original_lock is None:
            original_lock = _try_lock('original-' + source_key)
            if original_lock is not None:
                break
            if time.monotonic() >= deadline:
                raise _busy()
            time.sleep(.025)
        yield
    finally:
        for fd in (original_lock, request_slot, route_slot):
            if fd is not None:
                os.close(fd)


@contextlib.contextmanager
def capacity(route):
    if not isinstance(route, str) or not re.fullmatch(r"[0-9]{6}", route):
        raise ValueError("Invalid PDF route")
    if getattr(_held, "owner", None) == (os.getpid(), route):
        yield
        return
    ROOT.mkdir(mode=0o700, parents=False, exist_ok=True)
    # One reader per route on a one-CPU pod; another route can use slot two.
    route_key = hashlib.sha256(route.encode()).hexdigest()[:24]
    waiting = route_waiting = active = route_lock = None
    try:
        deadline = time.monotonic() + WAIT_SECONDS
        while active is None:
            with _schedule(deadline):
                oldest = _oldest_waiter()
                if ((waiting is None and oldest is None)
                        or (waiting is not None and oldest == os.fstat(waiting).st_ino)):
                    route_lock = _try_lock('route-' + route_key)
                    if route_lock is not None:
                        active = _available("active", ACTIVE_SLOTS)
                        if active is None:
                            os.close(route_lock)
                            route_lock = None
                if active is None and waiting is None:
                    route_waiting = _available("route-waiting-" + route_key, ROUTE_WAITING_SLOTS)
                    if route_waiting is None:
                        raise _busy()
                    waiting = _available("waiting", WAITING_SLOTS)
                    if waiting is None:
                        raise _busy()
                    os.ftruncate(waiting, 0)
                    os.write(waiting, f'{time.monotonic_ns()} {route_key}'.encode())
                if active is not None:
                    for fd in (waiting, route_waiting):
                        if fd is not None:
                            os.close(fd)
                    waiting = route_waiting = None
                    break
            if time.monotonic() >= deadline:
                raise _busy()
            time.sleep(.01)
        _held.owner = (os.getpid(), route)
        yield
    finally:
        _held.owner = None
        for fd in (active, route_lock, waiting, route_waiting):
            if fd is not None:
                os.close(fd)
