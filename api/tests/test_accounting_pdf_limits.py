"""Isolated synthetic resource/cleanup checks. No database or real PDFs."""
import importlib.util
import asyncio
import hashlib
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from .. import accounting_pdf_capacity as budget
from ..accounting_pdf_process import run_bounded


def held_job(route, records):
    with budget.capacity(route):
        records.put(("enter", route, time.monotonic()))
        time.sleep(.12)
        records.put(("exit", route, time.monotonic()))


def hold_until_terminated(ready):
    with budget.capacity("555555"):
        ready.set()
        time.sleep(10)


def hold_request_until_terminated(ready):
    with budget.request_capacity("555555", "synthetic-original"):
        ready.set()
        time.sleep(10)


def queued_reader_process(route, label, inherited_active, queued, entered, release, records):
    # Fork inherits open descriptions. Only the parent should own these two
    # synthetic blockers; close child copies before attempting reader admission.
    for fd in inherited_active:
        os.close(fd)
    original_available = budget._available
    def observed_available(prefix, count):
        fd = original_available(prefix, count)
        if prefix == 'waiting' and fd is not None:
            queued.set()
        return fd
    budget._available = observed_available
    try:
        with budget.capacity(route):
            records.put(label)
            entered.set()
            if not release.wait(5):
                raise AssertionError('synthetic reader was not released')
    finally:
        budget._available = original_available


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="pdf-budget-fixture-")) / "locks"
        self.patch = patch.object(budget, "ROOT", self.root)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_global_reader_limit_spans_processes(self):
        context = multiprocessing.get_context("fork")
        records = context.Queue()
        routes = ["111111", "111111", "111111", "222222", "333333", "444444"]
        jobs = [context.Process(target=held_job, args=(route, records)) for route in routes]
        for job in jobs: job.start()
        events = [records.get(timeout=8) for _ in range(2 * len(jobs))]
        for job in jobs:
            job.join(5)
            self.assertEqual(job.exitcode, 0)
        active, same_route, peak = 0, {}, 0
        for event, route, _ in sorted(events, key=lambda row: row[2]):
            delta = 1 if event == "enter" else -1
            active += delta
            same_route[route] = same_route.get(route, 0) + delta
            peak = max(peak, active)
            self.assertLessEqual(active, 2)
            self.assertLessEqual(same_route[route], 1)
        self.assertEqual(peak, 2)
        self.assertEqual(active, 0)

    def test_full_queue_cannot_be_bypassed_by_a_newcomer(self):
        self.root.mkdir(mode=0o700)
        waiters = [budget._available("waiting", 8) for _ in range(8)]
        self.assertNotIn(None, waiters)
        try:
            with self.assertRaises(budget.HTTPException):
                with budget.capacity('222222'): self.fail('bypassed existing waiters')
        finally:
            for fd in waiters: os.close(fd)
        with budget.capacity('222222'): pass

    def test_waiter_is_not_overtaken_and_stale_ticket_does_not_block(self):
        self.root.mkdir(mode=0o700)
        occupied = [budget._available('active', 2) for _ in range(2)]
        admitted, release = threading.Event(), threading.Event()
        order = []
        def existing():
            with budget.capacity('111111'):
                order.append('existing'); admitted.set()
                self.assertTrue(release.wait(3))
        def newcomer():
            with budget.capacity('222222'):
                order.append('newcomer')
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(existing)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with budget._schedule(deadline):
                    if budget._oldest_waiter() is not None: break
                time.sleep(.001)
            else:
                self.fail('first reader did not queue')
            os.close(occupied.pop())
            second = pool.submit(newcomer)
            try:
                self.assertTrue(admitted.wait(2))
                self.assertEqual(order, ['existing'])
            finally:
                release.set()
                for fd in occupied: os.close(fd)
            first.result(2); second.result(2)
        self.assertEqual(order, ['existing', 'newcomer'])
        # Released lock files retain their tickets, but no longer own queue order.
        with budget.capacity('333333'): pass

    def test_stalled_scheduling_gate_times_out_without_leaking_slots(self):
        self.root.mkdir(mode=0o700)
        gate = budget._try_lock('scheduling')
        try:
            with patch.object(budget, 'WAIT_SECONDS', .03), self.assertRaises(budget.HTTPException):
                with budget.capacity('123456'): pass
        finally:
            os.close(gate)
        with budget.capacity('123456'): pass

    def test_queue_overflow_and_timeout_are_retryable_and_release_locks(self):
        self.root.mkdir(mode=0o700)
        active = [budget._available("active", 2) for _ in range(2)]
        waiting = [budget._available("waiting", 8) for _ in range(8)]
        try:
            with self.assertRaises(budget.HTTPException) as error:
                with budget.capacity("123456"): self.fail("admitted")
            self.assertEqual(error.exception.status_code, 503)
            self.assertEqual(error.exception.headers["Retry-After"], "5")
            for fd in waiting: os.close(fd)
            waiting = []
            with patch.object(budget, "WAIT_SECONDS", .05):
                with self.assertRaises(budget.HTTPException):
                    with budget.capacity("123456"): self.fail("admitted")
            waiting = [budget._available("waiting", 8) for _ in range(8)]
            self.assertNotIn(None, waiting)
        finally:
            for fd in active + waiting:
                if fd is not None: os.close(fd)

    def test_process_death_releases_route_and_global_slots(self):
        context = multiprocessing.get_context("fork")
        ready = context.Event()
        job = context.Process(target=hold_until_terminated, args=(ready,))
        job.start()
        self.assertTrue(ready.wait(5))
        job.terminate(); job.join(5)
        self.assertFalse(job.is_alive())
        with patch.object(budget, "WAIT_SECONDS", .1):
            with budget.capacity("555555"):
                with budget.capacity("555555"):
                    pass

    def test_one_route_cannot_fill_the_shared_waiting_queue(self):
        self.root.mkdir(mode=0o700)
        route_key = hashlib.sha256(b'123456').hexdigest()[:24]
        active = [budget._available('active', 2) for _ in range(2)]
        pending = [budget._available('route-waiting-'+route_key, 2) for _ in range(2)]
        try:
            with self.assertRaises(budget.HTTPException) as error:
                with budget.capacity('123456'):
                    self.fail('same route exceeded its waiting allowance')
            self.assertEqual(error.exception.status_code, 503)
            # The offending route has not consumed a shared queue slot.
            global_waiters = [budget._available('waiting', 8) for _ in range(8)]
            self.assertNotIn(None, global_waiters)
            for fd in global_waiters:
                os.close(fd)
        finally:
            for fd in active+pending:
                os.close(fd)

    def test_waiting_busy_route_leaves_second_reader_slot_for_another_route(self):
        first_entered, release, second_entered = threading.Event(), threading.Event(), threading.Event()
        queued, other_entered = threading.Event(), threading.Event()
        original_available = budget._available
        def observed_available(prefix, count):
            fd = original_available(prefix, count)
            if prefix == 'waiting' and fd is not None:
                queued.set()
            return fd
        def read_one():
            with budget.capacity('123456'):
                first_entered.set()
                self.assertTrue(release.wait(3))
        def read_two():
            with budget.capacity('123456'):
                second_entered.set()
        def independent():
            with budget.capacity('222222'):
                self.assertFalse(second_entered.is_set())
                other_entered.set()
        with patch.object(budget, '_available', observed_available), ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(read_one)
            self.assertTrue(first_entered.wait(2))
            second = pool.submit(read_two)
            try:
                self.assertTrue(queued.wait(2))
                self.assertFalse(second_entered.is_set())
                # The older ticket belongs to a currently active route. It must
                # not block a different route from the unused second reader.
                pool.submit(independent).result(1)
                self.assertTrue(other_entered.is_set())
                self.assertFalse(second_entered.is_set())
            finally:
                release.set()
            first.result(2); second.result(2)
            self.assertTrue(second_entered.is_set())

    def test_request_admission_bounds_retained_sources_without_using_reader_slots(self):
        self.root.mkdir(mode=0o700)
        held = []
        try:
            for index in range(3):
                entry = budget.request_capacity('123456', str(index))
                entry.__enter__(); held.append(entry)
            with self.assertRaises(budget.HTTPException):
                with budget.request_capacity('123456', 'fourth'): pass
            # A DB wait in all admitted requests still leaves both readers free.
            readers = [budget._available('active', 2) for _ in range(2)]
            self.assertNotIn(None, readers)
            for fd in readers: os.close(fd)
            with budget.request_capacity('222222', 'independent'): pass
            for index in range(7):
                entry = budget.request_capacity(f'{index+200000:06d}', str(index))
                entry.__enter__(); held.append(entry)
            with self.assertRaises(budget.HTTPException):
                with budget.request_capacity('333333', 'overflow'): pass
        finally:
            for entry in reversed(held): entry.__exit__(None, None, None)
        with budget.request_capacity('333333', 'after-release'): pass

    def test_request_original_timeout_and_body_error_release_all_admission(self):
        held = []
        with budget.request_capacity('123456', 'same-original'):
            with patch.object(budget, 'WAIT_SECONDS', .05):
                with self.assertRaises(budget.HTTPException) as error:
                    with budget.request_capacity('123456', 'same-original'):
                        self.fail('duplicate acquired a held original')
            self.assertEqual(error.exception.status_code, 503)
            self.assertEqual(error.exception.headers['Retry-After'], '5')
            try:
                # Timeout must release its request tokens while the original
                # owner remains active. Both remaining route slots fit.
                for index in range(2):
                    entry = budget.request_capacity('123456', f'independent-{index}')
                    entry.__enter__(); held.append(entry)
            finally:
                for entry in reversed(held): entry.__exit__(None, None, None)
        with self.assertRaisesRegex(RuntimeError, 'synthetic failed transaction'):
            with budget.request_capacity('123456', 'same-original'):
                raise RuntimeError('synthetic failed transaction')
        # The exact-original lock and all three route slots must be reusable.
        held = []
        try:
            for original in ['same-original', 'a', 'b']:
                entry = budget.request_capacity('123456', original)
                entry.__enter__(); held.append(entry)
            for index in range(7):
                entry = budget.request_capacity(f'{300000+index:06d}', 'global-reuse')
                entry.__enter__(); held.append(entry)
        finally:
            for entry in reversed(held): entry.__exit__(None, None, None)

    def test_process_death_releases_original_and_request_admission(self):
        context = multiprocessing.get_context('fork')
        ready = context.Event()
        job = context.Process(target=hold_request_until_terminated, args=(ready,))
        job.start()
        try:
            self.assertTrue(ready.wait(5))
        finally:
            job.terminate(); job.join(5)
        self.assertFalse(job.is_alive())
        held = []
        try:
            with patch.object(budget, 'WAIT_SECONDS', .1):
                for original in ['synthetic-original', 'a', 'b']:
                    entry = budget.request_capacity('555555', original)
                    entry.__enter__(); held.append(entry)
                for index in range(7):
                    entry = budget.request_capacity(f'{400000+index:06d}', 'global-reuse')
                    entry.__enter__(); held.append(entry)
        finally:
            for entry in reversed(held): entry.__exit__(None, None, None)

    def test_original_lock_namespace_is_bounded_across_unique_requests(self):
        # Use a small namespace to exercise wraparound without leaving thousands
        # of scratch files. Production keeps the configured 4096 stripes.
        with patch.object(budget, 'ORIGINAL_LOCK_STRIPES', 4):
            for index in range(64):
                with budget.request_capacity('123456', f'unique-original-{index}'):
                    pass
        names = {path.name for path in self.root.glob('original-*')}
        self.assertEqual(names, {'original-0', 'original-1', 'original-2', 'original-3'})

    def test_original_stripe_collision_serializes_and_releases(self):
        # Deliberate collision: distinct original identities share one lock;
        # they must serialize instead of bypassing duplicate exclusion.
        with patch.object(budget, 'ORIGINAL_LOCK_STRIPES', 1):
            with budget.request_capacity('123456', 'first-original'):
                with patch.object(budget, 'WAIT_SECONDS', .05):
                    with self.assertRaises(budget.HTTPException) as error:
                        with budget.request_capacity('123456', 'different-original'):
                            self.fail('colliding original bypassed held stripe')
                self.assertEqual(error.exception.status_code, 503)
                self.assertEqual(error.exception.headers['Retry-After'], '5')
            with budget.request_capacity('123456', 'different-original'):
                pass
            self.assertEqual(len(list(self.root.glob('original-*'))), 1)

    def test_cross_process_fifo_skips_dead_oldest_waiter(self):
        context = multiprocessing.get_context('fork')
        self.root.mkdir(mode=0o700)
        blockers = [budget._available('active', 2) for _ in range(2)]
        self.assertNotIn(None, blockers)
        records = context.Queue()
        jobs, signals = [], []
        def launch(route, label):
            queued, entered, release = context.Event(), context.Event(), context.Event()
            job = context.Process(target=queued_reader_process,
                                  args=(route, label, list(blockers), queued, entered, release, records))
            jobs.append(job); signals.append((queued, entered, release))
            job.start()
            self.assertTrue(queued.wait(3), 'reader did not register a waiting slot')
            return job, entered, release
        try:
            dead, dead_entered, _ = launch('111111', 'dead')
            older, older_entered, older_release = launch('222222', 'older')
            self.assertFalse(dead_entered.is_set())
            dead.terminate(); dead.join(3)
            self.assertFalse(dead.is_alive())
            newer, newer_entered, newer_release = launch('333333', 'newer')
            # One free reader slot: the live older waiter wins even if the
            # newcomer reuses the dead waiter's lower-numbered queue file.
            os.close(blockers[0]); blockers[0] = None
            self.assertTrue(older_entered.wait(2))
            self.assertFalse(newer_entered.wait(.05))
            self.assertEqual(records.get(timeout=2), 'older')
            older_release.set()
            self.assertTrue(newer_entered.wait(2))
            self.assertEqual(records.get(timeout=2), 'newer')
            newer_release.set()
            for job in (older, newer):
                job.join(3)
                self.assertEqual(job.exitcode, 0)
        finally:
            for _, _, release in signals:
                release.set()
            for job in jobs:
                if job.is_alive(): job.terminate()
                job.join(3)
            for fd in blockers:
                if fd is not None: os.close(fd)
        # Unlocked stale ticket bytes do not obstruct a fresh request.
        with budget.capacity('444444'):
            pass


class OutputTests(unittest.TestCase):
    def command(self, code):
        return [sys.executable, "-I", "-S", "-c", code]

    def test_stdout_and_stderr_are_limited_during_read(self):
        for target in (1, 2):
            start = time.monotonic()
            with self.assertRaises(ValueError):
                run_bounded(self.command(f'import os;os.write({target},b"x"*1048576)'),
                            timeout=3, stdout_limit=1024, new_session=True)
            self.assertLess(time.monotonic() - start, 2)

    def test_timeout_and_closed_output_do_not_hang(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            run_bounded(self.command('import os,time;os.close(1);os.close(2);time.sleep(10)'),
                        timeout=.1, stdout_limit=1024, new_session=True)

    def test_environment_is_cleared_and_nonzero_output_is_not_in_error(self):
        with patch.dict(os.environ, {"POSTGRES_PASSWORD": "synthetic-sentinel", "PYTHONPATH": "/untrusted"}):
            result = run_bounded(self.command('import os;print("POSTGRES_PASSWORD" in os.environ or "PYTHONPATH" in os.environ)'),
                                 timeout=3, stdout_limit=1024, new_session=True)
        self.assertEqual(result.strip(), b"False")
        with self.assertRaises(subprocess.CalledProcessError) as error:
            run_bounded(self.command('print("SYNTHETIC PRIVATE TEXT");raise SystemExit(3)'),
                        timeout=3, stdout_limit=1024, new_session=True)
        self.assertIsNone(error.exception.output)
        self.assertIsNone(error.exception.stderr)


class AnalysisAdmissionTests(unittest.TestCase):
    def test_database_wait_does_not_serialize_distinct_statements(self):
        from ..routers import accounting
        from .. import accounting_evidence as evidence
        from .test_accounting import MemoryStore, analysis
        store = MemoryStore()
        manifest = evidence.upload(store, '123456', 'statement', [
            ('first.pdf', 'application/pdf', b'%PDF-1.4\nfirst\n%%EOF'),
            ('second.pdf', 'application/pdf', b'%PDF-1.4\nsecond\n%%EOF')])
        blocked, release = threading.Event(), threading.Event()
        local = threading.local()
        @contextmanager
        def transaction(*args):
            if getattr(local, 'block_once', False):
                local.block_once = False
                blocked.set()
                self.assertTrue(release.wait(3))
            yield store
        def execute(index):
            local.block_once = index == 0
            path = f"statement-reviews/{manifest['id']}/{manifest['files'][index]['id']}/analyze"
            return accounting.execute('owner', '123456', '2026-09-19', 'POST', path,
                                      {}, [], False, str(uuid4()), 'synthetic')
        root = Path(tempfile.mkdtemp(prefix='pdf-db-wait-fixture-')) / 'locks'
        with patch.object(budget, 'ROOT', root), patch.object(accounting, 'accounting_transaction', transaction), \
             patch.object(evidence, 'analyze_bytes', return_value=analysis()), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(execute, 0)
            self.assertTrue(blocked.wait(2))
            try:
                second = pool.submit(execute, 1).result(1)
                self.assertIsNotNone(second)
            finally:
                release.set()
            first.result(2)
        self.assertEqual(len(store.attempts), 2)

    def test_reanalysis_reuses_verified_large_original_before_parsing(self):
        from ..routers import accounting
        from .. import accounting_evidence as evidence
        from .test_accounting import MemoryStore, analysis
        store = MemoryStore()
        content = b'%PDF-1.4\n' + b'x'*(15*1024**2-16) + b'\n%%EOF\n'
        self.assertEqual(len(content), 15*1024**2)
        manifest = evidence.upload(store, '123456', 'statement', [('fixture.pdf', 'application/pdf', content)])
        path = f"statement-reviews/{manifest['id']}/{manifest['files'][0]['id']}/analyze"
        @contextmanager
        def transaction(*args):
            yield store
        root = Path(tempfile.mkdtemp(prefix='pdf-reanalysis-fixture-')) / 'locks'
        with patch.object(budget, 'ROOT', root), patch.object(accounting, 'accounting_transaction', transaction), \
             patch.object(evidence, 'analyze_bytes', return_value=analysis()):
            accounting.execute('owner', '123456', '2026-09-19', 'POST', path,
                               {}, [], False, str(uuid4()), 'first')
            with patch.object(store, 'original', wraps=store.original) as original:
                accounting.execute('owner', '123456', '2026-09-19', 'POST', path,
                                   {}, [], False, str(uuid4()), 'second')
            # One verified read before parsing and one at the result commit.
            # Looking up the older attempt must not read the 15 MiB blob again.
            self.assertEqual(original.call_count, 2)
        self.assertEqual(len(store.attempts), 2)

    def test_overlapping_duplicate_reuses_attempt_and_releases_database_during_read(self):
        from ..routers import accounting
        from .. import accounting_evidence as evidence
        from .test_accounting import MemoryStore, analysis

        store = MemoryStore()
        manifest = evidence.upload(store, '123456', 'statement',
                                   [('fixture.pdf', 'application/pdf', b'%PDF-1.4\nfixture\n%%EOF')])
        path = f"statement-reviews/{manifest['id']}/{manifest['files'][0]['id']}/analyze"
        database = threading.Lock()
        started, release, queued = threading.Event(), threading.Event(), threading.Event()
        calls = []

        @contextmanager
        def transaction(*args):
            with database:
                yield store

        def reader(*args):
            # A normal operation can acquire the transaction lock while parsing.
            self.assertTrue(database.acquire(blocking=False))
            database.release()
            calls.append(1)
            started.set()
            self.assertTrue(release.wait(3))
            return analysis()

        def execute(second=False):
            if second:
                queued.set()
            return accounting.execute('owner', '123456', '2026-09-19', 'POST', path,
                                      {}, [], False, str(uuid4()), 'synthetic-fingerprint')

        root = Path(tempfile.mkdtemp(prefix='pdf-analysis-fixture-')) / 'locks'
        with patch.object(budget, 'ROOT', root), patch.object(accounting, 'accounting_transaction', transaction), \
             patch.object(evidence, 'analyze_bytes', reader), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(execute)
            self.assertTrue(started.wait(2))
            second = pool.submit(execute, True)
            self.assertTrue(queued.wait(2))
            # Wait for the second request to enter admission before releasing the first.
            deadline = time.monotonic() + 2
            while not (root/'request-1').exists() and time.monotonic() < deadline:
                time.sleep(.005)
            self.assertTrue((root/'request-1').exists())
            ordinary = accounting.execute('owner', '123456', '2026-09-19', 'GET', 'expenses',
                                          None, [], False, None, None)
            self.assertEqual(ordinary['items'], [])
            release.set()
            one, two = first.result(2), second.result(2)
        self.assertEqual(calls, [1])
        self.assertEqual(one['id'], two['id'])
        self.assertEqual(len(store.attempts), 1)


class UploadHashReuseTests(unittest.TestCase):
    def test_fingerprint_and_saved_original_reuse_exact_server_hash(self):
        from ..routers import accounting
        from .. import accounting_evidence as evidence
        from .test_accounting import MemoryStore
        files = evidence.UploadFiles([
            ('one.pdf', 'application/pdf', b'%PDF-1.4\none\n%%EOF'),
            ('two.pdf', 'application/pdf', b'%PDF-1.4\ntwo\n%%EOF')])
        reference = accounting.upload_hashes(list(files))
        store = MemoryStore()
        with patch.object(evidence.hashlib, 'sha256', wraps=hashlib.sha256) as hashed:
            self.assertEqual(accounting.upload_hashes(files), reference)
            manifest = evidence.upload(store, '123456', 'statement', files)
            self.assertEqual(hashed.call_count, 2)
        self.assertEqual([item['sha256'] for item in manifest['files']], [row[2] for row in reference])
        # Old tuple/list callers, including email intake, still compute their own.
        other = evidence.upload(MemoryStore(), '123456', 'statement', list(files))
        self.assertEqual([item['sha256'] for item in other['files']], [row[2] for row in reference])

    def test_changed_and_mutable_bytes_cannot_reuse_an_old_hash(self):
        from ..routers import accounting
        from .. import accounting_evidence as evidence
        from .test_accounting import MemoryStore
        files = evidence.UploadFiles([('same.pdf', 'application/pdf', b'%PDF-1.4\nold\n%%EOF')])
        old_hash = accounting.upload_hashes(files)[0][2]
        changed = b'%PDF-1.4\nnew\n%%EOF'
        files[0] = ('same.pdf', 'application/pdf', changed)
        saved = evidence.upload(MemoryStore(), '123456', 'statement', files)
        self.assertEqual(saved['files'][0]['sha256'], hashlib.sha256(changed).hexdigest())
        self.assertNotEqual(saved['files'][0]['sha256'], old_hash)
        mutable = bytearray(b'first')
        first = files.digest(mutable)
        mutable[:] = b'other'
        self.assertNotEqual(files.digest(mutable), first)
        self.assertEqual(files.digest(mutable), hashlib.sha256(mutable).hexdigest())


class UploadResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_hashing_yields_to_reads_on_the_same_event_loop(self):
        import httpx
        from fastapi import FastAPI
        from ..routers import accounting
        from ..dependencies import get_firestore, verify_firebase_token
        from ..middleware.rate_limit import limiter
        from .test_accounting import MemoryStore
        from .test_business_access import fixture

        app = FastAPI()
        app.include_router(accounting.router, prefix='/api')
        app.dependency_overrides[get_firestore] = fixture
        app.dependency_overrides[verify_firebase_token] = lambda: {'uid': 'owner'}
        store = MemoryStore()
        started, release = threading.Event(), threading.Event()
        content = b'%PDF-1.4\n' + b'x'*(2*1024**2-16) + b'\n%%EOF\n'
        real_hashes = accounting.upload_hashes
        def slow_hashes(files):
            started.set()
            release.wait(3)
            return real_hashes(files)
        @contextmanager
        def transaction(*args):
            yield store
        with patch.object(accounting, 'accounting_transaction', transaction), \
             patch.object(accounting, 'upload_hashes', slow_hashes), \
             patch.object(accounting, 'UPLOAD_SLOTS', asyncio.Semaphore(1)), \
             patch.object(limiter, 'enabled', False):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                upload = asyncio.create_task(client.post('/api/business/accounting/statements?route=989262',
                    files={'files': ('fixture.pdf', content, 'application/pdf')},
                    headers={'Idempotency-Key': str(uuid4())}))
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 2))
                    self.assertFalse(upload.done())
                    read = await asyncio.wait_for(client.get('/api/business/accounting/expenses?route=989262'), .5)
                    self.assertEqual(read.status_code, 200)
                    self.assertFalse(upload.done())
                finally:
                    release.set()
                    response = await upload
                self.assertEqual(response.status_code, 201)
                self.assertEqual(response.json()['files'][0]['sha256'],
                                 hashlib.sha256(content).hexdigest())


if __name__ == "__main__": unittest.main()
