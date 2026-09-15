"""Coalesced, fair retry work for a running config listener.

One latest action per document, one drainer, no database connection held while
waiting. Failures remain pending; a concurrent replacement is never acknowledged
by an older action. This is process-local, not a durable deletion journal.
"""
from collections import OrderedDict
import threading
import time


def exception_types(error):
    """Safe causal diagnostics, including an SDK rollback masking begin failure."""
    names, seen = [], set()
    while error is not None and id(error) not in seen and len(names) < 6:
        seen.add(id(error))
        names.append(type(error).__name__)
        error = error.__cause__ or error.__context__
    return '>'.join(names)


class LatestConfigWork:
    def __init__(self, retry_delay=5.0, clock=time.monotonic):
        self._lock = threading.Lock()
        self._drainer = threading.Lock()
        self._groups = OrderedDict()
        self._retry_after = {}
        self._retry_delay = retry_delay
        self._clock = clock

    def put(self, group, key, action):
        with self._lock:
            bucket = self._groups.setdefault(group, OrderedDict())
            bucket[key] = action

    def drain(self, limit=25, time_budget=1.0):
        if not self._drainer.acquire(blocking=False):
            return 0
        completed = 0
        attempted = set()
        started = self._clock()
        try:
            for _ in range(limit):
                # Do not start another call after the budget. An in-flight call
                # retains its own existing database/RPC timeout.
                if self._clock() - started >= time_budget:
                    break
                with self._lock:
                    selected = None
                    for group, bucket in self._groups.items():
                        for key, action in bucket.items():
                            if ((group, key) not in attempted and
                                    self._retry_after.get((group, key), 0) <= self._clock()):
                                selected = group, key, action
                                break
                        if selected:
                            break
                    if selected is None:
                        break
                    group, key, action = selected
                    attempted.add((group, key))
                    self._groups.move_to_end(group)
                    self._groups[group].move_to_end(key)
                try:
                    success = action() is True
                except Exception:
                    # The action owns safe logging; never discard on an exception.
                    success = False
                if success:
                    with self._lock:
                        bucket = self._groups[group]
                        if bucket.get(key) is action:
                            del bucket[key]
                        self._retry_after.pop((group, key), None)
                        if not bucket:
                            del self._groups[group]
                    completed += 1
                else:
                    with self._lock:
                        # Even a replacement waits briefly after a failed call;
                        # rapid edits must not bypass outage backoff.
                        self._retry_after[(group, key)] = self._clock() + self._retry_delay
            return completed
        finally:
            self._drainer.release()

    def __len__(self):
        with self._lock:
            return sum(len(bucket) for bucket in self._groups.values())
