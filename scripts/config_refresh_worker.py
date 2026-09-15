"""Single bounded refresh worker. Snapshot callbacks submit hints, never RPCs."""
import threading

try:
    from .config_sync_retry import LatestConfigWork, exception_types
except ImportError:
    from config_sync_retry import LatestConfigWork, exception_types


class RefreshWorker:
    def __init__(self, enqueue, enabled, retry_delay=5):
        self.enqueue = enqueue
        self.enabled = enabled
        self.work = LatestConfigWork(retry_delay=retry_delay)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    def submit(self, uid, route):
        with self._lock:
            if self._stop.is_set() or not self.enabled(route):
                return False
            def apply():
                if self._stop.is_set():
                    return False
                if not self.enabled(route):
                    return True
                try:
                    self.enqueue(uid, route)  # Re-reads authority and settings.
                    return True
                except Exception as error:
                    if self._stop.is_set():
                        return False
                    if getattr(error, 'status_code', None) == 403:
                        return True
                    print(f'  [SendReminder] enqueue retry route={route} error_chain={exception_types(error)}')
                    return False
            self.work.put(route, uid, apply)
            return True

    def drain(self):
        if self._stop.is_set():
            return 0
        return self.work.drain(limit=1)

    def start(self):
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError('Refresh worker already stopped')
            if self._thread is not None:
                return
            def run():
                while not self._stop.wait(.25):
                    self.drain()
            self._thread = threading.Thread(target=run, name='config-send-refresh', daemon=True)
            self._thread.start()

    def request_stop(self):
        self._stop.set()

    def join(self, timeout=5):
        if self._thread is not None:
            self._thread.join(timeout)
            return not self._thread.is_alive()
        return True
