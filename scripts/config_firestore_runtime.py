"""Owned Firestore connections: ordinary RPCs never share watch connections.

High-level Firestore has no public transport argument. The small Client subclass
uses its existing lazy transport helper, preserving credential, database, endpoint
and keepalive handling. This SDK-private extension point is covered by transport
tests; no _firestore_api_internal injection or production insecure channel.
"""
import os
import threading

from google.cloud import firestore
from google.cloud.firestore_v1.services.firestore import client as gapic
from google.cloud.firestore_v1.services.firestore.transports.grpc import FirestoreGrpcTransport


class IsolatedTransport(FirestoreGrpcTransport):
    @classmethod
    def create_channel(cls, host, credentials=None, **kwargs):
        options = dict(kwargs.pop('options', ()))
        options['grpc.use_local_subchannel_pool'] = 1
        return super().create_channel(host, credentials=credentials, options=tuple(options.items()), **kwargs)


class IsolatedFirestoreClient(firestore.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._isolated_init_lock = threading.Lock()
        self._isolated_closed = False

    @property
    def _firestore_api(self):
        if self._emulator_host is not None:
            raise RuntimeError('Production isolated client refuses emulator transport')
        with self._isolated_init_lock:
            if self._isolated_closed:
                raise RuntimeError('Firestore client closed')
            return self._firestore_api_helper(IsolatedTransport, gapic.FirestoreClient, gapic)

    def close(self):
        with self._isolated_init_lock:
            if self._isolated_closed:
                return
            self._isolated_closed = True
            if self._firestore_api_internal is not None:
                self._firestore_api_internal.transport.close()
        super().close()


class WatchLease:
    def __init__(self, pool, slot, watcher):
        self.pool, self.slot, self.watcher = pool, slot, watcher
        self._lock = threading.Lock()
        self._closed = False

    def unsubscribe(self):
        with self._lock:
            if self._closed:
                return
            # Keep reservation if unsubscribe fails; do not overfill a channel.
            self.watcher.unsubscribe()
            self._closed = True
            with self.pool._lock:
                self.slot[1] -= 1
                self.pool._leases.discard(self)


class FirestoreRuntime:
    def __init__(self, factory, watches_per_channel=24, max_watch_channels=32):
        if type(watches_per_channel) is not int or not 1 <= watches_per_channel <= 24:
            raise ValueError('Invalid watch channel budget')
        if type(max_watch_channels) is not int or not 1 <= max_watch_channels <= 32:
            raise ValueError('Invalid watch pool budget')
        self.factory = factory
        self.limit, self.max_channels = watches_per_channel, max_watch_channels
        self._lock = threading.Lock()
        self._slots = []
        self._leases = set()
        self._closed = False
        self.rpc = factory()

    def watch(self, reference, callback):
        with self._lock:
            if self._closed:
                raise RuntimeError('Firestore runtime stopped')
            slot = next((s for s in self._slots if s[1] < self.limit), None)
            if slot is None:
                if len(self._slots) >= self.max_channels:
                    raise RuntimeError('config_watch_capacity_exceeded')
                slot = [self.factory(), 0]
                self._slots.append(slot)
            slot[1] += 1
        try:
            watcher = reference(slot[0]).on_snapshot(callback)
        except Exception:
            with self._lock:
                slot[1] -= 1
            raise
        lease = WatchLease(self, slot, watcher)
        with self._lock:
            closed = self._closed
            if not closed:
                self._leases.add(lease)
        if closed:
            lease.unsubscribe()
            raise RuntimeError('Firestore runtime stopped during registration')
        return lease

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            clients = [s[0] for s in self._slots]
            leases = list(self._leases)
        errors = []
        # Cancel ordinary RPCs first so callbacks can return, then unsubscribe
        # watches cleanly before closing their transports. Closing watch channels
        # first causes SDK background-thread Cancelled exceptions.
        try:
            self.rpc.close()
        except Exception as error:
            errors.append(type(error).__name__)
        for lease in leases:
            try:
                lease.unsubscribe()
            except Exception as error:
                errors.append(type(error).__name__)
        for client in clients:
            try:
                client.close()
            except Exception as error:
                errors.append(type(error).__name__)
        if errors:
            raise RuntimeError('Firestore channel close failed: ' + ','.join(errors))


def from_service_account(path):
    if os.environ.get('FIRESTORE_EMULATOR_HOST'):
        raise RuntimeError('Production listener refuses emulator configuration')
    # Same immutable credentials/project/database configuration for every client.
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(path)
    project = credentials.project_id
    if not project:
        raise ValueError('Service account project missing')
    return FirestoreRuntime(lambda: IsolatedFirestoreClient(project=project, database='(default)', credentials=credentials))
