"""Small Firestore fakes shared by route-transfer contract tests only."""

from __future__ import annotations

from copy import deepcopy


class FakeSnapshot:
    def __init__(self, path, data):
        self.id = path[-1]
        self._data = deepcopy(data) if data is not None else None
        self.exists = data is not None

    def to_dict(self):
        return deepcopy(self._data) if self._data is not None else None

    def data(self):
        return self.to_dict()


class FakeQuery:
    def __init__(self, db, collection_path, *, limit_value=None):
        self.db = db
        self.collection_path = tuple(collection_path)
        self.limit_value = limit_value

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, value):
        return FakeQuery(self.db, self.collection_path, limit_value=value)

    def get(self):
        snapshots = self.db.collection_snapshots(self.collection_path)
        return snapshots[: self.limit_value] if self.limit_value else snapshots

    def stream(self):
        return iter(self.get())


class FakeCollection(FakeQuery):
    def __init__(self, db, path):
        super().__init__(db, path)

    def document(self, doc_id):
        return FakeDocument(self.db, self.collection_path + (doc_id,))


class FakeDocument:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)
        self.id = self.path[-1]

    def collection(self, name):
        return FakeCollection(self.db, self.path + (name,))

    def get(self, transaction=None):
        del transaction
        return FakeSnapshot(self.path, self.db.get_document(self.path))


class FakeTransaction:
    def __init__(self, db):
        self.db = db

    def set(self, ref, data, merge=False):
        self.db.set_document(ref.path, data, merge=merge)

    def update(self, ref, data):
        self.db.update_document(ref.path, data)

    def delete(self, ref):
        self.db.delete_document(ref.path)


class FakeFirestore:
    def __init__(self, documents=None, *, delete_field=None):
        self.documents = {
            self._normalize_path(path): deepcopy(data)
            for path, data in (documents or {}).items()
        }
        self.delete_field = delete_field

    @staticmethod
    def _normalize_path(path):
        if isinstance(path, str):
            return tuple(part for part in path.split("/") if part)
        return tuple(path)

    def collection(self, name):
        return FakeCollection(self, (name,))

    def transaction(self):
        return FakeTransaction(self)

    def get_document(self, path):
        data = self.documents.get(self._normalize_path(path))
        return deepcopy(data) if data is not None else None

    def collection_snapshots(self, collection_path):
        prefix = self._normalize_path(collection_path)
        rows = []
        for path, data in self.documents.items():
            if len(path) == len(prefix) + 1 and path[:-1] == prefix:
                rows.append(FakeSnapshot(path, data))
        return rows

    def set_document(self, path, data, *, merge=False):
        normalized = self._normalize_path(path)
        existing = self.documents.get(normalized, {}) if merge else {}
        self.documents[normalized] = {**deepcopy(existing), **deepcopy(data)}

    def update_document(self, path, updates):
        normalized = self._normalize_path(path)
        if normalized not in self.documents:
            raise AssertionError(f"Missing document for update: {'/'.join(normalized)}")

        target = self.documents[normalized]
        for dotted_path, value in updates.items():
            parts = dotted_path.split(".")
            cursor = target
            for part in parts[:-1]:
                cursor = cursor.setdefault(part, {})
            if value is self.delete_field:
                cursor.pop(parts[-1], None)
            else:
                cursor[parts[-1]] = value

    def delete_document(self, path):
        self.documents.pop(self._normalize_path(path), None)

