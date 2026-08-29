from __future__ import annotations

import asyncio
import unittest

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.routers import settings


class _Snapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _Ref:
    def __init__(self, data):
        self.data = data
        self.updates = []

    def get(self):
        return _Snapshot(self.data)

    def update(self, fields):
        self.updates.append(dict(fields))


class _Collection:
    def __init__(self, refs):
        self.refs = refs

    def document(self, uid):
        return self.refs.setdefault(uid, _Ref(None))


class _Db:
    def __init__(self, users):
        self.users = {uid: _Ref(data) for uid, data in users.items()}

    def collection(self, name):
        if name != "users":
            raise AssertionError(f"unexpected collection: {name}")
        return _Collection(self.users)


class ProfileTimezoneTests(unittest.TestCase):
    def test_valid_timezone_is_trimmed_and_written_by_trusted_endpoint(self):
        db = _Db({"owner": {"profile": {"role": "owner"}, "timestamps": {}}})
        response = asyncio.run(settings.set_profile_timezone(
            settings.ProfileTimezoneRequest(timezone=" America/Denver "),
            {"uid": "owner"},
            db,
        ))
        self.assertEqual(response.timezone, "America/Denver")
        update = db.users["owner"].updates[0]
        self.assertEqual(update["profile.timezone"], "America/Denver")
        self.assertIn("timestamps.updatedAt", update)

    def test_unknown_or_path_like_zone_fails_without_write(self):
        for value in ("Not/AZone", "../etc/passwd", " "):
            db = _Db({"owner": {"profile": {"role": "owner"}}})
            with self.assertRaises(StructuredApiError) as raised:
                asyncio.run(settings.set_profile_timezone(
                    settings.ProfileTimezoneRequest(timezone=value),
                    {"uid": "owner"},
                    db,
                ))
            self.assertEqual(raised.exception.code, "TIMEZONE_INVALID")
            self.assertEqual(db.users["owner"].updates, [])

    def test_missing_user_or_profile_fails_closed(self):
        for users, code in (({}, "USER_PROFILE_NOT_FOUND"), ({"owner": {}}, "USER_PROFILE_INCOMPLETE")):
            with self.assertRaises(StructuredApiError) as raised:
                asyncio.run(settings.set_profile_timezone(
                    settings.ProfileTimezoneRequest(timezone="America/Denver"),
                    {"uid": "owner"},
                    _Db(users),
                ))
            self.assertEqual(raised.exception.code, code)


if __name__ == "__main__":
    unittest.main()
