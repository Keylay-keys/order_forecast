from __future__ import annotations

import importlib
import os
import sys
import unittest
from unittest import mock


class ManualPushScriptSafetyTests(unittest.TestCase):
    def test_import_has_no_firebase_or_expo_effect(self) -> None:
        with (
            mock.patch("google.cloud.firestore.Client.from_service_account_json") as client,
            mock.patch("requests.post") as post,
        ):
            sys.modules.pop("order_forecast.scripts.test_send_push", None)
            module = importlib.import_module("order_forecast.scripts.test_send_push")

        self.assertFalse(module.__test__)
        client.assert_not_called()
        post.assert_not_called()

    def test_main_requires_both_independent_real_send_gates(self) -> None:
        module = importlib.import_module("order_forecast.scripts.test_send_push")
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    "test_send_push.py",
                    "--service-account",
                    "/does/not/matter.json",
                    "--route",
                    "989262",
                    "--send-real-push",
                ],
            ),
            mock.patch.dict(os.environ, {module.REAL_PUSH_ENV: ""}),
        ):
            with self.assertRaisesRegex(RuntimeError, "real push denied"):
                module.main()


if __name__ == "__main__":
    unittest.main()
