import unittest

from order_forecast.api.routers.deliveries import _is_safe_pcf_resource_id


class DeliveryResourceIdTests(unittest.TestCase):
    def test_accepts_numeric_and_existing_alphanumeric_ids(self):
        self.assertTrue(_is_safe_pcf_resource_id("1806213633"))
        self.assertTrue(_is_safe_pcf_resource_id("900000-DEL-001"))
        self.assertTrue(_is_safe_pcf_resource_id("C01"))
        self.assertTrue(_is_safe_pcf_resource_id("LOAD_2026_08"))

    def test_rejects_path_and_control_characters(self):
        for value in ("", ".", "..", "../delivery", "delivery/container", " delivery", "delivery ", "delivery\n"):
            with self.subTest(value=value):
                self.assertFalse(_is_safe_pcf_resource_id(value))

    def test_rejects_oversized_ids(self):
        self.assertTrue(_is_safe_pcf_resource_id("A" * 128))
        self.assertFalse(_is_safe_pcf_resource_id("A" * 129))


if __name__ == "__main__":
    unittest.main()
