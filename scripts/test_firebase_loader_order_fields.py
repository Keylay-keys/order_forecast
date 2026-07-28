from order_forecast.scripts.firebase_loader import _decode_order


class _Doc:
    id = "order-988200-1"

    def to_dict(self):
        return {
            "id": self.id,
            "routeNumber": "988200",
            "userId": "user-1",
            "orderDate": "2026-07-27",
            "expectedDeliveryDate": "2026-07-30",
            "expectedLoadDate": "2026-07-29",
            "scheduleKey": "monday",
            "status": "finalized",
            "stores": [
                {
                    "storeId": "store-1",
                    "storeName": "Store One",
                    "items": [{"sap": "28934", "quantity": 15}],
                }
            ],
            "coreItemPolicyVersion": 1,
            "coreItemOverrides": [{"storeId": "store-1", "sap": "28934"}],
        }


def test_decode_order_preserves_operational_fields_without_changing_quantities():
    order = _decode_order(_Doc())

    assert order.expected_delivery_date == "2026-07-30"
    assert order.stores[0].items[0].quantity == 15
    assert order.meta["expectedLoadDate"] == "2026-07-29"
    assert order.meta["coreItemPolicyVersion"] == 1
    assert order.meta["coreItemOverrides"] == [{"storeId": "store-1", "sap": "28934"}]
