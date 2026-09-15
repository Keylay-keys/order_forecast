import inspect
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from pydantic import ValidationError

from order_forecast.api.routers import metrics
from order_forecast.api.tests.route_transfer_fakes import FakeFirestore, FakeSnapshot


def settlement_fixture():
    return {
        "schemaVersion": 1,
        "reviewStatus": "accepted",
        "routeNumber": "989262",
        "currency": "USD",
        "periodStart": "2026-08-20",
        "periodEnd": "2026-08-26",
        "statementReceivedAt": "2026-08-31T19:21:00-06:00",
        "availableAt": "2026-08-31T19:21:00-06:00",
        "sourceDocumentId": "settlement-week-2026-08-26",
        "sourcePageCount": 6,
        "pay": {
            "totalPurchases": 17220.23,
            "grossSalesLessBuyback": 10650.55,
            "sbtGross": 5690.81,
            "totalMarginDiscount": 2642.93,
            "salesReturns": 102.19,
            "netCashSales": 360.86,
            "hhcRentalFee": 20.00,
            "sbtCredits": 109.95,
            "priorAdjustments": -3679.27,
            "currentAdjustments": 4558.14,
            "newSettlement": 2049.93,
        },
        "account": {
            "previousBalance": 0,
            "payment": 0,
            "totalNewSettlement": 2049.93,
            "amountToPay": 2049.93,
        },
        "margin": {
            "marginOnPurchases": 2895.59,
            "marginDiscountPromotions": -156.26,
            "marginDiscountSbt": -96.40,
            "totalMarginDiscount": 2642.93,
        },
        "sales": {
            "grossSales": 16341.36,
            "returns": -102.19,
            "promotions": -1400.44,
            "networkAllowances": -36.04,
            "netSales": 14802.69,
        },
        "inventory": {
            "initialInventory": 8784.28,
            "currentInventory": 9663.15,
            "truckInventoryChange": -878.87,
        },
    }


class SettlementMetricTests(unittest.IsolatedAsyncioTestCase):
    def test_supplied_statement_reconciles_exactly(self):
        normalized = metrics._normalize_settlement(
            FakeSnapshot(("routes", "989262", "settlements", "week-2026-08-26"), settlement_fixture()),
            "989262",
        )

        self.assertTrue(normalized["pay"]["reconciled"])
        self.assertEqual(normalized["pay"]["calculatedNewSettlement"], 2049.93)
        self.assertEqual(normalized["pay"]["difference"], 0.0)
        self.assertEqual(normalized["inventory"]["truckInventoryChange"], -878.87)
        self.assertNotIn("truckInventoryChange", [row["key"] for row in normalized["pay"]["components"]])

    def test_rejects_unreviewed_financial_candidates(self):
        data = settlement_fixture()
        data["reviewStatus"] = "candidate"
        with self.assertRaisesRegex(ValueError, "not accepted"):
            metrics._normalize_settlement(FakeSnapshot(("settlements", "candidate"), data), "989262")

    async def test_endpoint_is_owner_only_and_route_scoped(self):
        db = FakeFirestore({
            "routes/989262/settlements/week-2026-08-26": settlement_fixture(),
        })
        handler = inspect.unwrap(metrics.list_settlement_metrics)

        with patch.object(
            metrics,
            "require_business_access",
            new=AsyncMock(return_value={"profile": {"role": "owner", "routeNumber": "989262"}}),
        ):
            result = await handler(
                request=None,
                route="989262",
                limit=13,
                decoded_token={"uid": "owner"},
                db=db,
            )

        self.assertTrue(result["hasData"])
        self.assertEqual(result["latest"]["routeNumber"], "989262")

        with patch.object(
            metrics,
            "require_business_access",
            new=AsyncMock(side_effect=HTTPException(403, "Access denied")),
        ):
            with self.assertRaises(HTTPException) as raised:
                await handler(
                    request=None,
                    route="989262",
                    limit=13,
                    decoded_token={"uid": "member"},
                    db=db,
                )
        self.assertEqual(raised.exception.status_code, 403)

    def test_business_cost_inputs_fail_closed(self):
        with self.assertRaises(ValidationError):
            metrics.ExpenseCreateRequest(
                routeNumber="989262",
                date="2026-08-22",
                category="fuel",
                amount="-1.00",
            )

        with self.assertRaises(ValidationError):
            metrics.PayrollCreateRequest(
                routeNumber="989262",
                periodStart="2026-08-20",
                periodEnd="2026-08-26",
                workerLabel="Route helper",
                workerType="w2_employee",
                payType="hourly",
                grossPay="500.00",
            )

        with self.assertRaises(ValidationError):
            metrics.PayrollCreateRequest(
                routeNumber="989262",
                periodStart="2026-08-20",
                periodEnd="2026-08-26",
                workerLabel="Route helper",
                workerType="contractor_1099",
                payType="flat",
                grossPay="500.00",
                status="paid",
            )


if __name__ == "__main__":
    unittest.main()
