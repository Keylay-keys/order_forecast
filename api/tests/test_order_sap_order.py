import pytest
from pydantic import ValidationError

from order_forecast.api.models import OrderUpdateRequest


def test_sap_order_accepts_unique_valid_codes():
    payload = OrderUpdateRequest(stores=[], sapOrder=["54511", "ABC-2"])
    assert payload.sapOrder == ["54511", "ABC-2"]


@pytest.mark.parametrize("sap_order", [["54511", "54511"], ["valid", "not valid"]])
def test_sap_order_rejects_duplicates_and_invalid_codes(sap_order):
    with pytest.raises(ValidationError):
        OrderUpdateRequest(stores=[], sapOrder=sap_order)


def test_sap_order_has_a_bounded_payload():
    with pytest.raises(ValidationError):
        OrderUpdateRequest(stores=[], sapOrder=[f"SAP-{index}" for index in range(2001)])
