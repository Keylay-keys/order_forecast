from experimental_pos_truth import (
    build_reference_upc_index,
    gs1_check_digit,
    has_valid_gs1_check_digit,
    reference_catalog_keys,
    resolve_invoice_upc,
)


def test_observed_retailer_upc_encodings_normalize_to_catalog_body():
    assert reference_catalog_keys("7373100249")[-1] == "7373100249"
    assert reference_catalog_keys("07373100249")[-1] == "7373100249"
    assert reference_catalog_keys("00073731002490")[-1] == "7373100249"


def test_invalid_gtin_does_not_drop_digits_into_a_false_catalog_match():
    assert reference_catalog_keys("00073731002491") == ("00073731002491",)


def test_gs1_check_digit_matches_observed_walmart_gtin():
    assert gs1_check_digit("0007373100249") == 0
    assert has_valid_gs1_check_digit("00073731002490")


def test_index_ignores_inactive_and_empty_catalog_rows():
    index = build_reference_upc_index(
        [
            {"sap": "11371", "upc": "73731-00249", "active": True},
            {"sap": "old", "upc": "73731-00249", "active": False},
            {"sap": "blank", "upc": None, "active": True},
        ]
    )
    assert index == {"7373100249": ("11371",)}


def test_unique_reference_match_resolves_all_three_retailer_formats():
    index = {"7373100249": ("11371",)}
    for value in ("7373100249", "07373100249", "00073731002490"):
        result = resolve_invoice_upc(value, index)
        assert result.resolved_sap == "11371"
        assert result.status == "unique_catalog"


def test_alias_match_fails_closed_without_unique_route_context():
    index = {"7373107156": ("41094", "51233")}
    unresolved = resolve_invoice_upc("07373107156", index)
    assert unresolved.resolved_sap is None
    assert unresolved.status == "ambiguous_catalog"

    resolved = resolve_invoice_upc(
        "07373107156", index, supported_route_saps={"51233"}
    )
    assert resolved.resolved_sap == "51233"
    assert resolved.status == "unique_route_context"
