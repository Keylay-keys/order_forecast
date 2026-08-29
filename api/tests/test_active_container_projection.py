from __future__ import annotations

from order_forecast.api.routers.deliveries import _process_active_container


class _ContainerDocument:
    def __init__(self, data: dict, document_id: str = "C01") -> None:
        self.id = document_id
        self._data = data

    def to_dict(self) -> dict:
        return self._data


def test_list_projection_omits_pages_but_preserves_page_count_and_identity() -> None:
    container = _ContainerDocument(
        {
            "pages": [
                {"items": [{"product": "1001", "days": "4"}]},
                {"items": [{"product": "1002", "days": "3"}]},
            ]
        }
    )

    projected = _process_active_container(container, {}, include_pages=False)

    assert projected["pages"] == []
    assert projected["pageCount"] == 2
    assert [item["pageNumber"] for item in projected["products"]] == [1, 2]


def test_visibility_and_short_code_controls_match_active_serving_projection() -> None:
    container = _ContainerDocument(
        {
            "pages": [
                {
                    "pageNumber": 7,
                    "items": [
                        {
                            "product": "1001",
                            "days": "",
                            "isShortCoded": True,
                            "visibleOnList": False,
                        },
                        {
                            "product": "1002",
                            "days": "12*",
                        },
                    ],
                }
            ]
        }
    )

    projected = _process_active_container(container, {}, include_pages=False)

    first, second = projected["products"]
    assert first["isShortCoded"] is True
    assert first["visibleOnList"] is False
    assert second["isShortCoded"] is True
    assert second["visibleOnList"] is True
    assert first["pageNumber"] == 7
    assert "isLowQuantity" not in first
