#!/usr/bin/env python3
"""Extract read-only order summaries from a managed Firestore export.

Managed Firestore exports use LevelDB's 32 KiB log-record framing and legacy
EntityProto payloads. This reader verifies every physical-record CRC before
decoding only routes/{route}/orders/{orderId} documents.
"""

from __future__ import annotations

import argparse
import array
import json
import struct
import sys
from pathlib import Path
from typing import Any, Iterator

import google_crc32c


BLOCK_SIZE = 32 * 1024
HEADER = struct.Struct("<IHB")
FULL, FIRST, MIDDLE, LAST = 1, 2, 3, 4
CRC_MASK_DELTA = 0xA282EAD8
ENTITY_PROTO_MEANING = 19
EMPTY_LIST_MEANING = 24
TIMESTAMP_MEANING = 7


def _configure_legacy_protobuf(gcloud_sdk_lib: Path):
    sys.path.insert(0, str(gcloud_sdk_lib))
    from googlecloudsdk.appengine.proto import ProtocolBuffer
    from googlecloudsdk.appengine.release.v3 import entity_pb

    def merge_partial_from_bytes(message, contents: bytes) -> None:
        values = array.array("B")
        values.frombytes(contents)
        message.TryMerge(ProtocolBuffer.Decoder(values, 0, len(values)))

    def get_prefixed_bytes(decoder):
        length = decoder.getVarInt32()
        if decoder.idx + length > decoder.limit:
            raise ProtocolBuffer.ProtocolBufferDecodeError("truncated")
        result = decoder.buf[decoder.idx:decoder.idx + length]
        decoder.idx += length
        return result.tobytes()

    def get_raw_bytes(decoder):
        result = decoder.buf[decoder.idx:decoder.limit]
        decoder.idx = decoder.limit
        return result.tobytes()

    # Cloud SDK's generated legacy runtime still calls array.fromstring and
    # array.tostring, which were removed in modern Python. Patch only the
    # in-process decoder methods to their frombytes/tobytes equivalents.
    ProtocolBuffer.ProtocolMessage.MergePartialFromString = merge_partial_from_bytes
    ProtocolBuffer.Decoder.getPrefixedString = get_prefixed_bytes
    ProtocolBuffer.Decoder.getRawString = get_raw_bytes
    return entity_pb


def _unmask_crc(masked_crc: int) -> int:
    rotated = (masked_crc - CRC_MASK_DELTA) & 0xFFFFFFFF
    return ((rotated >> 17) | (rotated << 15)) & 0xFFFFFFFF


def _records(reader) -> Iterator[bytes]:
    chunks: list[bytes] | None = None
    while True:
        block_remaining = BLOCK_SIZE - (reader.tell() % BLOCK_SIZE)
        if block_remaining < HEADER.size:
            reader.read(block_remaining)
            continue

        header = reader.read(HEADER.size)
        if len(header) == 0:
            return
        if len(header) != HEADER.size:
            raise ValueError("truncated managed-export record header")
        masked_crc, length, record_type = HEADER.unpack(header)
        if length + HEADER.size > block_remaining:
            raise ValueError("managed-export record crosses a block boundary")

        data = reader.read(length)
        if len(data) != length:
            raise ValueError("truncated managed-export record payload")
        if record_type == 0:
            reader.read(BLOCK_SIZE - (reader.tell() % BLOCK_SIZE))
            chunks = None
            continue

        actual_crc = google_crc32c.value(bytes([record_type]) + data)
        if actual_crc != _unmask_crc(masked_crc):
            raise ValueError("managed-export record CRC mismatch")

        if record_type == FULL:
            yield data
        elif record_type == FIRST:
            chunks = [data]
        elif record_type == MIDDLE and chunks is not None:
            chunks.append(data)
        elif record_type == LAST and chunks is not None:
            chunks.append(data)
            yield b"".join(chunks)
            chunks = None
        else:
            raise ValueError(f"invalid managed-export record sequence: {record_type}")


def _text(value: bytes | str) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _entity_proto(entity_pb, contents: bytes):
    message = entity_pb.EntityProto()
    message.MergePartialFromString(contents)
    return message


def _property_value(entity_pb, prop) -> Any:
    meaning = prop.meaning() if prop.has_meaning() else None
    value = prop.value()
    if meaning == EMPTY_LIST_MEANING:
        return []
    if meaning == ENTITY_PROTO_MEANING and value.has_stringvalue():
        return _entity_properties(entity_pb, _entity_proto(entity_pb, value.stringvalue()))
    if value.has_booleanvalue():
        return bool(value.booleanvalue())
    if value.has_int64value():
        number = int(value.int64value())
        return {"timestampMicros": number} if meaning == TIMESTAMP_MEANING else number
    if value.has_doublevalue():
        return float(value.doublevalue())
    if value.has_stringvalue():
        raw = value.stringvalue()
        try:
            return _text(raw)
        except UnicodeDecodeError:
            return {"binaryBytes": len(raw)}
    if value.has_referencevalue():
        return {
            "path": [
                {
                    "kind": _text(element.type()),
                    "name": _text(element.name()) if element.has_name() else element.id(),
                }
                for element in value.referencevalue().pathelement_list()
            ]
        }
    return None


def _entity_properties(entity_pb, message) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for prop in list(message.property_list()) + list(message.raw_property_list()):
        name = _text(prop.name())
        value = _property_value(entity_pb, prop)
        if prop.has_multiple() and prop.multiple():
            existing = result.get(name)
            if not isinstance(existing, list):
                existing = []
                result[name] = existing
            existing.append(value)
        else:
            result[name] = value
    return result


def _key_path(message) -> list[tuple[str, str | int]]:
    return [
        (
            _text(element.type()),
            _text(element.name()) if element.has_name() else int(element.id()),
        )
        for element in message.key().path().element_list()
    ]


def _number(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _order_summary(route_number: str, order_id: str, data: dict[str, Any]) -> dict[str, Any]:
    stores = data.get("stores")
    stores = stores if isinstance(stores, list) else []
    line_count = 0
    total_units = 0
    for store in stores:
        if not isinstance(store, dict):
            continue
        items = store.get("items")
        items = items if isinstance(items, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            quantity = _number(item.get("quantity"))
            total_units += quantity
            if quantity != 0:
                line_count += 1

    created_at = data.get("createdAt")
    created_at_micros = created_at.get("timestampMicros") if isinstance(created_at, dict) else None
    return {
        "orderId": order_id,
        "routeNumber": route_number,
        "status": data.get("status"),
        "scheduleKey": data.get("scheduleKey") or "unknown",
        "deliveryDate": data.get("expectedDeliveryDate") or data.get("deliveryDate"),
        "createdAtMicros": created_at_micros,
        "lineCount": line_count,
        "totalUnits": total_units,
        "storeCount": len(stores),
    }


def extract(export_dir: Path, gcloud_sdk_lib: Path) -> dict[str, Any]:
    entity_pb = _configure_legacy_protobuf(gcloud_sdk_lib)
    output_files = sorted(
        export_dir.glob("all_namespaces/all_kinds/output-*"),
        key=lambda path: int(path.name.split("-", 1)[1]),
    )
    orders: list[dict[str, Any]] = []
    records_read = 0
    for output_file in output_files:
        with output_file.open("rb") as reader:
            for record in _records(reader):
                records_read += 1
                message = _entity_proto(entity_pb, record)
                path = _key_path(message)
                if len(path) != 2 or path[0][0] != "routes" or path[1][0] != "orders":
                    continue
                data = _entity_properties(entity_pb, message)
                orders.append(_order_summary(str(path[0][1]), str(path[1][1]), data))

    orders.sort(key=lambda row: (row["routeNumber"], row["orderId"]))
    return {
        "mode": "read_only_managed_export_decode",
        "exportDirectory": str(export_dir),
        "outputFiles": len(output_files),
        "recordsRead": records_read,
        "ordersFound": len(orders),
        "finalizedOrders": sum(row["status"] == "finalized" for row in orders),
        "orders": orders,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export_dir", type=Path)
    parser.add_argument(
        "--gcloud-sdk-lib",
        type=Path,
        default=Path("/opt/homebrew/share/google-cloud-sdk/lib"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = extract(args.export_dir, args.gcloud_sdk_lib)
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(encoded + "\n", encoding="utf-8")
        print(json.dumps({key: value for key, value in result.items() if key != "orders"}, indent=2))
    else:
        print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
