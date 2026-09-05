from __future__ import annotations

import io
import json
import struct
import unittest

from protocol import MAX_FRAME_BYTES, ProtocolError, read_frame, write_frame


class ProtocolTests(unittest.TestCase):
    def test_round_trip_uses_four_byte_big_endian_length(self) -> None:
        stream = io.BytesIO()
        write_frame(stream, {"version": 1, "id": "request-1", "operation": "health", "params": {}})

        encoded = stream.getvalue()
        self.assertEqual(struct.unpack(">I", encoded[:4])[0], len(encoded[4:]))
        self.assertEqual(read_frame(io.BytesIO(encoded)), {
            "version": 1,
            "id": "request-1",
            "operation": "health",
            "params": {},
        })

    def test_read_frame_accepts_split_reads(self) -> None:
        payload = json.dumps({"ok": True}, separators=(",", ":")).encode("utf-8")

        class SplitReader:
            def __init__(self, data: bytes) -> None:
                self.data = data
                self.offset = 0

            def read(self, size: int) -> bytes:
                if self.offset >= len(self.data):
                    return b""
                end = min(self.offset + 1, len(self.data), self.offset + size)
                chunk = self.data[self.offset:end]
                self.offset = end
                return chunk

        self.assertEqual(read_frame(SplitReader(struct.pack(">I", len(payload)) + payload)), {"ok": True})

    def test_oversized_frame_is_rejected_before_body_read(self) -> None:
        stream = io.BytesIO(struct.pack(">I", MAX_FRAME_BYTES + 1))
        with self.assertRaisesRegex(ProtocolError, "exceeds maximum"):
            read_frame(stream)

    def test_truncated_frame_is_rejected(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "unexpected EOF"):
            read_frame(io.BytesIO(struct.pack(">I", 5) + b"{}"))

    def test_non_object_json_is_rejected(self) -> None:
        payload = b"[]"
        with self.assertRaisesRegex(ProtocolError, "JSON object"):
            read_frame(io.BytesIO(struct.pack(">I", len(payload)) + payload))

    def test_non_finite_json_number_is_rejected(self) -> None:
        payload = b'{"volume":NaN}'
        with self.assertRaisesRegex(ProtocolError, "valid UTF-8 JSON"):
            read_frame(io.BytesIO(struct.pack(">I", len(payload)) + payload))

    def test_overflowed_json_number_is_rejected(self) -> None:
        payload = b'{"volume":1e999}'
        with self.assertRaisesRegex(ProtocolError, "valid UTF-8 JSON"):
            read_frame(io.BytesIO(struct.pack(">I", len(payload)) + payload))

    def test_excessively_nested_json_is_rejected(self) -> None:
        payload = ('{"value":' + '[' * 5000 + '0' + ']' * 5000 + '}').encode()
        with self.assertRaisesRegex(ProtocolError, "valid UTF-8 JSON"):
            read_frame(io.BytesIO(struct.pack(">I", len(payload)) + payload))

    def test_duplicate_json_key_is_rejected(self) -> None:
        payload = b'{"id":"first","id":"second"}'
        with self.assertRaisesRegex(ProtocolError, "duplicate JSON key"):
            read_frame(io.BytesIO(struct.pack(">I", len(payload)) + payload))

    def test_non_finite_value_cannot_be_written(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "not JSON serializable"):
            write_frame(io.BytesIO(), {"value": float("nan")})


if __name__ == "__main__":
    unittest.main()
