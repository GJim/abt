from __future__ import annotations

import json
import math
import struct
from collections.abc import Mapping
from typing import Any, BinaryIO

PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 4 * 1024 * 1024


class ProtocolError(RuntimeError):
    pass


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number {value}")
    return parsed


def _reject_excessive_nesting(body: bytes, maximum_depth: int = 128) -> None:
    depth = 0
    in_string = False
    escaped = False
    for byte in body:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                in_string = False
            continue
        if byte == 34:
            in_string = True
        elif byte in (91, 123):
            depth += 1
            if depth > maximum_depth:
                raise ValueError(f"JSON nesting exceeds maximum {maximum_depth}")
        elif byte in (93, 125):
            depth -= 1


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def decode_json_object(body: bytes) -> dict[str, Any]:
    try:
        _reject_excessive_nesting(body)
        value = json.loads(
            body.decode("utf-8"),
            parse_constant=_reject_non_finite_constant,
            parse_float=_finite_float,
            object_pairs_hook=_unique_object,
        )
    except ProtocolError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise ProtocolError("frame body is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ProtocolError("frame body must be a JSON object")
    return value


def _read_exact(stream: BinaryIO, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise ProtocolError("unexpected EOF while reading frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(stream: BinaryIO) -> dict[str, Any]:
    header = _read_exact(stream, 4)
    size = struct.unpack(">I", header)[0]
    if size > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame size {size} exceeds maximum {MAX_FRAME_BYTES}")
    return decode_json_object(_read_exact(stream, size))


def encode_frame(value: Mapping[str, Any]) -> bytes:
    try:
        body = json.dumps(
            value, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ProtocolError("frame contains a value that is not JSON serializable") from error
    if len(body) > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame size {len(body)} exceeds maximum {MAX_FRAME_BYTES}")
    return struct.pack(">I", len(body)) + body


def write_frame(stream: BinaryIO, value: Mapping[str, Any]) -> None:
    stream.write(encode_frame(value))
    stream.flush()
