from __future__ import annotations

import base64
import ctypes
import hashlib
import sys
from functools import lru_cache
from typing import Protocol


class HardwareKeyStore(Protocol):
    """A non-exportable ECDSA P-256 device identity key."""

    def public_key_pem(self) -> str:
        """Return the public key that is safe to submit during enrollment."""

    def sign(self, payload: bytes) -> bytes:
        """Sign payload without exposing the device private key."""


class UnsupportedKeyStore(RuntimeError):
    """Raised when the host lacks the required CNG/TPM/PKCS#11 provider."""


_DWORD = ctypes.c_uint32
_STATUS = ctypes.c_int32
_HANDLE = ctypes.c_void_p
_BYTE = ctypes.c_ubyte
_PBYTE = ctypes.POINTER(_BYTE)

_MS_KEY_STORAGE_PROVIDER = "Microsoft Software Key Storage Provider"
_NCRYPT_ECDSA_P256_ALGORITHM = "ECDSA_P256"
_NCRYPT_EXPORT_POLICY_PROPERTY = "Export Policy"
_NCRYPT_KEY_USAGE_PROPERTY = "Key Usage"
_NCRYPT_ALLOW_SIGNING_FLAG = 0x00000002
_NTE_BAD_KEYSET = 0x80090016
_NTE_EXISTS = 0x8009000F
_BCRYPT_ECCPUBLIC_BLOB = "ECCPUBLICBLOB"
_BCRYPT_ECDSA_PUBLIC_P256_MAGIC = 0x31534345
_P256_COORDINATE_SIZE = 32


@lru_cache(maxsize=1)
def _ncrypt() -> ctypes.CDLL:
    """Load NCrypt and describe only the functions this adapter calls."""

    try:
        library = ctypes.WinDLL("ncrypt.dll", use_last_error=True)
    except OSError as error:
        raise UnsupportedKeyStore("Windows CNG is unavailable") from error

    library.NCryptOpenStorageProvider.argtypes = [
        ctypes.POINTER(_HANDLE),
        ctypes.c_wchar_p,
        _DWORD,
    ]
    library.NCryptOpenStorageProvider.restype = _STATUS
    library.NCryptOpenKey.argtypes = [
        _HANDLE,
        ctypes.POINTER(_HANDLE),
        ctypes.c_wchar_p,
        _DWORD,
        _DWORD,
    ]
    library.NCryptOpenKey.restype = _STATUS
    library.NCryptCreatePersistedKey.argtypes = [
        _HANDLE,
        ctypes.POINTER(_HANDLE),
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        _DWORD,
        _DWORD,
    ]
    library.NCryptCreatePersistedKey.restype = _STATUS
    library.NCryptSetProperty.argtypes = [
        _HANDLE,
        ctypes.c_wchar_p,
        _PBYTE,
        _DWORD,
        _DWORD,
    ]
    library.NCryptSetProperty.restype = _STATUS
    library.NCryptGetProperty.argtypes = [
        _HANDLE,
        ctypes.c_wchar_p,
        _PBYTE,
        _DWORD,
        ctypes.POINTER(_DWORD),
        _DWORD,
    ]
    library.NCryptGetProperty.restype = _STATUS
    library.NCryptFinalizeKey.argtypes = [_HANDLE, _DWORD]
    library.NCryptFinalizeKey.restype = _STATUS
    library.NCryptExportKey.argtypes = [
        _HANDLE,
        _HANDLE,
        ctypes.c_wchar_p,
        ctypes.c_void_p,
        _PBYTE,
        _DWORD,
        ctypes.POINTER(_DWORD),
        _DWORD,
    ]
    library.NCryptExportKey.restype = _STATUS
    library.NCryptSignHash.argtypes = [
        _HANDLE,
        ctypes.c_void_p,
        _PBYTE,
        _DWORD,
        _PBYTE,
        _DWORD,
        ctypes.POINTER(_DWORD),
        _DWORD,
    ]
    library.NCryptSignHash.restype = _STATUS
    library.NCryptDeleteKey.argtypes = [_HANDLE, _DWORD]
    library.NCryptDeleteKey.restype = _STATUS
    library.NCryptFreeObject.argtypes = [_HANDLE]
    library.NCryptFreeObject.restype = _STATUS
    return library


def _status_code(status: int) -> int:
    return ctypes.c_uint32(status).value


def _require_success(status: int, operation: str) -> None:
    if status:
        raise UnsupportedKeyStore(f"{operation} failed (0x{_status_code(status):08X})")


def _dword_buffer(value: int) -> ctypes.Array[_BYTE]:
    return (_BYTE * ctypes.sizeof(_DWORD)).from_buffer_copy(_DWORD(value))


def _der_integer(value: bytes) -> bytes:
    encoded = value.lstrip(b"\x00") or b"\x00"
    if encoded[0] & 0x80:
        encoded = b"\x00" + encoded
    return b"\x02" + bytes((len(encoded),)) + encoded


def _der_ecdsa_signature(raw_signature: bytes) -> bytes:
    """Convert CNG's fixed-width r || s signature to the DER ECDSA form."""

    if len(raw_signature) != _P256_COORDINATE_SIZE * 2:
        raise UnsupportedKeyStore("CNG returned an unexpected ECDSA P-256 signature size")
    sequence = _der_integer(raw_signature[:_P256_COORDINATE_SIZE]) + _der_integer(
        raw_signature[_P256_COORDINATE_SIZE:]
    )
    return b"\x30" + bytes((len(sequence),)) + sequence


class WindowsCNGKeyStore:
    """A named, non-exportable ECDSA P-256 key in the current Windows user store."""

    def __init__(self, name: str) -> None:
        if sys.platform != "win32":
            raise UnsupportedKeyStore("Windows CNG is only available on Windows")
        if not isinstance(name, str) or not name.strip() or "\x00" in name:
            raise ValueError("CNG key name must be a non-empty string without NUL characters")

        self._library = _ncrypt()
        self._provider: _HANDLE | None = None
        self._key: _HANDLE | None = None
        created = False
        try:
            provider = _HANDLE()
            _require_success(
                self._library.NCryptOpenStorageProvider(
                    ctypes.byref(provider), _MS_KEY_STORAGE_PROVIDER, 0
                ),
                "NCryptOpenStorageProvider",
            )
            self._provider = provider

            key = _HANDLE()
            status = self._library.NCryptOpenKey(provider, ctypes.byref(key), name, 0, 0)
            if _status_code(status) == _NTE_BAD_KEYSET:
                key, created = self._create_key(provider, name)
            else:
                _require_success(status, "NCryptOpenKey")
            self._key = key
            self._verify_non_exportable()
            self._public_blob()
        except Exception:
            if created:
                self._delete_key_silently()
            self.close()
            raise

    def _create_key(self, provider: _HANDLE, name: str) -> tuple[_HANDLE, bool]:
        key = _HANDLE()
        status = self._library.NCryptCreatePersistedKey(
            provider,
            ctypes.byref(key),
            _NCRYPT_ECDSA_P256_ALGORITHM,
            name,
            0,
            0,
        )
        if _status_code(status) == _NTE_EXISTS:
            _require_success(
                self._library.NCryptOpenKey(provider, ctypes.byref(key), name, 0, 0),
                "NCryptOpenKey",
            )
            return key, False
        _require_success(status, "NCryptCreatePersistedKey")
        try:
            export_policy = _dword_buffer(0)
            _require_success(
                self._library.NCryptSetProperty(
                    key,
                    _NCRYPT_EXPORT_POLICY_PROPERTY,
                    export_policy,
                    ctypes.sizeof(export_policy),
                    0,
                ),
                "NCryptSetProperty(Export Policy)",
            )
            key_usage = _dword_buffer(_NCRYPT_ALLOW_SIGNING_FLAG)
            _require_success(
                self._library.NCryptSetProperty(
                    key,
                    _NCRYPT_KEY_USAGE_PROPERTY,
                    key_usage,
                    ctypes.sizeof(key_usage),
                    0,
                ),
                "NCryptSetProperty(Key Usage)",
            )
            _require_success(
                self._library.NCryptFinalizeKey(key, 0),
                "NCryptFinalizeKey",
            )
        except Exception:
            self._library.NCryptDeleteKey(key, 0)
            raise
        return key, True

    def _verify_non_exportable(self) -> None:
        key = self._require_key()
        policy = _dword_buffer(0)
        result_size = _DWORD()
        _require_success(
            self._library.NCryptGetProperty(
                key,
                _NCRYPT_EXPORT_POLICY_PROPERTY,
                policy,
                ctypes.sizeof(policy),
                ctypes.byref(result_size),
                0,
            ),
            "NCryptGetProperty(Export Policy)",
        )
        if result_size.value != ctypes.sizeof(_DWORD) or int.from_bytes(policy, "little") != 0:
            raise UnsupportedKeyStore("CNG key is exportable")

    def _require_key(self) -> _HANDLE:
        if self._key is None or not self._key.value:
            raise UnsupportedKeyStore("CNG key store is closed")
        return self._key

    def _public_blob(self) -> bytes:
        key = self._require_key()
        result_size = _DWORD()
        _require_success(
            self._library.NCryptExportKey(
                key,
                None,
                _BCRYPT_ECCPUBLIC_BLOB,
                None,
                None,
                0,
                ctypes.byref(result_size),
                0,
            ),
            "NCryptExportKey(size)",
        )
        blob = (_BYTE * result_size.value)()
        _require_success(
            self._library.NCryptExportKey(
                key,
                None,
                _BCRYPT_ECCPUBLIC_BLOB,
                None,
                blob,
                ctypes.sizeof(blob),
                ctypes.byref(result_size),
                0,
            ),
            "NCryptExportKey",
        )
        result = bytes(blob[: result_size.value])
        expected_size = 8 + (_P256_COORDINATE_SIZE * 2)
        if (
            len(result) != expected_size
            or int.from_bytes(result[:4], "little") != _BCRYPT_ECDSA_PUBLIC_P256_MAGIC
            or int.from_bytes(result[4:8], "little") != _P256_COORDINATE_SIZE
        ):
            raise UnsupportedKeyStore("CNG key is not ECDSA P-256")
        return result

    def public_key_pem(self) -> str:
        """Return the CNG key's public P-256 SubjectPublicKeyInfo PEM."""

        public_blob = self._public_blob()
        point = public_blob[8:]
        subject_public_key_info = (
            bytes.fromhex("3059301306072A8648CE3D020106082A8648CE3D03010703420004") + point
        )
        encoded = base64.b64encode(subject_public_key_info).decode("ascii")
        lines = (encoded[index : index + 64] for index in range(0, len(encoded), 64))
        return "-----BEGIN PUBLIC KEY-----\n" + "\n".join(lines) + "\n-----END PUBLIC KEY-----\n"

    def sign(self, payload: bytes) -> bytes:
        """Sign *payload* with ECDSA SHA-256, returning an ASN.1 DER signature."""

        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        digest = hashlib.sha256(payload).digest()
        digest_buffer = (_BYTE * len(digest)).from_buffer_copy(digest)
        key = self._require_key()
        result_size = _DWORD()
        _require_success(
            self._library.NCryptSignHash(
                key,
                None,
                digest_buffer,
                ctypes.sizeof(digest_buffer),
                None,
                0,
                ctypes.byref(result_size),
                0,
            ),
            "NCryptSignHash(size)",
        )
        signature = (_BYTE * result_size.value)()
        _require_success(
            self._library.NCryptSignHash(
                key,
                None,
                digest_buffer,
                ctypes.sizeof(digest_buffer),
                signature,
                ctypes.sizeof(signature),
                ctypes.byref(result_size),
                0,
            ),
            "NCryptSignHash",
        )
        return _der_ecdsa_signature(bytes(signature[: result_size.value]))

    def _delete_key_silently(self) -> None:
        if self._key is not None and self._key.value:
            self._library.NCryptDeleteKey(self._key, 0)
            self._key = None

    def delete(self) -> None:
        """Permanently remove this named key; intended for explicit deprovisioning."""

        key = self._require_key()
        _require_success(self._library.NCryptDeleteKey(key, 0), "NCryptDeleteKey")
        self._key = None

    def close(self) -> None:
        """Release CNG handles without deleting the persisted key."""

        if self._key is not None and self._key.value:
            self._library.NCryptFreeObject(self._key)
            self._key = None
        if self._provider is not None and self._provider.value:
            self._library.NCryptFreeObject(self._provider)
            self._provider = None

    def __enter__(self) -> WindowsCNGKeyStore:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
