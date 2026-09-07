from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import secrets
import shutil
import stat
import threading
import uuid
from pathlib import Path
from typing import Any

import pkcs11
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pkcs11 import Attribute, KeyType, Mechanism, ObjectClass
from pkcs11.exceptions import UserAlreadyLoggedIn
from pkcs11.util.ec import encode_ec_public_key, encode_ecdsa_signature, encode_named_curve_parameters


class PKCS11IdentityError(RuntimeError):
    pass


class IdentityExistsError(PKCS11IdentityError):
    pass


_LOCK = threading.RLock()
_LIBRARIES: dict[str, Any] = {}
_CTYPES_LIBRARIES: dict[str, Any] = {}
_ACTIVE_CONFIGS: dict[str, str] = {}
_OPEN_MODULE_COUNTS: dict[str, int] = {}
_OPEN_PATH_COUNTS: dict[str, int] = {}
_CK_OK = 0
_CKR_CRYPTOKI_ALREADY_INITIALIZED = 0x191
_CKF_RW_SESSION = 0x2
_CKF_SERIAL_SESSION = 0x4
_CKU_SO = 0


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _require_mode(path: Path, expected: int) -> None:
    try:
        actual = _mode(path)
    except OSError as error:
        raise PKCS11IdentityError(f"identity state is missing: {path.name}") from error
    if actual != expected:
        raise PKCS11IdentityError(f"identity state has unsafe permissions: {path.name}")


def _config_text(token_directory: Path) -> str:
    return (
        f"directories.tokendir = {token_directory}\n"
        "objectstore.backend = file\n"
        "log.level = ERROR\n"
        "slots.removable = false\n"
    )


def _write_private(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _check_rc(result: int, operation: str, *, allowed: tuple[int, ...] = ()) -> None:
    if result != _CK_OK and result not in allowed:
        raise PKCS11IdentityError(f"{operation} failed (0x{result:08X})")


def _initialize_token(module_path: Path, config_path: Path, token_label: str, so_pin: bytes, user_pin: bytes) -> None:
    with _LOCK:
        _initialize_token_locked(module_path, config_path, token_label, so_pin, user_pin)


def _initialize_token_locked(module_path: Path, config_path: Path, token_label: str, so_pin: bytes, user_pin: bytes) -> None:
    previous = os.environ.get("SOFTHSM2_CONF")
    os.environ["SOFTHSM2_CONF"] = str(config_path)
    try:
        module_key = str(module_path.resolve())
        existing = _LIBRARIES.get(module_key)
        if existing is not None and _ACTIVE_CONFIGS.get(module_key) != str(config_path.resolve()):
            existing.reinitialize()
            _ACTIVE_CONFIGS[module_key] = str(config_path.resolve())
        library = _CTYPES_LIBRARIES.get(module_key)
        if library is None:
            library = ctypes.CDLL(str(module_path))
            _CTYPES_LIBRARIES[module_key] = library
        ulong = ctypes.c_ulong
        library.C_Initialize.argtypes = [ctypes.c_void_p]
        library.C_Initialize.restype = ulong
        library.C_Finalize.argtypes = [ctypes.c_void_p]
        library.C_Finalize.restype = ulong
        library.C_GetSlotList.argtypes = [ctypes.c_ubyte, ctypes.POINTER(ulong), ctypes.POINTER(ulong)]
        library.C_GetSlotList.restype = ulong
        library.C_InitToken.argtypes = [ulong, ctypes.c_void_p, ulong, ctypes.c_void_p]
        library.C_InitToken.restype = ulong
        library.C_OpenSession.argtypes = [ulong, ulong, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ulong)]
        library.C_OpenSession.restype = ulong
        library.C_Login.argtypes = [ulong, ulong, ctypes.c_void_p, ulong]
        library.C_Login.restype = ulong
        library.C_InitPIN.argtypes = [ulong, ctypes.c_void_p, ulong]
        library.C_InitPIN.restype = ulong
        library.C_Logout.argtypes = [ulong]
        library.C_Logout.restype = ulong
        library.C_CloseSession.argtypes = [ulong]
        library.C_CloseSession.restype = ulong

        _check_rc(library.C_Initialize(None), "C_Initialize", allowed=(_CKR_CRYPTOKI_ALREADY_INITIALIZED,))
        count = ulong()
        _check_rc(library.C_GetSlotList(0, None, ctypes.byref(count)), "C_GetSlotList(count)")
        if count.value == 0:
            raise PKCS11IdentityError("SoftHSM exposes no slots")
        slots = (ulong * count.value)()
        _check_rc(library.C_GetSlotList(0, slots, ctypes.byref(count)), "C_GetSlotList")
        label = token_label.encode("utf-8")
        if len(label) > 32:
            raise PKCS11IdentityError("token label exceeds 32 bytes")
        padded_label = label.ljust(32, b" ")
        so_buffer = ctypes.create_string_buffer(so_pin)
        label_buffer = ctypes.create_string_buffer(padded_label)
        _check_rc(
            library.C_InitToken(slots[0], so_buffer, len(so_pin), label_buffer),
            "C_InitToken",
        )
        _check_rc(library.C_Finalize(None), "C_Finalize")
        _check_rc(library.C_Initialize(None), "C_Initialize(after token init)")

        count = ulong()
        _check_rc(library.C_GetSlotList(1, None, ctypes.byref(count)), "C_GetSlotList(initialized count)")
        slots = (ulong * count.value)()
        _check_rc(library.C_GetSlotList(1, slots, ctypes.byref(count)), "C_GetSlotList(initialized)")
        initialized = False
        for slot in slots:
            session = ulong()
            if library.C_OpenSession(slot, _CKF_RW_SESSION | _CKF_SERIAL_SESSION, None, None, ctypes.byref(session)) != _CK_OK:
                continue
            try:
                if library.C_Login(session, _CKU_SO, so_buffer, len(so_pin)) != _CK_OK:
                    continue
                try:
                    user_buffer = ctypes.create_string_buffer(user_pin)
                    _check_rc(library.C_InitPIN(session, user_buffer, len(user_pin)), "C_InitPIN")
                    initialized = True
                    break
                finally:
                    library.C_Logout(session)
            finally:
                library.C_CloseSession(session)
        if not initialized:
            raise PKCS11IdentityError("could not initialize the SoftHSM user PIN")
        _check_rc(library.C_Finalize(None), "C_Finalize(after PIN init)")
        if existing is not None:
            _check_rc(library.C_Initialize(None), "C_Initialize(restore wrapper state)")
            existing.reinitialize()
            _ACTIVE_CONFIGS[module_key] = str(config_path.resolve())
    finally:
        if previous is None:
            os.environ.pop("SOFTHSM2_CONF", None)
        else:
            os.environ["SOFTHSM2_CONF"] = previous


def _activate_library(module_path: Path, config_path: Path) -> Any:
    module = str(module_path.resolve())
    config = str(config_path.resolve())
    previous = os.environ.get("SOFTHSM2_CONF")
    os.environ["SOFTHSM2_CONF"] = config
    try:
        library = _LIBRARIES.get(module)
        if library is None:
            library = pkcs11.lib(module)
            _LIBRARIES[module] = library
            _ACTIVE_CONFIGS[module] = config
        elif _ACTIVE_CONFIGS.get(module) != config:
            library.reinitialize()
            _ACTIVE_CONFIGS[module] = config
        return library
    finally:
        if previous is None:
            os.environ.pop("SOFTHSM2_CONF", None)
        else:
            os.environ["SOFTHSM2_CONF"] = previous


class LinuxPKCS11KeyStore:
    def __init__(self, identity_path: Path, module_path: Path, metadata: dict[str, str], pin: str = "") -> None:
        self.identity_path = identity_path
        self.module_path = module_path
        self.metadata = metadata
        self._pin = pin
        self._session: Any | None = None
        self._private_key: Any | None = None
        self._public_key: Any | None = None
        self._registered = False

    @classmethod
    def open(cls, identity_path: Path | str, *, module_path: Path | str) -> "LinuxPKCS11KeyStore":
        with _LOCK:
            return cls._open_locked(identity_path, module_path=module_path)

    @classmethod
    def _open_locked(cls, identity_path: Path | str, *, module_path: Path | str) -> "LinuxPKCS11KeyStore":
        path = Path(identity_path)
        module = Path(module_path)
        _require_mode(path, 0o700)
        _require_mode(path / "tokens", 0o700)
        _require_mode(path / "softhsm2.conf", 0o600)
        _require_mode(path / "user-pin", 0o600)
        _require_mode(path / "identity.json", 0o600)
        if not module.is_file():
            raise PKCS11IdentityError("PKCS#11 module is unavailable")
        try:
            metadata = json.loads((path / "identity.json").read_text(encoding="utf-8"))
            pin = (path / "user-pin").read_text(encoding="ascii")
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise PKCS11IdentityError("identity state is unreadable") from error
        if set(metadata) != {"provider", "token_label", "key_id", "key_label"} or metadata["provider"] != "PKCS11-SOFTWARE":
            raise PKCS11IdentityError("identity metadata is invalid")
        store = cls(path, module, metadata, pin=pin)
        try:
            store._open_session(pin)
        except Exception as error:
            store.close()
            if isinstance(error, PKCS11IdentityError):
                raise
            raise PKCS11IdentityError("SoftHSM login or key lookup failed") from error
        return store

    def _open_session(self, pin: str) -> None:
        with _LOCK:
            self._pin = pin
            library = _activate_library(self.module_path, self.identity_path / "softhsm2.conf")
            token = library.get_token(token_label=self.metadata["token_label"])
            try:
                self._session = token.open(rw=False, user_pin=pin)
            except UserAlreadyLoggedIn:
                self._session = token.open(rw=False)
            key_id = bytes.fromhex(self.metadata["key_id"])
            self._private_key = self._session.get_key(
                object_class=ObjectClass.PRIVATE_KEY,
                key_type=KeyType.EC,
                id=key_id,
            )
            self._public_key = self._session.get_key(
                object_class=ObjectClass.PUBLIC_KEY,
                key_type=KeyType.EC,
                id=key_id,
            )
            if not self._registered:
                module_key = str(self.module_path.resolve())
                path_key = str(self.identity_path.resolve())
                _OPEN_MODULE_COUNTS[module_key] = _OPEN_MODULE_COUNTS.get(module_key, 0) + 1
                _OPEN_PATH_COUNTS[path_key] = _OPEN_PATH_COUNTS.get(path_key, 0) + 1
                self._registered = True

    def _refresh_session(self) -> None:
        with _LOCK:
            if self._session is not None:
                try:
                    self._session.close()
                except Exception:
                    pass
                self._session = None
                self._private_key = None
                self._public_key = None
            self._open_session(self._pin)

    def _require_open(self) -> None:
        if self._session is None or self._private_key is None or self._public_key is None:
            raise PKCS11IdentityError("PKCS#11 key store is closed")

    def _require_active_session(self) -> None:
        self._require_open()
        module_key = str(self.module_path.resolve())
        expected_config = str((self.identity_path / "softhsm2.conf").resolve())
        if _ACTIVE_CONFIGS.get(module_key) != expected_config:
            self._refresh_session()

    def public_key_pem(self) -> str:
        with _LOCK:
            self._require_active_session()
            try:
                der = encode_ec_public_key(self._public_key)
            except (pkcs11.exceptions.SessionHandleInvalid, pkcs11.exceptions.SessionClosed):
                self._refresh_session()
                der = encode_ec_public_key(self._public_key)
            public_key = serialization.load_der_public_key(der)
            return public_key.public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            ).decode("ascii")

    def sign(self, payload: bytes) -> bytes:
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        with _LOCK:
            self._require_active_session()
            digest = hashlib.sha256(payload).digest()
            try:
                raw_signature = self._private_key.sign(digest, mechanism=Mechanism.ECDSA)
            except (pkcs11.exceptions.SessionHandleInvalid, pkcs11.exceptions.SessionClosed):
                self._refresh_session()
                raw_signature = self._private_key.sign(digest, mechanism=Mechanism.ECDSA)
            return encode_ecdsa_signature(raw_signature)

    def private_key_attributes(self) -> dict[str, bool]:
        with _LOCK:
            self._require_active_session()
            try:
                key = self._private_key
                return {
                    "sign": bool(key[Attribute.SIGN]),
                    "sensitive": bool(key[Attribute.SENSITIVE]),
                    "extractable": bool(key[Attribute.EXTRACTABLE]),
                    "private": bool(key[Attribute.PRIVATE]),
                }
            except (pkcs11.exceptions.SessionHandleInvalid, pkcs11.exceptions.SessionClosed):
                self._refresh_session()
                key = self._private_key
                return {
                    "sign": bool(key[Attribute.SIGN]),
                    "sensitive": bool(key[Attribute.SENSITIVE]),
                    "extractable": bool(key[Attribute.EXTRACTABLE]),
                    "private": bool(key[Attribute.PRIVATE]),
                }

    def close(self) -> None:
        with _LOCK:
            close_error: Exception | None = None
            if self._session is not None:
                try:
                    self._session.close()
                except (pkcs11.exceptions.SessionHandleInvalid, pkcs11.exceptions.SessionClosed):
                    pass
                except Exception as error:
                    close_error = error
                finally:
                    self._session = None
                    self._private_key = None
                    self._public_key = None
            if self._registered:
                module_key = str(self.module_path.resolve())
                path_key = str(self.identity_path.resolve())
                _OPEN_MODULE_COUNTS[module_key] -= 1
                _OPEN_PATH_COUNTS[path_key] -= 1
                if _OPEN_MODULE_COUNTS[module_key] == 0:
                    del _OPEN_MODULE_COUNTS[module_key]
                if _OPEN_PATH_COUNTS[path_key] == 0:
                    del _OPEN_PATH_COUNTS[path_key]
                self._registered = False
            if close_error is not None:
                raise PKCS11IdentityError("PKCS#11 session close failed") from close_error

    def delete(self) -> None:
        identity_path = self.identity_path
        self.close()
        delete_identity(identity_path)

    def __enter__(self) -> "LinuxPKCS11KeyStore":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise PKCS11IdentityError("atomic no-replace rename is unavailable on this Linux host")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in (errno.EEXIST, errno.ENOTEMPTY):
        raise IdentityExistsError("identity already exists")
    raise PKCS11IdentityError(f"atomic identity publish failed: {os.strerror(error_number)}")


def _provision_identity(identity_path: Path | str, *, module_path: Path | str) -> LinuxPKCS11KeyStore:
    with _LOCK:
        return _provision_identity_locked(identity_path, module_path=module_path)


def _provision_identity_locked(identity_path: Path | str, *, module_path: Path | str) -> LinuxPKCS11KeyStore:
    final_path = Path(identity_path)
    module = Path(module_path)
    if final_path.exists():
        raise IdentityExistsError("identity already exists")
    if not module.is_file():
        raise PKCS11IdentityError("PKCS#11 module is unavailable")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    staging = final_path.parent / f".{final_path.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        token_directory = staging / "tokens"
        token_directory.mkdir(mode=0o700)
        token_label = f"abt-{uuid.uuid4().hex[:20]}"
        key_id = secrets.token_bytes(16)
        key_label = "abt-worker-identity"
        so_pin = secrets.token_urlsafe(32).encode("ascii")
        user_pin = secrets.token_urlsafe(32).encode("ascii")
        staging_config = staging / ".provisioning.conf"
        _write_private(staging_config, _config_text(token_directory).encode("utf-8"))
        try:
            _initialize_token(module, staging_config, token_label, so_pin, user_pin)
            os.chmod(token_directory, 0o700)
            _write_private(staging / "user-pin", user_pin)
            metadata = {
                "provider": "PKCS11-SOFTWARE",
                "token_label": token_label,
                "key_id": key_id.hex(),
                "key_label": key_label,
            }
            _write_private(
                staging / "identity.json",
                json.dumps(metadata, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            )
            _write_private(staging / "softhsm2.conf", _config_text(final_path / "tokens").encode("utf-8"))

            with _LOCK:
                library = _activate_library(module, staging_config)
                token = library.get_token(token_label=token_label)
                module_key = str(module.resolve())
                _OPEN_MODULE_COUNTS[module_key] = _OPEN_MODULE_COUNTS.get(module_key, 0) + 1
                try:
                    with token.open(rw=True, user_pin=user_pin.decode("ascii")) as session:
                        public_key_object, private_key_object = session.generate_keypair(
                            KeyType.EC,
                            store=True,
                            label=key_label,
                            id=key_id,
                            public_template={
                                Attribute.EC_PARAMS: encode_named_curve_parameters("secp256r1"),
                                Attribute.VERIFY: True,
                                Attribute.PRIVATE: False,
                            },
                            private_template={
                                Attribute.SIGN: True,
                                Attribute.SENSITIVE: True,
                                Attribute.EXTRACTABLE: False,
                                Attribute.PRIVATE: True,
                            },
                        )
                        self_test_payload = b"abt-pkcs11-provisioning-self-test"
                        raw_signature = private_key_object.sign(
                            hashlib.sha256(self_test_payload).digest(),
                            mechanism=Mechanism.ECDSA,
                        )
                        public_key = serialization.load_der_public_key(encode_ec_public_key(public_key_object))
                        if not isinstance(public_key, ec.EllipticCurvePublicKey):
                            raise PKCS11IdentityError("generated key is not an EC public key")
                        public_key.verify(
                            encode_ecdsa_signature(raw_signature),
                            self_test_payload,
                            ec.ECDSA(hashes.SHA256()),
                        )
                finally:
                    _OPEN_MODULE_COUNTS[module_key] -= 1
                    if _OPEN_MODULE_COUNTS[module_key] == 0:
                        del _OPEN_MODULE_COUNTS[module_key]
            _rename_noreplace(staging, final_path)
        finally:
            staging_config.unlink(missing_ok=True)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    published_provisioning_config = final_path / ".provisioning.conf"
    try:
        store = LinuxPKCS11KeyStore.open(final_path, module_path=module)
    except Exception:
        shutil.rmtree(final_path, ignore_errors=True)
        raise
    finally:
        published_provisioning_config.unlink(missing_ok=True)
    payload = b"abt-pkcs11-provisioning-self-test"
    public_key = serialization.load_pem_public_key(store.public_key_pem().encode("ascii"))
    try:
        public_key.verify(store.sign(payload), payload, ec.ECDSA(hashes.SHA256()))
    except Exception:
        store.close()
        shutil.rmtree(final_path, ignore_errors=True)
        raise
    return store


def delete_identity(identity_path: Path | str) -> None:
    path = Path(identity_path)
    with _LOCK:
        if not path.exists():
            raise PKCS11IdentityError("identity does not exist")
        _require_mode(path, 0o700)
        if _OPEN_PATH_COUNTS.get(str(path.resolve()), 0):
            raise PKCS11IdentityError("identity is in use; close all key stores before deleting it")
        shutil.rmtree(path)


class LinuxPKCS11KeyStoreFactory:
    """Create, open, and explicitly delete named software-backed Worker identities."""

    def __init__(self, root: Path | str, *, module_path: Path | str) -> None:
        self.root = Path(root)
        self.module_path = Path(module_path)

    def _path(self, key_name: str) -> Path:
        allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        if (
            not isinstance(key_name, str)
            or not key_name
            or len(key_name) > 128
            or key_name in {".", ".."}
            or any(character not in allowed for character in key_name)
        ):
            raise ValueError("PKCS#11 key name contains unsafe characters")
        return self.root / key_name

    def enroll(self, key_name: str) -> LinuxPKCS11KeyStore:
        path = self._path(key_name)
        if path.exists():
            return self.open(key_name)
        return self.create(key_name)

    def create(self, key_name: str) -> LinuxPKCS11KeyStore:
        if self.root.exists():
            _require_mode(self.root, 0o700)
        else:
            self.root.mkdir(parents=True, mode=0o700)
        return _provision_identity(self._path(key_name), module_path=self.module_path)

    def open(self, key_name: str) -> LinuxPKCS11KeyStore:
        path = self._path(key_name)
        _require_mode(self.root, 0o700)
        return LinuxPKCS11KeyStore.open(path, module_path=self.module_path)

    def delete(self, key_name: str) -> None:
        path = self._path(key_name)
        _require_mode(self.root, 0o700)
        delete_identity(path)
