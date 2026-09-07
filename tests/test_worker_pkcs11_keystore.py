from __future__ import annotations

import base64
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pkcs11 import Attribute
from pkcs11.exceptions import AttributeSensitive

from abt.controlplane.crypto import verify_worker_proof, worker_proof_payload
from abt.worker.pkcs11_keystore import (
    IdentityExistsError,
    LinuxPKCS11KeyStore,
    LinuxPKCS11KeyStoreFactory,
    PKCS11IdentityError,
)


def _find_softhsm_module() -> Path:
    if "ABT_SOFTHSM2_MODULE" in os.environ:
        return Path(os.environ["ABT_SOFTHSM2_MODULE"])
    for candidate in (
        Path.home() / ".local/usr/lib/x86_64-linux-gnu/softhsm/libsofthsm2.so",
        Path.home() / ".local/usr/lib/softhsm/libsofthsm2.so",
        Path("/usr/lib/softhsm/libsofthsm2.so"),
        Path("/usr/lib/x86_64-linux-gnu/softhsm/libsofthsm2.so"),
    ):
        if candidate.is_file():
            return candidate
    return Path("/usr/lib/softhsm/libsofthsm2.so")


MODULE_PATH = _find_softhsm_module()


@unittest.skipUnless(MODULE_PATH.is_file(), "requires SoftHSM2")
class LinuxPKCS11KeyStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name) / "worker.keys"
        self.factory = LinuxPKCS11KeyStoreFactory(self.root, module_path=MODULE_PATH)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_create_sign_and_reopen_preserves_controller_identity(self) -> None:
        store = self.factory.create("device-key")
        pem = store.public_key_pem()
        payload = worker_proof_payload(
            purpose="worker_enrollment",
            worker_id="worker-123",
            nonce="nonce-123",
        )
        signature = store.sign(payload)
        verify_worker_proof(
            pem,
            base64.b64encode(signature).decode("ascii"),
            purpose="worker_enrollment",
            worker_id="worker-123",
            nonce="nonce-123",
        )
        store.close()

        reopened = self.factory.open("device-key")
        try:
            self.assertEqual(pem, reopened.public_key_pem())
            public_key = serialization.load_pem_public_key(pem.encode("ascii"))
            public_key.verify(reopened.sign(b"restart"), b"restart", ec.ECDSA(hashes.SHA256()))
        finally:
            reopened.close()

    def test_create_refuses_replacement_and_open_never_regenerates(self) -> None:
        store = self.factory.create("device-key")
        store.close()
        with self.assertRaises(IdentityExistsError):
            self.factory.create("device-key")

        shutil.rmtree(self.root / "device-key")
        with self.assertRaises(PKCS11IdentityError):
            self.factory.open("device-key")
        self.assertFalse((self.root / "device-key").exists())

    def test_private_key_is_signing_sensitive_and_not_extractable(self) -> None:
        store = self.factory.create("device-key")
        try:
            self.assertEqual(
                {"sign": True, "sensitive": True, "extractable": False, "private": True},
                store.private_key_attributes(),
            )
            with self.assertRaises(AttributeSensitive):
                _ = store._private_key[Attribute.VALUE]
        finally:
            store.close()

    def test_delete_is_explicit_and_refuses_an_open_identity(self) -> None:
        store = self.factory.create("device-key")
        with self.assertRaises(PKCS11IdentityError):
            self.factory.delete("device-key")
        store.close()
        self.factory.delete("device-key")
        self.assertFalse((self.root / "device-key").exists())

    def test_factory_rejects_unsafe_key_names(self) -> None:
        for name in ("", ".", "..", "../escape", "nested/key", "nul\x00name"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.factory.open(name)

    def test_provider_metadata_and_permissions_are_explicit(self) -> None:
        store = self.factory.create("device-key")
        try:
            self.assertEqual("PKCS11-SOFTWARE", store.metadata["provider"])
            self.assertEqual(0o700, (self.root / "device-key").stat().st_mode & 0o777)
            self.assertEqual(0o700, (self.root / "device-key" / "tokens").stat().st_mode & 0o777)
            self.assertEqual(0o600, (self.root / "device-key" / "user-pin").stat().st_mode & 0o777)
        finally:
            store.close()
