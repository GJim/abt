from __future__ import annotations

import base64
import os
import shutil
import stat
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from pkcs11 import Attribute
from pkcs11.exceptions import AttributeSensitive

from abt.controlplane.crypto import verify_worker_proof, worker_proof_payload
from pkcs11_keystore import (
    IdentityExistsError,
    IdentityStateError,
    PKCS11KeyStore,
    delete_identity,
    provision_identity,
)

MODULE_PATH = Path(os.environ.get("ABT_TEST_PKCS11_MODULE", "/usr/lib/softhsm/libsofthsm2.so"))


@unittest.skipUnless(MODULE_PATH.is_file(), "requires the SoftHSM2 PKCS#11 module")
class PKCS11KeyStoreIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.identity_path = Path(self.temporary_directory.name) / "identity"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_stale_interrupted_staging_is_never_promoted(self) -> None:
        stale = self.identity_path.parent / f".{self.identity_path.name}.staging-{uuid4().hex}"
        stale.mkdir(mode=0o700)
        (stale / ".provisioning.conf").write_text("directories.tokendir = incomplete\n", encoding="utf-8")
        (stale / "incomplete").write_text("not-an-identity", encoding="utf-8")
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(store.close)
        self.assertTrue(stale.is_dir())
        self.assertEqual("PKCS11-SOFTWARE", store.metadata["provider"])
        self.assertEqual("not-an-identity", (stale / "incomplete").read_text(encoding="utf-8"))

    def test_provision_sign_and_reopen_preserves_controller_identity(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        public_pem = store.public_key_pem()
        payload = worker_proof_payload(purpose="prototype", worker_id="worker-1", nonce="nonce-1")
        signature = store.sign(payload)
        store.close()

        verify_worker_proof(
            public_pem,
            base64.b64encode(signature).decode("ascii"),
            purpose="prototype",
            worker_id="worker-1",
            nonce="nonce-1",
        )
        reopened = PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(reopened.close)
        self.assertEqual(public_pem, reopened.public_key_pem())
        public_key = serialization.load_pem_public_key(public_pem.encode("ascii"))
        self.assertIsInstance(public_key, ec.EllipticCurvePublicKey)
        self.assertIsInstance(public_key.curve, ec.SECP256R1)
        public_key.verify(reopened.sign(b"restart-proof"), b"restart-proof", ec.ECDSA(hashes.SHA256()))

    def test_provision_is_atomic_and_refuses_replacement(self) -> None:
        first = provision_identity(self.identity_path, module_path=MODULE_PATH)
        first_pem = first.public_key_pem()
        first.close()
        with self.assertRaises(IdentityExistsError):
            provision_identity(self.identity_path, module_path=MODULE_PATH)
        reopened = PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(reopened.close)
        self.assertEqual(first_pem, reopened.public_key_pem())
        self.assertEqual(stat.S_IMODE(self.identity_path.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.identity_path / "user-pin").stat().st_mode), 0o600)

    def test_missing_or_wrong_credential_fails_without_regeneration(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        original_pem = store.public_key_pem()
        store.close()
        credential = self.identity_path / "user-pin"
        original_pin = credential.read_bytes()
        credential.unlink()
        with self.assertRaises(IdentityStateError):
            PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        credential.write_bytes(b"definitely-wrong-pin")
        credential.chmod(0o600)
        with self.assertRaises(IdentityStateError):
            PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        credential.write_bytes(original_pin)
        credential.chmod(0o600)
        reopened = PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(reopened.close)
        self.assertEqual(original_pem, reopened.public_key_pem())

    def test_missing_token_and_broad_permissions_fail_closed(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        store.close()
        token_dir = self.identity_path / "tokens"
        moved = self.identity_path / "tokens.missing"
        token_dir.rename(moved)
        with self.assertRaises(IdentityStateError):
            PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        moved.rename(token_dir)
        (self.identity_path / "user-pin").chmod(0o644)
        with self.assertRaises(IdentityStateError):
            PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)

    def test_private_key_attributes_and_standard_export_boundary(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(store.close)
        attributes = store.private_key_attributes()
        self.assertEqual(
            attributes,
            {"sign": True, "sensitive": True, "extractable": False, "private": True},
        )
        with self.assertRaises(AttributeSensitive):
            _ = store._private_key[Attribute.VALUE]

    def test_softhsm_config_environment_is_restored(self) -> None:
        sentinel = "/tmp/caller-owned-softhsm.conf"
        previous = os.environ.get("SOFTHSM2_CONF")
        os.environ["SOFTHSM2_CONF"] = sentinel
        try:
            store = provision_identity(self.identity_path, module_path=MODULE_PATH)
            store.close()
            self.assertEqual(sentinel, os.environ.get("SOFTHSM2_CONF"))
            reopened = PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
            reopened.close()
            self.assertEqual(sentinel, os.environ.get("SOFTHSM2_CONF"))
        finally:
            if previous is None:
                os.environ.pop("SOFTHSM2_CONF", None)
            else:
                os.environ["SOFTHSM2_CONF"] = previous

    def test_pin_is_not_present_in_metadata_config_environment_or_argv(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        store.close()
        pin = (self.identity_path / "user-pin").read_bytes()
        self.assertGreaterEqual(len(pin), 32)
        inspected = [
            (self.identity_path / "identity.json").read_bytes(),
            (self.identity_path / "softhsm2.conf").read_bytes(),
            "\0".join(sys.argv).encode("utf-8"),
            "\0".join(f"{key}={value}" for key, value in os.environ.items()).encode("utf-8"),
        ]
        for value in inspected:
            self.assertNotIn(pin, value)

    def test_concurrent_sign_calls_are_serialized_and_valid(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(store.close)
        public_key = serialization.load_pem_public_key(store.public_key_pem().encode("ascii"))
        payloads = [f"concurrent-proof-{index}".encode("ascii") for index in range(16)]
        with ThreadPoolExecutor(max_workers=4) as executor:
            signatures = list(executor.map(store.sign, payloads))
        for payload, signature in zip(payloads, signatures, strict=True):
            public_key.verify(signature, payload, ec.ECDSA(hashes.SHA256()))

    def test_two_open_sessions_for_the_same_identity_can_sign(self) -> None:
        first = provision_identity(self.identity_path, module_path=MODULE_PATH)
        second = PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        public_key = serialization.load_pem_public_key(first.public_key_pem().encode("ascii"))
        for store, payload in ((first, b"first-session"), (second, b"second-session")):
            public_key.verify(store.sign(payload), payload, ec.ECDSA(hashes.SHA256()))

    def test_provisioning_another_identity_requires_existing_sessions_to_close(self) -> None:
        current = provision_identity(self.identity_path, module_path=MODULE_PATH)
        self.addCleanup(current.close)
        replacement_path = Path(self.temporary_directory.name) / "identity-replacement"
        with self.assertRaises(IdentityStateError):
            provision_identity(replacement_path, module_path=MODULE_PATH)
        payload = b"still-valid-after-refused-provision"
        public_key = serialization.load_pem_public_key(current.public_key_pem().encode("ascii"))
        public_key.verify(current.sign(payload), payload, ec.ECDSA(hashes.SHA256()))
        self.assertFalse(replacement_path.exists())

    def test_delete_refuses_an_identity_with_an_open_session(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        with self.assertRaises(IdentityStateError):
            delete_identity(self.identity_path)
        self.assertTrue(self.identity_path.exists())
        store.close()
        delete_identity(self.identity_path)
        self.assertFalse(self.identity_path.exists())

    def test_rotation_candidate_uses_same_contract_and_a_distinct_identity(self) -> None:
        current = provision_identity(self.identity_path, module_path=MODULE_PATH)
        current_pem = current.public_key_pem()
        current.close()
        replacement_path = Path(self.temporary_directory.name) / "identity-replacement"
        replacement = provision_identity(replacement_path, module_path=MODULE_PATH)
        replacement_pem = replacement.public_key_pem()
        replacement.close()
        self.assertNotEqual(current_pem, replacement_pem)
        for path, pem, payload in (
            (self.identity_path, current_pem, b"old-rotation-proof"),
            (replacement_path, replacement_pem, b"new-rotation-proof"),
        ):
            store = PKCS11KeyStore.open(path, module_path=MODULE_PATH)
            try:
                public_key = serialization.load_pem_public_key(pem.encode("ascii"))
                public_key.verify(store.sign(payload), payload, ec.ECDSA(hashes.SHA256()))
            finally:
                store.close()

    def test_explicit_delete_removes_identity_and_does_not_recreate_it(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        store.close()
        delete_identity(self.identity_path)
        self.assertFalse(self.identity_path.exists())
        with self.assertRaises(IdentityStateError):
            PKCS11KeyStore.open(self.identity_path, module_path=MODULE_PATH)

    def test_copied_state_and_credential_can_clone_the_software_identity(self) -> None:
        store = provision_identity(self.identity_path, module_path=MODULE_PATH)
        original_pem = store.public_key_pem()
        store.close()
        clone_path = Path(self.temporary_directory.name) / "identity-clone"
        shutil.copytree(self.identity_path, clone_path)
        clone = PKCS11KeyStore.open(clone_path, module_path=MODULE_PATH)
        self.addCleanup(clone.close)
        self.assertEqual(original_pem, clone.public_key_pem())
        payload = b"clone-risk-proof"
        public_key = serialization.load_pem_public_key(original_pem.encode("ascii"))
        public_key.verify(clone.sign(payload), payload, ec.ECDSA(hashes.SHA256()))


if __name__ == "__main__":
    unittest.main()
