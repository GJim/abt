from __future__ import annotations

import sys
import unittest
from uuid import uuid4

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from abt.worker.keystore import WindowsCNGKeyStore


@unittest.skipUnless(sys.platform == "win32", "requires Windows CNG")
class WindowsCNGKeyStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.name = f"abt-test-cng-{uuid4()}"
        self.store = WindowsCNGKeyStore(self.name)

    def tearDown(self) -> None:
        try:
            self.store.delete()
        finally:
            self.store.close()

    def test_exports_a_p256_public_key_and_signs_for_cryptography(self) -> None:
        public_key = serialization.load_pem_public_key(self.store.public_key_pem().encode("ascii"))

        self.assertIsInstance(public_key, ec.EllipticCurvePublicKey)
        self.assertIsInstance(public_key.curve, ec.SECP256R1)

        payload = b"CNG ECDSA P-256 proof"
        public_key.verify(self.store.sign(payload), payload, ec.ECDSA(hashes.SHA256()))

    def test_reopening_the_name_preserves_the_public_identity(self) -> None:
        original_pem = self.store.public_key_pem()
        self.store.close()
        self.store = WindowsCNGKeyStore(self.name)

        self.assertEqual(original_pem, self.store.public_key_pem())
