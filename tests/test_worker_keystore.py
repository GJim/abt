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


class PlatformKeyStoreProviderSelectionTests(unittest.TestCase):
    def test_default_provider_returns_cng_on_windows(self) -> None:
        from pathlib import Path
        from abt.worker.keystore import WindowsCNGKeyStoreProvider, default_key_store_provider

        provider = default_key_store_provider(Path("/tmp/worker.json"), platform="win32")
        self.assertIsInstance(provider, WindowsCNGKeyStoreProvider)

    def test_default_provider_fails_closed_on_unsupported_platform(self) -> None:
        from pathlib import Path
        from abt.worker.keystore import UnsupportedKeyStore, default_key_store_provider

        with self.assertRaisesRegex(UnsupportedKeyStore, "unsupported on darwin"):
            default_key_store_provider(Path("/tmp/worker.json"), platform="darwin")

    def test_default_provider_fails_closed_when_pkcs11_module_missing(self) -> None:
        from pathlib import Path
        from abt.worker.keystore import UnsupportedKeyStore, default_key_store_provider

        with self.assertRaisesRegex(UnsupportedKeyStore, "SoftHSM2 PKCS#11 module is unavailable"):
            default_key_store_provider(
                Path("/tmp/worker.json"),
                platform="linux",
                module_path=Path("/nonexistent/libsofthsm2.so"),
            )

    def test_default_provider_returns_linux_pkcs11_on_linux_when_module_available(self) -> None:
        from pathlib import Path
        from abt.worker.keystore import default_key_store_provider
        from abt.worker.pkcs11_keystore import LinuxPKCS11KeyStoreFactory
        from tests.test_worker_pkcs11_keystore import MODULE_PATH

        if not MODULE_PATH.is_file():
            self.skipTest("SoftHSM2 module not available")

        provider = default_key_store_provider(
            Path("/tmp/worker.json"),
            platform="linux",
            module_path=MODULE_PATH,
        )
        self.assertIsInstance(provider, LinuxPKCS11KeyStoreFactory)

