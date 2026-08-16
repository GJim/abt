from __future__ import annotations

import unittest
from pathlib import Path

import yaml


class ComposeDeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        compose_path = Path(__file__).parents[1] / "deploy" / "docker-compose.yml"
        self.compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    def test_only_cloudflared_can_reach_the_controller(self) -> None:
        services = self.compose["services"]
        self.assertEqual({"controller", "openbao", "softhsm", "cloudflared"}, set(services))
        self.assertTrue(all("ports" not in service for service in services.values()))
        self.assertEqual(["controller"], services["cloudflared"]["depends_on"])
        self.assertEqual("cloudflare/cloudflared:latest", services["cloudflared"]["image"])
        self.assertEqual({"ingress"}, set(services["cloudflared"]["networks"]))
        self.assertEqual({"ingress", "secrets"}, set(services["controller"]["networks"]))
        self.assertEqual("172.30.0.3", services["controller"]["networks"]["ingress"]["ipv4_address"])
        self.assertEqual({"secrets", "hsm", "plugin-registry"}, set(services["openbao"]["networks"]))
        self.assertEqual({"hsm"}, set(services["softhsm"]["networks"]))
        self.assertEqual({"openbao"}, {name for name, service in services.items() if "plugin-registry" in service["networks"]})
        self.assertEqual("172.30.0.2", services["cloudflared"]["networks"]["ingress"]["ipv4_address"])
        self.assertNotIn("ABT_TRUSTED_PROXY_IPS", services["controller"]["environment"])
        self.assertEqual("http://openbao:8200/v1/abt/data/health", services["controller"]["environment"]["ABT_OPENBAO_HEALTH_URL"])
        self.assertEqual(
            "${ABT_OPENBAO_HEALTH_TOKEN:?Set ABT_OPENBAO_HEALTH_TOKEN}",
            services["controller"]["environment"]["ABT_OPENBAO_HEALTH_TOKEN"],
        )
        self.assertEqual("172.30.0.0/24", self.compose["networks"]["ingress"]["ipam"]["config"][0]["subnet"])
        self.assertEqual(
            ["CMD", "python", "-c", "from urllib.request import urlopen; urlopen('http://localhost:8000/health', timeout=2)"],
            services["controller"]["healthcheck"]["test"],
        )
        self.assertNotIn("ABT_OPENBAO_HEALTH_URL", services["cloudflared"].get("environment", {}))
        self.assertEqual(
            "${ABT_CLOUDFLARE_TUNNEL_TOKEN:?Set ABT_CLOUDFLARE_TUNNEL_TOKEN}",
            services["cloudflared"]["environment"]["TUNNEL_TOKEN"],
        )

    def test_sensitive_state_uses_distinct_persistent_volumes(self) -> None:
        volumes = self.compose["volumes"]
        self.assertEqual(
            {"controller_ledger", "openbao_raft", "softhsm_tokens", "controller_backups"},
            set(volumes),
        )
        services = self.compose["services"]
        self.assertIn("controller_ledger:/var/lib/abt", services["controller"]["volumes"])
        self.assertIn("openbao_raft:/var/lib/openbao", services["openbao"]["volumes"])
        self.assertIn("softhsm_tokens:/var/lib/softhsm/tokens", services["openbao"]["volumes"])
        self.assertNotIn("volumes", services["cloudflared"])
        self.assertIn("controller_backups:/var/backups/abt", services["controller"]["volumes"])
        self.assertIn("openbao_raft:/backup-source/openbao-raft:ro", services["controller"]["volumes"])
        self.assertIn("softhsm_tokens:/backup-source/softhsm-tokens:ro", services["controller"]["volumes"])
        deployment_guide = (Path(__file__).parents[1] / "deploy" / "README.md").read_text(encoding="utf-8")
        self.assertIn("controller_ledger", deployment_guide)
        self.assertIn("openbao_raft", deployment_guide)
        self.assertIn("softhsm_tokens", deployment_guide)
        self.assertIn("ABT_CLOUDFLARE_TUNNEL_TOKEN", deployment_guide)

    def test_openbao_auto_unseal_plugin_is_version_and_digest_pinned(self) -> None:
        self.assertEqual(
            {"context": "..", "dockerfile": "deploy/openbao.Dockerfile"},
            self.compose["services"]["openbao"]["build"],
        )
        dockerfile = (Path(__file__).parents[1] / "deploy" / "openbao.Dockerfile").read_text(encoding="utf-8")
        self.assertIn("FROM openbao/openbao:2.6.1", dockerfile)
        self.assertIn("apk add --no-cache softhsm", dockerfile)
        entrypoint = (Path(__file__).parents[1] / "deploy" / "openbao-entrypoint.sh").read_text(encoding="utf-8")
        self.assertIn("chown -R openbao:openbao /var/lib/openbao /var/lib/softhsm/tokens", entrypoint)
        self.assertIn("exec su-exec openbao docker-entrypoint.sh", entrypoint)
        config_path = Path(__file__).parents[1] / "deploy" / "openbao.hcl"
        config = config_path.read_text(encoding="utf-8")
        self.assertIn('plugin_directory = "/var/lib/openbao/plugins"', config)
        self.assertIn('image = "ghcr.io/openbao/openbao-plugin-kms-pkcs11"', config)
        self.assertIn('version = "v0.1.0"', config)
        self.assertIn('sha256sum = "55245882727535579e710672f0eae1bcdddc846006db857baaa6e09e33d40faf"', config)
        self.assertIn('seal "pkcs11"', config)

    def test_controller_uses_exactly_one_asgi_worker(self) -> None:
        dockerfile = (Path(__file__).parents[1] / "deploy" / "controller.Dockerfile").read_text(encoding="utf-8")
        self.assertIn('ENV PATH="/opt/abt/.venv/bin:${PATH}"', dockerfile)
        self.assertIn('"--workers", "1"', dockerfile)

    def test_bootstrap_requires_and_starts_managed_tunnel(self) -> None:
        bootstrap = (Path(__file__).parents[1] / "deploy" / "bootstrap-openbao.sh").read_text(encoding="utf-8")
        self.assertIn("ABT_CLOUDFLARE_TUNNEL_TOKEN", bootstrap)
        self.assertIn("transit/keys/abt-device-certificates type=ecdsa-p256", bootstrap)
        self.assertIn('"${compose[@]}" up -d --build controller cloudflared', bootstrap)
        self.assertIn('"${compose[@]}" ps --status running --services | grep -Fxq cloudflared', bootstrap)
