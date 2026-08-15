ui = false
api_addr = "http://openbao:8200"
cluster_addr = "http://openbao:8201"
plugin_directory = "/var/lib/openbao/plugins"
plugin_auto_download = true
plugin_auto_register = true
plugin_download_behavior = "fail"

storage "raft" {
  path = "/var/lib/openbao"
  node_id = "abt-controller"
}

listener "tcp" {
  address = "0.0.0.0:8200"
  cluster_address = "0.0.0.0:8201"
  tls_disable = true
}

plugin "kms" "pkcs11" {
  image = "ghcr.io/openbao/openbao-plugin-kms-pkcs11"
  version = "v0.1.0"
  binary_name = "openbao-plugin-kms-pkcs11"
  sha256sum = "55245882727535579e710672f0eae1bcdddc846006db857baaa6e09e33d40faf"
}

seal "pkcs11" {
  lib = "/usr/lib/softhsm/libsofthsm2.so"
  token_label = "abt-openbao"
  key_label = "abt-root"
}
