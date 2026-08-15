# 本地/自建 Secrets 管理方案評估

> 研究目的：為主控台 Web 服務的裝置憑證簽發、指令簽章金鑰與 Windows MT5 工作者的本機秘密，選擇可自建且可整合的方案。
> 撰寫時間：2026-08-15

## 需求

- 主控台需保存高價值私鑰並簽發、撤銷工作者裝置憑證。
- Python Web service 需有受支援的執行時 API。
- Windows 工作者只應保存本機裝置私鑰與 MT5 憑證。
- 1 台主控台與少量跨網路工作者，不依賴雲端 KMS。
- 必須能備份、審計與撤銷，且授權適合自建。

## 候選比較

| 方案 | 適合的角色 | 優點 | 主要代價 |
|---|---|---|---|
| **OpenBao** | 集中式主要 secrets/PKI 服務 | MPL-2.0、KV、PKI、AppRole、政策與審計；Raft 可單節點運作；Vault HTTP API 相容 | 預設 Shamir seal，重啟後需安全地 unseal |
| HashiCorp Vault CE | OpenBao 的替代 | 成熟的 KV、PKI、AppRole 與 Raft | BUSL-1.1 授權，不如 OpenBao 適合新自建採用 |
| Infisical 自建版 | 偏好 Web UI 的集中式服務 | 機器身分、PKI、官方 Python SDK、無 unseal 流程 | 需維護 PostgreSQL 與 Redis，營運面較大 |
| SOPS + age | 離線/部署時靜態 secrets | 檔案加密、易於備份與 Git 管理 | 無執行時 API、動態 PKI、RBAC 或審計；不能獨立作為主服務 |
| Windows Credential Manager / CNG | Windows 工作者最後一哩 | 使用者/機器保護的本機秘密與不可匯出私鑰 | 無集中管理、跨節點撤銷或動態簽發能力 |

## 建議：OpenBao 為主，SOPS + age 與 Windows 原生儲存為輔

OpenBao 最適合目前架構：

1. **裝置憑證**：以 PKI secrets engine 簽發具 worker/account 綁定與 TTL 的 X.509 憑證；撤銷時更新 CRL/撤銷資料。
2. **主控台服務身分**：主控台以最小權限 AppRole 或等效受限 token 讀取指令簽章金鑰與呼叫 PKI；不得使用 root token。
3. **資料面**：使用 Raft integrated storage，單一主控台不需額外資料庫服務；定期建立加密 snapshot。
4. **工作者本機**：裝置私鑰保存在 Windows Certificate Store/CNG，MT5 密碼維持 Windows Credential Manager；不將主控台 CA 私鑰下放給工作者。
5. **離線復原**：SOPS + age 加密保存 unseal shares、bootstrap 資料與 Raft snapshot；root token 在初始化後立即撤銷或封存。

OpenBao 的關鍵營運決策是 unseal：若接受主機重啟後由受信任操作者人工 unseal，可保持最小部署；若要求無人值守重啟，必須另評估受管 KMS 或 PKCS#11/HSM 型 auto-unseal，不能把 unseal key 放入同一台主控台。

## 次選：Infisical

若管理者需要完整的 secrets Web UI、希望避免 unseal，且可接受 PostgreSQL 與 Redis 的維運，Infisical 自建版是合理次選。它有機器身分、憑證管理與官方 Python SDK；對本專案而言，額外基礎設施是其相對 OpenBao 的主要取捨。

## 不採用作為主方案

- **Vault CE**：功能適合但新部署優先選 MPL-2.0 的 OpenBao。
- **SOPS + age 單獨使用**：無法執行時簽發/撤銷憑證或管理機器授權。
- **OS 原生儲存單獨使用**：只解決每台主機上的秘密保存，未解決控制平面信任鏈。

## 一手來源

- [OpenBao — What is OpenBao](https://openbao.org/docs/what-is-openbao/)
- [OpenBao — Security model](https://openbao.org/docs/internals/security/)
- [OpenBao — AppRole authentication](https://openbao.org/docs/auth/approle/)
- [OpenBao — PKI secrets engine](https://openbao.org/docs/secrets/pki/)
- [OpenBao — seal configuration](https://openbao.org/docs/configuration/seal/)
- [OpenBao — Raft integrated storage](https://openbao.org/docs/configuration/storage/raft/)
- [OpenBao source repository](https://github.com/openbao/openbao)
- [Infisical — self-hosting](https://infisical.com/docs/self-hosting/overview)
- [Infisical — machine identities](https://infisical.com/docs/documentation/platform/identities/machine-identities)
- [Infisical — PKI](https://infisical.com/docs/documentation/platform/pki/overview)
- [Infisical — SDKs](https://infisical.com/docs/sdks/overview)
- [Infisical source repository](https://github.com/Infisical/infisical)
- [SOPS](https://getsops.io/)
- [age specification](https://age-encryption.org/v1)
- [HashiCorp Vault licensing/product information](https://www.hashicorp.com/products/vault)
