# Linux Worker：TPM 角色的開源替代方案

日期：2026-09-03

## 結論

如果 ABT 必須保留「ECDSA P-256 私鑰不可匯出、綁定實體裝置、可簽任意 Worker challenge」這三個條件，**純軟體開源方案不能真正替代 TPM 的安全角色**；SoftHSM 與 kernel keyring 都仍是軟體／作業系統管理的儲存邊界。[2][5] 最合理的替代不是 SoftHSM，而是新增一個 `PKCS11KeyStore` adapter，搭配 USB Hardware Security Module。

建議接受順序：

1. **Nitrokey HSM 2 / SmartCard-HSM + OpenSC PKCS#11**：最符合「開源 Linux stack + 實體不可匯出金鑰」。OpenSC 官方資料將 SmartCard-HSM/Nitrokey HSM 描述為硬體安全模組，提供 RSA、ECC、AES secure key store，並提供 PKCS#11 module；但初始化與一般操作涉及 SO-PIN / user PIN。[1]
2. **YubiHSM 2 + PKCS#11**：功能與無人值守操作通常更接近伺服器 HSM；Yubico 提供正式 PKCS#11 component。不過硬體本身不是完整 open hardware，且會引入 HSM authentication key/credential。[4]
3. **OpenBao Transit remote signer**：完全開源的集中式 signing service。Transit 可簽章與驗章，應用程式不取得服務端私鑰。[3] 但它把信任從「本機實體裝置」改成「遠端服務 + access token + 網路」，不符合目前 same-host hardware-bound identity 的原始 threat model。
4. **SoftHSM2**：只適合 CI、介面測試與開發。它是 PKCS#11 軟體實作，底層仍是主機上的軟體儲存，因此不能證明私鑰對 root/磁碟擷取不可匯出。[2]
5. **Linux kernel keyring**：只是在 kernel 中快取 cryptographic keys、tokens 等資料，受 process/key permissions 管理。[5] 它不是獨立安全硬體，不能提供 TPM/HSM 等級的不可匯出保證。

## 如果不要求實體不可匯出

最直接方案是 **SoftHSM2 + PKCS#11 adapter**。SoftHSM2 本身就是軟體 PKCS#11 token，可讓 ABT 使用與實體 HSM 相同的 slot/token/key object model。[2]

建議設定：

- 用 `python-pkcs11` 實作 `PKCS11KeyStore`，只依賴 PKCS#11 contract。
- 在 SoftHSM 內產生 P-256 key，設 `CKA_SIGN=true`、`CKA_SENSITIVE=true`、`CKA_EXTRACTABLE=false`。
- 將 token directory 權限設為 `0700`、檔案 `0600`；config 只保存 module path、token label、key ID，不保存 PIN。
- 啟動 PIN 可由互動輸入、Linux Secret Service，或專用 credential file 提供；若採 unattended credential file，必須承認同機 root 能同時取得 token database 與 PIN。
- **Stock SoftHSM2 不能把 PIN 設成空值**：目前原始碼的預設 `MIN_PIN_LEN` 是 4，而且 `C_InitToken`、`C_InitPIN` 都會拒絕 NULL 或不足長度的 PIN。要維持 Windows 一樣的零提示 UX，應使用隨機內部 PIN並由 Worker 自動登入，而不是宣稱 token 沒有 PIN。
- controller 仍接收相同 SubjectPublicKeyInfo PEM 與 DER ECDSA-SHA256 signature，不必更改 challenge protocol。
- 把 provider metadata 誠實標成 `SOFTHSM` 或 `PKCS11-SOFTWARE`，不可標成 hardware-backed。

這條路保留 PKCS#11 的可替換性：開發與低風險部署用 SoftHSM；未來插入 Nitrokey/YubiHSM 時只換 module/token config，不改 Worker domain contract。[1][2][4]

代價是 SoftHSM token directory 可以備份或複製；`CKA_EXTRACTABLE=false` 只限制正常 PKCS#11 API，不構成對主機管理員的硬體防線。[2]

若已經接受檔案型私鑰，而且未來也不打算接硬體 HSM，直接用 `cryptography` + encrypted PKCS#8 會比 PKCS#11 簡單。只有在需要統一 provider API、slot/key lifecycle 或保留未來換硬體的能力時，SoftHSM + PKCS#11 才值得。

## 選項比較

| 選項 | 實體不可匯出 | ECDSA P-256 任意 payload | 開源介面/stack | 無網路 | 主要代價 |
|---|---:|---:|---:|---:|---|
| Nitrokey HSM 2 + OpenSC | 是 | 是，需實機確認 mechanism | 是 | 是 | PIN/session 與 USB lifecycle |
| YubiHSM 2 + PKCS#11 | 是 | 是 | SDK/connector 是，硬體否 | 是 | 成本、auth key 管理 |
| OpenBao Transit | 由遠端服務保管 | 是，需選定 ECDSA key type | 是 | 否 | token bootstrap、HA、網路依賴 |
| SoftHSM2 | 否 | 是 | 是 | 是 | 只能測 contract，不能當 production root |
| Linux keyring / 加密檔案 | 否 | 是 | 是 | 是 | privileged attacker 可取得 key material |

## 對 ABT 的建議架構

保留現有 `HardwareKeyStore` contract：

```python
class HardwareKeyStore(Protocol):
    def public_key_pem(self) -> str: ...
    def sign(self, payload: bytes) -> bytes: ...
```

新增三個明確 provider：

- `tpm2`：原規格，`tpm2-pytss` ESAPI，優先。
- `pkcs11`：Nitrokey HSM 2 或 YubiHSM 2；production 可接受的硬體替代。
- `softhsm`：只允許 test/dev build，production startup 必須 fail closed。

不要把 Linux kernel keyring、LUKS 內的 PEM、SoftHSM 或本機 OpenBao 單節點檔案儲存標成 `hardware-backed`。

## Contract 影響

PKCS#11 HSM 可以維持目前 controller 的 P-256 public PEM + ASN.1 DER ECDSA signature contract，因此 controller 不必新增第二套 challenge protocol。需要改的是本機 adapter/config：

- PKCS#11 module path、slot/token label、key label/ID。
- PIN/auth credential 的 memory-only 載入方式。
- USB 拔除、session expiry、token replacement、key rotation 的 failure semantics。
- Enrollment metadata 從硬編碼 `CNG` 改為明確 provider，例如 `TPM2` 或 `PKCS11-HSM`；這仍不是 remote attestation。

若完全不能接受 PIN/auth credential，又要求無人值守、實體不可匯出，則應優先找具備 machine authentication/session 機制的 server HSM；一般 smart-card token 並不是 TPM 的零操作摩擦替代品。

## Sources

[1] https://github.com/OpenSC/OpenSC/wiki/SmartCardHSM
[2] https://github.com/softhsm/SoftHSMv2
[3] https://openbao.org/docs/secrets/transit
[4] https://developers.yubico.com/YubiHSM2/Component_Reference/PKCS_11
[5] https://www.kernel.org/doc/html/latest/security/keys/core.html
