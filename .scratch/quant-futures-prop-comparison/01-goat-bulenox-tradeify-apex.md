# 期貨量化／自動化交易 prop firm 比較：Goat、Bulenox、Tradeify、Apex

**研究日／資料擷取日：2026-08-10**
**範圍：**僅採各公司第一方網站、第一方 Help Center、合約或條款。TradingPilot 只可用作候選發現，**未作任何事實證據**。金額為美元。
**重要方法：**各方案與階段（評估、模擬 funded、live）分開看待；沒有在可存取第一方資料中明示的欄位一律寫「未明示／未知」，不以一般業界做法補足。頁面上的促銷價、方案與規則可變，採購前應再以結帳頁及書面回覆核對。

## 一頁結論

|公司|自動化量化適配判斷|明示 NQ/MNQ|最關鍵的可行性／風險|
|---|---|---|---|
|**Goat Funded Futures**|**有條件可用；四者中政策最直接**|**有**：官方合約表列 NQ、MNQ|自有 EA／自動策略可用；自己的帳戶可 copy。但禁止外部訊號／他人 copy，並禁止 latency/data-feed/system 漏洞利用與 tick scalping；EOD funded 還有新聞窗、日獲利上限與 2 分鐘持倉獲利不計入等機械策略衝突。|
|**Bulenox**|**有條件可用，但文件較舊且須先書面確認**|**有**：合約清單列 NQ、MNQ|官方寫明以第三方 API／Trader Composite Software 連 Rithmic 可用、每月另收 $100；但可存取資料沒有完整、現行的 bot／copy／跨帳戶 hedge 政策。|
|**Tradeify**|**可用於「自有、非 HFT、僅 Tradeify」策略；限制很強**|未明示／未知|官方合約允許自有 bot，但不得跨 firms、不得 HFT，且可要求證明所有權；他人 copy 禁止、相反部位／同 Product Group hedge 禁止。方案頁的價格資訊與首頁／合約的 billing 敘述出現衝突。|
|**Apex Trader Funding**|**未知；不應以未驗證資料做自動化部署決定**|未知／未驗證|擷取日官方主站、support 與 sitemap 都回傳 Cloudflare **403**；本研究不能以第三方 FAQ 填補。須先取得其合規團隊的書面 bot/API/copy/hedging、NQ/MNQ 及 payout 規則。|

## 評分（0–2；不是安全或投資評等）

**計分規則：**2 = 第一方資料清楚支持、且對自動化可操作；1 = 有部分支持但有重大限制／未覆蓋目標細節；0 = 明示不相容或未由可存取第一方材料證實。總分只供排序，不會把「未知」誤當作通過。

|因素（0–2）|Goat|Bulenox|Tradeify|Apex|
|---|---:|---:|---:|---:|
|NQ 與 MNQ 均明示可交易|2|2|0|0|
|bot/EA/API 自動化政策可驗證|2|1|2|0|
|自己多帳戶 copy 的可行性清楚|2|1|1|0|
|hedging／對沖界線清楚|1|0|2|0|
|費用、階段、時限可審核|2|1|1|0|
|payout/KYC/付款資料可審核|2|2|1|0|
|**合計／12**|**11**|**7**|**7**|**0**|

> 「自己多帳戶 copy」不是跨公司、也不是 copy 他人；所有分數均只反映下文引用的明示資料。Bulenox 的「1」表示平台官方提到整合 copy trading／帳戶關聯，但可存取政策不足以確認擬用 copier 的合規性。

---

## 1. Goat Funded Futures

### 可交易性與自動化

* **NQ/MNQ：已明示。**首頁的 Supported futures／commission table 列出 E-mini Nasdaq-100 **NQ** 與 Micro Nasdaq-100 **MNQ**。[G1]
* **自動化：允許自有策略與 EA。**官方 FAQ 明示「your own automated strategies and EAs」可用，條件是反映自己的交易，且不得利用 latency、data-feed anomalies 或 system vulnerabilities。[G2]
* **copy／訊號：**可在自己 GFF 帳戶、及自己在別處持有的帳戶之間 copy；multi-account bundle 的 copy 是預期用途。copy 他人、訊號服務、交易群組、第三方 mirror/copy service，或追隨外部來源的 EA 都是禁止交易行為，可能終止帳戶；相同來源造成多名無關交易者同時相同持倉也可能連帶停權。[G2]
* **hedging：**在本次可存取官方材料中，未找到明示的 hedge／相反部位規則；**未知，先取得書面確認。**
* **量化衝突：**官方 EOD 規則禁止任一帳戶 tick scalping（秒級開平以捕捉小 tick），該類獲利可被覆核扣除；交易開倉後 2 分鐘內平倉的獲利不計入餘額和 payout。EOD funded 在高影響新聞前後各 2 分鐘不得開、平或觸發掛單，且日獲利達帳戶餘額 5% 自動停止當日交易。[G3] 這對高頻、極短持有、新聞反應模型是實質不相容，不應嘗試規避。

### 方案、費用與時間（可核實的 EOD Challenge；其他方案不外推）

官方另列 Sprint、Flex、Instant Classic，但本段的數字**只屬 EOD Challenge**。[G4]

|EOD 一階段 evaluation|50K|100K|150K|
|---|---:|---:|---:|
|頁面所示價格（並列原價）|$69（from $138）|$132（from $264）|$185（from $370）|
|profit target|$3,000 / 6%|$6,000 / 6%|$9,000 / 6%|
|reset|$59|$120|$170|
|activation|$0|$0|$0|
|time limit／最低交易日|無／無|無／無|無／無|

通過後，官方稱會 review，funded 於 2 個工作日內啟用；每個 challenge 有兩次 reset。[G3] 無 recurring fee 在該頁的明示說明為**未知**；勿將「activation $0」誤寫成所有後續費用均為 $0。

### Payout、KYC 與款項路徑

* **資格／頻率：**EOD funded 每 payout cycle 要 7 個 winning days；winning day = 當日 net P&L 至少起始餘額 0.2%（50K 為 $100）。申請窗為每月 5–8 與 20–23 日。[G4]
* **金額／buffer／split：**最低 $500；無預先 profit buffer，但每次只可取可用 profit 的 50%，其餘 50%留在帳戶。cap 依帳戶及第幾次 payout 升高：50K 第 1/2/3/第4次後為 $1,500/$2,000/$2,500/$2,750；100K 為 $2,000/$2,750/$3,250/$3,750；150K 為 $2,500/$3,250/$3,750/$4,250。預設 split 80/20；可加價 challenge 價格 20% 改為 90/10。[G3][G5]
* **KYC／付款：**EOD、Sprint、Flex 必須在升級 funded 前完成 KYC；需政府照片 ID 與三個月內住址證明。所有 payout 僅經 Rise (Riseworks)；FAQ 稱 crypto「coming soon」，故不可把 crypto 當作已提供的支付路徑。[G6][G7]

### 內部規則衝突／待確認

* 同一 FAQ collection 同時出現 EOD/Flex/Sprint/Instant Classic；payout split、winning days、cap 不可交叉搬用。[G4][G5]
* 目標若是完全自動 NQ/MNQ，先書面確認：使用的平台/API、策略持倉時間分布、新聞時的 order handling、是否有任何對沖邏輯，以及 copier 僅複製本人帳戶。

---

## 2. Bulenox

### 可交易性與自動化

* **NQ/MNQ：已明示。**Qualification Account 的官方合約清單列 E-mini Nasdaq-100 (**NQ**) 與 Micro E-mini Nasdaq-100 (**MNQ**)。[B1]
* **API／algo：部分明示。**官方同頁寫明，經 Rithmic 接到第三方 API 或 Trader Composite Software 時，每月另收 **$100**；它們由獨立公司提供，Bulenox 不控制可用性／性能或損失。[B1] 這證實有該連線收費規則，但**不是對每一種 bot、order rate 或 strategy 的全面批准**。
* **copy：資料不足。**Connection 頁描述 ProjectX 的 integrated copy trading，並警示 copy 可能因市場與 copier logic 不同步；但未在可存取材料中給出本人／他人 copy、跨帳戶或 payout 合規的完整政策。[B2] 因此不能據此推定擬用量化 copier 合規。
* **hedging：未明示／未知。**

### 階段、費用、時間

* 官方描述流程為 **Qualification Account → Master Account → Funded Account**；完成 Master 的三次 successful payouts 後，仍「subject to Risk Management Department sole discretion」才 transition 至 real-capital Funded Account。[B3]
* Qualification Account：無最低或最高 trading days；訂閱每 30 天自動續費，取消前持續。reset 不改訂閱到期日。官方頁列 reset：10K $38、25K $48、50K $58、100K $78。[B1]
* Master Account：官方稱無 reset 選項；開啟前須完成 application 和簽 contract。[B4]
* **費用不足處：**可存取頁雖列 Master activation $143（25K）、$148（50K）、$248（100K）、$498（150K）、$898（250K），但頁面自標 `Updated May/2/2022`；Qualification activation table 則自標 `Updated August/14/2023`，且含 10K $98。[B1][B4] 本研究不把它們當作 2026 現行報價。Qualification 的每月方案價格／所有現行 activation fee = **未由擷取日可存取 checkout 明確驗證**。

### Payout、KYC／支付

* Master payout：可在月內任時 request、每週三處理；至少 10 個 individual trading days，最低 $1,000。頭三次上限依 25K/50K/100K/150K/250K 為 $1,000/$1,500/$1,750/$2,000/$2,500；第三次後無上限。[B4]
* 留存 safety threshold：依上述五種大小為 $1,600/$2,600/$3,100/$4,600/$5,600；首次提款時計入 40% consistency。最高單日盈利不得超過 total profit 40%，否則 payout 不處理但帳戶不違規。[B4]
* split：前 $10,000 無公司 commission，之後公司 10%、trader 90%。付款路徑為 ACH/Wire、PayPal、Wise；美國人收 1099、其他國籍 W-8BEN（這是稅表說明，不等同一般 KYC 清單）。[B4]
* **KYC／身分驗證要求：未明示／未知。**

### 官方資料衝突／警訊

* 同一官方 Help Center 的 Master active-account 上限不一致：Qualification 頁寫最多 **11** active Master，[B1] Master 頁寫最多 **5**。[B4] 不能自行取其中一值；採購前必須書面確認。
* 多項資料頁標示 2021–2023 更新，與頁尾 2026 版權並不代表規則於 2026 已更新。量化使用者至少要確認 Rithmic API 方案、每月 $100 是否仍適用、bot/copy/hedge、NQ/MNQ session 與風控限制。

---

## 3. Tradeify

### 可交易性與自動化

* **NQ/MNQ：未明示／未知。**本次可存取 Tradeify 官方首頁僅泛稱可交易 indices，沒有在所引用的官方合約／產品頁列 NQ 或 MNQ；依本報告門檻，不能從「futures」或「Nasdaq」行銷推論特定 symbol 可用。[T1][T2]
* **bot／algo：可用但條件嚴格。**Funded Trader Agreement §6.6：只能用 trader 能證明為其 sole owner 的 bot／strategy，且他人不得存取／使用；可在本人帳戶使用，但跨多家公司使用違反 Tradeify policy；禁止 HFT bot；公司可在風控標記時要求資料或文件。[T2]
* **copy：**他人間 copy/mirror 嚴格禁止；本人 Trader-Controlled Accounts 間只在 Help Center rules「expressly allowed」的範圍內才可，並且不得造成下述 Opposing Positions。[T2] 由於該 Help Center 於擷取時不允許存取，具體條件為**未知**。
* **hedging：明示禁止。**所有 Trader-Controlled Accounts（含 evaluation、Simulated Funded、Elite Live）在同一 futures product，或同一 Company 指定 Product Group，不得持有 Opposing Positions；無論意圖皆屬 breach。[T2] 任何跨帳戶套利、beta-neutral 或對沖引擎均應視為不可用，除非公司書面明確批准。

### 方案、費用、時間

* 官方首頁列三種：**Growth**（「Pass in 1 day」）、**Select**（「Pass in 3 days」、daily payouts）、**Lightning**（instant funding、skip evaluations）；平台選項列 Tradovate、NinjaTrader、Wealthcharts、TradeSea。[T1]
* Select 產品頁稱 evaluation 6% target、40% consistency、最快 3 trading days；通過後選 Daily 或 5-Day payout path，funded 為 0% consistency。[T3]
* **費用資訊存在第一方衝突：**首頁稱「No activation fee」「price you see is what you pay」；[T1] 但 Select 產品頁的文字化比較表在 funded 欄位顯示 Daily activation $1,500、Flex $4,000，且頁面還混有 `Lorem ipsum` placeholder；[T3] 另 Agreement §7.5 稱目前所有帳戶／服務為 one-time purchases、沒有 recurring billing，並把當前帳戶型別及費用指向 Help Center。[T2] 因此：現行 upfront、reset、activation 及方案尺寸價格都**不可由此報告定稿**，需 checkout/Help Center 書面確認；不得選擇性採信某一頁。
* evaluation／funded 的 time limit 在上述可存取來源**未明示／未知**。

### Payout、KYC／付款

* 首頁稱 Select daily payouts、其他方案 5-day payouts、profit split 90%；[T1] Select 頁稱 daily 最快約 60 分鐘。[T3] 這些是行銷／一般方案敘述，不足以核實每個 account size 的 eligibility、cap、buffer 或實際處理 SLA。
* Select 頁文字化表列 150K Daily cap $1,250、Flex cap「None」，但同頁出現 placeholder，且未能取得 Help Center rule，故不可泛化到其他大小或視為最終現行條款。[T3]
* Agreement 提及公司可能要求 KYC/KYB，個人／企業 payout 情況有 KYB；但可存取資料未列完整 KYC 文件、支付路徑或所有 payout eligibility。均標 **未知／需確認**。[T2]

---

## 4. Apex Trader Funding

* **可存取性結論：**在擷取日，`https://apextraderfunding.com/`、`https://support.apextraderfunding.com/hc/en-us` 與 `https://apextraderfunding.com/sitemap_index.xml` 回傳 Cloudflare 403。這是官方網域的存取結果，**不是**「沒有規則」的證據。
* 因沒有可引用、可存取的第一方內容，本報告不能驗證：NQ/MNQ、平台或 API、EA/bot、copy 或 hedge、方案／stage、upfront／recurring／reset／activation fee、time limit、payout eligibility/frequency/cap/buffer/split、KYC 或支付路徑。所有欄位均為 **未知／未驗證**。
* **採購／部署前必備書面問題：**請 Apex compliance/support 明確列出：(1) NQ、MNQ 是否可在目標方案與目標平台交易；(2) API/bot 的允許方式、order-rate/HFT／latency 限制；(3) own-account copier、他人 signal／copy、跨帳戶與相反部位／hedge；(4) 每一 stage 的費用、續費、reset、activation、expiry；(5) payout days/caps/buffer/split/KYC/payment rails。未獲答覆前評分保持 0，不以第三方網站補值。

---

## 部署前共同合規清單

1. 將策略描述（訊號來源是自有、程式所有權、帳戶清單、交易頻率、最短持有期、新聞時 order 行為、是否 ever produce opposing positions）交給每家公司 compliance，取得**書面**核可。
2. 逐一在結帳頁截圖保存 account size、平台、促銷碼、一次性／月費、data/API surcharge、reset、activation；不可用不同方案的價格互補。
3. 先以最小方案、單一帳戶、低速非 HFT 模式測試；不要以多人共享策略、外部 signals 或 hedge 繞過條款。
4. 在申請 payout 前再核對該 stage 的 winning-day、consistency、最高單日、cap、buffer、KYC 與收款國家可用性；特別注意 Bulenox 文件版本老舊、Tradeify 產品頁／合約衝突、Goat 的短持倉與新聞限制、Apex 未驗證。

---

## 第一方來源（均於 2026-08-10 擷取）

### Goat Funded Futures

* [G1] Goat Funded Futures，首頁（Supported futures／NQ、MNQ contract table）：<https://goatfundedfutures.com/>。
* [G2] Goat Help Center，*Signals, copy trading and automated strategies*：<https://help.goatfundedfutures.com/en/articles/15945150-signals-copy-trading-and-automated-strategies>。
* [G3] Goat Help Center，*EOD Challenge: specifications, rules and payouts*：<https://help.goatfundedfutures.com/en/articles/14095302-eod-challenge-specifications-rules-and-payouts>。
* [G4] Goat Help Center，*What are the payout requirements?*：<https://help.goatfundedfutures.com/en/articles/14095663-what-are-the-payout-requirements>。
* [G5] Goat Help Center，*What is the profit split?*；*Are there payout caps?*：<https://help.goatfundedfutures.com/en/articles/14095688-what-is-the-profit-split>；<https://help.goatfundedfutures.com/en/articles/14095690-are-there-payout-caps>。
* [G6] Goat Help Center，*What payout methods are available?*：<https://help.goatfundedfutures.com/en/articles/14095695-what-payout-methods-are-available>。
* [G7] Goat Help Center，*Do I need KYC verification for payouts?*：<https://help.goatfundedfutures.com/en/articles/14095703-do-i-need-kyc-verification-for-payouts>。

### Bulenox

* [B1] Bulenox Help Center，*Qualification Account*（instrument list、API fee、reset、訂閱／multiple-account 資料）：<https://bulenox.com/index.php/help/qualification-account/>。
* [B2] Bulenox Help Center，*Connection*（ProjectX integrated copy trading）：<https://bulenox.com/index.php/help/connection/>。
* [B3] Bulenox Help Center，*Funded Account*：<https://bulenox.com/index.php/help/funded-account/>。
* [B4] Bulenox Help Center，*Master Account*（activation、payout、split、threshold、consistency、支付）：<https://bulenox.com/index.php/help/master-account/>。

### Tradeify

* [T1] Tradeify，首頁（方案名稱、平台、一般 payout／split／activation 主張）：<https://tradeify.co/>。
* [T2] Tradeify，*Funded Trader Agreement*（§6.6 bot、§6.7 copy/hedging、§7 fees/billing、KYC/KYB）：<https://tradeify.co/funded-trader-agreement>。
* [T3] Tradeify，*SELECT Plan*（evaluation/payout path 及頁面內費用表）：<https://tradeify.co/select-plan>。

### Apex Trader Funding

* [A1] Apex 官方主站（擷取時 Cloudflare 403）：<https://apextraderfunding.com/>。
* [A2] Apex 官方 Support（擷取時 Cloudflare 403）：<https://support.apextraderfunding.com/hc/en-us>。
* [A3] Apex 官方 sitemap（擷取時 Cloudflare 403）：<https://apextraderfunding.com/sitemap_index.xml>。
