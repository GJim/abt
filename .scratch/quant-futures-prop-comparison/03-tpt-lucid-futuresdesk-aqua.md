# 期貨量化／自動化交易適配研究：Take Profit Trader、Lucid Trading、The Futures Desk、Aqua Futures

> **範圍與方法**：僅採用各公司自有網站、其官方知識庫或官方載入的知識庫資料。TradingPilot 僅為候選發現來源，**未作任何事實證據**。以下「未載明／未知」不是不允許，而是截至本報告指定存取日，在可存取的第一方資料中無法證實。產品、費率與規則可能變動；付款前應以結帳頁、帳戶協議與書面合規答覆覆核。
>
> **指定存取日：2026-08-10。** URL 後的所有資料均標此日；個別官方頁面如自身標示更新日／生效日，另在文中標出。Aqua Futures 網域於核查時已顯示停放頁，見其段落。
>
> **評分法（每項 0–2）**：2＝官方資料明確、且對「可程式化的 NQ/MNQ 量化期貨交易」正向；1＝可交易或資訊部分成立但有重大限制／需另確認；0＝官方明確禁止或與目標直接衝突。無可驗證第一方根據一律寫「未知」，**不給分**。分數不是安全性、付款可靠性、監管狀態或推薦。

## 摘要比較

|公司|NQ、MNQ 明示|自動化／EA／API 政策|量化自動化適配分|主要決定性限制|
|---|---|---|---:|---|
|Take Profit Trader（TPT）|明示 NQ、MNQ（2）|所有 bots／algos／automated execution 不允許（0）|**0/2**|即使自有帳戶可用核准 copier，同一公司通用政策仍禁止 bot／algo；不可把 copier 例外推論為 EA/API 例外。|
|Lucid Trading|明示 NQ、MNQ（2）|官方明示 automated systems、trade copiers permitted（2）|**2/2**|禁止 HFT、對沖；軟體錯誤由交易者負責。未找到官方 API 規格。|
|The Futures Desk（TFD）|明示 NQ、MNQ（2）|官方明示 bots／algorithmic trading、automated strategies 可用（2）|**2/2**|必須由本人單獨控制，且 CME/CFTC/法律合規；live news 時段有 5 micros 與 stop 規則。|
|Aqua Futures|未知|未知|**未知**|官方 `aquafutures.com` 已是 Searchvity 停放 frameset，無可驗證產品／規則／費率。|

---

## 1. Take Profit Trader（TPT）

### 可證實的產品、標的與階段

* **NQ/MNQ**：官方「Approved Instruments」逐項列出 `MNQ Micro E-mini Nasdaq-100 Index Futures` 與 `NQ E-mini Nasdaq-100 Futures`，故標的可用性 **2/2**。[TPT-1]
* **階段／名稱**：通用政策明確適用於 **Test、PRO、PRO+**；官網亦描述為 one-step Test 後成為 PRO，並可升級 PRO+。[TPT-2][TPT-3] 這些階段不可混為同一規則集。
* **時間**：Test 為月訂閱，官方說「no time limit」達標；通過後訂閱自動取消。[TPT-4]

### 自動化、copier 與反向／協同交易（核心相容性）

* **自動化分數 0/2（直接不適配）**：UTP 說「Automated trading systems, bots, or algorithmic execution tools are not permitted」且適用 Test/PRO/PRO+；PRO 規則亦稱所有交易均須由交易者手動執行。[TPT-2][TPT-5]
* **Copier 不是 algo 例外**：官方 Copier Policy 只允許**本人擁有並控制**帳戶間的核准 copier（包括平台原生工具；另列特定工具），禁止不同使用者／身份間複製或同步；不得用來建立反向、抵銷、風險中性部位或 pass/payout service。[TPT-6]
* **反向／對沖限制**：不得在同一或密切相關商品持反向部位；官方舉 NQ↔MNQ 為密切相關對，違規可自動平倉。[TPT-7] 也禁止不同使用者的即時鏡像、集中控制、協同風險分散；僅本人控制帳戶可同步，但仍不得用於風險中性／抵銷。[TPT-8]
* **官方規則衝突／解讀**：Independent Execution Policy 說可使用「assist with order execution or trade management」的工具（含 copier），但 UTP 對「automated trading systems/bots/algorithmic execution」採明確禁止。對量化 EA／API 應以較直接且較嚴格的禁止為準，不能由 copier 政策推論自動策略被允許。[TPT-2][TPT-6][TPT-8]

### 成本、重設與交易期

* **前期／經常費**：Test 是按月訂閱；首頁目前公開 50K 例示為 **$170/month**，但此單一公開卡片不能代表所有尺寸或促銷後價格。[TPT-3][TPT-4] 因官方可驗證資料未在同一靜態規則頁完整列出全尺寸當期費率，完整費率矩陣：**未知／結帳時確認**。
* **PRO activation**：首頁稱 PRO 是一次性 **$130 fee**；促銷頁／代碼可能改變，應以購買時條款核實。[TPT-3]
* **Test reset**：25K/50K/75K/100K/150K 分別 $79/$99/$139/$169/$199；reset 不改訂閱續費日。[TPT-9]
* **PRO reset**：每個 PRO 最多三次；25K/50K/75K/100K/150K 分別 $449/$649/$799/$999/$1,499。[TPT-10]

### 出金、KYC、支付

* **PRO eligibility／buffer／split**：PRO 可 day one 提款，但須先建立等於最大 drawdown 的 buffer；一般 split 80/20（交易者 80%）。例：50K 需到 $52K 才可按 80% 提款。buffer 內提款須終止帳戶，且帳戶開立未滿／超過 60 trading days 的 buffer 取得比例為 50%／80%。[TPT-11]
* **頻率／上限**：首頁聲稱 day-one、immediate／automated withdrawal；但在可存取的官方規則頁未找到可普遍套用的每期上限或頻率參數，故為**未知，勿由宣傳詞推論**。[TPT-3][TPT-11]
* **KYC**：官方說所有新註冊者必須完成 KYC；可能人工審核。[TPT-12]
* **付款路徑**：US bank 透過 Plaid；非 US bank 可填 Wise 或 PayPal。官方說 Plaid 通常即時、亦可能 1–2 business days；Wise/PayPal 通常於 12 business hours 內處理。帳戶／收款資料須與註冊人相符。[TPT-13][TPT-14]

### TPT 小結與量化評分

|因子|評分|第一方依據／理由|
|---|---:|---|
|NQ/MNQ|2|官方逐項列名。[TPT-1]|
|自動策略／EA|0|明確禁止 bot、algo、automated execution。[TPT-2][TPT-5]|
|API|未知|未找到官方 API／程式化下單規格；不得推論平台連接就是 API 許可。|
|自有多帳戶 copier|1|可用但限核准 copier、本人控制帳戶，且不得風險中性。[TPT-6]|
|反向／對沖兼容|0|明確禁止，NQ↔MNQ亦列為相關對。[TPT-7]|
|成本透明度|1|可驗證月訂閱、部分公開例價與 reset，但完整當期費率矩陣未於規則頁固定列示。|
|出金可預測性|1|buffer、80/20、KYC、路徑明確；普遍頻率／上限未證實。|

**結論**：若「automated quantitative」意指策略自行送單，TPT 依其明文政策為不相容；僅手動交易加自有帳戶 copier 也必須先取得合規書面確認。

---

## 2. Lucid Trading

### 標的、方案與階段

* **NQ/MNQ 2/2**：官方 approved-products 表列 NQ 與 MNQ，且分別列每邊 commission $1.75／$0.50。[LUCID-1]
* **現行知識庫方案**：LucidFlex、LucidDaily、LucidPro、LucidDirect；另有 LucidMaxx（官方稱 invite-only）與 LucidBlack（分類明示 Legacy）。不要把 legacy payout／live 條件套到現行方案。[LUCID-2][LUCID-3]
* **評估時間**：LucidPro、LucidDaily 評估頁都稱 one-time fee、無 monthly rebilling、可花任意時間通過；LucidDirect 是無 evaluation 的直達 funded 方案。[LUCID-4][LUCID-5][LUCID-6]

### 自動化、copier、HFT、對沖

* **自動化 2/2**：官方明示「Automated trading systems and trade copiers are permitted」；使用者對軟體錯誤、故障、非預期結果負全責。[LUCID-7]
* **API**：**未知**。官方列支援平台，但可存取資料未見 Lucid 自己提供的 API 規格、認證方法、下單限速或 API 專屬許可；不能將「automated systems permitted」擴張為「官方 API 已提供」。
* **HFT 衝突**：高頻交易被禁止；官方定義為極短時間內高量交易，常由 algorithms 驅動，重複違規可移除利潤、關閉帳戶及永久限制。[LUCID-8] 因此一般排程／低頻量化可與規則相容，不代表任何演算法均可用。
* **對沖衝突**：禁止同／跨帳戶、跨不同使用者、跨不同 firm/platform 的所有形式風險規避；官方以一帳戶 long NQ、另一帳戶 short NQ 為例。[LUCID-9]
* **其他 execution 風險**：官方 micro-scalping 規則將「超過 50% profits 來自持有 <=5 seconds」列為觸發人工審查門檻；這對超短週期策略是重大相容性風險。[LUCID-10]

### 費用、重設與出金（不得混用方案）

* **費用架構**：官方稱 LucidPro evaluation 與 LucidDirect 是一次性費用、無訂閱／自動續費；LucidPro evaluation 升 funded 無 activation fee；reset 可購買但非免費。該頁**未列美元金額**，因此所有方案的具體 upfront/reset 金額為**未知／應以官方結帳頁確認**。[LUCID-11]
* **LucidFlex funded**：官方寫無 DLL、無 consistency、無 payout buffer、90/10 split；25K–150K，採 EOD trailing drawdown 與 scaling。[LUCID-12] Payout 文稱無固定窗口、無 buffer、可隨時 request，通常 2 business days；但仍須遵守規則與該方案要求。[LUCID-13]
* **LucidDaily funded**：官方稱可每日 request、90/10、無 consistency；但 funded 禁止 USD high-impact red-folder news，在前後各一分鐘內必須 flat。[LUCID-14] 其 payout 頁另有適格性條件；在此摘要不把其他方案的 buffer／cap 移植過來。[LUCID-15]
* **LucidPro funded**：模擬 funded、90/10、無 simulated payout caps，但 payout 必須符合 40% consistency（每次 payout 後重置）及該方案的 profit/buffer 條件。[LUCID-16][LUCID-17]
* **LucidDirect**：直接 funded（無 evaluation），但 funded payout consistency 為 20%，每次 payout 後重置。[LUCID-6][LUCID-18]
* **付款／KYC**：官方 payout methods 頁與 business-registration 頁可查 payment method／business KYC；但此輪可存取材料沒有建立一條適用所有個人、所有國家的單一付款路徑，故個人收款國別、KYC 與可用通道：**按方案／居住地未知，先書面確認**。[LUCID-19]

### Lucid 小結與量化評分

|因子|評分|第一方依據／理由|
|---|---:|---|
|NQ/MNQ|2|官方逐項列名。[LUCID-1]|
|自動策略／copier|2|明示 permitted。[LUCID-7]|
|API|未知|無官方 API 文件可證。|
|對沖兼容|0|明確禁止跨帳戶／跨 firm hedge。[LUCID-9]|
|超短週期策略|1|自動化允許，但 HFT 禁止、micro-scalping 有明確審查門檻。[LUCID-8][LUCID-10]|
|成本透明度|1|費用型態與免 activation 已證；金額未於官方規則頁固定列示。|
|出金可預測性|2|各主要方案各自有明示頻率／split／buffer或consistency規則；仍須選定方案後逐條核對。|

**結論**：四者中，Lucid 是有第一方明文允許自動系統／copier且列明 NQ/MNQ 的候選。但量化策略必須避開 HFT、<=5 秒利潤集中及任何跨帳戶／跨 firm 對沖；API 供給仍是未證事項。

---

## 3. The Futures Desk（TFD）

### 階段、NQ/MNQ、時間與成本

* **NQ/MNQ 2/2**：官方 Products/Instruments 清單明列 NQ（Nasdaq）與 MNQ（Micro Nasdaq）。[TFD-1]
* **階段**：官方流程為 **Assessment → Sim Brokerage → Live Brokerage**。Assessment 至少 5 days；Sim Brokerage 建至等於 drawdown 的 buffer 後解鎖 payout 並進入 live；Live 可週一至週五 daily payout、無 minimum days。[TFD-2]
* **時間規則的官方衝突須先釐清**：TFD overview 說 Assessment minimum 5 days；但 Minimum Trading Days FAQ 說建立 plan 時可選 minimum days。兩頁均為官方資料，沒有可見頁面解釋哪一設定優先，故應以實際 plan checkout／書面答覆確認，不能擅自取其中一個。[TFD-2][TFD-3]
* **Recurring**：Assessment 每 31 days 自動 billing extension；TFD-X $29、Rithmic $49；通過即取消。這是已驗證 recurring 成本。[TFD-4]
* **Reset**：reset 後 fresh account、0 balance／0 traded days，並給 free 31-day extension；依 Assessment max drawdown：$100–$3,000=$75、$3,000–$5,750=$150、$6,000=$229。[TFD-5]
* **Activation／data**：官方稱通過 Assessment 後無 funded activation fee；sim brokerage 全部 data 與 live 一個 data fee 由公司負擔；額外 CME/CBOT/COMEX/NYMEX 每月 $135、EUREX $80。[TFD-6] Assessment 初次費用未在該知識庫靜態文章列出，故**未知／結帳確認**。

### 自動化、copier、hedging 與 live 風險限制

* **自動化 2/2**：Trading Guidelines 明示 Bots/Algorithmic Trading 可用，但須「solely controlled by you」；copy trader 可於 Assessment 與 Funded Desks 用。自動策略／copier 造成損失或失敗，公司不提供技術協助；不得違反 CME rules。[TFD-7]
* **API**：**未知**。官方有 Rithmic、ProjectX／平台知識庫與自動策略許可，但未在可存取資料提供 TFD API endpoint／credential／rate-limit 文件；不能推論 API 已允許。
* **Hedging**：同一帳戶內不同 products 或 expiry 的 hedging 在指南表格寫 Yes；但這不能推廣成跨帳戶或任何 CME 不允許的交易。[TFD-7] 因官方未給跨帳戶 hedge 明確答案，該面向評為**未知**而不是 2。
* **Live conflict**：Live Brokerage 對新聞前後一分鐘限最多 5 micros，且必須放合理 stop；違反時 TFD 可 flatten。14 consecutive days inactivity 且無法聯繫，帳戶可能終止、先前 gains 失去。[TFD-8] 這會影響 unattended automation 的風控與監控設計。

### 出金與帳戶數

* **Sim→live／buffer**：Sim Brokerage 的 buffer 等於交易時的 drawdown；達成後解鎖 payout。官方說可自行選擇提至 buffer／starting balance、無 penalty；超出 buffer 的 sim profit 不會轉到 live。[TFD-9][TFD-2]
* **split／頻率／cap**：交易者保留 80%，公司 20%；Live payout 週一至週五，截止 11 AM ET 通常當日、之後下一 business morning；最低 $100、無上限。[TFD-9][TFD-10]
* **支付／身份核驗**：Rise 或 ACH；轉 live 時收到 Rise invitation 並完成 quick identity check，之後可收至 Rise wallet；ACH 則連結銀行。[TFD-9] 除該 live identity check 外，普遍 KYC、國別資格與支付可用性未在可存取官方文章完整載明，故為**未知**。
* **多帳戶**：最多四個 active assessments，總 drawdown 不超過 $6,000；限制也適用 Assessment、SimBrokerage、Live。每人限一 username，另建帳號繞過可被取消／封禁。Live 建議合併，最多維持兩個 separate。[TFD-11]

### TFD 小結與量化評分

|因子|評分|第一方依據／理由|
|---|---:|---|
|NQ/MNQ|2|官方逐項列名。[TFD-1]|
|自動策略／copier|2|明示 bots/algos 與 copy traders 可用，僅本人控制並須 CME 合規。[TFD-7]|
|API|未知|未找到 API 官方規格。|
|對沖|1|同帳戶不同產品／到期月明示 Yes；跨帳戶未明示。|
|成本透明度|2|31日續費、reset、activation/data與額外 data 明示；初始 assessment 價格仍須 checkout。|
|出金可預測性|2|buffer、80/20、日頻、$100 minimum、無上限與 Rise/ACH 明示。|

**結論**：TFD 是第一方規則最直接允許 algo／copier 的候選之一，且 NQ/MNQ 明列。適合評估有「本人獨立控制、CME 合規、新聞微型倉位限制、無人值守中斷監測」的系統；不可把這些許可解讀成供應官方 API 或跨帳戶對沖許可。

---

## 4. Aqua Futures

### 可驗證狀態

官方 `https://aquafutures.com/`（及 www 變體）回傳的是指向 `searchvity.com` 的停放 frameset，而非 Aqua Futures 的可用官方服務／規則頁。故在「第一方官方來源限定」下，以下一律**未知**：NQ、MNQ、可用平台、EA/algorithm/API、copier、hedging、方案／階段、評估時限、費用與 reset／activation、payout eligibility／frequency／cap／buffer／split、KYC 與付款路徑。[AQUA-1]

|因子|評分|理由|
|---|---:|---|
|NQ/MNQ|未知|無可存取官方產品清單。|
|自動化／EA／API|未知|無可存取官方規則／文件。|
|copier／hedging|未知|無可存取官方規則。|
|費用|未知|無可存取官方 pricing／checkout。|
|payout／KYC|未知|無可存取官方條款。|

**結論**：不得以第三方比較網站、舊評測、社群貼文或網域歷史補洞。就本研究的嚴格證據標準，Aqua Futures 應排除，直到其提供可存取、可歸屬且書面現行的官方規則與費率。

---

## 5. 購買／部署前必須取得的書面確認

1. 策略是否屬於公司定義的 **automated／algorithmic／HFT／microscalping**；提供策略的最大下單頻率、持倉時間分佈、每秒 order/cancel 上限及是否用外部 VPS/API。
2. 目標帳戶、data feed、平台、NQ/MNQ 合約月與策略是否可在 **evaluation、sim funded、live** 每個階段獨立運行；不要假定 simulation 許可可延續到 live。
3. copier 是否僅複製同一 beneficial owner 的帳戶；是否存在跨帳戶、跨 firm、同／反向或相關商品（含 NQ/MNQ）曝險。TPT 與 Lucid 的反向／風險中性設計不應嘗試繞過。
4. 是否有正式 API、批准的 platform automation、VPS／IP 限制、rate limits、kill switch、當斷線／reconnect／partial fill 時的責任分配。
5. 選定尺寸與 data feed 的**完整**一次性、訂閱、reset、activation、market-data、commission/exchange、withdrawal 費；把促銷另列，不可當常態價格。
6. 對選定方案與帳戶尺寸，要求確認 payout 的 minimum/maximum、request frequency、buffer、consistency、split、KYC、稅務表、居住國與收款路徑；要求引述現行帳戶協議條款。

---

## 官方來源索引（均按指定存取日 2026-08-10）

### TPT

* [TPT-1] TakeProfitTrader, **Approved Instruments & Permitted Products List** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172629238301-Approved-Instruments-Permitted-Products-List
* [TPT-2] TakeProfitTrader, **Universal Trading Policies (UTP)** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/34431153546397-TakeProfitTrader-Universal-Trading-Policies-UTP
* [TPT-3] TakeProfitTrader, homepage/account offering — https://takeprofittrader.com/
* [TPT-4] TakeProfitTrader, **Test Subscriptions** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15141145057053-Test-Subscriptions
* [TPT-5] TakeProfitTrader, **PRO Account Rules** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15171769361053-PRO-Account-Rules
* [TPT-6] TakeProfitTrader, **Trade Copier Policy** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/34431176505245-Trade-Copier-Policy
* [TPT-7] TakeProfitTrader, **Rule 6: No Counter Positions** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/30331694826909-Rule-6-No-Counter-Positions
* [TPT-8] TakeProfitTrader, **Independent Trade Execution Policy** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/34431223270557-Independent-Trade-Execution-Policy
* [TPT-9] TakeProfitTrader, **Resetting Your Test Account** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15140989806493-Resetting-Your-Test-Account
* [TPT-10] TakeProfitTrader, **Resetting a PRO Account** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15171895352733-Resetting-A-PRO-Account
* [TPT-11] TakeProfitTrader, **PRO Account Profit Split & Withdrawal Rules** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172219527581-PRO-Account-Profit-Split-Withdrawal-Rules
* [TPT-12] TakeProfitTrader, **KYC Procedure** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/22076187576605-KYC-Procedure
* [TPT-13] TakeProfitTrader, **Payout System** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172296875165-Payout-System
* [TPT-14] TakeProfitTrader, **How to Withdraw from the Wallet** — https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15965203144477-How-to-Withdraw-from-the-Wallet

### Lucid

* [LUCID-1] Lucid Trading, **Approved Products and Commissions** — https://support.lucidtrading.com/en/articles/11508978-approved-products-and-commissions
* [LUCID-2] Lucid Trading Help Center, collections — https://support.lucidtrading.com/en/
* [LUCID-3] Lucid Trading, **LucidMaxx Overview** — https://support.lucidtrading.com/en/articles/13891785-lucidmaxx-overview
* [LUCID-4] Lucid Trading, **LucidPro Evaluation Account** — https://support.lucidtrading.com/en/articles/12890029-lucidpro-evaluation-account
* [LUCID-5] Lucid Trading, **LucidDaily Evaluation** — https://support.lucidtrading.com/en/articles/15996664-luciddaily-evaluation
* [LUCID-6] Lucid Trading, **LucidDirect Funded Account** — https://support.lucidtrading.com/en/articles/12890148-luciddirect-funded-account
* [LUCID-7] Lucid Trading, **Other Activities** — https://support.lucidtrading.com/en/articles/11404728-other-activities
* [LUCID-8] Lucid Trading, **Prohibited: High Frequency Trading** — https://support.lucidtrading.com/en/articles/11404736-prohibited-high-frequency-trading
* [LUCID-9] Lucid Trading, **Prohibited: Hedging** — https://support.lucidtrading.com/en/articles/11404734-prohibited-hedging
* [LUCID-10] Lucid Trading, **Prohibited: Microscalping** — https://support.lucidtrading.com/en/articles/11404742-prohibited-microscalping
* [LUCID-11] Lucid Trading, **Simulated Account Fees** — https://support.lucidtrading.com/en/articles/11404620-simulated-account-fees
* [LUCID-12] Lucid Trading, **LucidFlex Funded Account** — https://support.lucidtrading.com/en/articles/12945795-lucidflex-funded-account
* [LUCID-13] Lucid Trading, **LucidFlex Payouts** — https://support.lucidtrading.com/en/articles/12945796-lucidflex-payouts
* [LUCID-14] Lucid Trading, **LucidDaily Funded Account** — https://support.lucidtrading.com/en/articles/15997244-luciddaily-funded-account
* [LUCID-15] Lucid Trading, **LucidDaily Payouts** — https://support.lucidtrading.com/en/articles/15997266-luciddaily-payouts
* [LUCID-16] Lucid Trading, **LucidPro Funded Account** — https://support.lucidtrading.com/en/articles/12890069-lucidpro-funded-account
* [LUCID-17] Lucid Trading, **LucidPro Consistency Percentage** — https://support.lucidtrading.com/en/articles/12890109-lucidpro-consistency-percentage
* [LUCID-18] Lucid Trading, **LucidDirect Consistency Percentage** — https://support.lucidtrading.com/en/articles/12890178-luciddirect-consistency-percentage
* [LUCID-19] Lucid Trading, **Registering as a Business** — https://support.lucidtrading.com/en/articles/11404634-registering-as-a-business

### The Futures Desk

* [TFD-1] The Futures Desk, **Products/Instruments** — https://www.thefuturesdesk.com/knowledge-base#article=tradeable-instruments
* [TFD-2] The Futures Desk, **TFD at a Glance** — https://www.thefuturesdesk.com/knowledge-base#article=tfd-overview
* [TFD-3] The Futures Desk, **Minimum Trading Days FAQ** — https://www.thefuturesdesk.com/knowledge-base#article=minimum-trade-days
* [TFD-4] The Futures Desk, **Extensions** — https://www.thefuturesdesk.com/knowledge-base#article=extensions
* [TFD-5] The Futures Desk, **Resets** — https://www.thefuturesdesk.com/knowledge-base#article=resets
* [TFD-6] The Futures Desk, **Fees** — https://www.thefuturesdesk.com/knowledge-base#article=data-fees
* [TFD-7] The Futures Desk, **Trading Guidelines** — https://www.thefuturesdesk.com/knowledge-base#article=trading-guidelines
* [TFD-8] The Futures Desk, **Live-Brokerage Guidelines** — https://www.thefuturesdesk.com/knowledge-base#article=brokerage-guidelines
* [TFD-9] The Futures Desk, **Payouts** — https://www.thefuturesdesk.com/knowledge-base#article=payouts
* [TFD-10] The Futures Desk, **Live-Brokerage** — https://www.thefuturesdesk.com/knowledge-base#article=live-brokerage
* [TFD-11] The Futures Desk, **Max Accounts** — https://www.thefuturesdesk.com/knowledge-base#article=multiple-accounts

### Aqua

* [AQUA-1] Aqua Futures domain landing response — https://aquafutures.com/
