# 剩餘 TradingPilot Futures 候選：官方來源嚴格核查（Day Traders、The Legends Trading、Blusky、Atlas Futures）

> **使用範圍與存取日：2026-08-10（依研究任務指定的比較截點）。** 只採用各業者自己網域及其由主站連結的官方 Help Center；TradingPilot 僅用於候選名稱發現，**未作任何事實證據或引用**。價格、促銷及動態規則均只能視為該截點頁面所示，購買前應取得書面確認。
>
> **嚴格納入門檻：** 必須有官方資料明確證明可交易 `NQ` 或 `MNQ`，且**明確允許**自建 bot／EA／API 自動下單；並能核實複製、對沖及出金的重大限制。平台／Rithmic 連線、網站提及「automation」或僅禁止 HFT，都**不等於**允許自建自動交易。本組沒有一家通過此門檻，故不應拿來補足「10 家量化自動化期貨 prop firm」名額。

## 結論總表

|候選|官方身分與期貨服務是否確認|NQ/MNQ 明示|自建 bot/EA/API 明示許可|嚴格量化適用性|結論|
|---|---|---|---|---|---|
|Day Traders（DayTraders.com）|確認為 funded futures 帳戶服務|**MNQ：是**；NQ：本次未逐一開啟官方商品條目確認|否；僅見「自動化 HFT 禁止」，且官方搜尋結果描述 HFT 為「Manual Trading Required」|不通過|**排除**：沒有肯定的 bot/EA/API 許可，且有自動化 HFT 禁令。|
|The Legends Trading（Legends Trading Group, Inc.）|確認主站自稱期貨與模擬評估／Master Simulation 服務|未明示／未知|未明示／未知|不通過|**排除**：找不到官方 NQ/MNQ 清單與自建自動下單許可；其官方 Knowledge 子站在檢索時受 Cloudflare 驗證阻擋。|
|Blusky（`blusky.com`）|否；官方首頁是「BluSky」生活／建築品牌，沒有期貨 prop 服務|不適用／未知|不適用／未知|不通過|**排除**：候選名稱無法由可存取官方頁面對應至期貨資助業者。|
|Atlas Futures（`atlasfutures.com`）|否；官方首頁明確是 agentic software 的 agent lab|不適用／未知|不適用／未知|不通過|**排除**：同名官方網域不是交易／期貨 prop firm。|

---

## 1. Day Traders（DayTraders.com）— 有部分期貨與規則證據，但不符合自建量化自動化門檻

### 官方身分、商品與階段

* 主站標題為「Funded Futures Trading Accounts for Day Traders」，列出 EOD、TRAIL、STATIC、INSTANT、LIVE 等帳戶頁籤；主站的流程則列「Evaluation → Get funded → Get paid → Move to live」。這表示頁面同時涉及評估、Pro／模擬資助及 live progression，**不可把各階段規則互相移植**。
  官方來源：[首頁](https://daytraders.com/)、[產品頁](https://daytraders.com/products)、[Evaluation Account Rules](https://daytraders.com/help/articles/14368378-evaluation-account-rules)
* **MNQ：已明示。** 官方 Help Center 對 `MNQ` 搜尋，結果含「Micro E-Mini Nasdaq-100 (MNQ)」的佣金頁、micro contracts 頁及 Instrument List；因此只將 **MNQ** 視為已官方確認。
  官方來源：[Commissions for Rithmic Accounts](https://daytraders.com/help/articles/10068152-commissions-for-rithmic-accounts)、[Can I Trade Micro Contracts?](https://daytraders.com/help/articles/9854897-can-i-trade-micro-contracts)、[Instrument List](https://daytraders.com/help/articles/9911473-instrument-list)
* **NQ：未知／本次未驗證。** 不可以因 MNQ、CME 或 Rithmic 而推論 NQ 可交易。

### 自動化、複製、對沖及其他交易限制

* **bot／EA／API：未找到明確許可，故不得當作允許。** 官方 evaluation 規則明確寫「Automated high-frequency trading is prohibited on all accounts」；Help Center 的官方搜尋結果對同一 HFT 規則摘要更寫為「Manual Trading Required」。這是明確的 HFT 禁止，但不是一般 bot/EA/API 的正面批准，也沒有公布可接受的訂單率、訊號來源、是否必須本人開發／操作、或 API 使用條件。
  官方來源：[Evaluation Account Rules](https://daytraders.com/help/articles/14368378-evaluation-account-rules)、[No High Frequency Trading](https://help.daytraders.com/help/articles/9918227-no-high-frequency-trading)
* **對沖：禁止。** evaluation 規則列「No Hedging」並連到官方專文；適用帳戶／跨帳戶和相反部位的完整邊界應以該專文及合規書面確認。
  官方來源：[Evaluation Account Rules](https://daytraders.com/help/articles/14368378-evaluation-account-rules)、[No Hedging](https://help.daytraders.com/help/articles/9918236-no-hedging)
* **copy／trade copier：未知。** 本次可存取官方規則未找到明示的 copier、訊號複製、跨帳戶同步或跨 firm 禁令／許可；不可推論可用。
* **新聞：評估規則明示允許 news trading，但提醒流動性／波動風險。** 這不改變 HFT 或自動化限制。
  官方來源：[Evaluation Account Rules](https://daytraders.com/help/articles/14368378-evaluation-account-rules)
* 另有 50% evaluation consistency、最低活動政策、最低日模擬利潤與「通過後停止交易」等要求；這些是評估階段規則，非 bot 許可。
  官方來源：[Evaluation Account Rules](https://daytraders.com/help/articles/14368378-evaluation-account-rules)

### 方案、費用、時間限制

* **可核實的公開產品標示：** 主站 EOD 方案在截點頁面顯示 25K $30.90、50K $46.90、150K $89.90、300K $159.90，均標為「One-time or Monthly」。此為主站價格呈現，未能從頁面本身可靠拆分一次性與月費的選擇／週期，不能假設為固定月訂閱。
  官方來源：[首頁價格區](https://daytraders.com/#pricing)
* **重置、activation、data fee：未知／未在上述可存取官方頁面核實。** 不以第三方彙整或結帳猜測補值。
* **時限：** 未見固定的「必須在 X 日內通過」上限；但 evaluation 規則明示各帳戶在每個 rolling 30-day cycle 要滿足最低交易日以保持 active。這是活動／失效規則，**不是**可安全解讀成無限期或固定評估期限。
  官方來源：[Evaluation Account Rules](https://daytraders.com/help/articles/14368378-evaluation-account-rules)、[Keep Your Accounts Active (Inactivity Rule)](https://help.daytraders.com/help/articles/9854806-keep-your-accounts-active-inactivity-rule)

### 出金、KYC、付款路徑

* **Pro／S2F 出金資格：** 要完成 QDays、達該帳戶／cycle profit target、維持提款前後餘額、遵守一致性及 drawdown，最低申請 $500；申請日不算 QDay。
  官方來源：[Payout FAQ (Pro Accounts + S2F)](https://daytraders.com/help/articles/14363990-payout-faq-pro-accounts-s2f)
* **頻率／cap／buffer（僅 Pro，不能套用 live）：** 每次申請最少相隔 8 QDays；$500 最低；各 account size 的「提款前最低餘額、提款後最低餘額、單次上限」在官方表格逐項列出。這些最低餘額就是應保留的 buffer，但不能在未指定帳戶尺寸下寫成單一數字。Pro payout split 為 **100% simulated profit split**。
  官方來源：[Payout FAQ (Pro Accounts + S2F)](https://daytraders.com/help/articles/14363990-payout-faq-pro-accounts-s2f)
* **付款路徑：** 官方指示在 dashboard 的 Payout Method 設定 provider，例如 **Plane**，再申請 payout；是否所有居住地可用、KYC／身分文件、稅務與 Plane 的可用提款管道，**未明示／未知**。
  官方來源：[Payout FAQ (Pro Accounts + S2F)](https://daytraders.com/help/articles/14363990-payout-faq-pro-accounts-s2f)
* **衝突／版本風險：** 首頁行銷「Trade Live Not Simulated」和「No Profit Split」，但同一官方 Help Center 的 Pro/S2F FAQ 明確是「100% simulated profit split」，且條件及限制依 Pro/S2F 方案而定。這不是可直接判定的矛盾（可能指不同階段），但正好說明**不得將 live 行銷語、Pro、S2F、evaluation 的費用／出金規則混用**。購前應以所選 checkout 方案與其書面條款為準。
  官方來源：[首頁](https://daytraders.com/)、[Payout FAQ (Pro Accounts + S2F)](https://daytraders.com/help/articles/14363990-payout-faq-pro-accounts-s2f)

**嚴格判定：排除。** 雖然 MNQ 有官方證據，且有可用的規則與出金資料，但沒有「自建 bot／EA／API 可用」的肯定官方許可，反而有自動化 HFT 禁令與手動交易表述；對自建量化策略屬重大未解／負面相容性風險。

---

## 2. The Legends Trading — 期貨網站存在，但核心量化與商品／規則資料未能核實

### 官方可核實內容

* 主站以 **Legends Trading Group, Inc.** 名義營運，頁尾有美國地址；其 Terms 頁亦有 futures risk disclosure。首頁／Plans 表示使用 Apprentice account、Simulation Account、Master Simulation stage；流程文案為選方案 → 完成 challenge（profit target、minimum days、避免 liquidation）→ Master Simulation。
  官方來源：[首頁](https://thelegendstrading.com/)、[Plans](https://thelegendstrading.com/plans)、[Terms & Conditions](https://thelegendstrading.com/terms-conditions)
* Plans 頁在截點顯示 Apprentice／Elite 分頁，包含每月價格（例如可見 50K Apprentice $29.50/month、100K $53.50/month、150K $69/month）及「Max contracts / micros」；另一些同頁載入內容顯示不同帳戶大小、profit goal、EOD trailing max loss、30% consistency、4 days，及 $99／$149／$199 activation fee。**該頁同時有促銷碼 LTG 與多組方案資料；未能把每一組數字無歧義綁定到完整目前產品／方案，故不整理成可比較費表。**
  官方來源：[Plans](https://thelegendstrading.com/plans)
* 頁面宣稱在達 consistency 後「up to twice per month」申請 payouts、90/10 profit split、在第 2 次 payout 後進 live funded；但未在可存取的公開頁面核實各帳戶 payout cap、minimum withdrawal、buffer／提款後餘額、完整 qualifying days、KYC、付款或重置／資料費。
  官方來源：[首頁](https://thelegendstrading.com/)、[Plans](https://thelegendstrading.com/plans)

### 必要欄位逐項狀態

|欄位|官方核查結果|
|---|---|
|NQ／MNQ|**未知。** 只見「top futures markets」、micro 數量與 Tradovate／NinjaTrader／Rithmic 等 logo；沒有以 NQ 或 MNQ 名稱列出商品的可存取官方清單。|
|bot／EA／API|**未知。** 首頁「Visual automation builder」是行銷／功能字樣，並非自建 bot、EA 或外部 API 可下單的政策；未找到正面許可。|
|copy／hedging|**未知。** 未找到可存取官方規則。|
|HFT、新聞、短持倉、訂單率|**未知。** 未找到可存取官方規則。|
|計畫／階段|可確認 Apprentice／Elite（頁面分頁）、Simulation challenge、Master Simulation，以及網頁所稱 live funded progression；各帳戶細則不可混用。|
|費用|可確認頁面有 recurring monthly 標示與某些 activation fee 文案；**reset、data fee、完整未折扣價格／方案對照未知。**|
|時間限制|只有 minimum days／某些載入內容的 4 days；是否有最大完成時限 **未知**。|
|出金|頁面聲稱最多每月兩次、90/10 split、第二次 payout 後 live；資格、單次／期間 cap、buffer、最低金額等 **未知**。|
|KYC／支付／出金管道|**未知。**|

### 存取限制與衝突風險

* 主站自己連到 `https://knowledge.thelegendstrading.com` 作為 Knowledge；檢索時該官方子站回應 Cloudflare「Performing security verification」。此僅代表本次無法存取，**不是**相關規則不存在的證據。未能以不可存取頁面反推允許／禁止。
  官方連結來源：[首頁](https://thelegendstrading.com/)；受阻 URL：[Knowledge](https://knowledge.thelegendstrading.com/)
* Plans／首頁同時呈現「Get funded in 1 day」、流程中的 minimum days，以及載入內容的 4 days。適用產品或版本沒有在可存取公開資料中清楚對應，故視為**未解版本／方案衝突**，不能挑其中一項作比較排名依據。

**嚴格判定：排除。** 未有 NQ/MNQ 明示及自建自動下單的明確許可；關鍵 policy 子站又不可存取。應先向 compliance 取得書面回答，才可重新評估。

---

## 3. Blusky — 無法以官方頁面確認為期貨 prop 候選

* 可存取的官方網域 `https://www.blusky.com/` 標題為 **BluSky**，頁面連結／內容為「FOUNDERS」、「LIFE ARCHITECTURE」及 `BLUSKY_LIVING`，而非 futures、evaluation、funded account 或 prop-trading 服務。
  官方來源：[BluSky 首頁](https://www.blusky.com/)
* 因官方身分不符，以下所有要求欄位均為 **不適用／未知**：NQ、MNQ、bot/EA/API、copy、hedging、HFT／新聞／短持倉、方案階段、訂閱／reset／activation／data fee、時間限制、payout eligibility／frequency／cap／buffer／split、KYC、付款與出金路徑。
* **嚴格判定：排除。** 不應用同名非金融品牌的官方內容去替某個未證實的期貨業者填資料，也不應以第三方候選頁作替代證據。

---

## 4. Atlas Futures — 官方網域明示為 AI agent lab，不是期貨 prop firm

* `https://atlasfutures.com/` 的官方首頁明確稱「Atlas Futures is an **agent lab**」，工作內容是 agentic software creation，產品是 Workshop.ai 與 Rayline.ai；沒有交易、期貨、資助帳戶或出金服務的表述。
  官方來源：[Atlas Futures | Agent Lab](https://atlasfutures.com/)
* 因官方身分與候選用途不符，NQ/MNQ、bot/EA/API 交易政策、copy／hedging、所有費用／階段／時限／出金、KYC 與付款路徑均為 **不適用／未知**。
* **嚴格判定：排除。** 「Agent lab」的 AI 軟體產品不能被當成期貨交易平台或 prop firm。

---

## 購買前必須向仍可能相關的業者取得的書面確認

對 DayTraders.com 與 The Legends Trading，若仍要列入候選，請先由其 compliance／support 以所選**精確方案、帳戶尺寸、平台／broker、evaluation vs simulated-funded vs live** 回答：

1. 是否可交易 **NQ 與／或 MNQ**（逐一列出 symbol、平台、資料來源）。
2. 是否允許本人編寫的 bot、EA、API、webhook、外部訊號與 trade copier；是否只允許某平台內建自動化。
3. HFT／order-rate、短持倉、新聞、DCA、延遲套利等邊界；是否所有階段相同。
4. 同人多帳戶、相同策略、copy、cross-firm copying、相反部位與 hedging 的禁止／許可範圍。
5. 完整未折扣費用：一次性／月費、reset、activation、資料／交易所、平台、佣金，以及未通過／取消時的處理。
6. 每階段最大期限與 inactivity；每種 payout 的 QDays／minimum days、頻率、單次／期間 cap、提款後 buffer、split、KYC、稅務、國家限制及付款／提款 provider。

在收到能對應到上述方案的正面官方書面答覆前，這四個候選都不應計入「適合自建自動化 Nasdaq futures 策略」的推薦數量。
