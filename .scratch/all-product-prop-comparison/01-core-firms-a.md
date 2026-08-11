# TradingPilot catalog — 核心 prop firms（A）官方規則核對

**檢索日：2026-08-11。** 本文以各品牌可公開存取的官方網站／官方 Help Center 為一手來源；TradingPilot 僅在「追蹤」欄作次要證據，絕不以其補足官方未公開的規則。價格是頁面顯示值（可能為促銷價、且可隨帳戶規模／平台而變），不是報價或推薦。

## 判讀規則與限制

* 「未載明／未驗證」不是「允許」。自動化只用下列三種結論：**明確允許**、**明確限制**、或 **在可存取官方規則未找到明確限制**（第三者不構成批准）。
* 「避險」指同帳戶或跨帳戶反向／風險中性部位；沒有精確官方條文時不推論。所有自動化、複製、HFT、外部訊號、避險與出金資格，都應在購買前向該公司 compliance 取得書面確認。
* 產品／階段不可互相移植。除非在下列某一精確產品頁列出，費用、DD、最少交易日、出金條件均標為未驗證。
* 「出金狀況」分成 (a) 官方摩擦（資格／節奏／下限／上限／buffer／split／KYC／rail）與 (b) TradingPilot 的次要追蹤；不可把行銷的「24/48 小時」當成保證已付款。

---

## 1. Blue Guardian

**可辨識產品／階段：** CFD `Instant`（已資助、無 evaluation）、`1 Step`、`2 Step`；官網另列 Futures 的 Standard／Premium／Live Express／Direct，不能與 CFD 混用。[官方首頁](https://blueguardian.com/)

* **費用：** 本次可存取首頁未取得可引用的各帳戶 checkout 金額、reset 或 activation 價格；**未驗證**。
* **通關：** Instant 明示「No evaluation」；1 Step 是一個 target，2 Step 是兩個 phase；頁面表格對 CFD 顯示最大交易日 `Indefinite`、profit split `up to 90%`。各 size 的 target／日 DD／總 DD／最少日數，**未從可公開頁的特定組合驗證**。[首頁](https://blueguardian.com/)
* **官方出金摩擦：** CFD Instant 顯示 `Instant Payouts`；1/2 Step 顯示「Payouts in 7 days」，但首頁未交代門檻、最小額、cap、buffer、KYC 或 rail，故均未驗證。Futures Standard/Premium 顯示每 3 日和隨 consistency 擴大的 cap，Live Express 顯示每工作日／每日 $1,100 cap；此為不同 Futures 產品，僅作識別，不能套到 CFD。[首頁](https://blueguardian.com/)
* **自動化：明確允許（限該 CFD 首頁表格）：** 表格寫 `Expert Advisors: Yes`、`Trade Copier: Yes`；仍未證明第三方訊號、HFT 或每一方案的 payout 合規性。
* **避險／不活躍終止／免費試用：** 可存取官方頁未驗證精確條文；不可據此認定可避險或無 inactive 終止。
* **出金狀況—TradingPilot（次要）：** 本次未取得可引用的 TradingPilot 個別追蹤頁；不報告其評分／核付量。

## 2. Maven Trading

**精確產品：Forex Standard，$2k 顯示檔位（首頁動態價格）。** [官方首頁／產品卡](https://maventrading.com/)

* **費用：** Standard 1-Step 顯示 `$14`（旁示 `$15` coupon 前），2-Step `$18`（`$19`），3-Step `$12`（`$13`）；頁面稱在**第 3 次 withdrawal 可退**。這是 $2k／頁面顯示促銷價，不能外推所有 size。reset／activation／訂閱費未驗證。
* **通關：** 1-Step：target 8%、max loss 5%、daily loss 3%；2-Step：8% + 5%、max 8%、daily 4%；3-Step：3% + 3% + 3%、max 3%、daily 2%。各卡列 funded split 80%、payout frequency 10 business days、`Consistency score: No`。最少交易日與 time limit 未在本卡片文字驗證。
* **官方出金摩擦：** 上列 Standard funded 為 80%／每 10 個工作日；首頁另說費用第 3 次出金可退。最低額、cap/buffer、KYC、支付軌道及 payout 審核規則未驗證。
* **自動化：在可存取官方規則未找到明確限制**（本次首頁／產品卡未見 EA/bot 條款；**不是批准**）。**避險／不活躍／免費試用：未驗證。**
* **TradingPilot（次要）：** 未取得可引用的個別追蹤頁。

## 3. Goat Funded Trader (GFT)

**可辨識產品：Instant Funding Account、Challenge Account（1/2/3 steps）。** [官方 Model 頁](https://www.goatfundedtrader.com/model)

* **費用：** Model 頁僅明示 Instant 為「pay a fee」及 Challenge 有「one-time 100% refundable fee」文案，未可靠列出本次選定 size 的數字；reset／activation／月費未驗證。
* **通關：** 可公開頁明示 Challenge 可為 1、2、3 steps；頁面展示的卡片有 target 10%（P1/P2/P3）、daily loss 4%、max loss 6%、`Consistency NO`、leverage up to 1:100，但該頁的動態卡片未清楚把它鎖定到某一 named model/size，故僅列為**頁面示例、不可外推**。另明示 unlimited trading period、news/weekend holding（頁面卡片）。
* **官方出金摩擦：** 同頁有「Rewards 5%」與「Profit Split 5%」的渲染文字但語意／產品對應不清，不能當 payout split；日期、下限、cap、buffer、KYC、rail 未驗證。
* **自動化：在可存取官方規則未找到明確限制**（非批准）；**避險／不活躍／免費試用：未驗證。**
* **TradingPilot（次要）：** 官方頁自己展示 `Listed on TradingPilot 4.7`（屬品牌轉載、非本研究獨立核驗）；不得視作 payout approval 或付款紀錄。[Model 頁](https://www.goatfundedtrader.com/model)

## 4. Atlas Funded

**可辨識產品：Atlas Access、Instant、Start Access、Free Access 等；本輪未找到可把完整規則鎖定至同一 size/platform 的公開方案表。** [官方 compare 頁](https://www.atlasfunded.com/compare)｜[Models](https://www.atlasfunded.com/models)

* **費用／reset／activation：未驗證。**
* **通關：** compare 頁有「100% profit split on funded accounts」及「No minimum trading days」的公開行銷文字，但不是完整、明確的單一方案規則表；target、DD、time limit、consistency 未驗證。
* **官方出金摩擦：** 頁面稱 `Reward Guarantee: Get paid in 24h or ... extra $1000`，但資格、最小額、cap/buffer、KYC、rail、完整審核條件未驗證；不把它寫成無條件付款保證。
* **自動化：在可存取官方規則未找到明確限制**（非批准）；**避險／不活躍／免費試用：未驗證**（雖有 `free-access` URL，未以該 URL 的規則證實其仍可用或條件）。
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。官方 compare 頁的「Verified Payout」推薦內容不是 TradingPilot 獨立追蹤，不能視作獨立證據。

## 5. The Trading Pit

**身份／產品：** 官方首頁可辨識其為 prop-trading service，並連至 Stocks Challenge；但本輪可存取首頁未產生精確計價／rule table 的可引用文字。[官方首頁](https://www.thetradingpit.com/)｜[官方 Stocks Challenge 資訊頁](https://info.thetradingpit.com/en-gb/the-stocks-challenge)

* **費用、reset、activation；通關（steps/target/DD/min-days/consistency/time）；官方出金摩擦；避險；不活躍終止；免費試用：未從可存取官方材料驗證。**
* **自動化：在可存取官方規則未找到明確限制**；這不是 bot/EA/API 許可。
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

## 6. For Traders

**產品：首頁稱 evaluations / virtual-capital accounts；精確產品例：Forex One-Step $100k。** [官方首頁與 FAQ](https://fortraders.com/)｜[官方 Help Center](https://help.fortraders.com/)

* **費用：** FAQ/首頁文字稱 one-time evaluation fee，並舉例 `$399` 對 `$100,000 Forex One-Step`；另說起始可低至 `$50`（$3,000 Crypto Instant Master）。頁面明示無 recurring monthly charges／hidden fees；reset/activation 未驗證。
* **通關：** 可存取文字未讓本輪將 target、DD、min days、consistency、time limit 可靠鎖到上述同一方案，故未驗證。
* **官方出金摩擦：** 頁面稱最多 90% split、平均 payout time 14h、48-hour Reward Guarantee，rail 列 Crypto、Rise、bank transfer/local payments；但條款明說：均為 demo/virtual-money accounts、payout discretionary and not guaranteed，且須 For Traders 接受及授權其 trading data。最低額、cap/buffer、完整 KYC/eligibility 未驗證。[首頁](https://fortraders.com/)
* **自動化：在可存取官方規則未找到明確限制**（非批准）；**避險／不活躍／免費試用：未驗證。**
* **TradingPilot（次要）：** 官方頁有「Best Payout Process Prop Firm Match」獎項，這是 Prop Firm Match 而非 TradingPilot；本輪無 TradingPilot 個別追蹤資料。

## 7. Get Leveraged (Leveraged)

**產品：Classic／Junior Portfolio Manager／Senior Portfolio Manager／Executive Portfolio Manager。** [官方 FAQ](https://getleveraged.com/faq/)｜[官方 Terms](https://getleveraged.com/terms/)

* **費用／通關／官方出金摩擦：** FAQ 目錄確實列有 simulation fee、是否 refundable、payout methods、payout time/fees、profit split、three DD definitions 與 consistency rule，但本輪頁面可存取內容未展開相應答案；不得從目錄推測數字或條件，故未驗證。
* **自動化：在可存取官方規則未找到明確限制**（FAQ 明列「Can I use Expert Advisors (EAs)?」問題但本次未取得其答案，**不能當允許**）。
* **避險／不活躍：** FAQ 有「Is there an inactivity rule?」「Prohibited Trading…」問題，但答案未驗證；避險未驗證。**免費試用：未驗證。**
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

## 8. Instant Funding

**精確可讀對照：Instant Funding/GO、IF Micro、One-Phase、One-Phase Micro、Two-Phase、Two-Phase Micro、crypto variants；不是同一規則。** [官方 Trading Rules](https://instantfunding.com/trading-rules/)｜[官方 FAQ sitemap 類目](https://instantfunding.com/help/)

* **費用：** 規則頁未提供可鎖定 size 的基本費／reset／activation；頁上促銷碼不作常態費用。**未驗證。**
* **通關：** 同頁對照表顯示：One-Phase target 10%、daily DD 4%、static DD 6%、min 3 days、無 time limit；Two-Phase target 8%+5%、daily 4%、static 8%、min 3 days、無 time limit。表中其他（Instant、Micro、Crypto）值不同，應逐欄查核而非套用。
* **官方出金摩擦：** Instant Funding／GO：首提須 initial trade 後 14 日、其後每 7 日；IF Micro／One-Phase Micro／Two-Phase Micro／IF Micro Crypto 可在符合資格時 On-Demand；One-Phase Crypto 有另列條件（本頁文字未完整顯示）。同頁另稱部分 One/Two Phase：總 gain 10% 且 first trade 至少 90 日。最小額、cap/buffer、split、KYC、rail 未在本輪驗證。
* **自動化：明確限制。** 官方規則禁止非本人帳戶的 copying；只允許本人帳戶間 copy（包括 prop/retail）；限制 public third-party EAs，並禁 group copying/account management。同一頁亦禁止 reverse trading 和 group hedging。這不是全面禁止自寫 EA 的完整答案，因此特定 bot 須書面確認。
* **避險：明確限制**（reverse trading/group hedging prohibited）。**不活躍終止／免費試用：未驗證。**
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

## 9. FundedElite

**可辨識方案 URL：Catalyst、Custom Challenge、Flash Activation、Free Retry、Instant Funding、Lite Account。** [官方 Challenges](https://fundedelite.com/challenges)｜[官方 Terms](https://fundedelite.com/terms-and-conditions)

* **費用／通關／官方出金摩擦：** Challenges 頁在本次機器可讀回應沒有展開方案值；不以搜尋摘要或第三方補填，故所有數字與條件未驗證。
* **自動化：在可存取官方規則未找到明確限制**（非批准）；**避險／不活躍：未驗證。**
* **免費試用：** `Free Retry` 是官方方案 URL 名稱，不足以證明免費 trial；**免費試用未驗證。**
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

## 10. BrightFunded

**精確產品：1-Step、2-Step Bright（以下為頁面 $5k 示例；頁面還列不同 size）。** [1-Step 官方頁](https://brightfunded.com/1-step)｜[2-Step Bright 官方頁](https://brightfunded.com/2-step-bright)｜[Terms](https://brightfunded.com/terms-and-conditions)

* **費用：** $5k 顯示促銷價：1-Step `€34.30`（原 €49；頁面文字另有 account fee，與顯示單位/區塊呈現有歧義），2-Step Bright `€35.25`（原 €47）。只能視為 2026-08-11 頁面示例；subscription/reset/activation 未驗證。頁面列 `100% Challenge Fee Refund` 為 add-on。
* **通關：** 1-Step：target 10%、daily loss 3%、total loss 6%、min 5 days；2-Step Bright：8%（P1）+5%（P2）、daily 4%、total 8%、min 5 days。兩者頁面均稱 unlimited trading period、no consistency rules；「No minimum trading days」是 add-on，不能覆蓋基本 5 日。
* **官方出金摩擦：** up to 90% payout ratio，頁面稱 `24-Hour Guaranteed Payouts`；weekly payouts、90% ratio 皆可作 add-on。最低額、cap/buffer、KYC、rail、觸發條件未驗證，因此不將 24h 當無條件保證。
* **自動化：在可存取官方規則未找到明確限制**（非批准）。**避險／不活躍／免費試用：未驗證。** 有 `Free 1K Challenge` 官網 URL，但未查得其完整條件，故不稱 free trial。[Free 1K Challenge](https://brightfunded.com/free-1k-challenge)
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

## 11. PineX Capital

**精確產品：1-Step Challenge、2-Step Challenge；同頁還列 Instant Funding/PineX Elite，勿混用。** [1-Step](https://pinexcapital.com/en/1-step-challenge)｜[2-Step](https://pinexcapital.com/en/2-step-challenge)｜[Rules](https://pinexcapital.com/en/rules)｜[Payouts](https://pinexcapital.com/en/payouts)

* **費用：** 頁面稱 one-time fee、100% fee refund 從第 3 次 payout 開始；checkout add-ons 及夏季折扣會改價。本輪未可可靠引用選定 size 的結帳基價，reset/activation 未驗證。
* **通關：** 1-Step 宣稱 9% target、static drawdown、no consistency、無 time limit；但其內嵌 checkout 摘要又可見 `Profit Target 8% | 5% / daily 4% / total 10%`，看起來是另一個選項，故不把後者套入 1-Step。2-Step 清楚列 8% + 5%，daily DD 4%、overall 10%，無 time limit，沒有 consistency rule。
* **官方出金摩擦：** 1-Step頁稱每 14 trading days 可 request、`up to 100%` split（add-on）、第 3 payout 起退費，核實後 2 business days 內處理。2-Step 亦稱每 14 日（需 add-on）及 2 business days。最低額、cap/buffer、KYC、rail 未驗證；「within 2 business days」仍以 verification 為前提。
* **自動化：在可存取官方規則未找到明確限制**（非批准）；**避險／不活躍／免費試用：未驗證。** `Free Pay Later Challenge` 是頁面促銷／bonus 字樣，並非已證明的免費試用。
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

## 12. ThinkCapital

**精確產品：$5k 示例的 One Step、Dual Step Intraday、Nexus 3-Step、BOLT Instant Funding。** [官方首頁／產品表](https://www.thinkcapital.com/)｜[Payout FAQ](https://www.thinkcapital.com/tc-faqs/funded-accounts/how-often-are-payouts-offered-for-simulated-funded-accounts/)｜[Inactivity FAQ](https://www.thinkcapital.com/tc-faqs/funded-accounts/how-long-can-i-be-inactive-on-a-trading-account/)

* **費用：** 顯示起價：One Step $5k `$59`、Dual Step $5k `$59`、Nexus $5k `$39`、BOLT $2.5k `$49`。此為頁面顯示起價；reset/activation/訂閱未驗證。
* **通關：** One Step：10%、daily 3%、challenge/funded max 6%、min 3 days、1:30；Dual Step：9%+5%、daily 4%、challenge max 7%、funded max 8%、min 3 days、dynamic up to 1:100；Nexus：7%+6%+5%、daily 4%、challenge/funded max 8%、min 3 days、1:100；BOLT：no evaluation、min days 無、dynamic up to 1:50（頁面未列 BOLT DD/target 值）。頁面未見各方案 consistency 或 time limit 值，標未驗證。
* **官方出金摩擦：** One/Dual/Nexus 顯示 up to 90% split、14 days（7-day add-on）；BOLT 14 days。rails 顯示 USDT、USDC、Rise Global Payouts、轉至 ThinkMarkets 個人帳戶，且某些 resident（包括 US）不可用。最低額、cap/buffer、完整 KYC 與 eligibility 未驗證。
* **自動化：在可存取官方規則未找到明確限制**（非批准）。**避險：未驗證。**
* **不活躍／免費試用：** 官網有 `Free Trial` / `ThinkTrader Trial Program` 入口，但本輪未讀到條件；同理雖有專屬 inactivity FAQ，未驗證答案，不能報天數或終止後果。
* **TradingPilot（次要）：** 未取得可引用個別追蹤頁。

---

## 購買前逐產品書面確認清單

1. 選定**產品、階段、帳戶 size、平台、居住地**後，確認一次性費、月費、reset、activation、退款和促銷到期日。
2. 確認 exact target、DD 的起算/浮動/靜態/尾隨定義、最少 profitable days、consistency、time/inactivity clock。
3. 確認出金的第一筆等待期、Q/profitable days、最低額、最大額／buffer／cap、split、KYC/tax、rail、地域限制和人工風控拒絕條款。
4. 若使用自寫、交易者控制的非 HFT 自動化：確認 EA/API/webhook 是否對**該產品階段**准許；order-rate、持倉時間、訊號來源、同人多帳戶 copier、跨 firm copy、對沖/反向單、news 行為及 payout eligibility。平台支援或未見禁令都不等於批准。

## 官方來源索引

以上每個超連結均為相應品牌的一手頁面，於 **2026-08-11** 讀取。動態首頁、checkout 和 Help Center 可能在後續改版；本文對未展開、衝突或無法綁定到精確產品的欄位一律保留「未驗證」，而非使用第三方比較表補值。
