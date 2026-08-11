# TradingPilot catalog：核心業者 B — 官方規則／出金摩擦核對

**檢索日：2026-08-11。** 本文件是產品／階段比較，不是投資建議或付款成功率預測。數字只在來源明確對應該產品時才列出；「未載明」不代表允許或不存在。價格為頁面顯示值，動態結帳、折扣和地區稅費可能不同。

## 方法與讀法

* **自動化三分類**嚴格採用：**明確允許**／**明確限制**／**在可存取官方規則未找到明確限制**（後者絕非批准）。平台、API、copy 工具存在本身也不證明 bot 合規。
* **避險**只報官方明示的 hedge／opposing-position 規則；未找到即「未載明」。
* **出金狀況**分開寫：(a) 官方的資格、週期、最低額、分潤、緩衝／KYC／支付軌道；(b) TradingPilot 的付款追蹤（次級證據）。本次可存取的 TradingPilot catalog 沒有可逐筆核驗、可引用的對應 payout-record URL，故各項均記為「未取得」；不可把它當作官方出金證據。
* 幾個網站回傳 WAF `403`（Nordic Funder、FXIFY、E8），這只表示本次無法讀到，不表示政策不存在或公司停業。

---

## 1. Darwinex Zero — 訂閱式 track-record／DarwinIA 路徑（非典型「挑戰通關即出金」）

**官方來源：**[價格與 FAQ](https://www.darwinexzero.com/zh/pricing)、[使用條款](https://www.darwinexzero.com/zh/legal/terms-of-use)。

* **費用：**會員是月／年／三年期的**訂閱**，可隨時取消；官方頁面本次將實際金額作為動態選項，未能可靠擷取固定方案價，故不轉錄。重啟帳戶要付「default monthly」restart fee，期貨另有 market-data fee；無獨立 activation fee／reset 固定額的可核驗公開值。
* **資格／規則：**校準期要至少 **25 個 risk-equivalent trading decisions、至少 15 個交易日**；**沒有報酬率要求**，達標後才自動建立 DARWIN 並進入符合資格的 DarwinIA Silver。這不是 1/2-step profit-target 挑戰；DD、時間上限、consistency 的公開固定門檻：未載明。
* **獎酬／出金摩擦：**官方頁將其定位為投資人資金／DarwinIA 路徑與 **15% real performance fees**，不是公開保證的 funded-account payout 表。最低額、申請 cadence、cap、buffer、KYC 與付款軌道：未載明，須以投資／合約文件確認。
* **自動化：**在可存取頁未找到 Zero 對 EA/bot 的明確准／禁規則，分類：**未找到明確限制（非批准）**。**避險：**未載明。**不活躍終止：**未載明（訂閱可取消）。**免費試用：**未載明。**TradingPilot 追蹤：**未取得可核驗記錄。

## 2. Trade The Pool — Day Trading FLEX／MAX；Swing FLEX／MAX

**官方來源：**[The Program（方案表、FAQ）](https://tradethepool.com/the-program/)、[Funded phase](https://tradethepool.com/funded-phase/)、[Program terms](https://tradethepool.com/program-terms/)。

* **費用：**Day $5K 顯示起價 FLEX **$59**、MAX **$47**（同頁其他帳戶／促銷／reset 未能逐一固定）；官方稱 realtime data 免費。Swing 購買費、reset／activation 固定額：本次未可靠擷取，勿外推。
* **通關：**Day FLEX/MAX：target **6%**；FLEX daily pause **2%**／max loss **4%**／至少 10 positions／無時限；MAX daily pause **1%**／max loss **3%**／至少 20 positions／**60 日**。評估期 FLEX 50%、MAX 30% best-position consistency。Swing FLEX/MAX：target **15%**、max DD **7%**、至少 5 positions；FLEX 30%／MAX 50% single-best-trade consistency，FLEX 60 日、MAX 無時限。這些是該頁版別的 exact product 描述，非所有舊方案。
* **出金摩擦（funded）：**分潤 **70/30（客戶／TTP）**；首次或前次出金後 **14 日**才可請款；最低 profit **$300**（$5K 為 $150）。FLEX 還需任一 14 日內 **3 個**各至少 buying power **0.5%** 獲利日。官方列 rail：wire、crypto、Hub credits 或 credit card；通常 **3–5 工作日**，且受國家／機構影響。buffer/cap/KYC：該頁未載明。
* **自動化：**外部或自動第三方 copy software **嚴禁**；手動 copy 在 evaluation/funded 可用，但平台的 copy feature 只限 evaluation。這不足以涵蓋所有 EA，故分類：**明確限制**（至少第三方自動複製）。**避險：**未載明。**不活躍終止：**未載明。**免費試用：**Day 與 Swing demo **14 日**。**TradingPilot：**未取得可核驗記錄。

## 3. Nordic Funder

**官方來源／限制：**[官方網域](https://nordicfunder.com/) 本次回應 `403`，未能讀取公開條款或確認當前產品。

* **產品／費用／reset／activation、pass steps/target/DD/min-days/consistency/time limit、官方 payout 摩擦、避險、不活躍、free trial：****未知／未能由可存取官方資料核驗**。
* **自動化：****未知／未能由可存取官方規則核驗**（不可改寫成允許）。
* **TradingPilot：**未取得可核驗 payout/review record。購買前應向合規／support 索取所選帳戶 size、平台及 stage 的完整規則與 payout policy。

## 4. Finotive Funding — Challenge（One Stage／Two Stages）、Instant、Pro、Standard、Lite

**官方來源：**[Trading Rules & Objectives](https://finotivefunding.com/rules)、[Payouts](https://finotivefunding.com/payouts)、[Terms](https://finotivefunding.com/terms-and-conditions)。

* **費用：**規則頁未提供可固定引用的 checkout 價格、reset 或 activation 額，均未載明（帳戶大小／promo 會改變）。
* **通關／風控（規則頁所列）：**One Stage daily DD **4%**、overall **7.5%**、**3 日各 0.5%**；Two Stages daily **4.5%**、overall **9%**、每階及每次 payout 前 **2 日各 0.5%**；Instant/Standard/Lite 的頁列 daily/overall 分別為 **4%/8%**、**3.5%/7%**、**3%/6%**，profitable-days 分別 3／0／5（Lite 為 5 日 0.5%）。觸及 daily／overall DD 是 hard breach，立即關戶且不退款。Profit target、時間限制因卡片／選項未能在公開 HTML 中無歧義配對，未填。
* **額外摩擦：**notional-volume 違規在 funded 每次使下一 payout cycle 降為 **10%**；Pro 又要求每 7 日 trade count/instrument volume 落在 challenge 與 funded 前 30 日平均的 ±25%，且每 90 日（已實現 payout + balance）至少初始餘額 **5%**，否則降回 Standard 並永久失 salary/100% split。這是極重要的 payout-compliance 限制。
* **官方 payout：**規則頁描述 eligibility 後 payout review；但最小額、cadence、split、buffer、KYC、rail 需對帳戶／dashboard 確認，公開規則頁未完整列出，故不臆測。**自動化／避險／不活躍／free trial：**可存取的 rules 頁未對 exact plan 明示，分別「未找到明確限制（非批准）／未載明／未載明／未載明」。**TradingPilot：**未取得可核驗記錄；官方另有自稱 verified payout 頁，這是業者自有陳述，非 TradingPilot 亦非獨立保證。

## 5. Axi Select — Seed → Incubation → Acceleration → Pro → Pro 500 → Pro M

**官方來源：**[Axi Select / Funded Trader Program](https://www.axi.com/int/funded-trader-program)。

* **費用：**加入沒有 registration 或 monthly fee；但有標準交易費，且要最低 **US$500** 自有 equity/deposit。無公開 reset/activation 固定額。
* **入場與各 stage：**先完成 **20 closed trades**、Edge score ≥**50**、存入 ≥$500。官方 pathway：Seed/Incubation/Acceleration/Pro/Pro500/ProM 的最低 equity **$500/$1k/$2k/$5k/$10k/$20k**，Edge **50/60/70/90/90/90**；allocation target 前五階 **7%**（Pro M 無），min stage days **30/60/60/60/60/—**，trades **20/40/50/50/50/—**，maximum loss **-7%**（Pro M **-10%**）。最大 funding 依序 $5k/$20k/$100k/$200k/$500k/$1m；profit share **0/40/50/60/70/80%**。
* **payout：**官方明示最高 split 80%，但申請週期、最低額、buffer、KYC、rail：該公開 pathway 未載明。提領自有資金後仍須維持當階最低 equity 才能留在 program，為持續資格摩擦。
* **自動化：**官方頁未對 EA/API/bot 明示，**未找到明確限制（非批准）**。**避險／不活躍終止／free trial：**未載明。**TradingPilot：**未取得可核驗記錄。

## 6. FXIFY

**官方來源／限制：**[官方網域](https://fxify.com/) 本次 `403`；不能取得 exact product 規則。

* **所有要求欄位（費用、pass、payout 摩擦、自動化、hedge、不活躍、free trial）：**未知／未能由本次可存取官方資料核驗；自動化不可視為批准。
* **TradingPilot：**未取得可核驗 payout/review record。

## 7. Alpha Capital Group — Alpha One／Qualified Analyst（公開 checkout 所示）

**官方來源：**[Alpha Capital 首頁與動態 checkout](https://alphacapitalgroup.uk/)、[Terms](https://alphacapitalgroup.uk/terms-and-conditions)、[Help Centre](https://help.alphacapitalgroup.uk/)。

* **費用：**checkout 顯示 **one-time payment**，可選 Bi-Weekly 或 On-Demand payout schedule；90% PFS 是在 On-Demand 價格上 +10%，關閉 overnight swap/rollover 約 +10%。因本次選擇器未固定 size/option，無可誠實引用的基礎價格；reset/activation 未載明。
* **階段／規則：**首頁稱通過 evaluation phases 到 Qualified Account；Qualified stage **0% profit target**、最高 **90%** performance fee。評估 target/DD/min days/consistency/time limit 必須取決於所選 Alpha One/size/option，公開頁未能可靠一一擷取，未填。
* **payout：**頁面將 payout schedule 指為 bi-weekly 或 on-demand；split 標準 80% 或加價至 90%。最低額、buffer/cap、KYC/rail 未載明。官方也明說為 simulated environment／performance fee，不是實盤資金承諾。
* **自動化：**MT5 文案稱「Expert Advisors supported」，因此對 **MT5 EA：明確允許**；不等同第三方訊號、copier、HFT 或其他平台的批准。**避險／不活躍／free trial：**未載明。**TradingPilot：**未取得可核驗記錄。

## 8. E8 Markets

**官方來源／限制：**[官方網域](https://e8markets.com/) 本次 `403`；未取得 exact plan 或官方 help article。

* **費用、通關、出金、自動化、避險、不活躍、free trial：**未知／未能從可存取的官方材料核驗。自動化分類為 **未知**，不是「無限制」。
* **TradingPilot：**未取得可核驗 payout/review record。

## 9. Ment Funding — Forex 1-step；Futures Static／Trailing；Equities

**官方來源：**[Ment Funding 首頁／Evaluation Rules](https://mentfunding.com/)、[refund policy](https://mentfunding.com/refund-policy.html)。

* **費用：**動態選擇器顯示，例如 Forex $1m price **$9,750**（促銷顯示 $8,600）；這不是所有 size 的價格。reset/activation 未載明。
* **Forex 1-step：**target **10%**、無時間上限、**6% static DD**、daily stop 為前一日 5pm EST balance 的 5%、**75% split**，無 consistency；funded 無 target。> $1m 帳戶 payout 前另有 **35% consistency**。EAs、hedging、scalping 「all permitted」：**自動化明確允許、避險明確允許**。
* **Futures：**Static target **15%**；Trailing target **10%**；無時限、兩者 max DD **6%**。頁面列 **33% consistency**（評估且每次 payout 前）、至少 3 個各 0.5% 日。分潤 **75%**；首次達 target/consistency 後、之後每 **30 日**可提款。最小額/KYC/rail 未載明。
* **Equities：**target 未能從選擇器無歧義取得；無時限、DD **6%**；**80% split**，payout 每 **14 日**、最低 **$100**，要 33% consistency 與至少 3 個 0.5% 日；最少持倉 1 分鐘。首次 payout 14 日後。官方沒有針對 Equities 明示 bot/hedging，故自動化為**未找到明確限制（非批准）**，hedge 未載明。
* **不活躍：**三類規則均稱至少每 **30 日**開或平一筆，否則 breach。**免費試用：**未載明（有 $0 月度 competition，但不是評估 free trial）。**TradingPilot：**未取得可核驗記錄；「not a single refused payout」是業者聲稱，不能替代 tracker。

## 10. City Traders Imperium (CTI) — 1-Step Challenge（明確產品）；其他 2-Step/Instant/Direct 僅作名稱，不混用

**官方來源：**[1-Step Challenge](https://citytradersimperium.com/1-step-challenge/)、[Terms](https://citytradersimperium.com/terms-conditions-of-service/)、[Payout proof](https://citytradersimperium.com/cti-payout-proof/)。

* **費用：**1-Step 為 one-time payment：$2.5K **$29**、$5K **$49**、$10K **$79**（頁面還有更大 sizes，未逐項轉錄）；failure reset 是當時費用 **15% discount**，非固定 reset 價。activation 未載明。
* **通關：**一階、target **8%**、balance-based **5% trailing DD**、無 daily DD、至少 **3 profitable days**、無時限；達標後 funded。新聞、overnight/weekend hold、martingale 都允許。
* **payout：**first payout **7 日**；起始分潤 **80%**，VIP 可到 90%/100%並解鎖 weekly/anytime payout。最小額、KYC、rail、cap：該頁未載明。DD floor 是 high-watermark − 初始餘額 5%，而且官方說 withdrawals 不影響其計算，故不應虛構「payout buffer」。
* **自動化：**1-Step 明示 EA（含 third-party）允許，分類 **明確允許**。注意 2-Step/Instant/Direct 只允許 personal EA，不能移植到 1-Step 以外。**避險：**未載明。**不活躍：**未載明。**免費試用：**有「no card needed」1-Step free trial。**TradingPilot：**未取得可核驗記錄；CTI 自家 payout proof 非次級 tracker。

## 11. Lark Funding

**官方來源／限制：**[官方網域](https://larkfunding.com/) 本次雖回 200，但可讀頁面未提供可核驗的產品、規則或公司服務內容（僅極少 HTML），故不能把同名／舊資料拼入。

* **身分／產品及所有欄位：**目前官方可存取材料不足，**未知／未核驗**。費用、pass、payout、自動化、hedge、不活躍、free trial 均不作猜測；自動化為**未知**。
* **TradingPilot：**未取得可核驗 payout/review record。

## 12. The5ers — 多產品（High Stakes／Bootcamp／Hyper Growth／ProGrowth／Futures）

**官方來源：**[FAQ index](https://the5ers.com/faqs/)、[Futures FAQ](https://the5ers.com/futures-faqs/)、[Prohibited Trading Practices](https://the5ers.com/faqs/prohibited-trading-practices/)、[Terms](https://the5ers.com/terms-and-conditions/)。

* 本次可存取官方 FAQ index 證實上述產品分列（且 Futures 是新類別），但文章內容／動態 checkout 未讓每個 product/size 的當期 fee、target、DD、min-days、consistency、time limit 可逐條可靠擷取。因此**不將網路常見的舊 High Stakes／Bootcamp 數值套用到現行產品**。
* **費用、reset、activation；official payout eligibility/cadence/min/cap/buffer/split/KYC/rail；hedge；inactivity；free trial：**按 exact plan 均**未核驗／未載明於本次可存取頁**。
* **自動化：**可存取索引與 prohibited-practices 入口不足以對 specific product/stage 給 EA/bot 批准，分類 **未找到明確限制（非批准）**；購買前須取得該方案的 written confirmation，尤其是 own EA、copy、signals、HFT、對沖／對向倉。
* **TradingPilot：**未取得可核驗 payout/review record。

---

## 結論／購買前書面確認清單

上述缺口應由業者 **compliance/support** 對「產品名 + account size + platform + evaluation/funded stage」書面回答：1) checkout 最終費、tax、activation/reset；2) equity/balance DD 的起算／trailing／daily reset；3) target、最少日、consistency 與 deadline；4) payout first/recurring cadence、最低額、cap、buffer、split、KYC、rail、審核拒絕條件；5) 自有 EA、第三方訊號／copier、VPS/API、HFT、新聞、跨帳戶及反向／hedging 是否可用；6) inactivity 的定義與關戶；7) demo/free trial 是否會建立任何付費義務。不要把本比較或 TradingPilot review 當作該書面確認的替代品。
