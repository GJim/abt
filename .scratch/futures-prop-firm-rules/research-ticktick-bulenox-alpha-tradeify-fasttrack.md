# 五家期貨 prop firm：官方規則核對（NQ/MNQ、考核、提款）

**查閱日：2026-08-10。** 本文件只採各公司自有網域／官方 Help Center；不以搜尋摘要、評論站或第三方比較補足缺口。金額均為來源所示 USD。`未載明` 意味著本次可取得的官方頁面沒有明確說明，**不是**允許、禁止或適用於任何居住地的推論。尤其本文件不判定台灣或任何司法管轄區可否開戶。

## 先讀：資料品質與範圍

|公司|官方來源取得情形／結論|
|---|---|
|TickTickTrader|主站及已知 Help Center 皆回 Cloudflare「security verification」；無法取得可核驗規則正文。因此除「未能由本次官方材料確認」外，不作數字或產品推論。|
|Bulenox|可讀取 Help Center；但頁面本身標示更新於 2021–2025，且相互有矛盾，見下方「衝突／過時」。|
|Alpha Futures|可讀取主站與官方 Help Center；採 Help Center 中最近更新的頁面，並保留其與價格頁不同處。|
|Tradeify|可讀取主站的即時價格／方案資料；其 Help Center 對本次請求為 403。因此只記錄主站明列內容；未由它補寫提款的門檻、KYC 或新聞限制。|
|Fast Track Trading|`https://fasttracktrading.net/` TLS handshake 失敗，且未找到本次可讀的官方規則頁；不以第三方轉述替代。|

## 總覽：標的 NQ / MNQ 的「明確」官方可用性

|公司|結論|官方證據（精確 URL；查閱 2026-08-10）|
|---|---|---|
|TickTickTrader|**未能確認**。遭 Cloudflare 驗證，沒有可讀的官方合約清單。|https://tickticktrader.com/ （Cloudflare 驗證頁）|
|Bulenox|**明確列出 NQ 與 MNQ。** Qualification Account 的「List of Futures Instruments」列 `E-mini NASDAQ 100 (NQ)`、`Micro E-Mini Nasdaq-100 (MNQ)`。|https://bulenox.com/help/qualification-account/|
|Alpha Futures|**未載明 NQ 或 MNQ 的明確可交易合約清單。** 官方有以 `NQ` 作為新聞規則範例，且方案以 mini/micro 合約數表示；這不足以證明 NQ/MNQ 可交易，故不推論。|https://help.alpha-futures.com/en/articles/9492063-news-trading-policy；https://alpha-futures.com/|
|Tradeify|**未載明 NQ 或 MNQ ticker 的明確合約清單。** 主站僅籠統寫 Nasdaq／期貨市場，不能當作 NQ/MNQ 上架證據。|https://tradeify.co/|
|Fast Track Trading|**未能確認。** 官方站在查閱時 TLS 無法建立連線。|https://fasttracktrading.net/|

---

## 1. TickTickTrader

### 方案／考核／限制

|主張|官方證據|
|---|---|
|考核方案、profit target、drawdown 類型／金額、交易天數、一致性、持倉／新聞／時段限制、訂閱／reset：**未能確認**。|https://tickticktrader.com/；https://help.tickticktrader.com/ — 兩者於查閱時均為 Cloudflare 403／security verification，非規則正文。|

### 提款／報酬

|階段／產品|可核驗規則|
|---|---|
|所有階段|資格、每次／每期上限、頻率、buffer、最低交易日、split／費用及 KYC：**未能確認**（同上官方存取阻斷）。|

**待書面確認清單：** NQ/MNQ 是否可交易、每一產品與階段的 payout policy、KYC／付款服務商及居住地可用性。

---

## 2. Bulenox

### Qualification（考核）與 Master / Funded 的規則

|階段／方案|可核驗目標與限制|官方證據|
|---|---|---|
|Qualification，通用|目標是達到 profit target 才進 Master；官方 Help Center 說**無最低交易日**、沒有最多日數，交易日為開過一筆交易的日子；交易日為 17:00–翌日16:00 CT／CST，週末假日不算，所有倉位 15:59 前平倉。可同時交易 standard 與 micro，1 standard = 10 micro。|https://bulenox.com/help/qualification-account/|
|Qualification Option 1「No Scaling」|trailing drawdown 會依帳戶當前／最高 balance 對已實現及未實現損益即時追蹤，含 commission；違反帳戶被封，可 reset 或新建帳戶。精確 drawdown／profit target 依所選帳戶，該頁未給完整價目表。|https://bulenox.com/help/qualification-account/|
|Qualification Option 2「EOD」|EOD drawdown 只於日終新高更新；Qualification 後 Master 於 initial balance + $100 停止移動。每日 loss limit（含 commission、即時/未實現）為 10K $400、25K $500、50K $1,100、100K $2,200、150K $3,300、250K $4,500；觸發只暫停至下個交易日，不算 rule violation。列有帳戶規模的 dynamic scaling（例如 50K：$0–1,500 為2 contracts、$1,501–4,000為4、$4,001+為7）。|https://bulenox.com/help/qualification-account/|
|Qualification reset／訂閱|reset 會重置初始 balance、既有損益及交易日，不改訂閱到期；訂閱每30日自動續，取消前持續。下個 billing date 前違規，該日期 free reset，已完成交易日保留；其他時間 reset $78。|https://bulenox.com/help/qualification-account/|
|Master 進入條件|Master 頁稱：達 profit target **且至少 1 個交易日**後送審，驗證最長24小時；要求 questionnaire／contract。Qualification 訂閱會由管理員取消。|https://bulenox.com/help/master-account/|
|Master|需完成申請與簽約才開戶；drawdown 規則同 Qualification、不可 reset，trailing/EOD 至 initial starting balance +$100 停止。TDA：25K $1,500、50K $2,500、100K $3,000、150K $4,500、250K $5,500；超過則關戶。|https://bulenox.com/help/master-account/|
|Funded（real capital）|完成 Master 的 3 次 successful payouts，且由 Risk Management 自行裁量，才轉 Funded；所有 active Master 併為一個 Funded。餘額 caps：25K $2,500、50K $5,000、100K $10,000、150K $15,000、250K $25,000。|https://bulenox.com/help/funded-account/|

**未載明／不作推論：** Qualification 各帳戶完整 profit-target 表；新聞交易限制；Option 1/2 對各商品的時段以外限制；KYC 的明確身分驗證流程。

### 提款／報酬（必須與階段分開）

|階段|資格、金額、頻率、buffer、split／費用、稅務／KYC|官方證據|
|---|---|---|
|Master|至少 **10 individual trading days**才可處理 payout；每月任何時間可申請、每週三處理；最低 $1,000。首3筆上限：25K $1,000／50K $1,500／100K $1,750／150K $2,000／250K $2,500；第3筆後無 maximum。|https://bulenox.com/help/master-account/|
|Master|提款後須留 safety threshold：25K $1,600／50K $2,600／100K $3,100／150K $4,600／250K $5,600；終止 Master Agreement 時才可取回該 reserve。每次提款套 40% consistency（最佳單日不得超過 total profit 的40%；首提的 safety threshold 也算入）。|https://bulenox.com/help/master-account/|
|Master|首 $10,000 提款公司不收 commission；之後公司10%、交易者90%。方式為 ACH/Wire、PayPal、Wise。申請提款時寄稅表；美國為 1099-Misc、外國人 W-8BEN（FAQ 另稱 1099-NEC，見衝突）。|https://bulenox.com/help/master-account/；https://bulenox.com/help/frequently-asked-questions/|
|Funded|任一 reward 至少 **5 individual trading days**才可處理；關於 Funded 的 reward 上限、頻率、split、reserve、方式／KYC：**未載明**。|https://bulenox.com/help/funded-account/|

### Bulenox 衝突／過時警示

* Qualification 頁說「無最低交易日」，但 Master 進入段落說至少1日；對「通過考核」應以較具體的 Master 進入條件處理，並應預先書面確認。
* Master 頁提款段落標「Updated 2/21/2023」且稱 1099-Misc；FAQ 稱 1099-NEC。不可自行挑一個作稅務結論。
* Qualification 的多帳戶段落同頁既稱最多11個 active Master，又另稱同時先啟動最多3個並以條件逐一增加；需書面確認。
* 官方 FAQ 將 **Taiwan** 列為 restricted countries（清單說可隨時變更）；這是來源明示的名單，不等於本文件判定任何人的資格。URL：https://bulenox.com/help/frequently-asked-questions/

---

## 3. Alpha Futures

### 產品與考核規則（Evaluation 與 Qualified 分列）

|產品／階段|目標、drawdown、交易日、一致性、限制、費用／reset|官方證據|
|---|---|---|
|Zero Evaluation（25K／50K／100K）|profit target $1,500／$3,000／$6,000；MLL $1,000／$2,000／$3,000；Daily Loss Guard $500／$1,000／$2,000；最大 1/3/6 minis 或10/30/60 micros；**1日**，無 Evaluation consistency、無新聞限制。月費 $79/$119/$239；reset $69/$109/$219。|https://help.alpha-futures.com/en/articles/11771813-zero-account-overview；https://alpha-futures.com/|
|Standard Evaluation（50K／100K／150K）|target $3,000/$6,000/$9,000；MLL $2,000/$3,000/$4,500；最大5/10/15 minis 或50/100/150 micros；最低2日；50% consistency；Evaluation 無 Daily Loss Guard、無新聞限制。主站列月費 $129/$239/$349、reset $109/$199/$289。|https://help.alpha-futures.com/en/articles/11632512-standard-account-overview；https://alpha-futures.com/|
|Advanced Evaluation（50K／100K／150K）|target $4,000/$8,000/$12,000；MLL $1,750/$3,500/$5,250；最大5/10/15 minis 或50/100/150 micros；最低3日；40% consistency；無 Daily Loss Guard、無新聞限制、無 scaling。主站列月費 $209/$349/$489、reset $189/$319/$449。|https://help.alpha-futures.com/en/articles/11634907-advanced-account-overview；https://alpha-futures.com/|
|共同 MLL 定義|所有帳戶皆 **EOD trailing**，不是 intraday trailing；按日終最高帳戶 balance 算，到起始 balance 停止 trailing；浮動或已實現 equity 碰到 MLL 即 liquidate。|https://help.alpha-futures.com/en/articles/9491999-maximum-loss-limit-mll|
|Zero / Standard Qualified|最低5個 winning trading days（每個至少 $200）才可申請；Zero／Standard Qualified 各為40% consistency。Zero 有 Daily Loss Guard；Standard Qualified 主站列 $1K/$2K/$3K。|https://help.alpha-futures.com/en/articles/9492051-payout-policy；https://alpha-futures.com/|
|Advanced Qualified|最低5個 $200 winning days；無 consistency、無 Daily Loss Guard、無新聞限制、無 scaling。|https://help.alpha-futures.com/en/articles/11634907-advanced-account-overview；https://help.alpha-futures.com/en/articles/9492051-payout-policy|
|Direct Qualified（無 Evaluation）|一次性費用 $349/$519/$689/$859（25/50/100/150K）、無月訂閱；MLL $1K/$2K/$3K/$4.5K，Daily Loss Guard $500/$1K/$2K/$3K，最大2/4/8/10 minis 或20/40/80/100 micros，無 scaling。每次提款要符合20% consistency及 fresh withdrawal target：首次 $1.5K/$3K/$6K/$9K，之後 $1K/$2K/$4K/$6K；不需 winning-day 條件。|https://help.alpha-futures.com/en/articles/15838742-direct-account-overview；https://help.alpha-futures.com/en/articles/9492051-payout-policy|
|新聞限制|所有 Evaluation 無限制；Advanced Qualified 無限制。Zero、Standard、Direct Qualified 可以持倉穿越新聞，但不得在 ForexFactory red-folder high-impact event 前／後2分鐘執行 orders；首次違反警告、其交易利潤 void 且 payout denied，之後違反 breach。|https://help.alpha-futures.com/en/articles/9492063-news-trading-policy|
|訂閱／reset|Evaluation 自註冊日起每月 rebill，直到通過或取消；通過即停止。reset 不改 rebill 日。Qualified reset 只給 Zero/Standard，帳戶從未達 payout request 才能用、單一帳戶最多2次、breach後7日內；Zero $399/$499/$799，Standard $599/$799/$999。|https://help.alpha-futures.com/en/articles/9492068-monthly-subscription；https://help.alpha-futures.com/en/articles/9492077-reset|

### Alpha 提款／performance fee

|產品／階段|資格與頻率|金額／buffer／split／方式|官方證據|
|---|---|---|---|
|Zero、Standard、Advanced Qualified|最多每月4次；每累積5個非連續、每個至少$200的 winning days 可申請。每次最多提帳上 profit 的50%（且受產品上限）；餘額留帳上作 drawdown／未來提款。|交易者收到申請額的90%（即10% fee/split）。|https://help.alpha-futures.com/en/articles/9492051-payout-policy|
|Advanced Qualified|最低 $1,000；每帳戶每次最高 $15,000；無 consistency。|該 $15,000 是「per request, per account」。|https://help.alpha-futures.com/en/articles/9492051-payout-policy；https://help.alpha-futures.com/en/articles/10491202-maximum-withdrawal-request|
|Zero Qualified|最低 $200；上限25K $1,000、50K $1,500、100K $2,500；40% consistency。|同上90%。|https://help.alpha-futures.com/en/articles/9492051-payout-policy|
|Standard Qualified|最低 $500；上限50K $3,000、100K $4,000、150K $5,000；40% consistency。|同上90%。|https://help.alpha-futures.com/en/articles/9492051-payout-policy；https://help.alpha-futures.com/en/articles/10491202-maximum-withdrawal-request|
|Direct Qualified|無 winning-day 需求；符合每次 fresh target＋20% consistency即可，最多每月4次。上限25/50/100/150K為$1K/$1.5K/$2.5K/$3K。|90% split；一次提款後目標重置，舊週期 leftover profit 不帶入。|https://help.alpha-futures.com/en/articles/15838742-direct-account-overview；https://help.alpha-futures.com/en/articles/10491202-maximum-withdrawal-request|
|Live program|無 maximum withdrawal request。其他 live 的資格、split／頻率：**本次頁面未載明**。|—|https://help.alpha-futures.com/en/articles/10491202-maximum-withdrawal-request|
|付款／KYC|付款方式 ACH（美國銀行）、Wire/SWIFT、Wise、Rise；均 USD。選 Rise 首次 payout 前會寄 agreement 要簽。一般身分 KYC 要求／費用：**未載明**。|https://help.alpha-futures.com/en/articles/10443600-payout-methods|

**Alpha 衝突／版本警示：** `monthly-subscription` 文章把 Advanced 50K/100K/150K 月費寫為 $139/$279/$419，而主站與 Advanced overview 寫 $209/$349/$489；並且該文章本身說「Updated over 2 weeks ago」。同一家公司官方頁相衝，購買前應以 checkout／書面答覆確認。來源：https://help.alpha-futures.com/en/articles/9492068-monthly-subscription、https://help.alpha-futures.com/en/articles/11634907-advanced-account-overview、https://alpha-futures.com/

---

## 4. Tradeify

### 可由主站即時方案資料確認的內容

|產品／階段|官方明列內容|官方證據|
|---|---|---|
|產品變體|主站把期貨方案分成 **Growth**（「Pass in 1 day」、5-day payouts）、**Select**（「Pass in 3 days」、daily payouts）、**Lightning**（instant funding、5-day payouts）。|https://tradeify.co/|
|Tradeify 共通主張|主站稱 funded 後 EOD drawdown、沒有 funded consistency（此為行銷總述；與某些動態方案表要逐產品核對）。|https://tradeify.co/|
|Growth Evaluation（主站動態表，例示25/50/100/150K）|一次性付款；profit target $1.5K/$3K/$6K/$9K；EOD trailing max drawdown $1K/$2K/$3.5K/$5K；Daily Loss Limit $600/$1,250/$2,500/$3,750（帳戶 profit 6% 時提高至 trailing max drawdown）；無 consistency；最大1/4/8/12 minis 或10/40/80/120 micros；reset $60/$95/$169/$229（25K 特價顯示）；主站表未列 minimum days。|https://tradeify.co/|
|Growth Funded|payout frequency 5 days；35% consistency；EOD drawdown $1K/$2K/$3.5K/$5K；最大合約同上；日損限制同表。主站該動態表未明列每次 payout cap、minimum profitable days／buffer。|https://tradeify.co/|
|Select Evaluation（動態表，25/50/100/150K）|一次性付款；target $1.5K/$3K/$6K/$9K；EOD trailing DD $1K/$2K/$3K/$4.5K；無 daily loss limit；40% consistency；max 1/4/8/12 minis、10/40/80/120 micros；reset $75/$109/$169/$239，提示最多10 resets（月或一次，依 broker table 不一致）；未列 minimum days。|https://tradeify.co/|
|Select Funded（動態表）|有 Daily 與 5-day 兩條 payout path；無 consistency。Daily max payout：25/50/100/150K=$600/$1,000/$1,500/$2,500；5-day max payout=$1,250/$3,000/$4,000/$5,000。Daily path 有 DLL $500/$1,000/$1,250/$1,750，5-day path DLL none；EOD DD在表中對應為 $1K/$2K/$2.5K或$3K/$3.5K或$4.5K（**不同 broker column 有不一致**，不可概括成單一數字）。|https://tradeify.co/|
|Lightning Funded（instant）|無 Evaluation；一次性付款；5-day payout frequency；最多5 accounts；consistency 首筆20%、第2筆25%、第3筆以後30%；EOD DD 25/50/100/150K=$1K/$2K/$4K/$5.25K；max 1/4/8/12 mini或10/40/80/120 micro。50/100/150K DLL=$1,250/$2,500/$3,000（25K 在表中 none）。|https://tradeify.co/|
|時間／新聞／KYC／明確持倉平倉規則|**未載明於本次可讀主站的規則文字**；Help Center 403，不能以推測補足。|https://tradeify.co/；https://help.tradeify.co/en/（403）|

### Tradeify 提款／報酬

|階段／產品|可核驗 payout 規則|缺口|
|---|---|---|
|Growth Funded|頻率5 days、35% consistency；主站首頁寫「Keep 90% of profit」。|最低交易日／winning days、每次上限、minimum withdrawal、buffer、KYC／付款方式／費用未載明於本次可讀規則。|
|Select Funded|Daily 或5-day，依上表各帳戶 cap；主站首頁稱90% profit。|每條 path 的 eligibility、最低日數、buffer、KYC／費用未載明。|
|Lightning Funded|5 days；首／第2／第3+ payout consistency 20%/25%/30%；主站首頁稱90% profit。|每次 cap、minimum days／profit、buffer、KYC／費用未載明。|

> **Tradeify 資料衝突／警示：** 同一主站的 Select funded 動態表隨 Tradovate、WealthCharts、Rithmic/TradeSea broker column 改變部分 DLL／EOD DD 表項，且首頁另有「No consistency once funded」總述。故上述只逐列轉載，不把它們合併為通用規則；需在選定 broker／checkout 內取得當前官方規則。

### BrowserOS 重新查核（2026-08-10，官方 Help Center）

先前一般瀏覽器被 Cloudflare 擋住；改用 BrowserOS 後，已可直接讀到官方 Help Center，以下以較新的／較具體的官方文章補正：

- **Select Evaluation 的 40% 定義已明示：** 任一單日 profit 不得超過「從首日到通過為止的總 profit」之 40%。`最大單日 profit ÷ 總 profit ≤ 40%`。佣金不計入 profit。它只適用於 **Select 的 evaluation**；通過後不論選 Flex 或 Daily，Select funded 不再有 consistency rule。此規則不會因當天賺太多而直接 breach；交易者可以繼續累積較小的盈利日，直到符合 40%。官方也明示 Select 最少 **3 個 trading days**。|[Rules: Consistency Rule](https://help.tradeify.co/en/articles/10468320-rules-consistency-rule)（2026-06-19）; [Select Evaluation Accounts](https://help.tradeify.co/en/articles/12853921-select-evaluation-accounts)（2026-04-02）|
- **Select 50K 官方範例：** Day 1–3 各 +$1,050，合計 +$3,150；任一日占 $1,050 ÷ $3,150 = 33%，因此低於 40% 並且超過 $3,000 target。|[Select Evaluation Accounts](https://help.tradeify.co/en/articles/12853921-select-evaluation-accounts)|
- **Select 出金的補正：** 通過後才選擇且不可變更。Flex：每 5 個 winning days、上限為當期 profit 的 50%（25/50/100/150K: $1,250/$3,000/$4,000/$5,000）、無 DLL。Daily：符合 buffer 後每日 eligible，cap $600/$1,000/$1,500/$2,500，DLL $500/$1,000/$1,250/$1,750，buffer $1,100/$2,100/$2,600/$3,600。官方此文也稱 Select funded 無月費、無 activation fee。|[Select Evaluation Accounts](https://help.tradeify.co/en/articles/12853921-select-evaluation-accounts)|

---

## 5. Fast Track Trading

|項目|結果／證據|
|---|---|
|NQ／MNQ 明確可用性|**未能確認**：官方站 TLS handshake 失敗，未能讀取合約清單。URL：https://fasttracktrading.net/（查閱 2026-08-10）|
|考核變體、目標、drawdown、交易日、一致性、新聞／持倉／時段、reset／訂閱|**未能確認**：沒有可讀的官方規則頁。|
|提款（按產品／階段之資格、上限、頻率、buffer、最低日數、split／費用、KYC）|**未能確認**：沒有可讀的官方 payout policy。|

---

## 購買／申請前必問的官方書面確認項目

1. 合約清單是否包含 **CME NQ 與／或 MNQ**，以及各產品／平台的最大口數、rollover/交易時段。
2. 所選「Evaluation、Qualified/Sim、Funded/Live」每階段的 profit target、EOD/intraday/trailing DD、daily limit、最少交易／winning days、consistency 計算分母與重置時點。
3. payout 是可申請、已核准還是已支付的日期如何計 cycle；每次／每月 cap、50% 留存、minimum、reserve／buffer、split 與所有 fees。
4. KYC、稅表、銀行／付款服務商以及**居住地**可用性；不得由網站可開啟、付款按鈕或未列國家反推資格。
5. 涉及自動化、複製、跨帳戶、對沖或新聞交易時，要求 compliance 對該做法書面確認。
