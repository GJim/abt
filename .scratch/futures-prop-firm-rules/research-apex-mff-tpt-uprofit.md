# 四家期貨 prop firm 規則：僅官方來源查核

- **查閱日期：2026-08-10**（所有 URL 於此日開啟/擷取）。
- **範圍與方法：**僅採公司自有網站、官方 Help Center / Zendesk 文件；未採用比較站、評論、社群貼文或推論。每個帳戶階段獨立列出。`未載明` 是指本次可存取的官方頁面沒有明示，不代表不存在。
- **重要限制：**本文件是規則摘錄，不是投資/適格性建議；沒有假定任何居住地或司法管轄區可參加。

## 快速結論（明示 NQ/MNQ）

|公司|官方對 CME Nasdaq NQ / MNQ 的明示可交易性|證據|
|---|---|---|
|Apex Trader Funding|**未知／未能驗證。**官方網域回傳 Cloudflare 403，故不以搜尋摘要或第三方補足。|https://apextraderfunding.com/ （存取時 Cloudflare 403）|
|MyFundedFutures (MFF)|**未載明 NQ 或 MNQ。**首頁僅說可在受監管美國交易所（含 CME Group）交易標準化期貨；這不能推論 NQ/MNQ 已明確提供。|https://myfundedfutures.com/|
|Take Profit Trader (TPT)|**明示兩者均有：**MNQ = Micro E-mini Nasdaq-100 Index Futures（CME）；NQ = E-mini Nasdaq-100 Futures（CME）。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172629238301-Approved-Instruments-Permitted-Products-List|
|UProfit|**MNQ 明示於舊 Basic Program 的官方描述**（CME e-micro MNQ、MES、M2K）；現行 DAY 文件只說 CME/COMEX/NYMEX、e-mini/e-micro，**未明列 NQ/MNQ**。因此：MNQ 有官方但可能是舊產品證據；NQ 未明示。|https://uprofit.com/；https://uprofit.com/help/uprofit-day-50k-program-32527248207124|

## 主張—來源證據表

### 1) Apex Trader Funding

|主張／欄位|官方證據與結果|
|---|---|
|NQ/MNQ、評鑑方案、目標、回撤、最低交易日、一致性、時段/新聞/持倉限制、reset/訂閱、提款資格/上限/頻率/buffer/分潤/費用/KYC|**未知／未驗證。**2026-08-10 對首頁及常見官方規則路徑（`/trading-rules/`、`/payouts/`、`/payout-policy/`、`/rules/`）皆收到官方站 Cloudflare **403**。遵守「僅官方」限制，沒有把未能開啟的規則以外部資料填入。來源：https://apextraderfunding.com/ |
|衝突/時效|無法取得官方內容，故無法判定。購買前應直接向 Apex 要求該商品當日的規則與 payout policy 書面版，尤其是 NQ/MNQ、PA/Performance Account 的條件。|

### 2) MyFundedFutures（MFF）

|階段／商品|官方明示規則（僅列頁面有明文者）|來源|
|---|---|---|
|所有方案：商品|只明示模擬、非執行環境內的「regulated U.S. futures exchanges」標準化期貨，例舉 CME、CBOT、NYMEX、COMEX；禁止股票、選擇權、加密、CFD、OTC。**未逐一列 NQ/MNQ。**|https://myfundedfutures.com/|
|Builder 評鑑（首頁當下選取的 50K 比較卡）|1 天通過；profit target $3,000；max drawdown $1.5k 或 $2k（首頁沒有把這兩個數字對應到完整尺寸/階段條件）；EOD trailing。|https://myfundedfutures.com/|
|Builder funded（同一比較卡）|每 48 小時可領；80/20；EOD trailing；50% consistency。|https://myfundedfutures.com/|
|Rapid / Pro funded|首頁明示 Rapid 可每 24 小時請款；Rapid 與 Pro funded 無 daily loss limit；Rapid 無 consistency。頁首當期「Rapid EOD」促銷說 funded 為 EOD drawdown、每日 payout、90/10 split。**沒有在此次可存取的頁面取得各尺寸 profit target、精確回撤額、buffer、單筆/累積提款上限或 KYC 規則，故未填。**|https://myfundedfutures.com/|
|訂閱/reset/新聞/隔夜/持倉限制/KYC/提款費|**未載明於本次可存取官方首頁。**`/rules` 會導向登入頁，不能以未登入內容推定。|https://myfundedfutures.com/rules （導向登入）|
|衝突/時效|首頁同時列多個方案與「LIMITED TIME」Rapid EOD；促銷條件不應外推至 Builder/Rapid/Pro。其首頁稱「one evaluation framework」，但各 funded 規則仍須按方案確認。|

### 3) Take Profit Trader（TPT）

|階段／商品|官方明示規則|來源|
|---|---|---|
|所有帳戶：NQ/MNQ|官方 permitted-products 表逐項列 MNQ 與 NQ，均為 CME；這是四家中最直接的 NQ/MNQ 證據。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172629238301-Approved-Instruments-Permitted-Products-List|
|Test（25K/50K/75K/100K/150K）|profit target 分別 **$1,500/$3,000/$4,500/$6,000/$9,000**；maximum EOD trailing drawdown 分別 **$1,500/$2,000/$2,500/$3,000/$4,500**。回撤以最高 EOD balance 向上追蹤，達初始餘額即停止追蹤；任何時間（含未實現損益）碰到 minimum balance 即清算。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15170265979165-Rule-3-Do-Not-Hit-End-Of-Day-EOD-Maximum-Trailing-Drawdown|
|Test：一致性與日數|至少 **5** 個 trading days（一天至少一筆交易）；單日最高 profit 必須 **低於總 net P/L 的 50%**。超過不是 Test fail，但須增加總獲利，調整後目標為 net P/L × 2。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15170316538013-Rule-5-Be-Consistent|
|Test：訂閱/reset|月訂閱、無達標期限；每月原購買日收費，違規不會自動取消（須手動取消）；可付費 reset，且不改續費日；通過後訂閱自動取消。reset 價格：25K $79、50K $99、75K $139、100K $169、150K $199。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15141145057053-Test-Subscriptions；https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15140989806493-Resetting-Your-Test-Account|
|所有 TPT 帳戶：時段/持倉及行為限制|禁止 bots/algos、協調交易、代管、風險中和架構與跨自己控制帳戶的反向倉；不得跨 session 持倉。一般 futures 可交易窗口 6pm–5pm ET，4:55pm ET 自動平倉；特定商品提早休市未能出場會清算。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/34431153546397-TakeProfitTrader-Universal-Trading-Policies-UTP；https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15170347090461-Rule-4-Trade-Approved-Products-During-Approved-Hours|
|PRO：回撤、活躍及新聞限制|與通過 Test 相同額度的 **intraday trailing** drawdown，最高值含 realized + unrealized，最高追至初始餘額；每週（日—五）至少一天完成 round-trip。禁止新聞前 1 分鐘、期間、後 1 分鐘持倉/掛單：FOMC announcement、NFP、CPI；另有原油庫存（Crude）與債券拍賣（10Y/30Y）商品限制。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15171769361053-PRO-Account-Rules|
|PRO：提款與 buffer|第 1 天即可提款，**但先建立等於 maximum drawdown 的 buffer**；達 buffer 後提款 80%。帳戶門檻（帳戶餘額）為：25K $26,500、50K $52,000、75K $77,500、100K $103,000、150K $154,500。若帳戶終止，可取 buffer 內 profit：開戶至終止 ≤60 **trading days** 為 buffer 的 50%，>60 天為 80%。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172219527581-PRO-Account-Profit-Split-Withdrawal-Rules|
|PRO+|EOD drawdown；每週至少一天 round-trip；同樣禁止 counter positions 與上述新聞窗口。該頁沒有列出專屬的提款額度/頻率。|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172006753821-PRO-Account-Rules|
|付款方式、費用、KYC|提款：美國銀行可 Plaid；國際為 PayPal/Wise；收款帳戶資料須與 TPT 註冊人相符，否則延遲審查或拒絕。wallet >$250 免提款費，≤$250 收 $50；PayPal 可能另扣其費用。KYC 為所有新註冊者強制身份驗證。**本次頁面未明示 PRO 請款頻率或單次上限。**|https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172296875165-Payout-System；https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/15172354954525-Withdrawal-Fees；https://takeprofittraderhelp.zendesk.com/hc/en-us/articles/22076187576605-KYC-Procedure|

### 4) UProfit

|階段／商品|官方明示規則|來源|
|---|---|---|
|現行頁面與產品時效|首頁大幅宣傳 **Day 2.0 / Day Soft / Day Flex**：2 天 pass、首筆 payout in 3、free activation；但可詳細查到的 DAY help 文件為 2025-06/07 的舊 DAY 規則。**不得把舊 DAY 參數外推到 Day 2.0 Soft/Flex。**|https://uprofit.com/；https://uprofit.com/day-programs|
|DAY 50K Program（舊 DAY evaluation）|月訂閱 **$39 / 每 30 日**；reset $39、不改訂閱。profit target $3,000；30% consistency；3 e-mini/30 e-micro；daily loss $1,100；EOD drawdown $2,000、起點 $48,000；CME/COMEX/NYMEX；6pm–4:10pm ET，不得跨 session swing。|https://uprofit.com/help/uprofit-day-50k-program-32527248207124|
|DAY 評鑑及 Virtual Live（50K/100K/150K 共通風控）|daily loss 分別 $1,100/$2,200/$3,000；EOD trailing drawdown 分別 $2,000/$3,000/$4,500，從 $48k/$97k/$145.5k，僅向上更新到初始餘額。達/超過會強制平倉、Breach、須 reset。每週至少一個 complete round-trip；6pm–4:10pm ET，跨 session 會自動平倉。best day 不得超過累積利潤 30%；不達不 Breach，但會阻礙通過/提款。|https://uprofit.com/help/trading-parameters-for-our-day-accounts-38280041407892|
|DAY Virtual Live 50K|activation $150；30% consistency；一週 inactivity；daily loss $1,100；EOD drawdown $2,000；SafetyNet $2,500；0–$1,500 profit 最多 2 e-mini/20 e-micro，其後 3/30；Live reset $499。|https://uprofit.com/help/uprofit-day-virtual-live-50k-32527978526612|
|DAY Virtual Live payout|profit split **80/20**；先有至少 **4 個 profitable trading days**；best day ≤自提出提款後累積 profit 的 30%（每次提款後重設計算）。SafetyNet 不可提：50K $2,500（需超過 $52,500）、100K $4,500（超過 $104,500）、150K $6,500（超過 $156,500）。符合條件後在 market close 5pm ET 後 24 business hours 內處理；若申請期間餘額跌破可提金額則取消。方法：RISE、USDC.ERC20、部分國家 PayRetailers。**頁面沒有列每次/總提款上限、提款手續費或 KYC 要求。**|https://uprofit.com/help/uprofit-day-payout-policy-32526417608212|
|NQ/MNQ|官方首頁殘留/舊資料中的 Basic Program 明示 CME e-micro **MNQ**（及 MES/M2K）。但現行 DAY 50K 寫的是交易所級別，未列 symbol；**NQ 未載明，MNQ 的現行 Day 2.0 可用性也未載明。**|https://uprofit.com/；https://uprofit.com/help/uprofit-day-50k-program-32527248207124|

## 已識別的官方頁面衝突／可能陳舊內容

1. **UProfit：**首頁（Day 2.0、Soft/Flex、2-day pass / first payout in 3 / free activation）與 DAY Help 的 2025-06/07 legacy 參數不是同一產品。DAY 主頁的 payout 摘要還顯示「best day 40%」，但 DAY detailed trading parameters/payout policy 為 **30%**；本表以詳細、較具體的 DAY 文件處理，並明確標成舊 DAY，不套用到 Soft/Flex。
2. **MFF：**Rapid EOD 被首頁標成「limited time」，不可把其 daily payout、90/10、EOD drawdown 當成一般 Rapid/Builder/Pro 的永久條款；且首次頁面卡片的 50K 數字未完整揭露尺寸對應。
3. **TPT：**同一公司不同階段回撤不同（Test=EOD trailing；PRO=intraday trailing；PRO+=EOD）。不可把 Test 或 PRO 的 buffer/payout 規則移植到 PRO+。
4. **Apex：**官方站的存取封鎖使本次無法做任何規則結論；這是資料缺口，不是「無規則」或「不可交易」。

## 購買／申請前應要求官方書面確認的未解欄位

- Apex：所有欄位（尤其 NQ/MNQ、方案/帳戶階段、payout eligibility）。
- MFF：NQ/MNQ 精確 symbol availability、所有尺寸的回撤額、Rapid/Pro/Builder 的提款上限/buffer/KYC/費用與持倉/新聞規則。
- UProfit：Day 2.0 Soft/Flex 的逐帳戶商品列表（NQ/MNQ）、profit target、回撤、consistency、payout cap/fees/KYC；並要求說明舊 DAY（30%）與頁面摘要（40%）何者適用。
- TPT：PRO+ 專屬 payout policy；PRO 請款頻率與任何單筆/總額上限。
