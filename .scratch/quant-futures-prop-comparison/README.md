# 自建量化 Nasdaq 期貨 Prop Firm：官方規則比較與嚴格候選名單

**資料截點：2026-08-10。** 以 [TradingPilot Futures](https://www.tradingpilot.com/?market=futures) 尋找候選；它是比較／導流網站，不作費用、規則、出金或自動化許可的事實證據。所有下列結論都需能追到業者官方網站或官方 Help Center。

> **目標使用方式：** 本人編寫、本人控制、非 HFT 的自動策略；不使用他人訊號、代操、跨使用者 copier、跨帳戶／跨 firm 對沖或風險中性設計。
>
> **不是獲利推薦：** Prop firm 的規則相容性、已支付的歷史資料或行銷文字，不能保證策略獲利、通過評估或順利出金。

## 結論：不能誠實地湊出 10 家「推薦」

TradingPilot Futures 篩選當時列出 16 家候選。依「官方明示自建自動化許可」與「官方明示 NQ/MNQ」這兩個硬門檻，最後只有 **4 家完整通過**；另有 **2 家可作條件式備選**。其餘平台要麼明確禁止 automated execution，要麼關鍵資訊未驗證／官方網域不是現行期貨 prop 服務。

因此本報告列出 **4 家優先候選 + 2 家條件式備選**，而不把不相容或未知的平台硬塞成 10 家推薦。

## 1. 優先候選：依自動化適配性與出金可審核性排序

| 排名 | 平台 | NQ/MNQ | 自建自動化 | 成本／帳戶期限 | 出金易讀度 | 為何列入／最重要限制 |
|---:|---|---|---|---|---|---|
| **1** | **The Futures Desk (TFD)** | 官方均明列 | 官方允許本人控制的 bots、algorithms、copier | Assessment 每 31 天 extension：TFD-X $29、Rithmic $49；reset $75–$229；funded activation $0；額外 CME 等資料 $135/月 | **高**：Sim 先建立等同 drawdown 的 buffer；Live 週一至週五可 daily request，$100 minimum、無上限、80/20、Rise/ACH | 規則最能直接支撐低頻自動化。**限制：**未證實自有 API；live 新聞前後 1 分鐘最多 5 micros 且需 stop；14 天無活動可能終止。|
| **2** | **Lucid Trading** | 官方均明列 | 官方明示 automated systems、trade copiers permitted | LucidPro / LucidDaily evaluation 是一次性費用、無月費；升 funded 無 activation；reset 可購但公開規則頁沒有固定金額 | **中高**：LucidFlex 無 buffer、90/10、可 request（通常約 2 business days）；LucidDaily 可 daily request；各方案條件不同 | 自動化與商品證據明確。**限制：**禁 HFT、禁跨帳戶／跨 firm hedge；若 >50% profits 來自持有 <=5 秒會受審查；API 未證實。|
| **3** | **Goat Funded Futures** | 官方均明列 | 官方允許自有 EA／自動策略；本人帳戶可 copy | EOD evaluation 公開例價 50/100/150K：$69/$132/$185；reset $59/$120/$170；無 activation；該方案無期限與最低日數 | **中**：每 cycle 需 7 winning days；每月 5–8、20–23 申請；$500 minimum；每次可取可用 profit 50%；80/20（可加價為 90/10）；Rise／KYC | 自有策略政策清楚。**限制：**禁 tick scalping；開倉 2 分鐘內平倉的利潤不計入；EOD funded 的高影響新聞前後 2 分鐘不得交易。|
| **4** | **TradeDay** | 官方均明列 | 只允許受支援平台上的**自建** ATS | 可驗證的動態促銷例價：50K $59/月、100K $108/月；其他費用／期限需按結帳方案確認 | **未知／未完整核驗**：官方有方案別 payout policy，但本輪未逐項確認 cap、buffer、KYC、付款路徑 | 適合低頻、自建、單帳戶策略。**限制：**不提供 API 或 Tradovate API；禁第三方 bot、HFT、>200 trades/day、跨帳戶對沖與多人相同策略。|

### 量化相容性不是「什麼 bot 都可以」

以上四家都仍應先給 compliance 你的策略摘要：程式為本人所有、使用的平台、每日成交／掛撤單量、持倉時間分布、新聞時的掛單行為、是否使用 copier，以及是否可能產生相關商品或跨帳戶的 opposite positions。

## 2. 條件式備選：不要在書面確認前下單

| 平台 | 已確認的優點 | 阻斷性缺口／限制 | 判定 |
|---|---|---|---|
| **Bulenox** | 官方明列 NQ/MNQ；官方提及 Rithmic 連第三方 API／Trader Composite Software（每月另 $100）；Master payout 有明示門檻與支付路徑。 | 多數可得文件更新於 2021–2023；bot/copy/hedge 政策不足，帳戶上限也有官方頁衝突。 | **僅書面確認後備選。** |
| **MyFundedFutures (Rapid)** | 官方允許自己設定的 automated strategies；Rapid 50K 促銷起價 $79/月，至少 2 日；payout daily、$500 minimum、90/10、明示 buffer。 | 官方尚未以 permitted-products 清單明示 NQ/MNQ；禁 HFT、copy 與任何 hedge；API／付款路徑／KYC 未證實。 | **僅書面確認 NQ/MNQ 與平台後備選。** |

## 3. 明確排除／不列為自動量化推薦

| 平台 | 排除理由 |
|---|---|
| **Take Profit Trader** | 官方通用政策明文禁止 automated trading systems、bots、algorithmic execution。 |
| **Tradeify** | 官方允許本人所有、非 HFT 的 bot，但在本研究資料截點未以官方 permitted-products 清單明示 NQ/MNQ；產品頁與合約的費用資料也衝突。可在取得書面 NQ/MNQ、費用與 payout 條款後重評。 |
| **Earn2Trade** | 有 weekly payout、no buffer、funded 無月費等優點，但未取得 NQ/MNQ、自動化、API、copy／hedge 的官方正面證據。 |
| **DayTraders.com** | MNQ 明示，但未有 bot/EA/API 正面許可，且有 automated HFT 禁止與 manual-trading 表述。 |
| **Apex、The Legends Trading** | 關鍵官方規則中心受 Cloudflare 或資料無法驗證；不以第三方補值。 |
| **Alpha Futures、Aqua Futures** | 對應官方網域在資料截點為停放／出售頁，沒有可驗證的現行服務條款。 |
| **Blusky、Atlas Futures** | 對應官方網域是非金融／非期貨 prop 的同名產品，不能作為候選。 |

## 4. 新手優先比較：費用、時間與出金的實際含義

| 問題 | 先看什麼 | 最容易誤解的事 |
|---|---|---|
| 前期成本 | 一次性／月費、資料費、reset、activation、commission | 促銷價不等於常態價；evaluation 和 funded 的費用不能混用。 |
| 是否有限時 | maximum days、月費 rebill、rolling activity、inactivity | 「沒有通過期限」不代表沒有月費或不活躍失效。 |
| 出金容易嗎 | winning/Q days、minimum、buffer、consistency、cap、split、request frequency | 「daily payouts」常是 *eligible to request*，不是每天保證獲款；仍可能有 buffer／審核／KYC。 |
| 自動化能否跑 | 自建或第三方、API、HFT／訂單率、新聞、最短持倉 | 平台有 Rithmic／Tradovate 不等於官方允許你的 API 或 bot。 |

## 5. 下單前一頁式書面確認清單

對任何最終候選，以選定的 **帳戶大小、方案、平台／broker、evaluation / sim / live 階段** 請對方書面回覆：

- [ ] `NQ` 和 `MNQ` 是否都可交易；最大口數、交易時段、rollover、commission 與 market-data 費。
- [ ] 本人編寫／本人控制的 bot、EA、webhook、API、VPS、copier 是否准許；HFT／order rate／cancel rate／最短持倉限制。
- [ ] 多帳戶同期策略、同標的／相關標的、opposite positions、hedging 或跨 firm 下單是否有任何違規風險。
- [ ] 評估費、續費、reset、activation、data/exchange、平台與 payout 全部費用；促銷結束後的價格。
- [ ] maximum time、minimum days、inactivity、profit target、drawdown、daily loss、consistency 與 breach 的後果。
- [ ] payout 的 winning/Q days、minimum、cap、buffer、split、request frequency、審核時間、KYC、稅表、居住國與收款 provider。

## 完整證據報告

每個數字與限制均帶官方 URL，詳細依公司分組：

1. [Goat Funded Futures、Bulenox、Tradeify、Apex](01-goat-bulenox-tradeify-apex.md)
2. [TradeDay、Alpha Futures、Earn2Trade、MyFundedFutures](02-tradeday-alpha-earn-mff.md)
3. [Take Profit Trader、Lucid Trading、The Futures Desk、Aqua Futures](03-tpt-lucid-futuresdesk-aqua.md)
4. [Day Traders、The Legends、Blusky、Atlas Futures](04-remaining-tradingpilot-futures.md)
