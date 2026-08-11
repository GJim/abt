# 期貨 Prop Firm：考試與出金規則（官方來源限定）

**查閱日期：2026-08-10（UTC）**

**研究範圍：** 13 家面向期貨交易者的 prop firm；優先檢核其官方網站是否**逐項明示**可交易 CME Nasdaq-100 futures 的 `NQ` 或 `MNQ`。所有金額為 USD。

> **重要閱讀規則**
>
> - 本研究**只**引用公司自有網域、官方 Help Center／Zendesk 文件。未使用比較站、社群或搜尋摘要。
> - 「未載明」只表示本次取得的官方頁面沒有明文；不代表禁止、可用、或對任何居住地有效。
> - Evaluation、simulated funded、live／contracted 階段分列；不得把某一方案的 drawdown、consistency 或 payout 規則外推至別的方案。
> - 官方頁面互相衝突、標為限時、或無法讀取時，保留為**購買前要求書面確認**的缺口，而不是選擇對自己有利的版本。
> - 本文件不是投資、法律、稅務、居住地適格性建議。

## 1. Nasdaq 指數期貨優先篩選

| 公司 | 官方是否明確列出 NQ/MNQ | 判定與應採取動作 | 完整官方證據 |
|---|---|---|---|
| **OneUp Trader** | **是：NQ、MNQ** | 官方 Evaluation Rules 逐項列為 permitted products。 | [完整查核](research-topstep-tradeday-earn2trade-oneup.md#4-oneup-trader) |
| **Take Profit Trader (TPT)** | **是：NQ、MNQ** | 官方 Approved Instruments 清單逐項列兩者及 CME。 | [完整查核](research-apex-mff-tpt-uprofit.md#3-take-profit-trader-tpt) |
| **Bulenox** | **是：NQ、MNQ** | 官方 Qualification Account futures instruments 清單逐項列兩者。 | [完整查核](research-ticktick-bulenox-alpha-tradeify-fasttrack.md#2-bulenox) |
| **UProfit** | **只見舊資料明列 MNQ** | 現行 Day 2.0／Soft／Flex 未逐項明列 NQ/MNQ；不可把舊 Basic Program 外推。 | [完整查核](research-apex-mff-tpt-uprofit.md#4-uprofit) |
| Topstep | 未能以本次官方頁面明確確認 | 規則頁未列 NQ/MNQ；請索取分階段、分平台合約清單。 | [完整查核](research-topstep-tradeday-earn2trade-oneup.md#1-topstep) |
| TradeDay | 未能以本次官方頁面明確確認 | 僅稱 permitted products，未列 ticker。 | [完整查核](research-topstep-tradeday-earn2trade-oneup.md#2-tradeday) |
| Earn2Trade | 未能以本次官方頁面明確確認 | 僅列 CME 等交易所；交易所名稱不足以證明 NQ/MNQ。 | [完整查核](research-topstep-tradeday-earn2trade-oneup.md#3-earn2trade) |
| MyFundedFutures (MFF) | 未載明 | 僅列 CME Group 等交易所，未列 ticker。 | [完整查核](research-apex-mff-tpt-uprofit.md#2-myfundedfuturesmff) |
| Alpha Futures | 未載明 | NQ 僅出現在新聞規則範例，非 permitted-products 證據。 | [完整查核](research-ticktick-bulenox-alpha-tradeify-fasttrack.md#3-alpha-futures) |
| Tradeify | 未載明 | 行銷提到 Nasdaq／futures market 不能當 NQ/MNQ 合約清單。 | [完整查核](research-ticktick-bulenox-alpha-tradeify-fasttrack.md#4-tradeify) |
| Apex Trader Funding | 無法驗證 | 官方站 Cloudflare 403；不以第三方資料補寫。 | [完整查核](research-apex-mff-tpt-uprofit.md#1-apex-trader-funding) |
| TickTickTrader | 無法驗證 | 官方站與 Help Center Cloudflare verification。 | [完整查核](research-ticktick-bulenox-alpha-tradeify-fasttrack.md#1-tickticktrader) |
| Fast Track Trading | 無法驗證 | 官方站 TLS handshake 失敗。 | [完整查核](research-ticktick-bulenox-alpha-tradeify-fasttrack.md#5-fast-track-trading) |

**嚴格的首選候選僅為 OneUp、TPT、Bulenox**：這是以「官方頁面明確列出 `NQ`／`MNQ`」為唯一篩選條件的結果，**不是**對其出金可靠性或個人適格性的推薦。

## 2. 考試與出金規則：快速比較

下表僅摘錄可由官方文件確定、且對篩選影響最大的條件；精確帳戶尺寸、所有限制、及每項 URL 請看下方三份完整 claim-to-source 報告。

| 公司／方案或階段 | 考試結構與主要限制（官方明文） | 出金／報酬（官方明文） | 重要風險／缺口 |
|---|---|---|---|
| **OneUp — Evaluation / Funded** | 1-step；最少 10 trading days；80% multi-day consistency；15:15 CT 前平倉。Funded 有重大新聞、scaling、短秒／HFT、複製及群組交易限制。 | 首 $10k profit 100%，其後 90%；首頁稱 day 1 起 unlimited withdrawals。 | 提款最低額、頻率、上限、KYC／費用未載明。 |
| **TPT — Test / PRO / PRO+** | Test：5 trading days、50% consistency、EOD trailing DD。PRO：intraday trailing DD、重大新聞前後 1 分鐘禁止持倉／掛單。PRO+ 是 EOD DD。 | PRO 需先建立等於 DD 的 buffer，之後可提 80%；KYC 強制。 | PRO+ 的專屬 payout policy；PRO 的頻率與上限未載明。 |
| **Bulenox — Qualification / Master / Funded** | Qualification：官方頁稱無最低日數；但進 Master 的段落稱至少 1 日。Master 有 10 個 individual trading days 才可 payout。 | Master：首 3 筆有尺寸上限，之後無 maximum；需留 safety threshold；首 $10k 無 company commission，後續 90/10。 | 官方頁對最少日數、稅表、多 Master 帳戶上限有衝突；Funded payout 細節未載明。 |
| **Topstep — XFA Standard / Consistency / LFA** | 此次來源未取得最新 Trading Combine 全套考試數字。 | XFA Standard：5 個 $150+ 盈利日；Consistency：3 日且最大日 ≤40%；有按尺寸的 request caps，LFA 規則不同。 | NQ/MNQ 未明示；2025 rules 與 2026 payout policy 時點不同；DLL double-cap 是無結束日的限時優惠。 |
| **TradeDay — Quick Pay / Fast Pass** | Quick Pay：5 eval days、30% consistency；Fast Pass：無最少 eval days、45% consistency；day trading only。 | Quick Pay Funded Sim：最少 1 payout day、$250 minimum；$4k 前 50/50，之後 80/20。 | NQ/MNQ、Fast Pass payout、KYC／費用／新聞限制未載明。 |
| **Earn2Trade — TCP / Gauntlet Mini** | 方案、帳戶大小各有目標／EOD DD／30% consistency；可交易到 15:50 CT；news allowed。 | LiveSim／Live：weekly payout，無 buffer／minimum days／consistency；$100 起，首筆成功出金扣 $139 activation fee。 | NQ/MNQ 未明示；詳細 withdrawal policy URL／cap／KYC 未取得。 |
| **MFF — Builder / Rapid / Pro** | 首頁公開的方案卡明示部分 target、EOD trailing、payout cadence；詳細 rules 需登入。 | Builder 48 小時、Rapid 24 小時的公開摘要；促銷 Rapid EOD 為 limited time。 | NQ/MNQ、尺寸對應 DD、buffer、cap、KYC、時段與新聞規則未公開可驗證。 |
| **UProfit — 舊 DAY / Virtual Live** | 50K 舊 DAY：$3k target、30% consistency、EOD DD $2k、daily loss $1.1k。 | Virtual Live：至少 4 profitable days、80/20、SafetyNet 不可提。 | 新 Day 2.0／Soft／Flex 不可套用舊 DAY；舊資料 30% vs 首頁摘要 40% 有衝突。 |
| **Alpha — Zero / Standard / Advanced / Direct** | Zero/Standard/Advanced 有不同目標、EOD MLL、日數與 consistency；Qualified 有新聞交易限制（Advanced 例外）。 | 多數 Qualified 每月最多 4 次、90%；產品別 minimum/cap/consistency 不同。 | NQ/MNQ 未明示；Advanced 月費官方頁衝突。 |
| **Tradeify — Growth / Select / Lightning** | Growth 1-day、Select 3-day、Lightning instant funding；方案與 broker 欄位不同。 | Growth／Lightning 5-day；Select 有 daily 或 5-day 路徑與不同 cap。 | NQ/MNQ、新聞／時段、KYC、buffer、完整 eligibility 缺口；同一主站 broker 欄位衝突。 |
| **Apex / TickTickTrader / Fast Track** | 無法由可讀的官方規則頁確認。 | 無法由可讀的官方 payout policy 確認。 | **不要以本研究或第三方摘要當購買依據。** |

## 3. 完整官方證據報告

每個數字、限制與未載明欄位都在下列檔案中採「**主張｜官方證據／精確 URL**」方式保存：

1. [Topstep、TradeDay、Earn2Trade、OneUp Trader](research-topstep-tradeday-earn2trade-oneup.md)
2. [Apex、MyFundedFutures、Take Profit Trader、UProfit](research-apex-mff-tpt-uprofit.md)
3. [TickTickTrader、Bulenox、Alpha Futures、Tradeify、Fast Track Trading](research-ticktick-bulenox-alpha-tradeify-fasttrack.md)

## 4. 下單／付款前統一書面確認清單

請對**選定帳戶大小、平台／broker 與每一帳戶階段**要求官方以書面確認：

- [ ] CME `NQ` 與／或 `MNQ` 是否為 permitted product，及每階段最大口數、資料源與 rollover／交易時段。
- [ ] Evaluation、simulated funded、live/contracted 的 target、EOD/intraday/trailing DD、daily limit、最少 trade/winning days、consistency 公式及 reset 時點。
- [ ] payout cycle 是以「申請、核准或實際支付」何者計算；每次與每月 cap、minimum、buffer/reserve、split 與全部 fees。
- [ ] KYC、稅表、付款服務商、銀行資料姓名要求，以及**居住地／國籍**的開戶與出金可用性。
- [ ] 自動化、copy trading、跨帳戶、反向倉／對沖、重大新聞交易是否允許；對擬採策略要求 compliance 書面確認。

## 5. 延伸研究

- [自建量化 Nasdaq 期貨 Prop Firm：官方規則比較與嚴格候選名單](../quant-futures-prop-comparison/README.md) — 使用 TradingPilot Futures 作候選池、以官方規則重新驗證；含自動化、NQ/MNQ、費用、時限與 payout 篩選。

## 6. 範圍界線與更新方式

「每個 prop firm」沒有可驗證的全球封閉名單；本研究因此明示為上述 13 家的**首輪研究 universe**，而非聲稱涵蓋所有世界各地業者。後續要納入新業者時，必須先找到其官方 NQ/MNQ permitted-products 證據；若未能明示，維持「未載明／待書面確認」，不由 CME 交易所級別資訊推論。
