# 自動化量化期貨適配性研究：TradeDay、Alpha Futures、Earn2Trade、MyFundedFutures

- **研究目的**：只以各公司自己的公開網站／說明中心為證據，評估其對「自建、自行設定的自動化量化期貨策略」的適配性；不把 TradingPilot 當成證據來源。
- **存取日期**：2026-08-10（所有連結）；價格／促銷、方案與規則可變，購買前須重查同一官方頁面及取得書面確認。
- **嚴格判準**：只有官方明確列出 `NQ` **及** `MNQ` 才標示「已確認」。僅稱 CME、micro，或在示例中提到商品，不推論為可交易。未在可存取官方材料中明說者一律寫「未載明／未驗證」。
- **分數**：每項 0–2；只有有相應事實時才評分，否則 `未知`。分數不是投資／交易建議。

## 快速結論

| 公司 | NQ + MNQ | 自動化政策 | 對自動量化的初步結論 | 可評合計* |
|---|---|---|---|---:|
| TradeDay | **已確認** | 可用受支援平台的自建 ATS；不提供 API／Tradovate API；第三方 bot 禁止 | **可考慮，但限制很強**：須自建、非高頻、≤200 筆／日，且不可複製／對沖 | 4/12 |
| Alpha Futures | 未驗證 | 未驗證 | **不可列入候選**：所給官方網域目前轉往 GoDaddy 停放／出售頁，無可驗證的現行產品 | 未知／不計 |
| Earn2Trade | 未驗證 | 未載明 | **資訊不足，不應假設可自動化** | 3/4（僅已可評項） |
| MyFundedFutures（MFFU） | 未驗證 | 自行設定的自動策略可用；高頻禁止；live 自動化須遵 CME 規則 | **可考慮，但先書面確認平台/API與策略細節**；禁止 copy、合作同向／反向與任何對沖 | 4/6（僅已可評項） |

\*可評合計只加總下方已獲支持的指標；`未知`不當成 0，也不把不同公司未公開的項目做假精確排名。

---

## 1. TradeDay

### 可驗證的商品、方案與交易自動化

| 欄位 | 官方可驗證結果 | 評分 |
|---|---|---:|
| NQ / MNQ | **是／是。** 官方「permitted products」表將 E-mini Nasdaq-100 列為 `NQ`，Micro E-mini Nasdaq-100 列為 `MNQ`；同頁說只可交易其允許的 CME Group 商品。[[TD-1]](#sources) | 2 |
| 自動化／EA／bot | 可使用 **NinjaTrader、Tradovate、TradingView、Jigsaw、Quantower** 之一來使用 ATS；但 TradeDay 不提供平台 API，也不暴露 Tradovate API 讓 ATS 直接連接。第三方購買的 algo/bot 禁止；多名使用者做相同交易會關戶／停權。[[TD-2]](#sources) | 1 |
| API | 明確**不提供**平台 API；Tradovate API 不對交易者暴露。[[TD-2]](#sources) | 0 |
| HFT／頻率 | 極高頻自動系統禁止；每天超過 **200 筆 trade** 的策略不允許。[[TD-3]](#sources) | 0 |
| copy／協同／hedge | 相同第三方 bot／ATS 或多用戶同一 bot 禁止；跨帳戶、同商品同時反向持倉的 hedge 明確禁止。[[TD-2]](#sources) [[TD-3]](#sources) | 0 |
| 方案與階段 | 官方 FAQ 現行名稱為 **Quick Pay** 與 **Fast Pass Evaluation**；資金階段 FAQ 區分 **Funded Sim** 與 **Funded Live**。[[TD-4]](#sources) [[TD-5]](#sources) | — |

**量化相容性解讀**：自建的、在所列平台內執行的非高頻策略並非一概禁止，但 API 直連不可用、第三方策略不可用，且反向帳戶與同一策略多使用者風險高。這與「多帳戶 copier、外部訊號／第三方 bot、HFT」不相容；不應嘗試規避。

### 費用、期限、出金（按方案／階段不得混用）

| 要求欄位 | 官方公開資訊（本次可驗證） | 未驗證／注意事項 |
|---|---|---|
| 前置費／定期費 | 主站價格元件（Intraday）顯示 **50k US$59/月**（原價 US$131）及 **100k US$108/月**（原價 US$240），並顯示「55% off」。[[TD-6]](#sources) | 這是促銷顯示、平台／帳型／drawdown 選項會變；其餘帳型與當期完整價格不可由此推算。 |
| reset／activation | **未載明／未驗證**於本次存取材料。 | 不得把其他公司或舊方案費用套用。 |
| 時限／最低日數 | **未在本次已核對的官方頁面完整驗證。** | Quick Pay／Fast Pass 的個別評估規則頁須在下單選項確定後重查。 |
| 出金資格、頻率、上限、buffer、split | 官方說明中心有獨立「Quick Pay Funded Sim Payout Policy」與出金／付款方式文章，但本次不以未逐項讀取的頁面補值。[[TD-7]](#sources) | **未驗證**資格、頻率、cap、buffer、split、KYC與付款路由；必須按 Funded Sim / Funded Live 與所選方案取得書面確認。 |
| KYC／付款路由 | **未驗證**。 | 不以 FAQ 分類或網站可註冊性推論。 |

### 規則衝突／採購前必問

1. 以書面確認策略是否被視為「自建」而非第三方 ATS，及每日期望成交筆數是否低於 200。
2. 確認特定平台是否支援策略所需的自動化方式；不可假設有 REST、WebSocket、Tradovate API 或其他 direct API。
3. 明確披露會否跨帳戶同步、同標的反向、與他人使用相同模型／訊號；已公開規則顯示這些情況可能違規。
4. 就選定的 Quick Pay／Fast Pass、帳戶大小、Intraday/EOD 和 Funded Sim/Live，索取當期 reset、activation、出金 KYC、付款路由、cap/buffer/split 的書面條款。

---

## 2. Alpha Futures

### 官方來源可用性：阻斷性問題

本任務提供的公司名稱未附可驗證的現行第一方規則中心或產品 URL。以名稱最直接對應的 `https://www.alphafutures.com/` 在本次存取時導向 GoDaddy 的 **for-sale／停放頁**，而非公司產品、規則或費用頁。[[AF-1]](#sources) 因此不能以搜尋結果、社群、評測、舊快取或第三方資料填補。

| 欄位 | 結果 | 評分 |
|---|---|---:|
| NQ | 未驗證 | 未知 |
| MNQ | 未驗證 | 未知 |
| algorithm / EA / API / automation | 未驗證 | 未知 |
| copy／hedging | 未驗證 | 未知 |
| 方案、階段、費用、reset、activation、期限 | 未驗證 | 未知 |
| payout 資格／頻率／cap／buffer／split／KYC／付款路由 | 未驗證 | 未知 |
| 可操作性 | 沒有可驗證的現行官方服務材料；不適合作為採購候選 | 0 |

**處置**：在對方提供可存取的官方主網域及指向現行規則、費用與支出條款的第一方 URL 前，排除。這不是宣稱公司已停止營業，只是公開官方證據不足以完成本研究的嚴格門檻。

---

## 3. Earn2Trade

### 方案／階段與已公開的 payout 條件

| 欄位 | 官方可驗證結果 | 評分 |
|---|---|---:|
| NQ / MNQ | **未驗證。** 本次可存取的 TCP 方案頁未提供 NQ/MNQ 的官方允許商品清單／合約規格。 | 未知 |
| 方案／階段 | 方案為 **Trader Career Path®（TCP）**，帳型選項 TCP25 / TCP50 / TCP100；頁面區分 **Evaluation → Funded LiveSim® Account → Funded Live Account**，且有資金成長梯。[[E2T-1]](#sources) | — |
| Evaluation 最低天數／重設 | TCP 評估頁稱 **No Minimum Trading Days**、**Free Reset When Rebilled**。[[E2T-1]](#sources) | 2 |
| Funded LiveSim／Live 定期與 activation | 兩個 funded 區段皆標示 **No Upfront Activation Fee**、**No Monthly/Subscription Fee**。[[E2T-1]](#sources) | 2 |
| payout 頻率／buffer | LiveSim 與 Live 均標示 **Weekly Payouts**、**No Buffer Requirement**。[[E2T-1]](#sources) | 2 |
| profit split | 兩個 funded 區段均顯示：US$1,500 以下 50%，超過 US$1,500 為 80%。[[E2T-1]](#sources) | 1 |
| 自動化／EA／API | **未載明／未驗證**於本次存取的官方 TCP 頁；不能因為有交易平台或期貨方案而推論可用 EA、copier 或 API。 | 未知 |
| copy／hedging | **未載明／未驗證**。 | 未知 |

### 費用、時間、KYC及資料缺口

- **前置評估費與續費金額**：本次可存取頁面可確認「rebilled」與免費重設，但沒有可安全引用的選定帳型／平台／地區結帳金額；**未驗證**。
- **reset**：僅確認「rebilled 時免費 reset」；單獨付費 reset 的價格／機制**未驗證**。
- **評估時間上限**：頁面公開「無最低交易天數」，但未以此推論無時間上限；**未驗證**。
- **出金資格／單次或累積 cap／KYC／付款路由**：**未驗證**。Weekly、no buffer 與 split 不等同於完整資格或付款渠道。

**量化結論**：TCP 的階段、無料 activation／funded subscription、weekly payout 與 no-buffer 都是可驗證優點；但自動化、API、copy/hedge和 NQ/MNQ 是本任務的關鍵 gate，均未由已存取的官方資料確立，故不評為自動量化適用。採購前需由 Earn2Trade 合規團隊針對策略、平台、是否自建／第三方、API、跨帳戶同步、NQ 與 MNQ 分別書面答覆。

---

## 4. MyFundedFutures（MFFU）

### 自動化、copy／hedge與商品判定

| 欄位 | 官方可驗證結果 | 評分 |
|---|---|---:|
| NQ / MNQ | **未驗證為可交易。** 公平交易政策以 E-mini NQ 與 Micro NQ 共享 NQ underlying 作為對沖示例，並不等同官方允許商品清單或合約規格。依本報嚴格規則不推論供應。[[MFF-1]](#sources) | 未知 |
| 自動化 | 可用「依自己特定設定」的自動策略，前提是不利用 simulated environment 的有利成交；live 帳戶自動化必須遵 CME guidelines。[[MFF-1]](#sources) | 2 |
| HFT | 高頻交易不允許。[[MFF-1]](#sources) | 0 |
| API／EA／平台 | **未載明／未驗證**：此政策允許自動策略不代表提供 API，也不表示任何 EA／外部平台均可接入。 | 未知 |
| copy／協同 | 每位交易者必須自行進出／取消；不得彼此 copy trade。跨不相連帳戶協作執行相同或相反策略也禁止。[[MFF-1]](#sources) | 0 |
| hedge | 任何 hedge 禁止；同時對同一 underlying 做 buy 與 sell 被定義為 hedge，並以 E-mini NQ/Micro NQ 為例。[[MFF-1]](#sources) | 0 |

### Rapid Plan：嚴格區分 Evaluation / Sim-funded / Live / payout

以下僅是 **Rapid**，不能套用到 Builder、Pro、Flex 或其他歷史方案。

| 階段／欄位 | 官方可驗證結果（25k / 50k / 100k / 150k） |
|---|---|
| 方案與價格 | Rapid 為一階段評估；50k 頁面顯示促銷起價 **US$79/月**、標價 **US$157**，並明說依 current promotions。[[MFF-2]](#sources) |
| Evaluation | 最低 2 天；profit target **1,500 / 3,000 / 6,000 / 9,000**；EOD max loss **1,000 / 2,000 / 3,000 / 4,500**；無 daily loss limit；50% consistency（僅 Eval）；Tier-1 news 可；activation US$0。[[MFF-2]](#sources) |
| Sim-funded | intraday trailing max-loss distance **1,000 / 2,000 / 3,000 / 4,500**，在 +US$100 鎖定；最多 mini/micro **3/30、5/50、8/80、10/100**；Tier-1 news 不可。[[MFF-2]](#sources) |
| Live | initial balance US$0；EOD max loss **1,000 / 2,000 / 3,000 / 4,500**，floor 在 US$0；最多 mini/micro **2/20、4/40、6/60、8/80**；daily payout；90/10 split。[[MFF-2]](#sources) |
| payout（Rapid） | daily；minimum US$500；90/10；required buffer **1,100 / 2,100 / 3,100 / 4,600**；無 consistency rule；activation US$0。[[MFF-2]](#sources) |
| reset | **未載明／未驗證**於上述現行公開 Rapid 頁。 |
| 時限 | 已確認「min 2 days」，但**未驗證**任何 maximum time limit。 |
| KYC／付款路由 | **未驗證**。網站宣傳的付款速度／recent payouts不取代可用渠道、KYC或地區資格的官方條款。 |

**顯著規則衝突**：Rapid 本身的公開細則同時顯示 Evaluation 可交易 Tier-1 news、Sim-funded 不可；切勿把評估權利帶到 funded。[[MFF-2]](#sources) 同時，自動化雖可，但 HFT、copy、同向／反向協作、NQ/MNQ 的跨商品對沖都可能構成違規。[[MFF-1]](#sources)

### 採購前書面確認清單

1. 要交易的 **NQ 和 MNQ** 是否在所選 Rapid 階段／平台的 permitted-products 清單，及目前合約、時段、佣金與資料權限。
2. 自動策略使用的平台、API／bridge、訊號來源是否被准許；是否屬「自定設定」及是否需要事先審核。
3. 預期每分鐘／每日下單與成交數、掛撤單方式、news 與 live-CME合規流程是否可接受。
4. 帳戶間是否有任何同步、共同裝置、相同訊號、反向持倉或 risk-neutral 設計；公開政策已將多種情況禁止。
5. 所選帳型的 reset、月費續費、KYC、付款服務商／可用國家、付款審核、daily payout cap 與 live transition 條件。

---

## 評分方法與逐項記錄

| 指標（0–2） | TradeDay | Alpha Futures | Earn2Trade | MFFU |
|---|---:|---:|---:|---:|
| NQ+MNQ 有官方清單明列（2=兩者） | 2 | 未知 | 未知 | 未知 |
| 自動化規則清晰且容許自建策略（2=明確可；1=僅有限） | 1 | 未知 | 未知 | 2 |
| API／直連（2=明確可；0=明確不可） | 0 | 未知 | 未知 | 未知 |
| 無重大 copier/hedge 衝突（2=無；0=明確禁止） | 0 | 未知 | 未知 | 0 |
| HFT 相容（2=明確可；0=明確禁止） | 0 | 未知 | 未知 | 0 |
| 費用／出金資料可逐項核驗（2=完整；1=部分） | 1 | 未知 | 1 | 2 |
| **可評合計** | **4/12** | **未知／不計** | **3/4** | **4/6** |

> 上表將「未知」排除於分母；分數不應被誤解為跨公司承諾或盈利機率。尤其是禁止 hedge/copy 雖然是合規限制，並非品質缺陷；它直接限制的只是本研究指定的自動量化操作模式。

---

## Sources

所有來源均為各公司營運的第一方主站或官方 help center；存取日 2026-08-10。

- <a id="TD-1"></a>**[TD-1] TradeDay — Permitted products guidelines**：https://tradeday.freshdesk.com/en/support/solutions/articles/103000008862-what-are-the-permitted-products-guidelines- （明列 NQ、MNQ 與 permissible CME products。）
- <a id="TD-2"></a>**[TD-2] TradeDay — Automated, Algo and Bot Trading**：https://tradeday.freshdesk.com/en/support/solutions/articles/103000085101-automated-algo-and-bot-trading （平台、無 API、第三方 bot／相同交易限制。）
- <a id="TD-3"></a>**[TD-3] TradeDay — Prohibited Trade Practices**：https://tradeday.freshdesk.com/en/support/solutions/articles/103000121031-prohibited-trade-practices （>200 trades/day、自動化、協同反向、hedge。）
- <a id="TD-4"></a>**[TD-4] TradeDay — Quick Pay / Fast Pass evaluation rules**：https://tradeday.freshdesk.com/en/support/solutions/articles/103000008847-what-are-the-objectives-and-rules-of-the-quick-pay-and-fast-pass-evaluation-
- <a id="TD-5"></a>**[TD-5] TradeDay — Funded Sim / Funded Live rules**：https://tradeday.freshdesk.com/en/support/solutions/articles/103000008889-what-are-the-rules-for-funded-funded-sim-and-funded-live-traders-
- <a id="TD-6"></a>**[TD-6] TradeDay — 主站 pricing 元件**：https://www.tradeday.com/ （動態促銷價格；頁面選項為平台、帳戶、drawdown。）
- <a id="TD-7"></a>**[TD-7] TradeDay — Quick Pay Funded Sim Payout Policy**：https://tradeday.freshdesk.com/en/support/solutions/articles/103000335937-quick-pay-funded-sim-payout-policy （本報不在未逐項驗證下擷取數字。）
- <a id="AF-1"></a>**[AF-1] Alpha Futures 對應網域存取結果**：https://www.alphafutures.com/ （本次導向 GoDaddy for-sale 停放頁；僅證明本次無可用公開規則，不證明企業狀態。）
- <a id="E2T-1"></a>**[E2T-1] Earn2Trade — Trader Career Path®**：https://www.earn2trade.com/trader-career-path （TCP帳型、Evaluation/LiveSim/Live、funded 費用、weekly payout、buffer、split。）
- <a id="MFF-1"></a>**[MFF-1] MyFundedFutures Help Center — Fair Play and Prohibited Trading Practices**：https://help.myfundedfutures.com/en/articles/8444599-fair-play-and-prohibited-trading-practices （頁面標示 2025-11-25。）
- <a id="MFF-2"></a>**[MFF-2] MyFundedFutures — Rapid Plan**：https://myfundedfutures.com/plans/rapid （含 `#full-rules`：各帳型 Evaluation／Sim-funded／Live／Payout 表。）
