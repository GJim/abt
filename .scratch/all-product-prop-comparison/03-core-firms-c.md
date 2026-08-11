# TradingPilot 型錄核心業者 C：官方條款比對（11 家）

- **擷取日：2026-08-11。**本報告只把品牌官方網站／官方說明中心列為規則與費用的證據；TradingPilot 僅應作為第二手的型錄／payout-review 追蹤，不用來補足官方未公開條款。
- **讀法與限制：**「未述明／未能從可存取官方材料核實」不是允許、沒有費用或沒有該限制。價格是頁面顯示值，未含促銷碼，且未把不同帳戶、平台、時期或 evaluation / simulated-funded 階段混寫。許多結帳頁為動態或 WAF 阻擋；沒有可重現的帳戶規模與 checkout 輸出時，不報一個貌似精確的費用。
- **自動化分類定義：**僅在官方明確允許該產品的 EA/bot 時為「明確允許」；明確禁止為「明確限制」；其餘是「可存取官方規則未找到明確限制」，**不等於核准**。平台連線、EA 文字或 API 存在，本身不保證可出金。
- **Payout situation 分兩欄：**(A)「官方摩擦」是資格、頻率、門檻／buffer、split、KYC、rail 等；(B) TradingPilot 是第二手追蹤。本次工作環境未能取得可引用的 TradingPilot 個別付款／review 記錄，故逐家標為「未擷取」而非「沒有付款」。

## 逐家產品／階段

### 1. Funded Trading Plus（FTP）
**官方材料：**其官方 FAQ 索引明列月費、退款、reset、payout request、KYC、EA/Algos/Bots、copy trading、hedging、帳戶到期等專題，但該索引本身不提供各產品數值；主站對本次 HTTP 擷取回覆 403。[FTP FAQ][1]

- **產品／費用與通關：**可存取官方材料未足以把某一帳戶規模、產品與一次性／月費／reset／activation、steps、target、DD、天數、consistency、期限做成可靠的原子條目；**未核實，不推估**。
- **出金／限制：**FAQ 證實官方有 payout、KYC、copy 與 hedge 政策文章，但本次未開到文章正文；資格、週期、最低額、buffer、split、KYC 時點與 rail **未核實**。不把「FAQ 有此標題」誤寫成允許／禁止。
- **自動化：**可存取材料只證實有 EA/Algo/Bots 專題，結論為**未核實（不可視為允許）**。**閒置／免費試用：**帳戶到期有專題，但期限未核實；免費試用未核實。**TradingPilot：**未擷取。

### 2. FTMO — FTMO Challenge（公開 Challenge 頁）／Free Trial Challenge
- **費用與通關：**Challenge 頁確認評估流程與產品選擇，但本次未能從靜態公開頁可靠提取「帳戶規模×normal/aggressive」當日收費與完整數值表，因此費用、reset、activation 及精確 target/DD/min-days/期限均**未在此報告核實**。[FTMO Challenge][2]
- **免費試用：**官方明確提供 **Free Trial Challenge**；該頁是試用存在的證據，不將其當作付費 Challenge 規則或出金資格。[FTMO Free Trial][3]
- **出金／限制：**本次可存取的兩個官方頁未給足 stage-specific payout cadence、minimum/buffer、split、KYC／rail；**未核實**。自動化、hedge、閒置終止亦未從這兩頁核實。**TradingPilot：**未擷取。

### 3. Top One Trader — 1-Step Flash（Challenge）→ Simulated Funded
- **費用：**此公開規則頁未展示本次可重現的 checkout 費、reset 或 activation；**未核實**。[Top One rules][4]
- **通關（Flash）：**10% profit target；至少 3 個 profitable trading days；最大日損為 initial balance 的 4%（hard breach）；最大 trailing DD 7%（hard breach）；期限 unlimited。評估期新聞交易不受限。[4]
- **Funded／出金摩擦：**同頁明示 funded payout 為「經一段設定期間後」並可每 14 日持續提款，但擷取片段沒有該等待期、minimum、buffer、split、KYC/rail 的完整數值，均**未核實**。Funded 階段高影響新聞前後各 5 分鐘不得執行或平倉；最大總 lot 20% account size、且最多 20 lots。[4]
- **自動化：****明確允許但有限制**：Challenge 可用自己的、獨特 EA；不能讓其他交易者用相同 EA 下 identical trades。此不等於允許第三方 signal/copier 或保證 funded payout 合規。[4]
- **Hedge／閒置／試用：**hedge 未在此頁擷取段核實；30 天至少下一筆交易，否則 hard breach；免費試用未核實。**TradingPilot：**未擷取。

### 4. Blueberry Funded — 挑戰帳戶（產品／帳戶規模未在此材料固定）
- **費用、pass 數值與 payout：**官方規則集合可確認它按交易計畫分規則，且有「minimum trading days」「payout after pass」等文章；本次未開到各篇正文或結帳價格，故一次性／訂閱／reset／activation、steps/target/DD/min days/consistency/期限、出金週期／最低額／split/KYC/rail 均**未核實**。[Blueberry rules collection][5]
- **自動化：****明確限制**：官方集合摘要說部分 EA 可以、部分不行；分界是「誰作交易決定」：輔助工具可，依自身邏輯自行交易的系統不可。不能把它概括成 EA 允許。[5]
- **Hedge／閒置／試用：**官方確有 hedging、inactivity、copy-trade 專題，但本次未取到正文，結論均**未核實**；免費試用未核實。注意集合標示 2026-03-12 前後有不同 prohibited-strategies 政策，不能跨日期搬用。[5]
- **TradingPilot：**未擷取。

### 5. Fintokei — StartTrader（3-step；公開 $5k 範例）
- **費用：**官方主頁顯示 StartTrader 3-step、帳戶最高 $100k、performance reward 50–100%、起價 $44；其公開 $5,000 例亦列 one-time fee **$44**。這是頁顯示價／範例，非所有規模價格。[Fintokei programs][6]
- **通關（$5k 公開欄位）：**Phase I/II/III target **2%/3%/6%**；Phase I/II 時限最少 3 天、最多 180 天；consistency 為一天最多可貢獻 profit target 的 40%；daily loss -3%、maximum loss -6%、單一 open-trade setup 最大風險 -3%。StartTrader 為 3-step；該列並顯示 phase III/StartTrader 沒有 target（以「–」表示）。[6]
- **出金摩擦：**官方主頁說 real money 每兩週；payout 頁稱申請後處理於 24 小時、選 e-wallet 可為 seconds（銀行到帳取決於銀行）。本次未找到 stage-specific 最低額／buffer、KYC、rail 與完整 split 條文；僅可報 performance reward 50–100%。[6][7]
- **自動化／hedge／閒置：**可存取此頁未見明確 bot/EA、hedging、inactivity 終止規則，故均為**未找到明確限制（非核准）**／未核實。**免費試用：**官方產品頁顯示 Free trial（例如 $20k）存在，但不把它當付費產品條款。[6] **TradingPilot：**未擷取。

### 6. FundedNext — Futures Rapid Challenge（官方條款 2026-05-05）
- **費用：**Futures Terms 說各 model parameters 於註冊提供；本次條款摘取未給可重現費表／reset／activation，**未核實**。[FundedNext Futures Terms][8]
- **通關（Rapid）：**25K/50K/100K 的 EOD-trailing max loss 分別 $1,000/$2,000/$2,500；profit target 分別 $1,500/$3,000/$5,000。損失閾值自 $24k/$48k/$97.5k 向上 trailing，最多到起始 balance；這是 **Futures Rapid**，不得套到 CFD 或 Legacy。[8]
- **payout／自動化／hedge／閒置／試用：**此擷取條款未提供足夠的 funded payout 摩擦、automation/hedge/inactivity/free-trial 結論，均**未核實**。**TradingPilot：**未擷取。
- **CFD 注意：**官方 CFD Challenge terms 明說該計畫是模擬評估、費用是 evaluation service fee，非客戶交易存款；但本報告未把 CFD 細節與 Futures Rapid 合併。[FundedNext CFD Terms][9]

### 7. Hantec Trader — challenge / funded（未能固定帳戶型號）
- **官方可證範圍：**官方 help centre 有 Account Rules、Trading Conditions、Prohibited Strategies/Practices 三組；索引明列 EA、copy trading、inactivity、stop loss、max allocation、scaling，以及 reverse hedging、scalping、group/copy trading 等專題。[Hantec help centre][10]
- **結論：**由於僅取得索引，並未取得正文或價目，費用（含 reset/activation）、steps/targets/DD/min-days/consistency/期限、出金摩擦、EA 允許與否、hedge、閒置天數、free trial 均**未核實**。尤其「有 EA 文章」或「有 reverse hedging 禁令文章」不等於一般 EA／hedge 的完整結論。**TradingPilot：**未擷取。

### 8. Audacity Capital — Ability Challenge → Verification → Live Account；Ability One → Live Account
- **費用：**主頁表格表示 Challenge fee 可退（Refundable Fee），但擷取表格沒有金額、觸發／退還細則、reset/activation；**未核實**。[Audacity Capital][11]
- **通關：**Ability Challenge：unlimited trading period、至少 4 天、daily loss 7.5%、max loss 15%、target 10%；Verification：unlimited、至少 4 天、daily 5%、max 10%、target 5%。Live：無 minimum days、daily 5%、max 10%、沒有 target。表中稱 consistency「Not anymore」。[11]
- **出金摩擦：**Live profit share up to 90%、payout「same day」；首頁另稱「upon approval」。這是速度／行銷陳述，不代表保證或完整資格。最低額、buffer、request cadence、KYC、rail **未核實**。[11]
- **自動化：****明確允許（頁面級別）**：表格列 EAs Allowed / Copy Trading Allowed；但未擷取到來源／跨帳戶／identical trade 等細則，不能延伸解讀。[11]
- **hedge／閒置：**未在該表核實；免費試用明確存在（首頁 CTA「Free Trial」），其條款未核實。**TradingPilot：**未擷取。

### 9. FundingPips
- **可存取性／身分：**本次嘗試的官方規則 URL 回 404，官方 help-centre 入口亦回 404；未找到可存取的官方產品條款或結帳表。此僅證明本次 URL 無法取得，**不證明業者停止或沒有規則**。
- **所有要求欄位：**精確產品／階段、費用、pass 規則、官方 payout 摩擦、automation、hedge、inactivity、free trial 均為**未知／未能從可存取第一方材料驗證**；TradingPilot：未擷取。採購前應向合規／support 要該帳戶與階段的書面條款。

### 10. AquaFunded
- **可存取性：**本次嘗試 `https://www.aquafunded.com/rules` 與 `/faq` 均為 404，未取得可引用的第一方規則頁。不能拿二手 prop-firm review 填洞。
- **所有要求欄位：**精確產品／費用／pass、官方 payout 摩擦、automation、hedge、inactivity、free trial 均**未知／未驗證**；TradingPilot：未擷取。

### 11. QT Funded
- **身分／產品性質：**官方 FAQ 說 QT Funded 是 trading evaluation platform，非 broker，不向客戶提供 CFD 或 live-market access；評估為 hypothetical、付費是 subscription fee，不是客戶資金／投資存款。[QT Funded FAQ][12]
- **規則與費用：**官方 `/rules/` 在本次導向 `qtfunded.quanttekel.com/rules/` 後回 404；因此 exact plan、subscription、reset/activation、pass targets/DD/min days/consistency/time limit 全部**未驗證**。
- **payout／自動化／hedge／閒置／試用：**可存取 FAQ 沒有足夠規則，均**未知／未驗證**；TradingPilot：未擷取。

## 採購／自動化前的必要書面確認
1. 指定 **產品名、帳戶規模、平台、evaluation 與 funded/live/simulated-funded 階段**，索取對應價目、稅費、subscription、reset、activation 與退款條款。
2. 對自動化要求明確問：自寫 trader-controlled EA 是否可、API/webhook 是否僅技術可用還是規則允許、HFT／下單率／短持倉、外部訊號、同人多帳戶 copier、跨 firm copier 及相反倉／hedge 是否合規、且出金時是否仍合規。
3. 索取 payout eligibility：KYC 時點、first-payout wait、request cadence、minimum／buffer／cap、consistency／winning day、split、支持 rail、處理與拒付／複核條件。

## 官方來源
[1]: https://help.fundedtradingplus.com/faqs/ "Funded Trading Plus Help Center — FAQ's"
[2]: https://ftmo.com/en/challenge/ "FTMO — What is the FTMO Challenge"
[3]: https://ftmo.com/en/ftmo-free-trial/ "FTMO — Free Trial Challenge"
[4]: https://checkout.toponetrader.com/rules/ "Top One Trader — Trading Rules for Every Account Type"
[5]: https://help.blueberryfunded.com/en/collections/14861030-trading-rules-guidelines "Blueberry Funded Help Center — Trading Rules & Guidelines"
[6]: https://www.fintokei.com/ "Fintokei — programs and public program cards"
[7]: https://www.fintokei.com/payouts/ "Fintokei — Receive high payouts"
[8]: https://fundednext.com/futures-challenge-terms "FundedNext — Futures Challenge Terms (last updated 2026-05-05)"
[9]: https://fundednext.com/cfd-challenge-terms "FundedNext — CFDs Challenge Terms (last updated 2026-05-05)"
[10]: https://help.htrader.hmarkets.com/en/support/solutions/158000590300 "Hantec Trader — General Trading Rules & Conditions"
[11]: https://audacity.capital/ "Audacity Capital — product comparison"
[12]: https://qtfunded.quanttekel.com/faq/ "QT Funded — FAQ"

> **非投資建議。**本文件比較的是頁面可核實的操作條款，不預測獲利、通關、帳戶存續或 payout 核准。
