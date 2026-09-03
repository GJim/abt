# 跨平台交易避險

此領域管理多個交易平台之間相互關聯的交易曝險，目的在於讓自動化策略的實驗風險可控。

## Language

**人工測試 CLI**:
直接連線至單一已登入 MT5 terminal 的獨立命令列工具，用於人工驗證 MT5 API 行為及交易；可操作模擬或實盤帳戶，不屬於主控台或帳戶工作者，不得承載配對反向避險的生產決策。

**安全通訊與唯讀對帳切片**:
跨平台避險系統的首個可交付垂直切片：主控台與帳戶工作者可完成經驗證的通訊、命令去重與帳戶狀態對帳，但不得建立、修改或取消任何 broker 訂單，也不提供 Trader 註冊或 intent API。管理站提供登入、worker 批准/拒絕、健康與對帳檢視、外部變動告警確認、憑證撤銷及稽核事件檢視。release gate 必須驗證三個網路環境的主控台復原與兩台工作者註冊/對帳、撤銷/故障/外部變動處置，並證明沒有 broker 寫入。

**跨網路部署**:
首個切片的驗收拓撲：主控台與兩個帳戶工作者各部署於不同網路環境的主機；每個工作者仍專屬於一個 MT5 terminal 與執行程序。

**工作者反向通道**:
帳戶工作者主動經由主控台的 Cloudflare Tunnel 公開入口建立 WSS 持久、安全且可驗證的雙向通道；主控台不得直接連入工作者所在網路。

**已驗證 WSS 工作階段**:
工作者以裝置憑證和對主控台 nonce 的私鑰簽章完成握手後建立的 WSS 通道；唯讀對帳事件以其訊息 ID 與重連 cursor 傳送。主控台對工作者的每一條指令仍須獨立簽署，工作階段本身不得取代指令驗證。

**唯讀帳戶對帳**:
帳戶工作者每分鐘在本機輪詢 MT5，並每 10 分鐘以已驗證 WSS 傳送全量帳戶快照。掛單或持倉的建立、成交、取消、修改、平倉與量變更必須立即傳送 cursor 化 delta；價格、浮動損益和一般帳戶摘要變更僅隨全量快照回報。

**測試 CLI 工作階段**:
人工測試 CLI 以具名稱的帳戶設定選定並登入 MT5 terminal 後建立的附著狀態；帳戶識別與授權狀態以 `account_info` 即時回傳的登入狀態為唯一權威。

**測試 CLI 帳戶設定**:
人工測試 CLI 管理的具名稱 MT5 連線設定，至少識別 terminal、登入帳號與伺服器；設定檔置於 CLI 執行檔所在目錄。多個設定可共用同一 terminal，選定設定時 CLI 重新登入該 terminal；帳戶設定本身不等同於 broker 的實際登入狀態。同一 terminal 的操作必須序列化。

**測試 CLI 憑證**:
人工測試 CLI 在互動登入成功後，將 MT5 密碼存於目前 Windows 使用者的 Windows Credential Manager；帳戶設定只保存其具名稱參照，不得保存明文密碼。

**測試 CLI terminal 生命週期**:
人工測試 CLI 選定 context 後可啟動其專屬 terminal 並登入。工作階段斷開只關閉 API 連線；停止 terminal 與登出 broker 帳戶是彼此獨立且需明確確認的操作。

**測試 CLI context 切換**:
多個測試 CLI context 共用 terminal 時，切換前若原帳戶仍有持倉或掛單，CLI 顯示其數量；互動模式要求確認，`--yes` 可略過。切換不會平倉或撤單。

**測試 CLI 寫入確認**:
人工測試 CLI 的交易、撤單、修改、平倉與 smoke test 等 broker 寫入操作，在互動模式只預覽交易參數並要求使用者回答 yes/no；一次性命令以 `--yes` 確認。

**相對保護價格**:
人工測試 CLI 以百分比、points 或 pips 表示的停損／停利距離，且可與絕對價格互斥選用。市價單預設依送單前可執行報價直接計算並附帶保護價格；只有明確指定時才以實際成交價為基準，在成交後設定保護。

**測試 CLI context 驗證**:
人工測試 CLI 每次操作前必須驗證 terminal 的實際登入帳戶與選定 context 的 login/server 一致；不一致時拒絕讀寫，直到重新登入或明確更新 context。

**測試 CLI 時間語意**:
每個測試 CLI context 保存使用者 IANA 時區；建立時必須明確設定且可後續變更。互動輸入以使用者時區表示，未帶 offset 的 datetime 以該時區解讀，帶 offset 的 ISO-8601 優先；DST 歧義必須拒絕。表格時間以使用者時區的 `YYYY-MM-DD HH:mm:ss` 顯示，JSON 保留 epoch、補充 UTC ISO-8601 欄位及該命令即時觀測的時間來源 metadata。市場資料 offset 依跨日 rotation 與 tick drift 偵測校準。交易寫入成功後，CLI 以該次動作前後可信 UTC 的中點與 broker record 秒級時間計算 trade-record 整秒 offset，保存樣本 ticket、來源、校準 UTC 及誤差；查回失敗不得覆寫舊值。交易紀錄呈現依序使用未過期 offset、過期 offset、UTC；offset 在使用者本地跨日後過期。`context time-status` 必須顯示各 API family 的校準、過期與回退狀態。broker 指定到期以該目標商品可取得的最新 tick server epoch 計算。相對到期時間必須保證不早於指定 duration，broker 正規化後若仍提早則取消掛單並回報失敗。

**管理站 UI 時間格式**:
所有管理站 Web UI 的時間戳一律顯示為 UTC `YYYY-MM-DD HH:mm:ss`，不得附加 `T`、時區 suffix 或 locale-dependent 格式。

**測試 CLI read-only 面**:
人工測試 CLI 第一版提供交易工作流資料、訂單計算與檢查、市場深度與 broker 狀態的查詢命令；不包含 MetaTrader5 的資料庫、圖表或 global variables 等低層 API。`symbol-select` 與 `symbol-hide` 是明確例外，分別加入或移除 terminal 的 Market Watch 商品，並在操作後驗證可見狀態。

**測試 CLI 寫入面**:
人工測試 CLI 以具型別命令支援 market、limit、stop、stop-limit、cancel、modify、close 與 close-by，並提供須明確標示的原生 JSON 請求進階入口；掛單涵蓋 buy/sell limit、buy/sell stop、buy/sell stop-limit；所有寫入均遵守相同的確認與 broker 預先檢查。填單模式只支援 FOK、IOC 與 RETURN，不支援 BOC。所有生產與人工測試訂單不得設定 magic number 或 comment；兩欄必須留空，且控制平面不得以 broker order metadata 作為 correlation 機制。

**測試 CLI 持倉模型**:
人工測試 CLI 同時支援 netting 與 hedging 帳戶；依帳戶保證金模式決定持倉操作語意，close-by 只適用於 hedging。平倉預設全數，只有明確指定 volume 時才部分平倉；hedging 以 position ticket 定位，netting 以 symbol 定位唯一持倉。

**配對反向避險**:
同一交易意圖在兩個平台上建立的相反方向訂單所組成的關係；兩筆訂單共享開倉、調整與平倉的生命週期。
_Avoid_: 反向訂單

**主控台**:
公開網路上的身分、授權、通訊轉送與稽核平面；它連接策略執行體與帳戶工作者，但不擁有或推導配對反向避險生命週期。
_Avoid_: 配對執行器

**主控台 Web 服務**:
主控台以 Docker Compose 部署，並經 Cloudflare Tunnel 以公開網域提供服務面，REST API 負責管理與註冊，持久 WSS 通道負責與已授權帳戶工作者交換事件及指令。只有 cloudflared 可連入主控台服務，所有服務不發布 host port，且主控台資料、OpenBao Raft、SoftHSM token 與 Tunnel 憑證各自使用持久 volume。

**主控台管理員**:
可批准工作者註冊、撤銷裝置憑證及執行人工復歸的本機帳密身分。僅能由主控台主機的 CLI 建立或重設，並自動生成 6 位大寫英文字母帳號及含大小寫字母、數字、`!@#$` 的 20 位密碼；管理站只允許登入，不提供 MFA 或 Web 帳號復原。

**管理站工作階段**:
主控台同源 React SPA 與管理 API 使用的 Secure、HttpOnly、SameSite cookie 工作階段；每個會改變狀態的請求都必須通過 CSRF 驗證，且管理 API 不得啟用寬鬆 CORS。管理員密碼以帶獨立 salt 的 Argon2id 雜湊保存；server-side session 在登入時輪替 ID、30 分鐘 idle 或 8 小時 absolute timeout 後失效，且本機重設密碼時撤銷該帳號所有 session。

**秘密控制面**:
由自建 OpenBao 管理主控台簽章金鑰、裝置憑證 PKI、MT5 憑證與其他集中 secrets 的服務，並以同機 SoftHSM、受版本鎖定的 PKCS#11 KMS plugin 與啟動 health check 進行 auto-unseal，確保重啟可用；工作者的裝置私鑰仍只保存在本機硬體/OS keystore。

**管理站 IP 鎖定**:
主控台 Web 服務只接受 Cloudflare Tunnel 入口，並以 Cloudflare 傳遞的受信任 client IP 作為來源。該 IP 登入失敗超過 3 次時無期限拒絕登入；僅能由主控台主機的 CLI 解鎖。

**帳戶工作者**:
專屬於一個交易帳戶的獨立 MT5 terminal 與執行程序，負責該帳戶的下單、輪詢和對帳；工作者獨佔 terminal 的啟動、附著與健康監控，該 terminal 不得用於人工交易。terminal/API 異常時，工作者以受限退避嘗試自動重啟或重新附著，並每次以 `account_info` 驗證預期 login/server；連續 3 次失敗或帳戶不一致時停止重試並標示需人工處理。主控台不得在工作者間切換終端登入。第一版允許工作者跨多台主機部署。

**帳戶復原生命週期**:
帳戶工作者以 broker observation 作事實持久維護的單一帳戶安全狀態；routine uncertain effect 以觀測後的新補償 effect 收斂，不以盲目重送處理。它不推導另一個帳戶或完整配對的狀態。

**已驗證受保護配對**:
策略執行體已用兩個帳戶工作者的當前事實確認 ticket、方向、數量及 SL/TP 均符合相同 pair revision 的開放避險。
_Avoid_: 已完成復原帳戶

**進場資格與減曝險權限**:
`READY` 是建立新曝險的唯一資格；減少既有已驗證曝險是獨立權限，不能因帳戶不是 `READY` 而一併拒絕。
_Avoid_: 通用 operation 權限

**受保護配對指令**:
策略執行體為配對反向避險持久化的開倉、保護更新或平倉意圖。平倉指令宣告兩腿的 desired state 為 `EMPTY`，不是要求重送特定 broker close；策略執行體以 Worker effect journal 與新的 broker observation 判斷收斂。
_Avoid_: 配對的逐腿 Trader worker operation

**Worker effect journal**:
帳戶工作者本機 SQLite/WAL 的 broker write 證據簿。每個 effect 在 MT5 write 前寫入 `prepared`，緊接 send 前寫入 `send_started`，receipt 或 broker observation 後保存證據。`send_started` effect 永遠不得以相同 effect ID 重送。

**工作者信任邊界**:
帳戶工作者僅在原生 Windows 的 CNG keystore 保存不可匯出的裝置私鑰；沒有受支援 CNG keystore 的主機不得成為工作者。MT5 憑證由 OpenBao 集中管理。工作者只接受已釘選主控台公開驗證金鑰所簽署、帶有交易者指令 ID 與期限的指令；工作者以裝置憑證及 challenge-response 向主控台證明身分，並必須持久去重該指令。

**裝置憑證**:
主控台簽發的工作者身分證明，綁定單一 worker ID、交易帳戶、ECDSA P-256 公鑰與有效期限；私鑰只保存於工作者本機硬體/OS keystore，主控台可撤銷該憑證以阻斷其後續連線。憑證有效期為 30 天，工作者以有效舊私鑰簽署輪替請求並生成新 key pair；主控台只在舊憑證未撤銷時簽發新憑證，短重疊期後舊憑證失效。

**工作者註冊**:
尚未授權的工作者在已登入 MT5 後，以其新生成公鑰簽署並透過 REST 提交 `account_info`、配對碼與連線來源資訊的程序。註冊需要管理員預先簽發、一次且短期有效的 invite，主控台不依賴 Cloudflare client IP 的註冊限流。待批准註冊在 15 分鐘後失效，拒絕後不得重新啟用；工作者必須重新註冊。管理員批准後，工作者以私鑰 challenge-response 輪詢領取裝置憑證；取得憑證前不得建立常態 WSS 通道。

**帳戶 bootstrap grant**:
已廢止的初始 worker password 交付方式。新的工作者註冊不使用 bootstrap grant。

**初始憑證交付**:
尚未批准的帳戶工作者在本機互動輸入 MT5 password、成功登入並取得帳戶與 terminal evidence 後，經 Cloudflare TLS 將 password 與簽署的註冊請求交給主控台。password 是不顯示、不回顯的傳送欄位；主控台立即將其寫入 OpenBao，註冊被拒絕或逾期時刪除該 secret，且 worker 不得持久保存或記錄 password。

**MT5 憑證代理**:
已批准工作者在每次需要登入 MT5 時，於已驗證 WSS 工作階段中請求其綁定帳戶的 MT5 password；主控台驗證 worker/account binding 後由內部 OpenBao 讀取並只在該工作階段回覆。工作者只可在記憶體保存 password，OpenBao 不得公開給工作者網路；password 下發信任 Cloudflare 的 TLS proxy，不另加應用層加密。

**工作者配對碼**:
工作者以已登入 MT5 的 `account_info` 註冊時在本機等待批准畫面顯示並提交至主控台的短期 8 位數人工比對碼；管理員必須將其與待批准申請的觀測來源、實際 broker 帳戶資訊比對後才可簽發裝置憑證。它只證明人工配對，不得作為帳戶身分或網路來源的唯一證據。

**生命週期時間權威**:
策略執行體的單調時鐘裁決配對期限，帳戶工作者在 broker send 前以指令絕對期限作最後安全檢查；各主機必須同步受信任 UTC 時間來源，broker 時間只作事件證據。

**限時同步配對退出**:
已驗證受保護配對超過其持久化最大持有時間後，由策略執行體依兩個帳戶工作者的當前報價、近期自然波動及 broker stop/freeze constraints 建立的近市價退出走廊。兩腿以相同 normalized market movement 為目標，不保證相同 broker fill；若只剩一腿，策略執行體保留 15 秒 orphan grace，逾時只 market-close 剩餘 owned ticket，若兩腿在走廊 armed 後 30 秒仍存在則並行 market-close。所有期限、targets、effects 與 observation evidence 必須 durable，重啟不得重新起算。

**失聯安全平倉**:
一個帳戶工作者無法經主控台 relay 與策略執行體保持有效通訊超過心跳寬限時間後，停止新進場並依本機 broker observation 進入帳戶復原生命週期。已受 broker SL/TP 保護的曝險不因短暫失聯盲目重送或修改，重連後必須重新觀測。

**撤銷安全平倉**:
撤銷工作者裝置憑證是緊急安全事件；工作者收到撤銷或無法再建立有效工作階段後，依本機帳戶復原生命週期停止新進場並處理未受保護曝險。主控台只執行身分撤銷與通知，不選擇 broker 操作。

**策略風險權責**:
策略執行體獨自決定活躍配對數、名目曝險、保證金使用量與損失上限；主控台不另設這些聚合風險硬上限，也不推導配對生命週期。

**策略執行體（Strategy Runtime）**:
以單一交易者身分運行策略決策與配對執行的持久程序；它擁有配對反向避險的完整生命週期，並以本地 durable state 在重啟後從帳戶工作者事實恢復。已配對進入配對執行單元的配對不適用本節；其生命週期改由持久配對路由指定的 leader 帳戶工作者擁有。
_Avoid_: 獨立 Trader Execution 服務、無狀態策略腳本

**配對執行單元（Pair Execution Cell）**:
在配對的兩台帳戶工作者上執行相同模組的深模組；leader/follower 角色由持久配對路由指定，不是租約，也沒有 epoch 或交易 TTL，且不綁定任何交易者身分。已配對者由其取代策略執行體成為配對反向避險生命週期擁有者，僅 leader 可建立進場嘗試並持有唯一的 canonical 策略政策；shared 政策由 leader 撰寫，follower 自行撰寫其自身風險參數並經配對接受載荷供 leader 逐字複製。follower 不推導獨立 edge 或 terminal 配對狀態。它保留 ADR-0009 的單一擁有者、durable state 與主控台不推導配對狀態等不變量，只搬移擁有者位置。
_Avoid_: 配對執行器（與主控台角色混淆）、配對租約、交易者綁定路由

**Peer Terminal Proof**:
同一配對嘗試中，一台 Worker 對另一台 Worker 所持鏡像 leg 已 terminal 的 authenticated、durable 狀態證據；它不是 relay acknowledgement、broker receipt 或沉默。只有本機 broker-verified `EMPTY` 與同一 attempt 的 peer terminal proof 都存在時，因缺少該 proof 而產生的停機保護才可自動解除。
_Avoid_: relay 成功確認、推定 peer 空倉

**Executable Protection Equality**:
將嘗試綁定 sizing plan 的 tick grid 視為 SL/TP broker evidence 的價格身分。MT5 binary-float 對同一已請求 tick 的微小序列化誤差等價；不同 executable tick、off-grid 預期價格或缺少該 attempt plan 則是不一致 evidence，必須 fail-closed。
_Avoid_: 浮點文字完全相等、以寬鬆價格容差接受不同 tick

**工作者主導配對（Worker-Initiated Pairing）**:
兩台帳戶工作者自行建立配對路由的程序；沒有管理員建立的配對路由。工作者於首次未配對連線時宣告期望角色，未宣告即為可用 follower；leader 向主控台查詢當前已連線、可用且未配對的 follower 並選定其一（互動選擇，或以明確 follower worker ID 非互動選定），再經已驗證主控台提出配對提議。非 TTY 且未指定 follower worker ID 的 leader 不得阻塞等待輸入，維持已驗證且未配對並輸出可行動診斷；互動模式取得空清單時亦同。被選中的 follower 只在自身 broker 驗證空倉、沒有任何未結嘗試或效果、具備配對所需就緒條件、可讀取 USD 帳戶且餘額為正、且仍未配對時自動接受，否則明確拒絕。配對以兩階段控制流程完成，不是單一 compare-and-swap。
_Avoid_: 管理員核准配對、配對合約核發、人工接受步驟、保留與建路由合併為單一 CAS

**配對保留（Pairing Reservation）**:
兩階段配對流程的第一階段：主控台以單一原子 transaction 產生唯一 `proposal_id` 並同時保留 leader 與 follower 兩台工作者，成功條件為雙方皆已驗證連線、未出現於任何其他路由或保留、無衝突的 live strategy runtime 擁有權，且可為雙方取得 `pair_execution_cell` 執行模式互斥。保留帶 30 秒配對保留逾時，該逾時純屬配對控制中繼資料，不是交易租約或 TTL，不 fence 任何 effect、嘗試或既有路由。主控台於保留成立後才轉送提議。follower 拒絕、逾時、任一方斷線或最終 transaction 失敗都會完整釋放保留，不留下路由、角色指派、執行模式主張或工作者本機配對狀態。兩個 leader 選中同一 follower 時，敗方於保留階段即取得決定性衝突並可立即重新列表。
_Avoid_: 配對租約、以保留逾時視為交易期限

**配對接受載荷（Pairing Acceptance Payload）**:
follower 接受配對時經不透明 relay 送交 leader 的內容，包含 `proposal_id`、自身 worker ID、當下讀取並凍結的啟動帳戶餘額與帳戶幣別（必須為 USD），以及自身撰寫的風險參數 `maximum_margin_fraction`、`daily_loss_fraction`、`trade_loss_fraction`、`maximum_loss_per_trade_usd`。leader 必須將其逐字複製為 canonical 政策中的 `follower_risk`，不得重算、夾限、換算或以自身值取代；載荷缺失、格式錯誤、非 USD 或餘額非正時 leader 失效關閉且不發布政策。主控台全程不解讀該載荷。
_Avoid_: leader 代訂 follower 風險、以預設值補齊 follower 參數

**剩餘可用單腿虧損摘要（Remaining Allowed Leg Loss Summary）**:
每台帳戶工作者持續向對側發布的版本化摘要，內容為自身 worker ID、單調版本與當前 `allowed_leg_loss_usd`；leader 必須持有對側當前摘要才可建立嘗試，且指派給任一腿的虧損不得超過該腿擁有者最後發布的額度，所用摘要版本凍結於該嘗試。摘要只是 leader 的最佳化，不構成對 follower 的權威：follower 於收到嘗試時仍以自身當下本機額度重新檢查，指派虧損超出即拒絕。該檢查屬本機風險運算，不涉及報價或 edge 評估。

**可用 follower（Available Follower）**:
已通過身分驗證、當前連線、尚未配對、未被其他配對提議保留，且宣告或預設為 follower 的帳戶工作者；主控台只以此清單回應提出配對的 leader，清單本身不構成任何交易授權。

**持久配對路由（Durable Pair Route）**:
一組 leader/follower 帳戶工作者的獨佔配對關係；身分為有序的 leader/follower 工作者組合加上主控台產生的全域唯一 `route_id`，不含交易者身分、租約、epoch 或 TTL。每次新配對取得新的 `route_id`，且永不重用。`route_id` 不是租約 epoch：不續期、不到期、不 fence 任何 broker effect，也不構成交易授權；工作者本機的 `recovery_epoch` 是另一個獨立概念，屬該工作者自身 broker session 的市場證據，兩者不得互相比較或推導。路由跨重連與重啟持續有效，且永遠優先於啟動時的角色旗標或預設角色，重連者恢復原指派角色。主控台的路由紀錄是 relay 權限與角色指派的權威，工作者的持久副本只是自身參與的復原證據；兩者不一致時工作者立即移除進場就緒並觸發 metadata 對帳，但絕不因此丟棄、平掉或推導本機曝險，也不得取得未被指派的角色。
_Avoid_: 配對租約、管理員授權路由、以撤銷代替解除配對、以 route_id 作為租約 epoch

**安全解除配對（Safe Unpair）與 UNPAIRING 狀態**:
解除配對必須經過明確的 `UNPAIRING` 路由狀態。任一已驗證工作者皆可以指令針對當前 `route_id` 進入該狀態，無提議者特權。進入即刻對雙方生效：雙方立即移除進場就緒，`UNPAIRING` 期間不得開始任何新嘗試，leader 停止候選評估；但保護、減曝險與收斂既有曝險完全不受限制。雙方各自以新鮮本機證據提出安全解除配對聲明（自身 broker 驗證空倉、所有嘗試與效果 terminal），聲明綁定當前 `route_id` 加上該工作者當前本機 attempt/effect 狀態版本；任何新增或變更的本機 effect 都會推進版本並使既有聲明失效，必須重新提出。主控台只在同時持有兩份新鮮且綁定當前值的聲明時才移除路由，並不自行推導空倉或 terminality；單邊、過期或綁定舊版本／舊 `route_id` 的聲明不生效。任一方可明確取消 `UNPAIRING` 回到正常運作。路由移除後雙方回到未配對、釋放執行模式主張，並可再次配對取得全新且不重用的 `route_id`。緊急身分撤銷是另一機制，會移除進場就緒但絕不等同 broker 空倉，也不構成安全解除配對。
_Avoid_: 單一訊息解除配對、以撤銷代替解除配對、無狀態版本綁定的聲明

**初始相容商品探索（Initial Compatible Product Discovery）**:
路由建立後、任何報價處理之前，兩台帳戶工作者經不透明 relay 交換版本化商品目錄與規格摘要以決定共同可交易宇宙的階段；主控台全程不解讀這些摘要。取交集前先對各自目錄套用 `trade_mode == 4`（完全交易）前置條件，與 `strategy/realtime_arbitrage.py` 一致；`trade_mode` 缺失、非整數或為布林值時該目錄視為無效，而非僅略過該符號。候選為兩份已過濾目錄的精確符號名稱交集（不做後綴正規化）；`trade_calc_mode`、`contract_size`、`volume_min`、`volume_step`、`allowed_directions` 任一不相等即排除，必須同時允許 LONG 與 SHORT，填單模式取雙方共有 FOK，否則共有 IOC，否則排除；base/profit 幣別、digits、point 與 trade tick size 不要求相等。錄取商品的 canonical 執行 point 取雙方較大值、共同 volume_max 取較小值，商品識別由精確符號名稱與 canonical 相容摘要雜湊決定性衍生。任一目錄不可得或無效、或無任何相容符號時探索失敗且不進場。
_Avoid_: 已建置商品宇宙、等名即相容、刷新時自動擴充宇宙、交集後才過濾 trade_mode

**宇宙世代（Universe Generation）**:
探索結果的識別碼，於同一路由內唯一且單調遞增；它命名一次探索結果，不命名配對關係，不取代 `route_id`，也不會到期。探索結果凍結於當前 `universe_generation`，每小時刷新只重新驗證與暫停既選符號，不自動納入新出現的符號，也不改變 `universe_generation`。任一已驗證工作者可請求明確重新探索，僅在雙方 broker 驗證空倉且無任何未結嘗試或效果時受理；重新探索在**同一路由**上安裝新的 `universe_generation`，`route_id`、角色與 canonical 政策皆不變，不是重新配對。重新探索失敗時保留原 `universe_generation`，不得安裝空宇宙。relay envelope 攜帶 `route_id`；政策、嘗試、商品、量規劃與隔離狀態在涉及商品識別處攜帶 `universe_generation`。主控台只驗證 `route_id` 與發送者角色，不驗證宇宙內容。
_Avoid_: 以路由世代同時表示配對與探索、重新探索產生新 route_id

**可交易商品宇宙（Eligible Product Universe）**:
由初始相容商品探索錄取、凍結於當前 `universe_generation`、未遭隔離，且雙方報價、point/tick size、帳戶幣別 tick value、volume、填單模式與保護校準皆新鮮完整的配對商品集合。任一商品資料缺失或過期只移除該商品資格，不觸發 signal-time RPC，也不使其他商品失去資格。

**最佳進場候選（Best Entry Candidate）**:
leader 對可交易商品宇宙內每個商品的兩個鏡像方向套用 edge-points 門檻（以該商品 canonical 執行 point 換算）後，以雙方預先快取之每 point USD 價值計算保守預期 edge USD 並選出的唯一候選；同值時依 normalized edge points、衍生商品識別、direction 決定固定順序。選定後商品、方向、量與排名證據凍結於該次嘗試。
_Avoid_: 最大 raw price edge

**配對商品隔離（Pair Product Quarantine）**:
任一進場腿收到 MT5 `10021 TRADE_RETCODE_PRICE_OFF`／`No prices` 後，配對執行單元針對該工作者配對與衍生商品識別持久建立的禁止進場狀態。隔離保存 offending Worker、attempt、broker receipt 證據與觀測當時的 `universe_generation`，跨重啟持續有效且無自動到期；因商品識別由精確符號名稱與 canonical 相容摘要雜湊決定性衍生，重新探索後識別相同者隔離仍然有效。顯示或恢復 bid/ask 不會自動解除，只能在沒有 unresolved attempt 時由已驗證操作員明確解除並留下稽核事件。
_Avoid_: 暫時沒有報價、報價過期

**策略政策雜湊（Canonical Policy Hash）**:
leader 持有並發布之唯一 canonical 策略與風控政策版本雜湊，涵蓋執行模式、雙方各自的 strategy budget 與逐商品量規劃依據、雙方各自的紐約已實現虧損預算、edge-points 門檻、報價新鮮度與 skew 上限、follower 確認逾時秒數與生命週期退出政策；它不預先選定單一商品或方向，也不列舉商品篩選清單。政策撰寫權責分明：shared 政策（`mode`、`entry_edge_points`、`quote_max_age_seconds`、`quote_max_skew_seconds`、`follower_confirmation_timeout_seconds`、`trading_blackout_start_ny`、`trading_blackout_end_ny`、`maximum_holding_seconds`）由 leader 獨自撰寫；`leader_risk` 由 leader 為自身撰寫；`follower_risk` 與 follower 的凍結啟動餘額只來自配對接受載荷並由 leader 逐字複製。紐約週一至週五的半開區間 `[trading_blackout_start_ny, trading_blackout_end_ny)` 禁止進場並使既有配對由兩個 Worker 各自進入 desired-`EMPTY`，紐約週六與週日全天禁止交易；holiday 暫由操作員處理。未提供設定檔時政策由執行期以預設值合成，其中每台工作者的 strategy budget 預設為其啟動時的 broker 帳戶**餘額**（必須為 USD 帳戶且餘額為正，否則失效關閉）並凍結於該次配對。follower 先逐位元組驗證自身區塊與所送內容相同，再只在自身帳戶已 broker 驗證空倉時依雜湊持久接受該政策。雙方必須持久保存**完整 canonical 政策內容**，而非僅保存雜湊；雜湊只是對雙方各自已持有內容的完整性檢查，重啟後不需重新索取政策即可驗證嘗試。政策內容變更需要新雜湊及兩台帳戶工作者的 broker 驗證空倉事實，主控台全程不解讀政策內容。
_Avoid_: 配對合約、配對租約、商品白名單、只保存政策雜湊

**配對設定檔權責（Role-Specific Configuration Authority）**:
配對執行單元不要求設定檔，缺少設定檔即由執行期合成預設值並可運作。若提供設定檔，其權限依該工作者實際取得的角色而定：follower 設定檔只能設定自身四項風險參數（`maximum_margin_fraction`、`daily_loss_fraction`、`trade_loss_fraction`、`maximum_loss_per_trade_usd`）與自身 `allow_live` 安全開關，不得設定 `entry_edge_points`、`quote_max_age_seconds`、`quote_max_skew_seconds`、`follower_confirmation_timeout_seconds`、`trading_blackout_start_ny`、`trading_blackout_end_ny`、`maximum_holding_seconds` 或 `mode`；leader 設定檔可設定 shared 政策與自身風險，但永遠不得設定 follower 的風險參數或預算。任何設定檔都不得含路由、`route_id`、`trader_id`、`built_products` 或 `eligible_products`。上述違反皆為啟動設定錯誤而非靜默忽略；取得的角色與設定檔權限不符時失效關閉，不得部分套用。
_Avoid_: 設定檔一律可覆寫任意 tunable、follower 覆寫共享政策

**執行模式權責與 allow_live**:
`mode` 是配對層級且由 leader 撰寫的值，預設為 `live`，follower 不撰寫也不改寫。follower 可於本機設定明確的 `allow_live` 安全開關（預設 `true`）；當 `allow_live` 為 `false` 而 canonical 政策為 `live` 時，follower 失效關閉：明確拒絕該政策、不發布接受、不受理任何嘗試，維持已配對但不具進場就緒。它不得將 `mode` 改寫為 `shadow`，不得在 leader 為 live 時靜默以 shadow 運作，也不進行協商。leader 只能將該拒絕視為對側未就緒而不建立任何嘗試，不得視為接受。化解方式為操作員行為：在雙方 broker 驗證空倉時由 leader 發布 `shadow` 政策，或調整 follower 的 `allow_live` 後於空倉狀態重新評估。
_Avoid_: follower 靜默降級為 shadow、以 follower 設定改寫配對政策

**逐商品量規劃（Per-Product Sizing Plan）**:
量規劃無法早於探索存在，因此順序為：建立路由、初始相容商品探索、接受 canonical 政策、開始收集報價、以**當前本機報價與當前 MT5 每手保證金計算**建立初始量規劃與緊急保護規劃，其後才開始候選評估；任一有效正值規劃缺失前不得處理任何候選。此後每小時刷新一次，依自身 `strategy_budget_usd`、`maximum_margin_fraction` 與當前 MT5 每手保證金本地計算最大量，連同 point、tick size、帳戶幣別 tick value、volume 上限、填單模式與粗略保護輸入一併快取；雙方交換版本化摘要後，配對量取兩側較低值並依共同 volume step 對齊。單一商品刷新失敗只暫停該商品至下次每小時刷新，不需 signal-time 保證金查詢。每個規劃版本不可變且具名，被取代的版本至少保留 `follower_confirmation_timeout_seconds` 加上有界 relay 處理時間，且在該期間仍可用於驗證已派遣的嘗試，使跨刷新界線的在途嘗試以其具名版本驗證而非必然遭拒；被取代的版本不得用於新候選。
_Avoid_: 啟動時即建立量規劃、刷新即刪除舊版本

**每帳戶紐約已實現虧損預算（Per-Worker NY Realized-Loss Budget）**:
每台帳戶工作者依 `America/New_York` 曆日獨立追蹤、只累計已平倉策略腿已實現損益（不含浮動損益）的每日虧損上限；單腿允許損失取 `trade_loss_fraction`、`maximum_loss_per_trade_usd` 與當日剩餘允許額度三者最小值，且停利與該允許損失金額 1:1 對稱。leader 以 `leader_risk`、follower 以 `follower_risk` 各自計算。`maximum_loss_per_trade_usd` 預設 `40` 沿用 legacy `--emergency-stop-loss-usd` 的數值，但語意是**刻意重新詮釋**：此處為送單前的每筆硬性上限（三個上限之一），而非 legacy 的緊急停損觸發；粗略緊急保護以當前計算所得的 `allowed_leg_loss_usd` 為目標，而非固定常數。任一帳戶額度用盡即不得再開新腿，另一帳戶額度不受影響。
_Avoid_: 視 maximum_loss_per_trade_usd 等同 legacy 緊急停損語意

**立即派遣進場（Immediate Entry Dispatch）**:
leader 選定最佳進場候選後，於同一次持久決策內同時完成：持久化不可變嘗試與自身已備妥的 broker effect、啟動只屬於自己的本地單調確認計時器、送出自身受保護市價進場，並將該不可變嘗試轉送 follower。嘗試攜帶 `route_id` 與 `universe_generation`，其 relay envelope 攜帶 `route_id`。follower 只執行非市場安全檢查（持久路由與當前 `route_id` 且路由不在 `UNPAIRING`、當前 `universe_generation`、政策雜湊逐字相符且已接受、量與商品資格、具名量規劃版本為當前版本或仍在保留期的舊版本、指派虧損不超過自身當前本機剩餘額度、隔離狀態、粗略保護可建構性），通過後立即啟動自己的本地單調確認計時器並送出自身受保護市價進場，不重新檢查報價或 edge，也不等待第二則 leader 訊息；相同 ID 且內容相同的重複投遞回傳既有結果，絕不重送。leader 與 follower 之間沒有 arm/commit 交握、沒有共同未來執行時間，也沒有 execution expiry。
_Avoid_: arm/commit、進場提交授權

**嘗試終止**:
配對嘗試只有在雙方都被證明安全後才是 terminal：兩台帳戶工作者各自在自己本地啟動、彼此獨立的單調確認計時器內取得雙方 exact broker 部位證據並套用精確保護後宣告 `ACTIVE`；或任一方計時器到期、拒單、送單結果未知、報價過期或 peer session 遺失時，各自僅以自身 durable effect 與新鮮 broker 事實收斂至 desired `EMPTY`，並盡力通知對側加速其收斂。逾時、報價過期或就緒條件消失都不得直接遺棄嘗試，也不得假設對側已完成或尚未送單。

**部署模式開關**:
每配對的執行模式（`strategy_runtime`、`pair_execution_cell` 或 `shadow`）；配對執行單元的執行模式預設為 live，`shadow` 需明確選擇。主控台強制同一配對的 `strategy_runtime` 與 `pair_execution_cell` 互斥存在，避免兩個生命週期擁有者同時啟用。配對執行單元的互斥主張在配對保留 transaction 中同時為**兩台**工作者取得，並於最終建立路由的 transaction 再次確認；其 owner kind 固定為 `pair_execution_cell` 且不帶 `trader_id`，legacy 策略執行體則保留自身交易者身分。安全解除配對移除路由時同時釋放雙方主張。

**影子模式**:
配對執行單元執行完整的候選排名與立即派遣進場決策路徑（報價一致性、就緒狀態、edge 決策），但以模擬 ticket 與模擬成交價取代真正的 `order_send`，不寫入 Worker effect journal 也不送出任何 broker 寫入；用於在正式切換前比較新舊路徑的候選決策與遙測。
_Avoid_: arm 模擬

**交易者指令 ID**:
策略執行體為每次配對生命週期變更建立的唯一識別碼；策略執行體與帳戶工作者以指令 ID、effect ID 與 payload hash 持久去重，主控台只轉送並稽核。

**交易者（Trader）**:
策略執行體向主控台證明並用來存取帳戶工作者的獨立服務身分；每個部署實例使用獨立憑證，策略名稱僅作為可稽核註記，不構成授權身分。

**Trader worker operation**:
策略執行體經主控台 relay 對單一明確指定工作者發起的 broker 寫入。主控台只驗證身分、路由及 envelope，帳戶工作者以 operation ID、payload hash 與 Worker effect journal 持久去重並防止 `send_started` operation 重送。

**Trader broker read**:
交易者經主控台向單一明確指定工作者發起的即時唯讀查詢；包含 account info、symbol info、historical ticks、目前掛單與目前持倉。回應只來自已連線工作者的當前 MT5 session，主控台不得以自己的持久資料替代。historical ticks 每批最多 1,000 筆，交易者自行拆解及串接大型查詢。

**交易者 WSS 工作階段**:
已批准交易者以其裝置憑證和私鑰證明身分後，與主控台建立的持久雙向 relay 通道；策略執行體與帳戶工作者的 versioned envelopes 經此路由。中斷後以最後 ACK cursor 至少一次重送，接收端以來源身分與不可變 event ID 去重。

**交易者註冊**:
尚未授權的交易者以新生成、不可匯出的 Windows CNG P-256 公鑰及其簽署證明，經 API 聲明策略名稱與公開 IP 的程序；註冊需要管理員預先簽發、一次且短期有效的 invite，主控台分配 registration ID，管理員依該聲明 IP、策略名稱與金鑰指紋審核後決定是否簽發交易者裝置憑證。待審核註冊在 15 分鐘後過期，拒絕與過期的 registration ID 均不得重用；交易者必須重新註冊。公開 IP 是交易者的宣告資料，不保存或比對 Cloudflare 觀測來源。交易者註冊不使用 MT5 證據或人工配對碼。

**註冊 Invite**:
管理員在管理站為 worker 或 Trader 簽發的角色綁定、一次性 enrollment 授權；自簽發起 60 分鐘有效，成功使用時立即消耗並開始 15 分鐘待審核 registration。管理員可撤銷未使用 invite；使用、過期與撤銷後均不可復原。切換至 invite 規則時，所有尚未批准且未使用 invite 的 worker registration 立即過期，已批准 worker 不受影響。完整值只在建立時顯示一次，控制平面帳本只保存其雜湊、目標角色、到期與使用或撤銷狀態，並為簽發與使用留下稽核事件。

**交易者裝置憑證**:
主控台簽發的交易者部署身分證明，綁定單一 trader ID、不可匯出的 Windows CNG ECDSA P-256 公鑰與有效期限；私鑰只能保存在 Windows CNG keystore。憑證有效期為 30 天，交易者以有效舊私鑰簽署輪替請求並生成新 key pair；主控台只在舊憑證未撤銷時簽發新憑證。

**交易者憑證撤銷**:
主控台撤銷交易者裝置憑證後立即終止其 WSS 工作階段並拒絕新 relay 請求；撤銷不替策略執行體選擇、取消或平倉任何 broker 操作。

**策略啟動對帳**:
策略執行體啟動或重啟後，以本地 durable state 與兩個帳戶工作者的當前掛單、持倉及 effect evidence 重建未終結配對的程序；完成前不得建立新曝險。

**外部變動**:
未對應帳戶工作者已知 effect 的訂單、持倉或保護價格變更；帳戶工作者必須立即回報，策略執行體據此更新完整配對 desired state，主控台只保留 relay audit copy。

**人工復歸**:
帳戶工作者無法自動恢復時，由 Worker operator 在本機執行的 break-glass 程序；主控台只記錄身分與 audit，不提供取消、平倉或配對操作。

**配對事件紀錄**:
策略執行體持久保存的配對生命週期狀態轉換紀錄，包含交易者指令 ID、兩腿 broker facts、決策原因與時間戳；主控台可保存不參與交易判斷的 immutable relay audit copy。

**控制平面備份**:
主控台 DuckDB ledger 與 OpenBao Raft 的可恢復快照，在本機每小時建立並自動輪替保留最近 24 份；每次裝置憑證批准、撤銷或輪替等 PKI 變更後必須立即建立加密快照。管理員每週至少一次手動將加密備份保存至異地，異地復原點目標因此為一週。

**控制平面帳本**:
主控台以 DuckDB 保存身分、授權、連線、relay delivery 與不可變 audit；它不是交易指令、broker facts 或配對生命週期的權威。單一 ASGI process 擁有所有讀寫，REST、WSS 與本機 CLI 寫入必須經同一序列化 transaction writer。

**主方向平台**:
被策略執行體選定為承載方向性訂單的平台。
_Avoid_: 主平台

**避險平台**:
被策略執行體選定為承載與主方向平台相反方向訂單的平台；其訂單不會觸發新的獨立配對反向避險。

**平台選定**:
策略執行體在配對進場前選出主方向平台與避險平台的決策；一經選定，該配對反向避險的生命週期內不得更換平台。

**平台合格性**:
平台選定前的硬性判定：兩個交易帳戶皆已授權、連線健康、目標商品可交易、報價未過期、能遵守帳戶操作規範，且各自有足夠保證金；任一條件不成立時不得建立配對。

**跨伺服器商品配對**:
一個無方向的商品關係，由兩個交易伺服器各自的一個精確名稱商品組成，可建立相反等值曝險。策略執行體從兩個帳戶工作者的當前商品目錄與規格判斷相容性、方向、填單模式與 sizing；主控台不建立或管理商品配對。
_Avoid_: Mapping

**交易成本排序**:
在全部通過平台合格性的候選組合中，策略執行體依該次交易的預期全成本選擇組合；排序須可重現且可解釋。

**市價單**:
以當時可得市場價格立即執行的訂單；其成本排序以即時 bid/ask 價差與報價新鮮度為核心。

**限價單**:
僅在指定價格或更有利價格成交的訂單；第一版只支援此類訂價單，並以預期成交機率與預期成本排序，未成交或到期屬正常結果。
_Avoid_: 訂價單

**雙限價進場**:
在主方向平台與避險平台同時掛出相反方向限價單的進場方式。FOK 進場只有兩腿皆全額成交時建立配對反向避險；IOC 進場依部分成交淨額處置建立已匹配的成交量。

**首成交腿**:
雙限價進場中最先被 broker facts 確認成交的訂單；無論位於主方向平台或避險平台，均會啟動另一腿的可容忍時間。

**Fill-or-Kill（FOK）**:
要求訂單一次全額成交，否則完全不成交的填單模式；使用 FOK 的雙限價進場兩腿均須全額成交。若即時對帳仍發現部分成交，視為可補救的 broker 異常，依 IOC 的取消、對帳與未避險差額平倉順序處置。

**IOC 部分成交淨額處置**:
IOC 進場後，策略執行體以兩腿實際成交量依商品配對比例換算可匹配量，保留較小量為已避險配對，先取消剩餘訂單並重新觀測，再以市價平掉未避險差額。不進行 IOC 補單；任何 broker 結果不確定時依 Worker effect evidence 與新 observation 恢復。

**成交權威**:
決定意圖訂單實際成交量的即時對帳快照；broker 送單回覆只證明接受或掛單，不能作為成交事實。
_Avoid_: 送單成交量、回執成交量

**執行識別碼**:
策略執行體在 dispatch 前持久化的不可變 execution identity；它不得寫入 broker comment 或 magic number。帳戶工作者以 effect ID、broker ticket 與 observation 建立執行證據，無法唯一確認時不得猜測或重送。
_Avoid_: ticket 配對、價格時間猜測

**不確定恢復結果**:
取消或市價平倉因斷線而未取得 broker 結果的狀態；該次交易參與的 worker 必須凍結，並在帳戶清空與人工解凍前拒絕任何新交易命令。
_Avoid_: 立即重試

**進場到期時間**:
策略執行體為一個配對進場指定的絕對失效時間；帳戶工作者在 broker send 前執行最後期限檢查，到期後策略執行體依當前 broker facts 處理未成交訂單。

**配對保護出場**:
在兩腿持倉上各自設定 broker 代管的停損與停利。任一腿因保護價格平倉時，帳戶工作者回報 broker fact，策略執行體將完整配對 desired state 改為 `EMPTY`。

**單腿成交逾時**:
配對進場中只有一腿被 broker facts 證明成交，且另一腿未在策略執行體允許時間內成交的狀態；策略執行體依風險規則與當前 Worker facts 選擇補救或將 desired state 改為 `EMPTY`。

**不完整進場**:
配對進場中一腿 effect 可能已建立曝險、另一腿卻未能建立的狀態；策略執行體必須以 Worker effect evidence 與新 broker observation 收斂，不得重送未知結果。

**交易帳戶**:
由一台帳戶工作者獨佔連線並下單的 MT5 帳戶，以已驗證的 MT5 login 與 server 識別；一個帳戶只能唯一綁定一台活躍帳戶工作者。第一版同時支援模擬帳戶與實盤帳戶。
_Avoid_: 測試帳戶

**平台操作規範**:
交易平台對下單、取消與修改等操作所施加的頻率或行為限制；遵守此規範優先於為消除些微未避險曝險而頻繁操作。

**帳戶操作規範**:
附屬於單一交易帳戶、可審核的操作規範，定義各類操作的最小間隔、滑動視窗上限、冷卻時間與拒絕後退避；主控台必須在送出操作前強制執行。

**未避險曝險**:
配對反向避險中，兩腿已成交的反向等值量不一致時，超出可配對等值量的部分。
