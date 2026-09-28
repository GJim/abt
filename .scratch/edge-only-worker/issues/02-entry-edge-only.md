# 02 — 進場收斂為 edge 唯一語意

**What to build:** 最佳進場候選只有 edge 定義：跨 broker 淨利差下限門檻加淨利差 USD 排序；趨勢定向、趨勢強度排序鍵、趨勢暖機閘門、趨勢時間尺度報價寬限與去 skew 豁免全部消失。

**Blocked by:** 01 — 政策模式鍵刪除.

**Status:** ready-for-human

- [x] 候選評估只使用跨 broker 年齡與 skew 上限的報價新鮮度
- [x] 逐門計數只剩 edge 閘門（缺報價／過期／skew／無量規劃等），無趨勢閘門
- [x] 趨勢偏置計算不再存在於進場路徑，影子模式同樣只演練 edge 決策
- [x] 趨勢進場的既有測試隨產品碼刪除，edge 候選排名測試覆蓋率不下降
- [x] 全量測試套件綠燈

## Comments

- 刪除 `donchian_bias`、`momentum_bias` 純函數與 `_trend_bias_for_product` 分派；`_candidates` 只剩淨利差門檻加淨利差 USD 排序；`entry_candidates` 觀測形狀固定為 `net_points`（不再含模式鍵）；研究腳本 `backtest_momentum_40min.py`、`check_momentum_usdjpy.py` 已刪除。
- 驗證：`tests.test_pair_cell` 331 過（含新增的候選形狀與 freshness／skew 回歸測試）。
