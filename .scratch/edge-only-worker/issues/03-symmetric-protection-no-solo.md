# 03 — 保護對稱化與單飛移除

**What to build:** 保護只有鏡像盒式一種形狀，兩腿永遠對稱，沒有獲利腿與避險腿之分；對側清空一律跟隨關閉，單飛續跑與單飛鎖定從行為與遙測中消失；「一腿最大化獲利」的不對稱目標被明確移除。

**Blocked by:** 02 — 進場收斂為 edge 唯一語意.

**Status:** ready-for-human

- [x] 已驗證受保護配對的兩腿共享同一組絕對價格邊界，任一邊界觸發即鎖定整對損益
- [x] 非對稱保護分支（獲利腿移動停損、避險腿靜態停損不收斂）不再存在
- [x] 對側腿清空時存活腿立即開始關閉並收斂至 desired-EMPTY，不存在 winner-only 續跑
- [x] cell transitions 不再產生單飛事件，只剩跟隨關閉事件
- [x] 限時同步配對退出、操作員關機、休市、完整性路徑維持跟隨關閉且各自收斂
- [x] 兩腿同受各自的日虧損預算與單筆上限約束，無獲利腿豁免
- [x] 單飛與獲利腿的既有測試隨產品碼刪除，edge 鏡像與跟隨關閉測試覆蓋率不下降
- [x] 全量測試套件綠燈

## Comments

- 刪除 `compute_sl_only`、`compute_trailing_sl`、`compute_solo_lock_sl`、`_asymmetric_precise_protection`、`_apply_solo_lock`、`_own_leg_in_profit`、`_restore_previous_precise_protection`、trailing 常數與 `NO_TAKE_PROFIT`；`peer_leg_empty` 一律跟隨關閉；`LegState` 移除 `previous_precise_*` 並容忍舊重啟載荷；對側 frozen 握手簡化為直接凍結。
- 驗證：`tests.test_pair_cell` 328 過、`tests.test_pair_cell_adapter` 131 過。
