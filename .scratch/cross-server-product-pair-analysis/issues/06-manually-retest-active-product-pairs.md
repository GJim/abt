# 06 — Manually retest active product pairs

**What to build:** Let an administrator manually run 配對重新檢測 for an active 跨伺服器商品配對 using any approved, healthy, connected worker from each of the pair's exact server endpoints. The re-test reuses the pair's original policy and creates fresh evidence without changing the original approval snapshot.

**Blocked by:** 04 — Build and replace active product pairs.

**Status:** done

- [x] Re-test selection accepts only healthy connected workers belonging to the active pair's two exact MT5 servers.
- [x] Re-test uses the pair's original immutable analysis policy and follows the same complete, read-only catalog/M15/M1 evidence requirements.
- [x] A successful or failed re-test records immutable summary, calibration evidence, metrics, hashes, actor, and source workers.
- [x] A failed re-test creates an alert and visibly marks the latest re-test as failed while leaving the pair active.
- [x] Re-testing neither rewrites the pair's reference snapshot nor retires, suspends, or executes broker trades automatically.
