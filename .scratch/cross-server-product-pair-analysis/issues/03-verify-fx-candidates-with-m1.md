# 03 — Verify FX candidates with M1

**What to build:** Let the Console automatically request M1 data only for M15-passing FX candidates, apply the final immutable policy thresholds, and present final passing and failing candidate evidence that an administrator can review before creating a 跨伺服器商品配對.

**Blocked by:** 02 — Screen FX candidates with M15.

**Status:** done

- [ ] Only M15-passing candidates advance to M1 verification.
- [ ] M1 verification evaluates the policy's coverage, return-correlation, median-difference, and P99-difference conditions against aligned UTC evidence.
- [ ] Candidate results visibly identify hard-block differences separately from warning differences.
- [ ] Analysis summaries retain specifications, policies, calibration evidence, metrics, and hashes for audit without retaining raw M1 bars.
- [ ] Incomplete M1 evidence fails the analysis atomically and leaves no candidate eligible for Build.
