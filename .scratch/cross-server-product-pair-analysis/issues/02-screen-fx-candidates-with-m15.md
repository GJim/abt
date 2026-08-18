# 02 — Screen FX candidates with M15

**What to build:** Extend a submitted 商品配對分析 so the Console obtains common M15 data for every eligible FX candidate over the controller-defined previous complete UTC trading week, applies the policy's screening thresholds, and shows reproducible screening outcomes with market-data calibration evidence.

**Blocked by:** 01 — Launch product catalog analysis.

**Status:** done

- [ ] M15 requests use the same UTC interval on both workers and preserve raw epoch, derived UTC, and calibration evidence.
- [ ] The analysis records coverage, return correlation, price-difference statistics, policy linkage, and content hashes without retaining raw bars.
- [ ] A worker participates in at most one analysis at a time; additional work queues and a failed request retries at most once.
- [ ] Any incomplete, malformed, timed-out, or disconnected M15 response fails the analysis without partial buildable results.
- [ ] Console-visible lifecycle and audit evidence distinguish catalog completion, M15 screening, retry, and failure.
