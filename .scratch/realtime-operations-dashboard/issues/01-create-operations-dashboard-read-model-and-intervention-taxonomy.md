# 01 — Create operations dashboard read model and intervention taxonomy

**What to build:** Give an authenticated operator one trustworthy operational-overview data contract that combines the existing workers, alerts, pending enrollments, account/reconciliation health, and product-pair context. The contract must classify what requires human intervention versus what is informational, retain the reason for that classification, and explicitly report paired-trade lifecycle data as unavailable until such records exist.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] The overview returns current, authorized operational data with a documented intervention category and reason for every surfaced item.
- [x] It distinguishes missing paired-trade lifecycle records from an idle or healthy hedge state.
- [x] Contract tests cover authorization, classification, freshness, and unavailable-data behaviour.
