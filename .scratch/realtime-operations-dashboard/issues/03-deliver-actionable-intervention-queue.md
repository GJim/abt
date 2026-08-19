# 03 — Deliver actionable intervention queue

**What to build:** Give an operator a persistent, severity-ordered intervention queue for pending approvals and control-plane states requiring human action. Each item explains why it needs attention, reveals relevant evidence on demand, and offers only the already-authorized safe actions such as approval, rejection, acknowledgement, or navigation to a dedicated workflow.

**Blocked by:** 01 — Create operations dashboard read model and intervention taxonomy; 02 — Adopt Astryx operations shell and progressive-disclosure primitives.

**Status:** done

- [x] Intervention-required items sort ahead of informational events and retain their classification reason.
- [x] Operators can complete existing safe actions from context and see the queue update without losing auditability.
- [x] Browser tests cover empty, urgent, failed-action, and acknowledged states.
