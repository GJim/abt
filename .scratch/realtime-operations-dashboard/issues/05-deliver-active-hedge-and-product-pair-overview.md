# 05 — Deliver active hedge and product-pair overview

**What to build:** Give an operator a lifecycle-oriented overview of active hedges and product pairs. The experience must clearly separate currently available product-pair state from unavailable paired-trade lifecycle data, never imply trading activity that the control plane cannot substantiate, and reveal supporting evidence only when requested.

**Blocked by:** 01 — Create operations dashboard read model and intervention taxonomy; 02 — Adopt Astryx operations shell and progressive-disclosure primitives.

**Status:** done

- [x] The overview makes current product-pair state and the availability of paired-trade lifecycle records unambiguous.
- [x] Operators can inspect relevant pair details without obscuring the fleet-level operational picture.
- [x] Browser tests cover active, inactive, unavailable, and error states without fabricated trading data.
