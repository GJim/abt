# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

The primary users are authorized control-plane operators who approve and oversee the account workers, traders, hedge settings, and active cross-platform hedges they are responsible for.

Their critical job is to safely configure the hedge environment, approve or manage its actors, and monitor multiple workers' positions, orders, hedge state, and—later—trader intents.

## Product Purpose

The product is a web control plane for operating and observing cross-platform MT5 hedging. It makes the relationship between workers, traders, hedge settings, positions, orders, and hedge lifecycle visible and manageable without turning the management surface into a direct credential or broker-control channel.

Success means operators can confidently approve, configure, and supervise a multi-worker hedging environment, identify exceptional conditions, and take accountable operational action.

## Positioning

The control plane coordinates independently deployed account workers through a security-first management model: workers initiate their own authenticated outbound connections, retain responsibility for exactly one MT5 account and terminal, and the control plane provides centralized approval, configuration, and oversight without exposing MT5 credentials.

## Operating Context

Operators work across multiple account workers and traders, reviewing worker health, approval state, hedge configuration, positions, orders, and hedge status. Trader intents are a planned monitoring surface.

The separate MT5 CLI is an internal development tool for inspecting MT5 data formats and performing smoke tests. It accelerates implementation validation and is not a product surface or an integration target for the control plane.

## Capabilities and Constraints

- Operators approve, monitor, and configure hedge settings, workers, and traders.
- The control plane monitors multi-worker hedging together with positions and orders.
- Each worker is bound to exactly one MT5 account and terminal.
- Workers establish authenticated outbound connections; the control plane must not directly connect into worker networks.
- The deployment boundary uses Cloudflare Tunnel for the control-plane ingress.
- The control plane must never expose MT5 credentials.

## Evidence on Hand

- The repository's `CONTEXT.md` defines the cross-platform hedging domain, worker lifecycle, security boundaries, and operational terminology.
- `README.md` documents the separate `mt5` CLI and its local manual-inspection role.
- No product marketing claims, customer references, pricing, or approved visual assets have been provided. Future product surfaces must not invent them.

## Product Principles

1. Make multi-worker hedge operations legible at a glance.
2. Keep authority explicit: approval, configuration, monitoring, and remediation must be distinguishable.
3. Preserve worker isolation and the outbound-only trust boundary.
4. Treat credentials and broker-side control as protected capabilities, never as incidental UI data.
5. Preserve an accountable operational record for consequential actions and changing system state.
