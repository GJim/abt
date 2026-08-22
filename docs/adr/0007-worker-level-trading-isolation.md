# Worker-Level Trading Isolation

All execution sources isolate failures by freezing the participating workers rather than individual pairs. A frozen worker cannot join new trades and must cancel every pending order, close every position, pass a broker-verified empty-account reconciliation, and receive explicit administrator release before it may be reused; this deliberately favors complete account safety and a single recovery model over preserving unrelated activity on a shared worker.
