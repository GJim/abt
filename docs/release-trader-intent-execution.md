# Trader intent execution release gate

Run this gate in a controlled environment before enabling broker-writing
intents. Keep the command output and the immutable ledger records identified
below with the release evidence.

## Topology

- Deploy the controller and two approved account workers on three separate
  networks.
- Bind each worker to a different approved broker endpoint of one active
  product pair.
- Enroll and approve one Trader with a hardware/OS-backed P-256 key and a
  valid device certificate.

## Automated evidence

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest
npm --prefix web run build
npm --prefix web run test:browser
```

The Python suite verifies invite lifecycle and migration, Trader identity and
WSS replay, command idempotency, dual preflight outcomes, FOK/IOC execution,
cancellation races, flattening, and immutable execution records. The browser
suite verifies invite issuance/revocation, Trader management, intent previews,
timelines, CSRF headers, and server-error display.

## Controlled execution

1. Issue a Trader invite in the management site, record the one-time secret
   in the controlled operator channel, enroll the Trader, and approve it.
2. Connect the Trader WSS session, submit an FOK intent, and have both workers
   return the exact successful `order_check` evidence. Confirm acceptance,
   execution records, and reconnect replay after withholding the final ACK.
3. Repeat with IOC and record partial-fill netting or its no-fill result.
   Confirm that no IOC top-up order was issued.
4. Submit an intent whose two legs remain zero-fill, request cancellation, and
   retain the two broker cancellation acknowledgements plus immediate
   reconciliation evidence.
5. From the management workspace, preview and confirm emergency flatten for a
   filled intent. Retain the cancellation/close responses and reconciliation
   result. If any recovery operation fails, verify the pair is frozen for
   human recovery.
6. Submit one request with a failed or timed-out `order_check` and one whose
   checks succeed but broker execution fails. The former must have
   `rejected_preflight` with both endpoint outcomes and no intent; the latter
   must retain an accepted intent with dispatch/execution records. Export both
   immutable timelines with the release evidence.

Do not enable the slice if any command succeeds without the corresponding
immutable ledger event, or if a failed recovery leaves the pair unfrozen.
