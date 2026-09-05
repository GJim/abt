# Linux → Wine → MT5 feasibility spike

## Verdict: GO for Wine/MT5 runtime feasibility

The native-Linux parent successfully owns a Windows Python child under Wine and
exchanges bounded, versioned, length-prefixed request/response frames over
inherited stdin/stdout pipes. The child attaches to the running MetaTrader 5
terminal and returns real broker data. The read-only path, the complete Python
MT5 API set currently called by Worker, and an explicitly authorized
minimum-volume FTMO-Demo open/close lifecycle have all been exercised.

This is a go decision for the Wine/MT5 compatibility boundary, not approval to
reuse the prototype as production Worker code. Production identity still
requires the specification's TPM gate. The daily read-only bridge remains
incapable of `order_send`; mutation exists only in the isolated one-shot
harness.

## Proven matrix

| Component | Proven value |
| --- | --- |
| Host | Ubuntu 24.04.4 LTS x86_64 |
| Kernel | 7.0.0-30-generic |
| Wine | wine-11.16 Staging, 64-bit prefix at `~/.mt5` |
| Windows API surface | Windows 10.0.22000 reported by Wine |
| Windows Python | CPython 3.13.13 at `C:\abt-python313` |
| Python installer SHA-256 | `3c9c81d80f91c002ced86d645422d81432c68c7d9b6b0e974768ca2e449a4d00` |
| MetaTrader5 wheel | 5.0.6090, `cp313-win_amd64` |
| NumPy wheel | 2.5.2, `cp313-win_amd64` |
| MT5 terminal | build 6157 |
| terminal64.exe SHA-256 | `93023bc503d0beef754991766d39b5e6c72aeed01509c24f498f15c484cc359f` |
| Broker used for read-only proof | FTMO-Demo |
| Symbol used | EURUSD |

## What worked

A real run through `client.py` and the Wine bridge proved:

- native Linux launches the child directly with `shell=False`;
- only inherited stdin/stdout carry protocol data; stderr is diagnostics only;
- bridge health and MT5 initialization;
- connected terminal and account evidence;
- 166-symbol catalog;
- live EURUSD symbol and tick evidence;
- ten M1 rates;
- zero starting orders and zero starting positions;
- minimum volume `0.01` for the tested symbol;
- margin calculation (`11.6` USD in the observed run);
- ten-point profit calculation (`0.1` USD in the observed run);
- broker `order_check` returned retcode `0` / `Done` for the minimum-volume buy;
- clean bridge shutdown.

The result intentionally omits the account login, account holder/name, and
balance from retained documentation.

## Worker Python MT5 API matrix

The APIs statically found under `abt/worker/` were exercised against the same
FTMO-Demo terminal:

- `initialize`, `shutdown`, `account_info`, `terminal_info`;
- `symbols_get`, `symbol_info`, `symbol_info_tick`;
- `copy_rates_range`, `copy_ticks_range`;
- `orders_get`, `positions_get`;
- `order_calc_margin`, `order_calc_profit`, `order_check`;
- idempotent `symbol_select(EURUSD, True)`, with the full Market Watch catalog
  proven unchanged;
- same-account `login`, with account fingerprint unchanged. This proved the
  stored-credential login path; the password argument was intentionally not
  exposed to the probe;
- `order_send`, proven by the separately authorized lifecycle below.

## Authorized demo lifecycle

On FTMO-Demo, the isolated harness opened one EURUSD BUY at broker minimum
volume `0.01`, observed the exact resulting position ticket, and sent one SELL
to close that ticket. Broker history contains exactly two corresponding deals:
open at `1.16196`, close at `1.16194`, realized demo P/L `-0.02` USD. Independent
post-trade reads proved zero orders and zero positions.

The first immediate post-close list read briefly observed stale state even
though the close receipt was accepted. The harness therefore initially emitted
a conservative failure. Broker history and independent reads resolved the
outcome as closed; the harness was then corrected to use a bounded read-only
postcondition poll without ever retrying `order_send`.

## Failure and safety evidence

- Split frame reads are reconstructed.
- Frames larger than 4 MiB are rejected before the body is read.
- Truncated frames and non-object JSON are rejected.
- Unknown protocol versions, operations, request fields, and operation
  parameters are rejected.
- Response IDs must match the request ID.
- EOF and timeout are explicit client failures.
- A live Wine bridge rejected `order_send` and remained healthy afterward.
- A live Wine bridge given a 4 MiB + 1 frame exited with code 3, emitted no
  stdout protocol data, and wrote a bounded diagnostic to stderr.
- The child environment is an allowlist and contains no environment variable
  names containing password, secret, token, or key.
- The read-only bridge accepts no MT5 password field and persists no broker
  credential.

## Run the read-only proof

Prerequisites are the already installed Wine prefix, running MT5 terminal, and
Windows Python dependencies described in the matrix.

```bash
cd /home/gjim/abt
source .venv/bin/activate
export DISPLAY=:0
export XAUTHORITY=/run/user/1000/.mutter-Xwaylandauth.4TFUU3
python prototypes/linux-wine-mt5-feasibility/client.py --symbol EURUSD
```

Run the protocol and failure tests:

```bash
cd /home/gjim/abt/prototypes/linux-wine-mt5-feasibility
/home/gjim/abt/.venv/bin/python -m unittest \
  test_protocol.py test_bridge.py test_client.py test_mutation_roundtrip.py -v
```

## What remains

1. Keep the read-only bridge permanently incapable of `order_send` and retain
   the one-shot mutation harness only as feasibility evidence.
2. Convert the proven protocol and lifecycle semantics into production Worker
   code rather than importing the prototype directly.
3. Add deterministic fake-broker fault injection for post-send disconnect and
   unknown-outcome handling; do not reproduce that fault against the real demo
   broker merely to obtain evidence.
4. Complete the production TPM identity gate on hardware exposing a real TPM
   2.0 device; this host currently has none.

Safe preflight (never calls `order_send`):

```bash
WINEPREFIX=/home/gjim/.mt5 WINEDEBUG=-all \
  wine 'C:\abt-python313\python.exe' \
  'Z:\home\gjim\abt\prototypes\linux-wine-mt5-feasibility\mutation_roundtrip.py' \
  --preflight --symbol EURUSD
```

The mutation mode additionally requires the exact `DEMO_OPEN_CLOSE` phrase and
an external operator-authorization reference. Do not invoke it from unattended
automation.

## Operational findings

- The Python installer must be run with the interactive desktop's `DISPLAY`
  and `XAUTHORITY`, even with `/quiet`; without them Wine reported that no GUI
  driver could be loaded and Python was not installed.
- Do not use `wineserver -w` while the MT5 terminal is intentionally running;
  it waits for every Wine process and therefore never completes during the
  experiment.
- The current host exposes neither `/dev/tpm0` nor `/dev/tpmrm0`. TPM work is
  outside this Wine/MT5 feasibility issue but remains mandatory for production
  Linux Worker identity acceptance.
