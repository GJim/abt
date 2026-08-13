# MT5 read-only smoke prototype

This is throwaway code for verifying that Python can communicate with a running MT5 demo terminal. It does not call order, position, or trade-modification APIs.

Run it from the repository root:

```powershell
uv run python prototypes\mt5_readonly_smoke.py
```

Set `MT5_TERMINAL_PATH` only when the terminal is not installed at the standard path:

```powershell
$env:MT5_TERMINAL_PATH = "C:\path\to\terminal64.exe"
uv run python prototypes\mt5_readonly_smoke.py EURUSD GBPUSD
```
