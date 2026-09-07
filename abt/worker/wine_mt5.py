from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from .wine_mt5_client import (
    DEFAULT_WINDOWS_PYTHON,
    DEFAULT_WINE_PREFIX,
    BridgeClient,
    BridgeClientError,
    OneShotMutationClient,
)


class MutationClient(Protocol):
    def order_send(
        self,
        request: dict[str, object],
        *,
        expected_login: int,
        expected_server: str,
    ) -> object: ...


class WineMetaTrader5Adapter:
    """Expose the Worker MT5 seam through a bounded inherited-pipe Wine child."""

    def __init__(
        self,
        *,
        client: BridgeClient | None = None,
        mutation_client_factory: Callable[[], MutationClient] | None = None,
        wine_prefix: Path = DEFAULT_WINE_PREFIX,
        windows_python: str = DEFAULT_WINDOWS_PYTHON,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._client = client or BridgeClient(
            wine_prefix=wine_prefix,
            windows_python=windows_python,
            timeout_seconds=timeout_seconds,
        )
        self._constants: dict[str, int] | None = None
        self._closed = False
        self._login: int | None = None
        self._server: str | None = None
        self._mutation_client_factory = mutation_client_factory or (
            lambda: OneShotMutationClient(
                wine_prefix=wine_prefix,
                windows_python=windows_python,
                timeout_seconds=timeout_seconds,
            )
        )

    def _request(self, operation: str, params: dict[str, object] | None = None) -> Any:
        if self._closed:
            raise BridgeClientError("Wine MT5 adapter is closed")
        return self._client.request(operation, params)

    def initialize(self) -> bool:
        result = self._request("initialize")
        return isinstance(result, dict) and result.get("initialized") is True

    def login(self, login: int, *, password: str, server: str) -> bool:
        result = self._request(
            "login",
            {"login": login, "password": password, "server": server},
        )
        logged_in = isinstance(result, dict) and result.get("logged_in") is True
        if logged_in:
            self._login = login
            self._server = server
        return logged_in

    def account_info(self) -> object:
        return self._request("account_info")

    def terminal_info(self) -> object:
        return self._request("terminal_info")

    def orders_get(self) -> object:
        return self._request("orders_get")

    def positions_get(self) -> object:
        return self._request("positions_get")

    def symbols_get(self) -> object:
        return self._request("symbols_get")

    def symbol_info(self, symbol: str) -> object:
        return self._request("symbol_info", {"symbol": symbol})

    def symbol_info_tick(self, symbol: str) -> object:
        return self._request("symbol_info_tick", {"symbol": symbol})

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        result = self._request("symbol_select", {"symbol": symbol, "enable": enable})
        return isinstance(result, dict) and result.get("selected") is True

    def copy_rates_range(self, symbol: str, timeframe: object, from_time: datetime, to_time: datetime) -> object:
        return self._request(
            "copy_rates_range",
            {"symbol": symbol, "timeframe": timeframe, "from": from_time.isoformat(), "to": to_time.isoformat()},
        )

    def copy_rates_from_pos(self, symbol: str, timeframe: object, start_pos: int, count: int) -> object:
        return self._request(
            "copy_rates_from_pos",
            {"symbol": symbol, "timeframe": timeframe, "start_pos": start_pos, "count": count},
        )

    def copy_ticks_range(self, symbol: str, from_time: datetime, to_time: datetime, flags: int) -> object:
        return self._request(
            "copy_ticks_range",
            {"symbol": symbol, "from": from_time.isoformat(), "to": to_time.isoformat(), "flags": flags},
        )

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> object:
        return self._request(
            "order_calc_margin",
            {"action": action, "symbol": symbol, "volume": volume, "price": price},
        )

    def order_calc_profit(
        self,
        action: int,
        symbol: str,
        volume: float,
        open_price: float,
        close_price: float,
    ) -> object:
        return self._request(
            "order_calc_profit",
            {
                "action": action,
                "symbol": symbol,
                "volume": volume,
                "open_price": open_price,
                "close_price": close_price,
            },
        )

    def order_check(self, request: dict[str, object]) -> object:
        return self._request("order_check", {"request": request})

    def order_send(self, request: dict[str, object]) -> object:
        if self._login is None or self._server is None:
            raise BridgeClientError("Wine MT5 mutation requires a verified account login")
        return self._mutation_client_factory().order_send(
            request,
            expected_login=self._login,
            expected_server=self._server,
        )

    def history_deals_get(self, *args: datetime, **kwargs: int) -> object:
        if kwargs:
            if set(kwargs) != {"position"}:
                raise TypeError("history_deals_get supports only the position keyword")
            return self._request("history_deals_get", {"position": kwargs["position"]})
        if len(args) != 2:
            raise TypeError("history_deals_get requires a date range or position keyword")
        return self._request(
            "history_deals_get",
            {"from": args[0].isoformat(), "to": args[1].isoformat()},
        )

    def last_error(self) -> object:
        return self._request("last_error")

    def __getattr__(self, name: str) -> object:
        if not name.startswith(("COPY_TICKS_", "TIMEFRAME_", "TRADE_", "ORDER_", "POSITION_", "DEAL_")):
            raise AttributeError(name)
        if self._constants is None:
            result = self._request("constants")
            if not isinstance(result, dict) or any(
                not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int)
                for key, value in result.items()
            ):
                raise BridgeClientError("Wine MT5 bridge returned invalid constants")
            self._constants = result
        try:
            return self._constants[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._client.close()

    def close(self) -> None:
        self.shutdown()
