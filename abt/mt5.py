from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import keyring
import MetaTrader5 as mt5

from .config import Context


CREDENTIAL_SERVICE = "abt.mt5"


class SessionError(RuntimeError):
    """Raised when the configured MT5 account cannot be used."""


def credential_name(context: Context) -> str:
    return f"{context.name}:{context.login}:{context.server}"


def save_password(context: Context, password: str) -> None:
    keyring.set_password(CREDENTIAL_SERVICE, credential_name(context), password)


def delete_password(context: Context) -> None:
    try:
        keyring.delete_password(CREDENTIAL_SERVICE, credential_name(context))
    except keyring.errors.PasswordDeleteError:
        pass


@contextmanager
def connected(context: Context) -> Iterator[object]:
    password = keyring.get_password(CREDENTIAL_SERVICE, credential_name(context))
    if password is None:
        raise SessionError(f"No saved password for context {context.name!r}; run `abt context login {context.name}`.")
    if not context.terminal_path.is_file():
        raise SessionError(f"Terminal executable does not exist: {context.terminal_path}")
    if not mt5.initialize(
        path=str(context.terminal_path),
        login=context.login,
        password=password,
        server=context.server,
        timeout=10_000,
    ):
        raise SessionError(f"MT5 initialization failed: {mt5.last_error()}")
    try:
        account = mt5.account_info()
        if account is None:
            raise SessionError(f"MT5 returned no account information: {mt5.last_error()}")
        if account.login != context.login or account.server != context.server:
            raise SessionError(
                f"Context {context.name!r} expects {context.login}@{context.server}, "
                f"but terminal is {account.login}@{account.server}."
            )
        yield mt5
    finally:
        mt5.shutdown()
