from __future__ import annotations

import argparse
import secrets
import string
from pathlib import Path
from typing import Sequence

from .ledger import ControlLedger, LedgerError


_PASSWORD_ALPHABET = string.ascii_letters + string.digits + "!@#$"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="abt-console")
    parser.add_argument("--ledger", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("create-admin")
    reset = commands.add_parser("reset-password")
    reset.add_argument("username", type=_username)
    unlock = commands.add_parser("unlock-ip")
    unlock.add_argument("source_ip")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    ledger = ControlLedger(args.ledger)
    try:
        if args.command == "create-admin":
            username = _generate_username()
            password = _generate_password()
            ledger.create_admin(username, password)
            print(f"username={username}")
            print(f"password={password}")
        elif args.command == "reset-password":
            password = _generate_password()
            ledger.reset_admin_password(args.username, password)
            print(f"username={args.username}")
            print(f"password={password}")
        elif args.command == "unlock-ip":
            ledger.unlock_ip(args.source_ip)
        return 0
    except LedgerError as error:
        parser.error(str(error))
    finally:
        ledger.close()
    return 2


def _generate_username() -> str:
    return "".join(secrets.choice(string.ascii_uppercase) for _ in range(6))


def _generate_password() -> str:
    characters = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
        secrets.choice("!@#$"),
    ]
    characters.extend(secrets.choice(_PASSWORD_ALPHABET) for _ in range(16))
    secrets.SystemRandom().shuffle(characters)
    return "".join(characters)


def _username(value: str) -> str:
    if len(value) != 6 or any(character not in string.ascii_uppercase for character in value):
        raise argparse.ArgumentTypeError("username must be six uppercase letters")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
