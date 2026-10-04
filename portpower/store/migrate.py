"""Command line for operating the v1 -> v2 migration.

Converters are application specific; build them in a factory and pass the
registry in.  The default registry identity-converts every key, which is
enough to operate the framework and migrate stores whose documents did not
change shape but still need the envelope (version tag) for future rounds.

    python -m portpower.store.migrate --data-dir ./data status
    python -m portpower.store.migrate --data-dir ./data run
    python -m portpower.store.migrate --data-dir ./data verify
    python -m portpower.store.migrate --data-dir ./data metrics
"""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from portpower.store.migration import MigrationRunner
from portpower.store.versioned import ConverterRegistry, VersionedStore


def build_registry() -> ConverterRegistry:
    """Override this (or compose your own CLI) with real converters."""

    return ConverterRegistry()


def _print(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="portpower.store.migrate")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument(
        "--no-dual-write",
        action="store_true",
        help="open with the compatibility layer off (cutover builds only)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="resume/run the background migration")
    sub.add_parser("verify", help="run the final digest verification sweep")
    sub.add_parser("status", help="print checkpoint and progress")
    sub.add_parser("metrics", help="print counters, gauges and compat ratio")
    args = parser.parse_args(argv)

    store = VersionedStore(
        args.data_dir,
        build_registry(),
        dual_write=not args.no_dual_write,
    )
    store.load()
    runner = MigrationRunner(store)

    if args.command == "run":
        _print(runner.run())
    elif args.command == "verify":
        _print(runner.run_verification())
    elif args.command == "status":
        _print(runner.status())
    elif args.command == "metrics":
        _print(store.metrics.snapshot())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
