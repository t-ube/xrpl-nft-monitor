#!/usr/bin/env python3
"""
Payment v2 monitor for GitHub Actions / cron.

- XRPL validated ledgerを順番に走査
- PaymentだけをVPC/Backfillと同じ共有parser v4へ通す
- payment_watch_list RPCを毎run読み込み、extract.set_watched()へ設定
- public.nft_payment_v2へtx_hash UPSERT
- 書き込み完了後だけpublic.monitor_stateの
  last_payment_v2_ledger_indexを進める

重要:
- cursorは「最後に見つかったPaymentのledger」ではなく
  「処理完了した最後のledger」を記録する。
- stateが無い場合、nft_payment_v2のMAX(ledger_index)から推測しない。
  Paymentが存在しないledgerを飛ばす危険があるため、初期値を明示する。
- public RPCを先に使い、XRPL_RPCが設定されていれば最後のfallbackとして使う。
  QuickNode等のcredits消費を抑えるため。
"""

from __future__ import annotations

import importlib.util
import os
import sys
import time
import types
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Environment / config
# =============================================================================

SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or ""

PAYMENT_TABLE = "nft_payment_v2"
STATE_KEY = os.environ.get(
    "PAYMENT_V2_STATE_KEY",
    "last_payment_v2_ledger_index",
)

EXPECTED_PARSER_VERSION = int(
    os.environ.get("PAYMENT_PARSER_VERSION", "4")
)
MAX_LEDGERS_PER_RUN = int(
    os.environ.get("MAX_LEDGERS_PER_RUN", "500")
)
PARALLEL_WORKERS = int(
    os.environ.get("PARALLEL_WORKERS", "5")
)
CHECKPOINT_INTERVAL = int(
    os.environ.get("CHECKPOINT_INTERVAL", "30")
)

HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "30"))
XRPL_TIMEOUT = float(os.environ.get("XRPL_TIMEOUT", "60"))

RIPPLE_EPOCH_OFFSET = 946684800


def _unique_rpc_endpoints() -> list[str]:
    """
    Credits節約のためpublic RPCを先に使う。
    XRPL_RPC (QuickNode等) は最後のfallback。
    """
    candidates = [
        "https://xrplcluster.com/",
        "https://s2.ripple.com:51234/",
        os.environ.get("XRPL_RPC"),
    ]

    out: list[str] = []
    seen: set[str] = set()

    for value in candidates:
        if not value:
            continue
        url = value.strip()
        key = url.rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        out.append(url)

    return out


XRPL_RPC_ENDPOINTS = _unique_rpc_endpoints()


def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}"
        )


# =============================================================================
# Load exact shared parser
# =============================================================================

def resolve_parser_dir() -> Path:
    script_dir = Path(__file__).resolve().parent

    candidates: list[Path] = []
    if os.environ.get("XRPL_PARSER_DIR"):
        candidates.append(Path(os.environ["XRPL_PARSER_DIR"]))

    # ユーザーのrepo構成:
    # repo/src/monitor_payment_rpc_v2.py
    # repo/src/xrpl_ws/{extract.py,codec.py}
    candidates.extend([
        script_dir / "xrpl_ws",
        script_dir / "vpc" / "xrpl_ws",
        script_dir.parent / "vpc" / "xrpl_ws",
        Path.cwd() / "src" / "xrpl_ws",
        Path.cwd() / "vpc" / "xrpl_ws",
    ])

    for candidate in candidates:
        candidate = candidate.resolve()
        if (
            (candidate / "extract.py").is_file()
            and (candidate / "codec.py").is_file()
        ):
            return candidate

    searched = "\n  ".join(
        str(candidate.resolve()) for candidate in candidates
    )
    raise FileNotFoundError(
        "Could not find shared parser (extract.py + codec.py).\n"
        "Set XRPL_PARSER_DIR or place it under src/xrpl_ws.\n"
        f"Searched:\n  {searched}"
    )


def load_shared_parser(parser_dir: Path):
    """
    extract.pyがrelative importでcodec.pyを読むので、
    parser_dirを一時packageとしてロードする。
    """
    package_name = "_xrpl_payment_monitor_parser"

    for key in list(sys.modules):
        if key == package_name or key.startswith(package_name + "."):
            del sys.modules[key]

    package = types.ModuleType(package_name)
    package.__path__ = [str(parser_dir)]
    package.__package__ = package_name
    sys.modules[package_name] = package

    def load(name: str, path: Path):
        spec = importlib.util.spec_from_file_location(
            f"{package_name}.{name}",
            path,
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    load("codec", parser_dir / "codec.py")
    extract = load("extract", parser_dir / "extract.py")

    required = (
        "extract_payment",
        "set_watched",
        "SKIP",
    )
    missing = [
        name for name in required
        if not hasattr(extract, name)
    ]
    if missing:
        raise RuntimeError(
            f"shared parser missing: {', '.join(missing)}"
        )

    return extract


# =============================================================================
# Helpers
# =============================================================================

def ripple_time_to_iso(ripple_time: int) -> str:
    unix_time = ripple_time + RIPPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(
        unix_time,
        tz=timezone.utc,
    ).isoformat()


def supabase_headers(
    *,
    prefer: str | None = None,
) -> dict[str, str]:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


# =============================================================================
# payment_watch_list
# =============================================================================

def fetch_payment_watch_list() -> list[str]:
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/payment_watch_list",
        headers=supabase_headers(),
        json={},
        timeout=HTTP_TIMEOUT,
    )

    if not response.ok:
        raise RuntimeError(
            "payment_watch_list RPC failed: "
            f"HTTP {response.status_code} "
            f"{response.text[:1000]}"
        )

    body = response.json() or []
    addresses: list[str] = []

    for row in body:
        if isinstance(row, dict) and row.get("address"):
            addresses.append(str(row["address"]))

    result = sorted(set(addresses))

    # 空watch listで走ると全PaymentがSKIPになり、
    # そのままcursorだけ進むので絶対に許可しない。
    if not result:
        raise RuntimeError(
            "payment_watch_list returned 0 addresses; "
            "refusing to advance payment cursor"
        )

    return result


# =============================================================================
# monitor_state
# =============================================================================

def get_last_ledger_index() -> int:
    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={
            "key": f"eq.{STATE_KEY}",
            "select": "value",
            "limit": "1",
        },
        headers=supabase_headers(),
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()

    rows = response.json() or []

    if not rows:
        explicit = os.environ.get("PAYMENT_V2_START_LEDGER")
        if explicit:
            value = int(explicit)
            upsert_last_ledger_index(value)
            print(
                f"Bootstrap: created {STATE_KEY}={value} "
                "from PAYMENT_V2_START_LEDGER"
            )
            return value

        raise RuntimeError(
            f"monitor_state key {STATE_KEY!r} does not exist. "
            "Create it explicitly before first run. "
            "Do NOT infer it from MAX(nft_payment_v2.ledger_index)."
        )

    return int(rows[0]["value"])


def upsert_last_ledger_index(
    ledger_index: int,
) -> None:
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"on_conflict": "key"},
        headers=supabase_headers(
            prefer="return=minimal,resolution=merge-duplicates"
        ),
        json={
            "key": STATE_KEY,
            "value": ledger_index,
            "updated_at": datetime.now(
                timezone.utc
            ).isoformat(),
        },
        timeout=HTTP_TIMEOUT,
    )

    if not response.ok:
        raise RuntimeError(
            "monitor_state upsert failed: "
            f"HTTP {response.status_code} "
            f"{response.text[:1000]}"
        )


# =============================================================================
# Supabase payment writer
# =============================================================================

def save_payment_history_batch(
    records: list[dict],
) -> None:
    if not records:
        return

    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/{PAYMENT_TABLE}",
        params={"on_conflict": "tx_hash"},
        headers=supabase_headers(
            prefer="return=minimal,resolution=merge-duplicates"
        ),
        json=records,
        timeout=HTTP_TIMEOUT,
    )

    if not response.ok:
        raise RuntimeError(
            f"{PAYMENT_TABLE} upsert failed: "
            f"HTTP {response.status_code} "
            f"{response.text[:2000]}"
        )


# =============================================================================
# XRPL RPC
# =============================================================================

def get_validated_ledger_index() -> int:
    last_error: Exception | None = None

    for rpc_url in XRPL_RPC_ENDPOINTS:
        try:
            response = requests.post(
                rpc_url,
                json={
                    "method": "ledger",
                    "params": [{
                        "ledger_index": "validated",
                    }],
                },
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            response.raise_for_status()

            result = response.json().get("result", {}) or {}

            if result.get("error"):
                raise RuntimeError(
                    f"XRPL {result.get('error')}: "
                    f"{result.get('error_message') or result}"
                )

            ledger_index = result.get("ledger_index")

            if ledger_index is None:
                ledger = result.get("ledger") or {}
                ledger_index = ledger.get("ledger_index")

            if ledger_index is not None:
                return int(ledger_index)

        except Exception as exc:
            last_error = exc
            print(
                f"  Validated-ledger RPC failed: "
                f"{type(exc).__name__}: {exc} ({rpc_url})"
            )

    raise last_error or RuntimeError(
        "All RPC endpoints failed for "
        "get_validated_ledger_index"
    )


def get_ledger_transactions(
    ledger_index: int,
) -> tuple[list[dict], int]:
    """
    transport error / HTTP一時障害 / XRPL error /
    empty payloadをretryし、その後次endpointへfallbackする。
    """
    last_error: Exception | None = None

    for rpc_url in XRPL_RPC_ENDPOINTS:
        for attempt in range(3):
            try:
                response = requests.post(
                    rpc_url,
                    json={
                        "method": "ledger",
                        "params": [{
                            "ledger_index": ledger_index,
                            "transactions": True,
                            "expand": True,
                        }],
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=XRPL_TIMEOUT,
                )

                if response.status_code in (
                    429, 500, 502, 503, 504
                ):
                    last_error = RuntimeError(
                        f"HTTP {response.status_code}"
                    )
                    if attempt < 2:
                        wait = 3 * (attempt + 1)
                        print(
                            f"    Ledger {ledger_index} "
                            f"retry {attempt + 1}: "
                            f"HTTP {response.status_code} "
                            f"({rpc_url})"
                        )
                        time.sleep(wait)
                        continue
                    break

                response.raise_for_status()

                body = response.json()
                result = body.get("result", {}) or {}

                rpc_error = result.get("error")
                if rpc_error:
                    detail = (
                        result.get("error_message")
                        or result.get("error_exception")
                        or result.get("error_code")
                        or rpc_error
                    )
                    last_error = RuntimeError(
                        f"XRPL {rpc_error}: {detail}"
                    )

                    if attempt < 2:
                        wait = 2 * (attempt + 1)
                        print(
                            f"    Ledger {ledger_index} "
                            f"retry {attempt + 1}: "
                            f"XRPL {rpc_error} "
                            f"({rpc_url})"
                        )
                        time.sleep(wait)
                        continue
                    break

                ledger = (
                    result.get("ledger")
                    or result.get("ledger_data")
                )

                # provider差異でresult自体がledger shapeの場合にも対応。
                if (
                    not ledger
                    and "transactions" in result
                    and (
                        "ledger_index" in result
                        or "ledger_hash" in result
                    )
                ):
                    ledger = result

                if not ledger:
                    last_error = RuntimeError(
                        f"ledger payload missing for "
                        f"{ledger_index}; "
                        f"result={str(result)[:300]}"
                    )

                    if attempt < 2:
                        wait = 2 * (attempt + 1)
                        print(
                            f"    Ledger {ledger_index} "
                            f"retry {attempt + 1}: "
                            f"empty ledger payload "
                            f"({rpc_url})"
                        )
                        time.sleep(wait)
                        continue
                    break

                transactions = ledger.get(
                    "transactions",
                    [],
                )
                close_time = int(
                    ledger.get("close_time") or 0
                )

                return transactions, close_time

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                last_error = exc

                if attempt < 2:
                    wait = 3 * (attempt + 1)
                    print(
                        f"    Ledger {ledger_index} "
                        f"retry {attempt + 1}: "
                        f"{type(exc).__name__} "
                        f"({rpc_url})"
                    )
                    time.sleep(wait)
                    continue

                break

            except Exception as exc:
                last_error = exc

                if attempt < 2:
                    wait = 2 * (attempt + 1)
                    print(
                        f"    Ledger {ledger_index} "
                        f"retry {attempt + 1}: "
                        f"{type(exc).__name__}: {exc} "
                        f"({rpc_url})"
                    )
                    time.sleep(wait)
                    continue

                break

        print(
            f"    Ledger {ledger_index}: "
            f"{rpc_url} failed, trying next endpoint"
        )

    raise last_error or RuntimeError(
        f"All RPC endpoints failed for "
        f"ledger {ledger_index}"
    )


def fetch_ledger(
    ledger_index: int,
) -> tuple[int, list[dict], int]:
    transactions, close_time = (
        get_ledger_transactions(ledger_index)
    )
    return ledger_index, transactions, close_time


# =============================================================================
# Parser bridge
# =============================================================================

def build_parser_message(
    ledger_index: int,
    tx: dict,
    close_time: int,
) -> dict:
    meta = tx.get(
        "meta",
        tx.get("metaData", {}),
    ) or {}

    tx_json = dict(tx)
    tx_json.pop("meta", None)
    tx_json.pop("metaData", None)

    return {
        "tx_json": tx_json,
        "meta": meta,
        "ledger_index": ledger_index,
        "close_time_iso": (
            ripple_time_to_iso(close_time)
            if close_time
            else None
        ),
        "hash": (
            tx_json.get("hash")
            or tx.get("hash")
            or ""
        ),
        "validated": True,
    }


def process_ledger(
    extract: Any,
    ledger_index: int,
    transactions: list[dict],
    close_time: int,
) -> tuple[list[dict], Counter]:
    """
    ledger全体をatomicにparseする。

    WATCHED外PaymentのSKIPは正常。
    parser exception / None / wrong table /
    version mismatchはledgerを失敗扱いにしてcursorを進めない。
    """
    records: list[dict] = []
    stats = Counter()

    for tx in transactions:
        if tx.get("TransactionType") != "Payment":
            continue

        meta = tx.get(
            "meta",
            tx.get("metaData", {}),
        ) or {}

        if meta.get("TransactionResult") != "tesSUCCESS":
            continue

        tx_hash = tx.get("hash", "")
        if not tx_hash:
            raise RuntimeError(
                f"ledger {ledger_index}: "
                "successful Payment has no hash"
            )

        stats["payment_candidates"] += 1

        try:
            result = extract.extract_payment(
                build_parser_message(
                    ledger_index,
                    tx,
                    close_time,
                )
            )
        except Exception as exc:
            raise RuntimeError(
                f"ledger {ledger_index} "
                f"tx {tx_hash}: "
                f"parser exception: {exc}"
            ) from exc

        if result is extract.SKIP:
            stats["expected_skip"] += 1
            continue

        if result is None:
            raise RuntimeError(
                f"ledger {ledger_index} "
                f"tx {tx_hash}: "
                "extract_payment returned None"
            )

        table, record, _uri = result

        if table != PAYMENT_TABLE:
            raise RuntimeError(
                f"ledger {ledger_index} "
                f"tx {tx_hash}: "
                f"parser returned {table}, "
                f"expected {PAYMENT_TABLE}"
            )

        if (
            record.get("parser_version")
            != EXPECTED_PARSER_VERSION
        ):
            raise RuntimeError(
                f"ledger {ledger_index} "
                f"tx {tx_hash}: "
                f"parser_version="
                f"{record.get('parser_version')} "
                f"expected="
                f"{EXPECTED_PARSER_VERSION}"
            )

        if record.get("tx_result") != "tesSUCCESS":
            raise RuntimeError(
                f"ledger {ledger_index} "
                f"tx {tx_hash}: "
                "non-success record"
            )

        status = record.get("settlement_status")
        if status not in {
            "exact",
            "partial",
            "unresolved",
        }:
            raise RuntimeError(
                f"ledger {ledger_index} "
                f"tx {tx_hash}: "
                f"invalid settlement_status="
                f"{status!r}"
            )

        records.append(record)

        stats["payments"] += 1
        stats[f"settlement_{status}"] += 1

    return records, stats


# =============================================================================
# Durable flush + checkpoint
# =============================================================================

def flush_and_checkpoint(
    *,
    pending_records: list[dict],
    high_water_ledger: int,
) -> None:
    """
    1. nft_payment_v2 UPSERT
    2. monitor_state更新

    DB書き込みに失敗したらstateは進まない。
    次回同じledgerを再処理してもtx_hash UPSERTなので安全。
    """
    if pending_records:
        save_payment_history_batch(
            pending_records
        )
        print(
            f"  V2 upsert: "
            f"{len(pending_records)} payments"
        )

    upsert_last_ledger_index(
        high_water_ledger
    )
    print(
        f"  Checkpoint: "
        f"{STATE_KEY}={high_water_ledger}"
    )


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    print("=== NFT PAYMENT v2 Monitor Start ===")
    started = time.time()

    require_env()

    parser_dir = resolve_parser_dir()
    extract = load_shared_parser(
        parser_dir
    )

    watch_list = fetch_payment_watch_list()
    extract.set_watched(watch_list)

    print(f"Parser: {parser_dir}")
    print(f"Target table: {PAYMENT_TABLE}")
    print(f"State key: {STATE_KEY}")
    print(
        f"Expected parser version: "
        f"{EXPECTED_PARSER_VERSION}"
    )
    print(
        f"Watched addresses: "
        f"{len(watch_list)}"
    )
    print(
        f"RPC endpoints: "
        f"{XRPL_RPC_ENDPOINTS}"
    )

    last_ledger_index = (
        get_last_ledger_index()
    )
    print(
        f"Last durable ledger: "
        f"{last_ledger_index}"
    )

    validated_index = (
        get_validated_ledger_index()
    )
    print(
        f"Current validated ledger: "
        f"{validated_index}"
    )

    start_index = last_ledger_index + 1
    end_index = min(
        validated_index,
        last_ledger_index
        + MAX_LEDGERS_PER_RUN,
    )

    if start_index > validated_index:
        print("No new ledgers to process")
        return 0

    print(
        f"Processing ledgers: "
        f"{start_index} -> {end_index} "
        f"({end_index - start_index + 1} ledgers)"
    )
    print(
        f"Parallel workers: "
        f"{PARALLEL_WORKERS}"
    )

    if end_index < validated_index:
        print(
            f"  {validated_index - end_index} "
            "ledgers remain for later runs"
        )

    totals = Counter()

    current_index = last_ledger_index
    last_saved_index = last_ledger_index
    ledgers_since_checkpoint = 0

    pending_records: list[dict] = []

    stop_error: Exception | None = None

    all_indices = list(
        range(
            start_index,
            end_index + 1,
        )
    )

    for batch_start in range(
        0,
        len(all_indices),
        PARALLEL_WORKERS,
    ):
        batch = all_indices[
            batch_start:
            batch_start + PARALLEL_WORKERS
        ]

        fetched: dict[
            int,
            tuple[list[dict] | None, int | Exception],
        ] = {}

        with ThreadPoolExecutor(
            max_workers=PARALLEL_WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    fetch_ledger,
                    idx,
                ): idx
                for idx in batch
            }

            for future in as_completed(futures):
                idx = futures[future]

                try:
                    (
                        ledger_index,
                        transactions,
                        close_time,
                    ) = future.result()

                    fetched[ledger_index] = (
                        transactions,
                        close_time,
                    )

                except Exception as exc:
                    print(
                        f"  Ledger {idx} "
                        f"fetch error: {exc}"
                    )
                    fetched[idx] = (
                        None,
                        exc,
                    )

        # 必ずledger順に処理する。
        for idx in batch:
            item = fetched.get(idx)

            if (
                item is None
                or item[0] is None
            ):
                if (
                    item is not None
                    and isinstance(
                        item[1],
                        Exception,
                    )
                ):
                    stop_error = RuntimeError(
                        f"ledger {idx} "
                        f"fetch failed: "
                        f"{item[1]}"
                    )
                else:
                    stop_error = RuntimeError(
                        f"ledger {idx} "
                        "fetch result missing"
                    )
                break

            transactions = item[0]
            close_time = int(item[1])

            try:
                (
                    ledger_records,
                    ledger_stats,
                ) = process_ledger(
                    extract,
                    idx,
                    transactions,
                    close_time,
                )

            except Exception as exc:
                stop_error = exc
                break

            # ledger全体parse成功後だけpendingへ追加。
            pending_records.extend(
                ledger_records
            )
            totals.update(ledger_stats)

            current_index = idx
            ledgers_since_checkpoint += 1

            if (
                ledgers_since_checkpoint
                >= CHECKPOINT_INTERVAL
            ):
                try:
                    flush_and_checkpoint(
                        pending_records=
                            pending_records,
                        high_water_ledger=
                            current_index,
                    )
                except Exception as exc:
                    stop_error = RuntimeError(
                        "flush/checkpoint failed "
                        f"at ledger "
                        f"{current_index}: {exc}"
                    )
                    break

                pending_records.clear()
                last_saved_index = current_index
                ledgers_since_checkpoint = 0

        if stop_error is not None:
            break

    # errorより前に成功済みだが、
    # checkpoint未保存のledgerがあれば保存する。
    if current_index > last_saved_index:
        try:
            flush_and_checkpoint(
                pending_records=
                    pending_records,
                high_water_ledger=
                    current_index,
            )
            pending_records.clear()
            last_saved_index = current_index

        except Exception as exc:
            if stop_error is None:
                stop_error = RuntimeError(
                    "final flush/checkpoint "
                    f"failed at "
                    f"{current_index}: {exc}"
                )
            else:
                print(
                    "WARNING: could not "
                    "checkpoint already successful "
                    f"ledgers up to "
                    f"{current_index}: {exc}"
                )

    elapsed = time.time() - started

    print()
    print(
        "=== NFT PAYMENT v2 "
        "Monitor Result ==="
    )
    print(
        f"Last durable ledger : "
        f"{last_saved_index}"
    )
    print(
        f"Payment candidates  : "
        f"{totals['payment_candidates']}"
    )
    print(
        f"Expected SKIP       : "
        f"{totals['expected_skip']}"
    )
    print(
        f"Payments written    : "
        f"{totals['payments']}"
    )
    print(
        f"  exact             : "
        f"{totals['settlement_exact']}"
    )
    print(
        f"  partial           : "
        f"{totals['settlement_partial']}"
    )
    print(
        f"  unresolved        : "
        f"{totals['settlement_unresolved']}"
    )
    print(
        f"Elapsed             : "
        f"{elapsed:.1f}s"
    )

    if stop_error is not None:
        print(
            f"ERROR: {stop_error}",
            file=sys.stderr,
        )
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
