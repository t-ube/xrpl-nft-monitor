#!/usr/bin/env python3
"""
NFT Sale v2 monitor for GitHub Actions / cron.

- XRPL validated ledgersを順番に走査
- tesSUCCESS NFTokenAcceptOfferだけを、VPCと同じ parser v4 に通す
- public.nft_sale_history_v2 へ tx_hash UPSERT
- URIをCACHE_APIへ送る
- public.monitor_state の last_sale_v2_ledger_index を
  「DB + cache への書き込みが完了した最後のledger」として保存

重要:
- checkpointはledger単位。ledger内で1件でもparse失敗したら、そのledgerは進めない。
- state更新はDB/cache flushの後だけ。
- 新しいSTATE_KEYが存在しない初回は nft_sale_history_v2 の max(ledger_index)
  から自動bootstrapする。今回のbackfill後の切替用。
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
CACHE_API_URL = (os.environ.get("CACHE_API_URL") or "").rstrip("/")

def _unique_rpc_endpoints() -> list[str]:
    # XRPL_RPC が s2 を指していても fallback が同じURLにならないようにする。
    candidates = [
        os.environ.get("XRPL_RPC"),
        "https://xrplcluster.com/",
        "https://s2.ripple.com:51234/",
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

SALE_TABLE = "nft_sale_history_v2"
STATE_KEY = os.environ.get("SALE_V2_STATE_KEY", "last_sale_v2_ledger_index")

EXPECTED_PARSER_VERSION = int(os.environ.get("SALE_PARSER_VERSION", "4"))
MAX_LEDGERS_PER_RUN = int(os.environ.get("MAX_LEDGERS_PER_RUN", "2000"))
PARALLEL_WORKERS = int(os.environ.get("PARALLEL_WORKERS", "5"))
CHECKPOINT_INTERVAL = int(os.environ.get("CHECKPOINT_INTERVAL", "30"))

HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "30"))
XRPL_TIMEOUT = float(os.environ.get("XRPL_TIMEOUT", "60"))
CACHE_TIMEOUT = float(os.environ.get("CACHE_TIMEOUT", "30"))

RIPPLE_EPOCH_OFFSET = 946684800


def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if not CACHE_API_URL:
        missing.append("CACHE_API_URL")
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")


# =============================================================================
# Load the exact same parser used by VPC
# =============================================================================

def resolve_parser_dir() -> Path:
    script_dir = Path(__file__).resolve().parent

    candidates: list[Path] = []
    if os.environ.get("XRPL_PARSER_DIR"):
        candidates.append(Path(os.environ["XRPL_PARSER_DIR"]))

    candidates.extend([
        script_dir / "vpc" / "xrpl_ws",
        script_dir.parent / "vpc" / "xrpl_ws",
        Path.cwd() / "vpc" / "xrpl_ws",
        script_dir / "xrpl_ws",
        Path.cwd() / "xrpl_ws",
    ])

    for candidate in candidates:
        candidate = candidate.resolve()
        if (candidate / "extract.py").is_file() and (candidate / "codec.py").is_file():
            return candidate

    searched = "\n  ".join(str(x.resolve()) for x in candidates)
    raise FileNotFoundError(
        "Could not find shared parser (extract.py + codec.py).\n"
        "Set XRPL_PARSER_DIR or place it under vpc/xrpl_ws.\n"
        f"Searched:\n  {searched}"
    )


def load_shared_parser(parser_dir: Path):
    """
    extract.py が `from .codec import ...` を使っているので、
    任意directoryを一時packageとしてロードする。
    """
    package_name = "_xrpl_sale_monitor_parser"

    for key in list(sys.modules):
        if key == package_name or key.startswith(package_name + "."):
            del sys.modules[key]

    package = types.ModuleType(package_name)
    package.__path__ = [str(parser_dir)]
    package.__package__ = package_name
    sys.modules[package_name] = package

    def load(name: str, path: Path):
        spec = importlib.util.spec_from_file_location(f"{package_name}.{name}", path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    load("codec", parser_dir / "codec.py")
    extract = load("extract", parser_dir / "extract.py")

    if not hasattr(extract, "extract_sale"):
        raise RuntimeError("shared extract.py has no extract_sale()")

    return extract


# =============================================================================
# Helpers
# =============================================================================

def ripple_time_to_iso(ripple_time: int) -> str:
    unix_time = ripple_time + RIPPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(unix_time, tz=timezone.utc).isoformat()


def supabase_headers(*, prefer: str | None = None) -> dict[str, str]:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


# =============================================================================
# monitor_state
# =============================================================================

def read_state() -> int | None:
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
        return None
    return int(rows[0]["value"])


def upsert_state(ledger_index: int) -> None:
    """
    PATCHではなくUPSERT。
    新しいlast_sale_v2_ledger_indexがまだ存在しなくても必ず作る。
    """
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"on_conflict": "key"},
        headers=supabase_headers(
            prefer="return=minimal,resolution=merge-duplicates"
        ),
        json={
            "key": STATE_KEY,
            "value": ledger_index,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()


def get_v2_max_sale_ledger() -> int | None:
    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/{SALE_TABLE}",
        params={
            "select": "ledger_index",
            "order": "ledger_index.desc",
            "limit": "1",
        },
        headers=supabase_headers(),
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    rows = response.json() or []
    if not rows:
        return None
    return int(rows[0]["ledger_index"])


def get_or_bootstrap_last_ledger() -> int:
    state = read_state()
    if state is not None:
        return state

    # v1 stateを流用しない。
    # v2 tableへ実際にbackfill済みの最終Saleから安全側にbootstrapする。
    table_max = get_v2_max_sale_ledger()

    if table_max is None:
        explicit = os.environ.get("SALE_V2_START_LEDGER")
        if not explicit:
            raise RuntimeError(
                f"{STATE_KEY} does not exist and {SALE_TABLE} is empty. "
                "Set SALE_V2_START_LEDGER explicitly."
            )
        table_max = int(explicit)

    upsert_state(table_max)
    print(
        f"Bootstrap: created monitor_state {STATE_KEY}={table_max} "
        f"from {SALE_TABLE}"
    )
    return table_max


# =============================================================================
# Supabase v2 writer
# =============================================================================

def save_sale_history_batch(records: list[dict]) -> None:
    if not records:
        return

    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/{SALE_TABLE}",
        params={"on_conflict": "tx_hash"},
        headers=supabase_headers(
            prefer="return=minimal,resolution=merge-duplicates"
        ),
        json=records,
        timeout=HTTP_TIMEOUT,
    )

    if not response.ok:
        raise RuntimeError(
            f"{SALE_TABLE} upsert failed: "
            f"HTTP {response.status_code} {response.text[:2000]}"
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
                    "params": [{"ledger_index": "validated"}],
                },
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            response.raise_for_status()
            result = response.json().get("result", {})
            ledger_index = result.get("ledger_index")
            if ledger_index is not None:
                return int(ledger_index)
        except Exception as exc:
            last_error = exc

    raise last_error or RuntimeError(
        "All RPC endpoints failed for get_validated_ledger_index"
    )


def get_ledger_transactions(ledger_index: int) -> tuple[list[dict], int]:
    """
    1つのendpointで一時的な transport error / XRPL error / empty payload が出ても
    リトライし、それでも駄目なら次のendpointへfallbackする。
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

                if response.status_code in (429, 500, 502, 503, 504):
                    last_error = RuntimeError(
                        f"HTTP {response.status_code}"
                    )
                    if attempt < 2:
                        wait = 3 * (attempt + 1)
                        print(
                            f"    Ledger {ledger_index} retry {attempt + 1}: "
                            f"HTTP {response.status_code} ({rpc_url})"
                        )
                        time.sleep(wait)
                        continue
                    break

                response.raise_for_status()

                body = response.json()
                result = body.get("result", {}) or {}

                # XRPL JSON-RPCはHTTP 200でも result.error を返すことがある。
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
                            f"    Ledger {ledger_index} retry {attempt + 1}: "
                            f"XRPL {rpc_error} ({rpc_url})"
                        )
                        time.sleep(wait)
                        continue
                    break

                ledger = result.get("ledger") or result.get("ledger_data")
                if not ledger:
                    # 一時的に空レスポンスが返る場合も、即失敗せずretry/fallback。
                    preview = str(result)[:300]
                    last_error = RuntimeError(
                        f"ledger payload missing for {ledger_index}; "
                        f"result={preview}"
                    )
                    if attempt < 2:
                        wait = 2 * (attempt + 1)
                        print(
                            f"    Ledger {ledger_index} retry {attempt + 1}: "
                            f"empty ledger payload ({rpc_url})"
                        )
                        time.sleep(wait)
                        continue
                    break

                return (
                    ledger.get("transactions", []),
                    int(ledger.get("close_time") or 0),
                )

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                last_error = exc
                if attempt < 2:
                    wait = 3 * (attempt + 1)
                    print(
                        f"    Ledger {ledger_index} retry {attempt + 1}: "
                        f"{type(exc).__name__} ({rpc_url})"
                    )
                    time.sleep(wait)
                    continue
                break

            except Exception as exc:
                last_error = exc
                # JSON decode等も一時障害の可能性があるので同endpointで再試行。
                if attempt < 2:
                    wait = 2 * (attempt + 1)
                    print(
                        f"    Ledger {ledger_index} retry {attempt + 1}: "
                        f"{type(exc).__name__}: {exc} ({rpc_url})"
                    )
                    time.sleep(wait)
                    continue
                break

        print(
            f"    Ledger {ledger_index}: {rpc_url} failed, "
            "trying next endpoint"
        )

    raise last_error or RuntimeError(
        f"All RPC endpoints failed for ledger {ledger_index}"
    )


def fetch_ledger(ledger_index: int) -> tuple[int, list[dict], int]:
    transactions, close_time = get_ledger_transactions(ledger_index)
    return ledger_index, transactions, close_time


# =============================================================================
# Cache API
# =============================================================================

def is_cacheable_uri(hex_uri: str) -> bool:
    if not hex_uri or not hex_uri.strip():
        return False

    try:
        decoded = bytes.fromhex(hex_uri).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return False

    decoded_s = decoded.strip()

    if decoded_s.startswith("{"):
        return True

    valid_schemes = ("ipfs://", "ar://", "http://", "https://", "data:")
    return decoded_s.lower().startswith(valid_schemes)


def send_to_cache_api_batch(hex_uris: list[str]) -> None:
    if not hex_uris:
        return

    response = requests.post(
        f"{CACHE_API_URL}/api/cache/batch",
        json={"hex_uris": hex_uris},
        headers={"Content-Type": "application/json"},
        timeout=CACHE_TIMEOUT,
    )
    response.raise_for_status()


# =============================================================================
# Sale extraction
# =============================================================================

def build_parser_message(
    ledger_index: int,
    tx: dict,
    close_time: int,
) -> dict:
    meta = tx.get("meta", tx.get("metaData", {})) or {}

    # WebSocket / Parquet parserと同じshapeへ寄せる。
    tx_json = dict(tx)
    tx_json.pop("meta", None)
    tx_json.pop("metaData", None)

    return {
        "tx_json": tx_json,
        "meta": meta,
        "ledger_index": ledger_index,
        "close_time_iso": (
            ripple_time_to_iso(close_time) if close_time else None
        ),
        "hash": tx_json.get("hash") or tx.get("hash") or "",
        "validated": True,
    }


def process_ledger(
    extract: Any,
    ledger_index: int,
    transactions: list[dict],
    close_time: int,
) -> tuple[list[dict], list[str], Counter]:
    """
    1 ledgerをatomicな単位としてparseする。

    1件でもparser exception / None / table mismatchがあればraiseし、
    このledgerのrecordは呼び出し側へ一切返さない。
    したがってcheckpointがそのledgerを飛び越すことはない。
    """
    records: list[dict] = []
    uris: list[str] = []
    stats = Counter()

    for tx in transactions:
        if tx.get("TransactionType") != "NFTokenAcceptOffer":
            continue

        meta = tx.get("meta", tx.get("metaData", {})) or {}
        if meta.get("TransactionResult") != "tesSUCCESS":
            continue

        tx_hash = tx.get("hash", "")
        if not tx_hash:
            raise RuntimeError(
                f"ledger {ledger_index}: NFTokenAcceptOffer has no hash"
            )

        try:
            result = extract.extract_sale(
                build_parser_message(ledger_index, tx, close_time)
            )
        except Exception as exc:
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: "
                f"parser exception: {exc}"
            ) from exc

        if result is extract.SKIP:
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: "
                "extract_sale unexpectedly returned SKIP"
            )

        if result is None:
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: "
                "extract_sale returned None"
            )

        table, record, uri = result

        if table != SALE_TABLE:
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: "
                f"parser returned {table}, expected {SALE_TABLE}"
            )

        if record.get("parser_version") != EXPECTED_PARSER_VERSION:
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: "
                f"parser_version={record.get('parser_version')} "
                f"expected={EXPECTED_PARSER_VERSION}"
            )

        if record.get("tx_result") != "tesSUCCESS":
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: non-success record"
            )

        status = record.get("settlement_status")
        if status not in {"exact", "partial", "unresolved"}:
            raise RuntimeError(
                f"ledger {ledger_index} tx {tx_hash}: "
                f"invalid settlement_status={status!r}"
            )

        records.append(record)
        stats["sales"] += 1
        stats[f"settlement_{status}"] += 1
        stats[f"mode_{record.get('accept_mode')}"] += 1

        if uri and is_cacheable_uri(uri):
            uris.append(uri)

    return records, uris, stats


# =============================================================================
# Durable flush + checkpoint
# =============================================================================

def flush_and_checkpoint(
    *,
    pending_records: list[dict],
    pending_uris: list[str],
    high_water_ledger: int,
) -> None:
    """
    書き込み順:
      1. v2 DB UPSERT
      2. cache API
      3. monitor_state

    途中で失敗したらstateは進めない。
    次回同じledgerを再処理してもtx_hash UPSERTなので安全。
    """
    if pending_records:
        save_sale_history_batch(pending_records)
        print(f"  V2 upsert: {len(pending_records)} sales")

    if pending_uris:
        # 1run内で同じURIを何度も投げない
        unique_uris = list(dict.fromkeys(pending_uris))
        for i in range(0, len(unique_uris), 100):
            chunk = unique_uris[i:i + 100]
            send_to_cache_api_batch(chunk)
            print(f"  Cache batch: {len(chunk)} URIs")

    upsert_state(high_water_ledger)
    print(f"  Checkpoint: {STATE_KEY}={high_water_ledger}")


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    print("=== NFT SALE v2 Monitor Start ===")
    started = time.time()

    require_env()

    parser_dir = resolve_parser_dir()
    extract = load_shared_parser(parser_dir)

    print(f"Parser: {parser_dir}")
    print(f"Target table: {SALE_TABLE}")
    print(f"State key: {STATE_KEY}")
    print(f"Expected parser version: {EXPECTED_PARSER_VERSION}")
    print(f"RPC endpoints: {XRPL_RPC_ENDPOINTS}")

    # 初回はbackfill済みv2 tableからbootstrap。
    last_ledger_index = get_or_bootstrap_last_ledger()
    print(f"Last durable ledger: {last_ledger_index}")

    validated_index = get_validated_ledger_index()
    print(f"Current validated ledger: {validated_index}")

    start_index = last_ledger_index + 1
    end_index = min(
        validated_index,
        last_ledger_index + MAX_LEDGERS_PER_RUN,
    )

    if start_index > validated_index:
        print("No new ledgers to process")
        return 0

    print(
        f"Processing ledgers: {start_index} -> {end_index} "
        f"({end_index - start_index + 1} ledgers)"
    )
    print(f"Parallel workers: {PARALLEL_WORKERS}")

    if end_index < validated_index:
        print(
            f"  {validated_index - end_index} ledgers remain "
            "for later runs"
        )

    totals = Counter()
    current_index = last_ledger_index
    last_saved_index = last_ledger_index
    ledgers_since_checkpoint = 0

    pending_records: list[dict] = []
    pending_uris: list[str] = []

    stop_error: Exception | None = None

    all_indices = list(range(start_index, end_index + 1))

    for batch_start in range(0, len(all_indices), PARALLEL_WORKERS):
        batch = all_indices[
            batch_start:batch_start + PARALLEL_WORKERS
        ]

        fetched: dict[int, tuple[list[dict], int]] = {}

        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = {
                executor.submit(fetch_ledger, idx): idx
                for idx in batch
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    ledger_index, transactions, close_time = future.result()
                    fetched[ledger_index] = (transactions, close_time)
                except Exception as exc:
                    print(f"  Ledger {idx} fetch error: {exc}")
                    # 実際に止める位置はledger順処理で決める。
                    fetched[idx] = (None, exc)  # type: ignore[assignment]

        # fetched順ではなくledger順に処理。
        for idx in batch:
            item = fetched.get(idx)

            if item is None or item[0] is None:
                if item is not None and isinstance(item[1], Exception):
                    stop_error = RuntimeError(
                        f"ledger {idx} fetch failed: {item[1]}"
                    )
                else:
                    stop_error = RuntimeError(
                        f"ledger {idx} fetch result missing"
                    )
                break

            transactions, close_time = item

            try:
                ledger_records, ledger_uris, ledger_stats = process_ledger(
                    extract,
                    idx,
                    transactions,
                    close_time,
                )
            except Exception as exc:
                stop_error = exc
                break

            # ledger全体のparse成功後だけpendingへ合流。
            pending_records.extend(ledger_records)
            pending_uris.extend(ledger_uris)
            totals.update(ledger_stats)

            current_index = idx
            ledgers_since_checkpoint += 1

            if ledgers_since_checkpoint >= CHECKPOINT_INTERVAL:
                try:
                    flush_and_checkpoint(
                        pending_records=pending_records,
                        pending_uris=pending_uris,
                        high_water_ledger=current_index,
                    )
                except Exception as exc:
                    stop_error = RuntimeError(
                        f"flush/checkpoint failed at ledger "
                        f"{current_index}: {exc}"
                    )
                    break

                pending_records.clear()
                pending_uris.clear()
                last_saved_index = current_index
                ledgers_since_checkpoint = 0

        if stop_error is not None:
            break

    # エラー地点より前に成功済みだが未checkpointのledgerは保存する。
    if current_index > last_saved_index:
        try:
            flush_and_checkpoint(
                pending_records=pending_records,
                pending_uris=pending_uris,
                high_water_ledger=current_index,
            )
            pending_records.clear()
            pending_uris.clear()
            last_saved_index = current_index
        except Exception as exc:
            if stop_error is None:
                stop_error = RuntimeError(
                    f"final flush/checkpoint failed at "
                    f"{current_index}: {exc}"
                )
            else:
                print(
                    "WARNING: could not checkpoint already successful "
                    f"ledgers up to {current_index}: {exc}"
                )

    elapsed = time.time() - started

    print()
    print("=== NFT SALE v2 Monitor Result ===")
    print(f"Last durable ledger : {last_saved_index}")
    print(f"Sales written       : {totals['sales']}")
    print(f"  exact             : {totals['settlement_exact']}")
    print(f"  partial           : {totals['settlement_partial']}")
    print(f"  unresolved        : {totals['settlement_unresolved']}")
    print(f"Elapsed             : {elapsed:.1f}s")

    if stop_error is not None:
        print(f"ERROR: {stop_error}", file=sys.stderr)
        # GitHub Actionsを赤にして次回/手動確認を促す。
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
