#!/usr/bin/env python3
"""
NFT 売買監視スクリプト
XRPL ledger RPCを使用して全NFTokenAcceptOfferトランザクションを監視し、
売買履歴を記録する。ブローカー取引（xrp.cafe等）に対応。
5分間隔のcron実行を想定
"""

import os
import time
import binascii
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from dotenv import load_dotenv
import requests
from xrpl.core import addresscodec

load_dotenv()

# 環境変数
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CACHE_API_URL = os.environ.get("CACHE_API_URL")

# XRPL RPC（フォールバック付き）
XRPL_RPC_ENDPOINTS = [
    os.environ.get("XRPL_RPC", "https://xrplcluster.com/"),
    "https://s2.ripple.com:51234/",
]

# 設定
MAX_LEDGERS_PER_RUN = 500
PARALLEL_WORKERS = 5

# NFTokenID デコード用定数
MASK32 = 0xFFFFFFFF
STATE_KEY = "last_sale_ledger_index"

# XRPL epoch (2000-01-01T00:00:00Z) と UNIX epoch の差分
RIPPLE_EPOCH_OFFSET = 946684800


@dataclass(frozen=True)
class NFTokenIDDecoded:
    nftoken_id: str
    flags: int
    transfer_fee: int
    issuer: str
    taxon: int
    sequence: int


def decode_nftoken_id(nft_id_hex: str) -> NFTokenIDDecoded:
    s = nft_id_hex.strip().upper()
    if s.startswith("0X"):
        s = s[2:]

    b = binascii.unhexlify(s)
    if len(b) != 32:
        raise ValueError("NFTokenID must be exactly 32 bytes")

    flags = (b[0] << 8) | b[1]
    transfer_fee = (b[2] << 8) | b[3]
    issuer_bytes = b[4:24]
    issuer = addresscodec.encode_classic_address(issuer_bytes)

    scrambled_taxon = (b[24] << 24) | (b[25] << 16) | (b[26] << 8) | b[27]
    token_sequence = (b[28] << 24) | (b[29] << 16) | (b[30] << 8) | b[31]

    scramble = (384160001 * token_sequence + 2459) & MASK32
    taxon = (scrambled_taxon ^ scramble) & MASK32

    return NFTokenIDDecoded(
        nftoken_id=s,
        flags=flags,
        transfer_fee=transfer_fee,
        issuer=issuer,
        taxon=taxon,
        sequence=token_sequence,
    )


def ripple_time_to_iso(ripple_time: int) -> str:
    """XRPL時間（Ripple Epoch）をISO 8601文字列に変換"""
    unix_time = ripple_time + RIPPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(unix_time, tz=timezone.utc).isoformat()


# ── Amount parser ──

def parse_amount(amount: Any) -> dict:
    result = {"drops": None, "currency": None, "value": None, "issuer": None}
    if isinstance(amount, str):
        try:
            result["drops"] = int(amount)
        except ValueError:
            pass
    elif isinstance(amount, dict):
        result["currency"] = amount.get("currency", "")
        result["value"] = amount.get("value", "")
        result["issuer"] = amount.get("issuer", "")
    return result


def format_price(parsed: dict) -> str:
    if parsed["drops"] is not None:
        return f"{parsed['drops'] / 1_000_000:.2f} XRP"
    elif parsed["value"]:
        return f"{parsed['value']} {parsed['currency']}"
    return "free"


def classify_sale(
    seller: str, buyer: str, sell_price: dict, decoded: NFTokenIDDecoded,
) -> str:
    is_free = (sell_price["drops"] or 0) == 0 and not sell_price["value"]
    if not is_free:
        return "sale"
    if seller == decoded.issuer:
        return "distribution"
    return "transfer"


# =============================================================================
# Supabase 操作
# =============================================================================

def get_last_ledger_index() -> int:
    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"key": f"eq.{STATE_KEY}", "select": "value"},
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
    )
    response.raise_for_status()
    data = response.json()
    return int(data[0]["value"]) if data else 0


def update_last_ledger_index(ledger_index: int) -> None:
    response = requests.patch(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"key": f"eq.{STATE_KEY}"},
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json={"value": ledger_index, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    response.raise_for_status()


def save_sale_history_batch(records: list[dict]) -> None:
    """NFTokenAcceptOffer履歴をSupabaseにバルクインサート"""
    if not records:
        return
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/nft_sale_history",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=ignore-duplicates",
        },
        json=records,
    )
    response.raise_for_status()


# =============================================================================
# XRPL RPC 操作
# =============================================================================

def get_validated_ledger_index() -> int:
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
            return result.get("ledger_index", 0)
        except Exception:
            continue
    raise RuntimeError("All RPC endpoints failed for get_validated_ledger_index")


def get_ledger_transactions(ledger_index: int) -> tuple[list[dict], int]:
    """
    指定レジャーの全トランザクションを展開して取得
    エンドポイントのフォールバック + リトライ
    """
    last_error = None
    for rpc_url in XRPL_RPC_ENDPOINTS:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    rpc_url,
                    json={
                        "method": "ledger",
                        "params": [
                            {
                                "ledger_index": ledger_index,
                                "transactions": True,
                                "expand": True,
                            }
                        ],
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
                if response.status_code in (429, 503):
                    wait = 3 * (attempt + 1)
                    print(f"    Ledger {ledger_index} retry {attempt + 1}: HTTP {response.status_code} ({rpc_url})")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                result = response.json().get("result", {})
                ledger = result.get("ledger", result.get("ledger_data", {}))
                close_time = ledger.get("close_time", 0)
                return ledger.get("transactions", []), close_time
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = 3 * (attempt + 1)
                    print(f"    Ledger {ledger_index} retry {attempt + 1}: {type(e).__name__} ({rpc_url})")
                    time.sleep(wait)
                    continue
                print(f"    Ledger {ledger_index}: {rpc_url} failed, trying next endpoint")
                break

    raise last_error or RuntimeError(f"All RPC endpoints failed for ledger {ledger_index}")


# =============================================================================
# 売買情報抽出
# =============================================================================

def extract_sale_info(tx: dict) -> dict:
    """
    NFTokenAcceptOffer から売買情報を抽出（expand済みtxから直接）

    取引パターン:
      1. sell offer のみ Accept → Account=buyer, offer.Owner=seller
      2. buy offer のみ Accept  → Account=seller, offer.Owner=buyer
      3. 両方指定（ブローカー） → Account=broker, sell.Owner=seller, buy.Owner=buyer
    """
    account = tx.get("Account", "")
    meta = tx.get("meta", tx.get("metaData", {}))

    info = {
        "nftoken_id": meta.get("nftoken_id"),
        "seller": None,
        "buyer": None,
        "broker": None,
        "sell_price": {"drops": None, "currency": None, "value": None, "issuer": None},
        "buy_price": {"drops": None, "currency": None, "value": None, "issuer": None},
        "uri": None,
    }

    # DeletedNode(NFTokenOffer) から売り/買いオファーを収集
    sell_offer = None
    buy_offer = None

    for node in meta.get("AffectedNodes", []):
        deleted = node.get("DeletedNode", {})
        if deleted.get("LedgerEntryType") != "NFTokenOffer":
            continue

        fields = deleted.get("FinalFields", {})
        offer_flags = fields.get("Flags", 0)
        owner = fields.get("Owner", "")
        amount = fields.get("Amount", "0")

        if not info["nftoken_id"]:
            info["nftoken_id"] = fields.get("NFTokenID")

        if offer_flags & 1:  # tfSellNFToken
            sell_offer = {"owner": owner, "amount": amount}
        else:
            buy_offer = {"owner": owner, "amount": amount}

    # パターン判定
    if sell_offer and buy_offer:
        info["seller"] = sell_offer["owner"]
        info["buyer"] = buy_offer["owner"]
        info["broker"] = account
        info["sell_price"] = parse_amount(sell_offer["amount"])
        info["buy_price"] = parse_amount(buy_offer["amount"])
    elif sell_offer:
        info["seller"] = sell_offer["owner"]
        info["buyer"] = account
        info["sell_price"] = parse_amount(sell_offer["amount"])
        info["buy_price"] = info["sell_price"]
    elif buy_offer:
        info["seller"] = account
        info["buyer"] = buy_offer["owner"]
        info["buy_price"] = parse_amount(buy_offer["amount"])
        info["sell_price"] = info["buy_price"]
    else:
        info["seller"] = ""
        info["buyer"] = account
        info["sell_price"] = parse_amount("0")
        info["buy_price"] = parse_amount("0")

    # URI: NFTokenPage の FinalFields から
    if info["nftoken_id"]:
        for node in meta.get("AffectedNodes", []):
            modified = node.get("ModifiedNode", {})
            if modified.get("LedgerEntryType") != "NFTokenPage":
                continue
            for token in modified.get("FinalFields", {}).get("NFTokens", []):
                nft = token.get("NFToken", {})
                if nft.get("NFTokenID") == info["nftoken_id"]:
                    info["uri"] = nft.get("URI")
                    break
            if info["uri"]:
                break

    return info


# =============================================================================
# キャッシュAPI
# =============================================================================

def is_cacheable_uri(hex_uri: str) -> bool:
    if not hex_uri or not hex_uri.strip():
        return False
    try:
        decoded = bytes.fromhex(hex_uri).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return False
    valid_schemes = ("ipfs://", "ar://", "http://", "https://")
    if not decoded.startswith(valid_schemes):
        preview = decoded[:80] if len(decoded) <= 80 else decoded[:80] + "..."
        print(f"    Skipped: {preview}")
        return False
    return True


def send_to_cache_api_batch(hex_uris: list[str]) -> dict | None:
    if not hex_uris:
        return None
    response = requests.post(
        f"{CACHE_API_URL}/api/cache/batch",
        json={"hex_uris": hex_uris},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()


# =============================================================================
# メイン処理
# =============================================================================

def fetch_ledger(ledger_index: int) -> tuple[int, list[dict], int]:
    transactions, close_time = get_ledger_transactions(ledger_index)
    return ledger_index, transactions, close_time


def process_transactions(
    ledger_index: int, transactions: list[dict], close_time: int,
    pending_records: list[dict], pending_uris: list[str],
    type_counts: dict[str, int],
) -> tuple[int, int, int]:
    """
    取得済みトランザクションからNFTokenAcceptOfferを処理する
    Returns: (processed, errors, skipped)
    """
    processed = 0
    errors = 0
    skipped = 0
    ledger_date = ripple_time_to_iso(close_time) if close_time else None

    for tx in transactions:
        if tx.get("TransactionType") != "NFTokenAcceptOffer":
            continue

        meta = tx.get("meta", tx.get("metaData", {}))
        if meta.get("TransactionResult") != "tesSUCCESS":
            continue

        tx_hash = tx.get("hash", "")
        if not tx_hash:
            continue

        try:
            sale = extract_sale_info(tx)
            nftoken_id = sale["nftoken_id"]

            if not nftoken_id:
                print(f"  [{tx_hash[:8]}...] Skipped: no NFTokenID found")
                continue

            decoded = decode_nftoken_id(nftoken_id)

            sale_type = classify_sale(
                seller=sale["seller"] or "",
                buyer=sale["buyer"] or "",
                sell_price=sale["sell_price"],
                decoded=decoded,
            )

            # ログ出力
            sell_str = format_price(sale["sell_price"])
            buy_str = format_price(sale["buy_price"])
            seller_short = (sale["seller"] or "?")[:8]
            buyer_short = (sale["buyer"] or "?")[:8]
            type_icon = {"sale": "💰", "distribution": "🎁", "transfer": "🔄"}[sale_type]

            if sale["broker"]:
                broker_short = sale["broker"][:8]
                print(f"  {type_icon} [{tx_hash[:8]}...] {seller_short}... -> {buyer_short}... via {broker_short}...")
                print(f"    Seller receives: {sell_str} | Buyer pays: {buy_str}")
            else:
                print(f"  {type_icon} [{tx_hash[:8]}...] {seller_short}... -> {buyer_short}... @ {buy_str} ({sale_type})")

            # URIをキャッシュ対象に追加
            uri_skipped = False
            if sale["uri"] and is_cacheable_uri(sale["uri"]):
                pending_uris.append(sale["uri"])
                print(f"    URI -> queued")
            elif sale["uri"]:
                uri_skipped = True

            if uri_skipped:
                skipped += 1

            # レコードをバッファに追加
            pending_records.append({
                "tx_hash": tx_hash,
                "nftoken_id": nftoken_id,
                "ledger_index": ledger_index,
                "tx_date": ledger_date,
                "issuer": decoded.issuer,
                "taxon": decoded.taxon,
                "seller": sale["seller"] or "",
                "buyer": sale["buyer"] or "",
                "broker": sale["broker"],
                "sell_price_drops": sale["sell_price"]["drops"],
                "sell_price_currency": sale["sell_price"]["currency"],
                "sell_price_value": sale["sell_price"]["value"],
                "sell_price_issuer": sale["sell_price"]["issuer"],
                "buy_price_drops": sale["buy_price"]["drops"],
                "buy_price_currency": sale["buy_price"]["currency"],
                "buy_price_value": sale["buy_price"]["value"],
                "buy_price_issuer": sale["buy_price"]["issuer"],
                "uri": sale["uri"],
                "sale_type": sale_type,
            })

            processed += 1
            type_counts[sale_type] = type_counts.get(sale_type, 0) + 1

        except Exception as e:
            print(f"  [{tx_hash[:8]}...] Error: {e}")
            errors += 1

    return processed, errors, skipped


def main():
    print("=== NFT SALE Monitor Start ===")
    start_time = time.time()

    # 1. 前回のledger_indexを取得
    last_ledger_index = get_last_ledger_index()
    print(f"Last processed ledger: {last_ledger_index}")

    # 2. 現在のvalidated ledger indexを取得
    validated_index = get_validated_ledger_index()
    print(f"Current validated ledger: {validated_index}")

    # 処理範囲を計算
    start_index = last_ledger_index + 1
    end_index = min(validated_index, last_ledger_index + MAX_LEDGERS_PER_RUN)

    if start_index > validated_index:
        print("No new ledgers to process")
        return

    ledger_count = end_index - start_index + 1
    print(f"Processing ledgers: {start_index} -> {end_index} ({ledger_count} ledgers)")
    print(f"Parallel workers: {PARALLEL_WORKERS}")

    if end_index < validated_index:
        print(f"  ⚠ {validated_index - end_index} ledgers remaining (will process in next run)")

    # 3. バッチ並列でレジャーを取得・処理
    total_processed = 0
    total_errors = 0
    total_skipped = 0
    current_index = start_index - 1
    checkpoint_interval = 30
    last_saved_index = last_ledger_index
    ledgers_since_checkpoint = 0
    pending_records = []
    pending_uris = []
    type_counts = {"sale": 0, "distribution": 0, "transfer": 0}

    all_indices = list(range(start_index, end_index + 1))

    for batch_start in range(0, len(all_indices), PARALLEL_WORKERS):
        batch = all_indices[batch_start : batch_start + PARALLEL_WORKERS]

        # 並列でレジャーデータを取得
        fetched = {}
        fetch_error = False
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = {executor.submit(fetch_ledger, idx): idx for idx in batch}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    ledger_index, transactions, close_time = future.result()
                    fetched[ledger_index] = (transactions, close_time)
                except Exception as e:
                    print(f"  Ledger {idx} fetch error: {e}")
                    total_errors += 1
                    fetch_error = True

        # レジャー順に処理（順序保証）
        for idx in batch:
            if idx not in fetched:
                break

            transactions, close_time = fetched[idx]
            try:
                processed, errors, skipped = process_transactions(
                    idx, transactions, close_time,
                    pending_records, pending_uris, type_counts,
                )
                total_processed += processed
                total_errors += errors
                total_skipped += skipped
                current_index = idx
                ledgers_since_checkpoint += 1

                # 30レジャーごとにフラッシュ
                if ledgers_since_checkpoint >= checkpoint_interval:
                    for i in range(0, len(pending_uris), 100):
                        chunk = pending_uris[i : i + 100]
                        send_to_cache_api_batch(chunk)
                        print(f"  Cache batch: {len(chunk)} URIs")
                    pending_uris = []

                    if pending_records:
                        save_sale_history_batch(pending_records)
                        print(f"  Bulk insert: {len(pending_records)} records")
                        pending_records = []

                    update_last_ledger_index(current_index)
                    last_saved_index = current_index
                    ledgers_since_checkpoint = 0
                    print(f"  Checkpoint: saved ledger index {current_index}")

            except Exception as e:
                print(f"  Ledger {idx} process error: {e}")
                total_errors += 1
                break
        else:
            if not fetch_error:
                continue
        break

    # 4. 残りをフラッシュ
    for i in range(0, len(pending_uris), 100):
        chunk = pending_uris[i : i + 100]
        send_to_cache_api_batch(chunk)
        print(f"  Cache batch: {len(chunk)} URIs")

    if pending_records:
        save_sale_history_batch(pending_records)
        print(f"  Bulk insert: {len(pending_records)} records")

    if current_index > last_saved_index:
        update_last_ledger_index(current_index)
        print(f"Updated last ledger index: {current_index}")

    elapsed = time.time() - start_time
    print(f"=== Done: {total_processed} sales ({total_skipped} skipped), {total_errors} errors, {elapsed:.1f}s ===")
    print(f"    💰 sales: {type_counts['sale']} | 🎁 distributions: {type_counts['distribution']} | 🔄 transfers: {type_counts['transfer']}")


if __name__ == "__main__":
    main()