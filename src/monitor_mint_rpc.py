#!/usr/bin/env python3
"""
NFTokenMint 監視スクリプト
XRPL ledger RPCを使用して全NFTokenMintトランザクションを監視する
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
MAX_LEDGERS_PER_RUN = 500  # 1回の実行で処理する最大レジャー数（5分≒60-100レジャー + バッファ）
PARALLEL_WORKERS = 5  # ledger RPC並列数

# NFTokenID デコード用定数
MASK32 = 0xFFFFFFFF

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

    def as_dict(self) -> dict[str, Any]:
        return {
            "nftoken_id": self.nftoken_id,
            "flags": self.flags,
            "transfer_fee": self.transfer_fee,
            "issuer": self.issuer,
            "taxon": self.taxon,
            "sequence": self.sequence,
        }


def decode_nftoken_id(nft_id_hex: str) -> NFTokenIDDecoded:
    """NFTokenIDをデコードしてissuer, taxon, sequenceなどを取得"""
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

    # Unscramble taxon
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


# =============================================================================
# Supabase 操作
# =============================================================================

def get_last_ledger_index() -> int:
    """Supabaseから最後に処理したledger_indexを取得"""
    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"key": "eq.last_mint_ledger_index", "select": "value"},
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
    )
    response.raise_for_status()
    data = response.json()
    if data:
        return int(data[0]["value"])
    return 0


def update_last_ledger_index(ledger_index: int) -> None:
    """Supabaseのledger_indexを更新"""
    response = requests.patch(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"key": "eq.last_mint_ledger_index"},
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json={"value": ledger_index, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    response.raise_for_status()


def save_mint_history_batch(records: list[dict]) -> None:
    """NFTokenMint履歴をSupabaseにバルクインサート"""
    if not records:
        return
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/nft_mint_history",
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
    """現在のvalidated ledger indexを取得"""
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
    Returns: (transactions, close_time)
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
                # このエンドポイントでリトライ上限 → 次のエンドポイントへ
                print(f"    Ledger {ledger_index}: {rpc_url} failed, trying next endpoint")
                break

    raise last_error or RuntimeError(f"All RPC endpoints failed for ledger {ledger_index}")


def extract_minted_nftoken_id(tx: dict) -> tuple[str, str] | None:
    """
    トランザクションのmetaから新規発行されたNFTokenIDとURIを抽出する
    """
    meta = tx.get("meta", tx.get("metaData", {}))
    affected_nodes = meta.get("AffectedNodes", [])

    for node in affected_nodes:
        for node_action in ["CreatedNode", "ModifiedNode"]:
            item = node.get(node_action)
            if not item or item.get("LedgerEntryType") != "NFTokenPage":
                continue

            final_fields = item.get("NewFields", item.get("FinalFields", {}))
            nftokens = final_fields.get("NFTokens", [])

            previous_fields = item.get("PreviousFields", {})
            previous_tokens = previous_fields.get("NFTokens", [])
            previous_ids = {t["NFToken"]["NFTokenID"] for t in previous_tokens}

            for t in nftokens:
                nft_id = t["NFToken"]["NFTokenID"]
                nft_uri = t["NFToken"].get("URI", "")
                if nft_id not in previous_ids:
                    return nft_id, nft_uri

    return None


# =============================================================================
# キャッシュAPI
# =============================================================================

def is_cacheable_uri(hex_uri: str) -> bool:
    """キャッシュ対象のURIかチェック"""
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
    """キャッシュAPIにバッチ送信（最大100件）"""
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
    """
    レジャーデータを取得する（並列実行用）
    Returns: (ledger_index, transactions, close_time)
    """
    transactions, close_time = get_ledger_transactions(ledger_index)
    return ledger_index, transactions, close_time


def process_transactions(
    ledger_index: int, transactions: list[dict], close_time: int,
    pending_records: list[dict], pending_uris: list[str],
) -> tuple[int, int, int]:
    """
    取得済みトランザクションを処理し、pending_records/pending_urisに追加する
    Returns: (processed, errors, skipped)
    """
    processed = 0
    errors = 0
    skipped = 0
    ledger_date = ripple_time_to_iso(close_time) if close_time else None

    for tx in transactions:
        # NFTokenMint かつ成功したトランザクションのみ
        if tx.get("TransactionType") != "NFTokenMint":
            continue

        meta = tx.get("meta", tx.get("metaData", {}))
        if meta.get("TransactionResult") != "tesSUCCESS":
            continue

        tx_hash = tx.get("hash", "")
        if not tx_hash:
            continue

        try:
            result = extract_minted_nftoken_id(tx)
            if not result:
                print(f"  [{tx_hash[:8]}...] Could not extract NFTokenID")
                continue

            nftoken_id, uri = result
            decoded = decode_nftoken_id(nftoken_id)

            owner = tx.get("Account", "")

            print(f"  [{tx_hash[:8]}...] NFT: {nftoken_id[:16]}... (issuer: {decoded.issuer[:8]}...)")

            # キャッシュ対象URIをバッファに追加
            if uri and is_cacheable_uri(uri):
                pending_uris.append(uri)
                print(f"    URI -> queued")
            elif uri:
                skipped += 1

            # レコードをバッファに追加
            pending_records.append({
                "tx_hash": tx_hash,
                "nftoken_id": nftoken_id,
                "ledger_index": ledger_index,
                "tx_date": ledger_date,
                "issuer": decoded.issuer,
                "owner": owner,
                "taxon": decoded.taxon,
                "sequence": decoded.sequence,
                "uri": uri,
            })

            processed += 1

        except Exception as e:
            print(f"  [{tx_hash[:8]}...] Error: {e}")
            errors += 1

    return processed, errors, skipped


def main():
    print("=== NFT MINT Monitor Start ===")
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
                    idx, transactions, close_time, pending_records, pending_uris
                )
                total_processed += processed
                total_errors += errors
                total_skipped += skipped
                current_index = idx
                ledgers_since_checkpoint += 1

                # 30レジャーごとにフラッシュ
                if ledgers_since_checkpoint >= checkpoint_interval:
                    # キャッシュAPIバッチ送信（100件ずつ）
                    for i in range(0, len(pending_uris), 100):
                        chunk = pending_uris[i : i + 100]
                        send_to_cache_api_batch(chunk)
                        print(f"  Cache batch: {len(chunk)} URIs")
                    pending_uris = []

                    # Supabaseバルクインサート
                    if pending_records:
                        save_mint_history_batch(pending_records)
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
        save_mint_history_batch(pending_records)
        print(f"  Bulk insert: {len(pending_records)} records")

    if current_index > last_saved_index:
        update_last_ledger_index(current_index)
        print(f"Updated last ledger index: {current_index}")

    elapsed = time.time() - start_time
    print(f"=== Done: {total_processed} mints ({total_skipped} skipped), {total_errors} errors, {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()