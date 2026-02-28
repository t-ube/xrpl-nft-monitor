#!/usr/bin/env python3
"""
DynamicNFT 変化監視スクリプト
NFTokenModify トランザクションを監視し、変更前後のURIをキャッシュAPIに送信する
"""

import os
import binascii
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

# APIs
XRPSCAN_API = "https://console.xrpscan.com/api/v1/search"
XRPL_RPC = "https://xrplcluster.com/"

# NFTokenID デコード用定数
MASK32 = 0xFFFFFFFF

MAX_BATCH_SIZE = 10000
MAX_CHUNK_SIZE = 100

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


def get_current_ledger_index() -> int:
    """XRPL RPCから現在の検証済みレジャーインデックスを取得"""
    response = requests.post(
        XRPL_RPC,
        json={
            "method": "ledger",
            "params": [{"ledger_index": "validated"}],
        },
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()["result"]["ledger_index"]


def get_last_ledger_index() -> int:
    """Supabaseから最後に処理したledger_indexを取得"""
    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/monitor_state",
        params={"key": "eq.last_modify_ledger_index_0", "select": "value"},
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
        params={"key": "eq.last_modify_ledger_index_0"},
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json={"value": ledger_index, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    response.raise_for_status()


def save_modify_history_bulk(rows: list[dict]) -> None:
    """NFTokenModify履歴をSupabaseにバルクインサート（100件ずつ）"""
    if not rows:
        return
    for i in range(0, len(rows), MAX_CHUNK_SIZE):
        chunk = rows[i : i + MAX_CHUNK_SIZE]
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/nft_modify_history",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal,resolution=ignore-duplicates",
            },
            json=chunk,
        )
        response.raise_for_status()


def search_nftoken_modify(lte_ledger_index: int, size: int = 100) -> list:
    """XRPScan APIでNFTokenModifyトランザクションを検索"""
    query = {
        "bool": {
            "must": [
                {"term": {"TransactionType": "NFTokenModify"}},
                {"term": {"meta.TransactionResult": "tesSUCCESS"}},
                {"range": {"ledger_index": {"lte": lte_ledger_index + size}}},
                {"range": {"ledger_index": {"gte": lte_ledger_index}}},
            ]
        }
    }

    response = requests.post(
        XRPSCAN_API,
        json=query,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    hits = response.json().get("hits", {}).get("hits", [])

    # 自前でledger_indexで昇順ソート
    hits.sort(key=lambda x: x.get("_source", {}).get("ledger_index", 0))
    
    # 最大だけ返す
    return hits[:size]


def get_tx_details(tx_hash: str) -> dict:
    """XRPL RPCでトランザクション詳細を取得"""
    response = requests.post(
        XRPL_RPC,
        json={
            "method": "tx",
            "params": [
                {
                    "transaction": tx_hash,
                    "binary": False,
                    "api_version": 2,
                }
            ],
        },
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json().get("result", {})


def extract_uris_from_tx(tx_result: dict, target_nftoken_id: str) -> tuple[str | None, str | None]:
    """トランザクション結果から変更前後のURIを抽出"""
    previous_uri = None
    current_uri = None

    # metaからNFTokenPageの変更を探す
    meta = tx_result.get("meta", {})
    affected_nodes = meta.get("AffectedNodes", [])

    for node in affected_nodes:
        modified = node.get("ModifiedNode", {})
        if modified.get("LedgerEntryType") != "NFTokenPage":
            continue

        # FinalFields（変更後）から対象NFTのURIを探す
        final_tokens = modified.get("FinalFields", {}).get("NFTokens", [])
        for token in final_tokens:
            nft = token.get("NFToken", {})
            if nft.get("NFTokenID") == target_nftoken_id:
                current_uri = nft.get("URI")
                break

        # PreviousFields（変更前）から対象NFTのURIを探す
        prev_tokens = modified.get("PreviousFields", {}).get("NFTokens", [])
        for token in prev_tokens:
            nft = token.get("NFToken", {})
            if nft.get("NFTokenID") == target_nftoken_id:
                previous_uri = nft.get("URI")
                break

        if current_uri or previous_uri:
            break

    return previous_uri, current_uri


def filter_valid_uris(hex_uris: set[str]) -> list[str]:
    """有効なスキームのURIだけをフィルタ"""
    valid_schemes = ('ipfs://', 'ar://', 'http://', 'https://')
    valid = []
    for hex_uri in hex_uris:
        try:
            decoded = bytes.fromhex(hex_uri).decode('utf-8')
            if decoded.startswith(valid_schemes):
                valid.append(hex_uri)
            else:
                print(f"  Skipped: unsupported scheme ({decoded[:30]}...)")
        except Exception:
            print(f"  Skipped: decode error ({hex_uri[:16]}...)")
    return valid


def send_to_cache_api_batch(hex_uris: list[str]) -> list[dict]:
    """キャッシュAPIにURIをバッチ送信（100件ずつ）"""
    if not hex_uris:
        return []
    results = []
    for i in range(0, len(hex_uris), MAX_CHUNK_SIZE):
        chunk = hex_uris[i : i + MAX_CHUNK_SIZE]
        response = requests.post(
            f"{CACHE_API_URL}/api/cache/batch",
            json={"hex_uris": chunk},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        results.append(response.json())
    return results


def main():
    print("=== NFT MODIFY Monitor Start ===")

    # 1. 現在の検証済みレジャーインデックスを取得（ガード用）
    current_ledger = get_current_ledger_index()
    print(f"Current validated ledger: {current_ledger}")

    # 2. 前回のledger_indexを取得
    last_ledger_index = get_last_ledger_index()
    print(f"Last ledger index: {last_ledger_index}")

    # 3. NFTokenModifyを検索（前回の次から）
    hits = search_nftoken_modify(last_ledger_index, MAX_BATCH_SIZE)
    print(f"Found {len(hits)} NFTokenModify transactions")

    if not hits:
        print("No new transactions")
        new_index = min(last_ledger_index + MAX_BATCH_SIZE, current_ledger)
        update_last_ledger_index(new_index)
        print(f"Updated last ledger index: {new_index}")
        return

    # 4. 各トランザクションを処理
    max_ledger_index = last_ledger_index
    processed = 0
    errors = 0
    cache_uris: set[str] = set()
    history_rows: list[dict] = []

    for hit in hits:
        source = hit.get("_source", {})
        tx_hash = source.get("hash", "")
        nftoken_id = source.get("NFTokenID", "")
        ledger_index = source.get("ledger_index", 0)

        if not tx_hash:
            continue

        try:
            # NFTokenIDをデコード
            decoded = decode_nftoken_id(nftoken_id)

            # tx APIで詳細を取得
            tx_result = get_tx_details(tx_hash)
            previous_uri, current_uri = extract_uris_from_tx(tx_result, nftoken_id)

            # トランザクション日時を取得
            tx_date = source.get("_date", "")
            owner = source.get("Account", "")

            print(f"  [{tx_hash[:8]}...] NFT: {nftoken_id[:16]}... (issuer: {decoded.issuer[:8]}...)")

            # キャッシュ対象URIを収集
            for uri in (previous_uri, current_uri):
                if uri and uri.strip():
                    cache_uris.add(uri)

            # 履歴行を収集
            history_rows.append({
                "tx_hash": tx_hash,
                "nftoken_id": nftoken_id,
                "ledger_index": ledger_index,
                "tx_date": tx_date,
                "issuer": decoded.issuer,
                "owner": owner,
                "taxon": decoded.taxon,
                "sequence": decoded.sequence,
                "previous_uri": previous_uri,
                "current_uri": current_uri,
            })

            processed += 1

        except Exception as e:
            print(f"  [{tx_hash[:8]}...] Error: {e}")
            errors += 1

        max_ledger_index = max(max_ledger_index, ledger_index)

    # 5. 履歴をバルクインサート
    if history_rows:
        save_modify_history_bulk(history_rows)
        print(f"Bulk insert: {len(history_rows)} rows")

    # 6. 収集したURIをバッチでキャッシュAPIに送信
    if cache_uris:
        valid_uris = filter_valid_uris(cache_uris)
        if valid_uris:
            result = send_to_cache_api_batch(valid_uris)
            print(f"Batch cache: {len(valid_uris)} URIs sent")

    # 7. ledger_indexを更新（現在のレジャーを超えないようにガード）
    if max_ledger_index > last_ledger_index:
        safe_index = min(max_ledger_index, current_ledger)
        update_last_ledger_index(safe_index)
        print(f"Updated last ledger index: {safe_index}")

    print(f"=== Done: {processed} processed, {errors} errors ===")


if __name__ == "__main__":
    main()