#!/usr/bin/env python3
"""
NFT 売買監視スクリプト
NFTokenAcceptOffer トランザクションを監視し、売買履歴を記録する
ブローカー取引（xrp.cafe等）に対応
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

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CACHE_API_URL = os.environ.get("CACHE_API_URL")

XRPSCAN_API = "https://console.xrpscan.com/api/v1/search"
XRPL_RPC = "https://xrplcluster.com/"

MASK32 = 0xFFFFFFFF
MAX_BATCH_SIZE = 100
STATE_KEY = "last_sale_ledger_index"


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


# ── Amount parser ──

def parse_amount(amount: Any) -> dict:
    """
    XRPLのAmount をパースして統一フォーマットに変換

    Returns:
        {
            "drops": int | None,       # XRP (in drops)
            "currency": str | None,    # IOU currency
            "value": str | None,       # IOU amount
            "issuer": str | None,      # IOU issuer
        }
    """
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
    """表示用の価格文字列"""
    if parsed["drops"] is not None:
        return f"{parsed['drops'] / 1_000_000:.2f} XRP"
    elif parsed["value"]:
        return f"{parsed['value']} {parsed['currency']}"
    return "free"


def classify_sale(
    seller: str,
    buyer: str,
    sell_price: dict,
    decoded: NFTokenIDDecoded,
) -> str:
    """
    取引タイプを分類

    sale         - 有償売買
    distribution - issuerからの配送（ランダムミント後の配布等）
    transfer     - 無償移動（ギフト、自己移動）
    """
    is_free = (sell_price["drops"] or 0) == 0 and not sell_price["value"]

    if not is_free:
        return "sale"

    if seller == decoded.issuer:
        return "distribution"

    return "transfer"


# ── Supabase helpers ──

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


def save_sale_history(
    tx_hash: str,
    nftoken_id: str,
    ledger_index: int,
    tx_date: str,
    decoded: NFTokenIDDecoded,
    seller: str,
    buyer: str,
    broker: str | None,
    sell_price: dict,
    buy_price: dict,
    uri: str | None,
    sale_type: str,
) -> None:
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/nft_sale_history",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json={
            "tx_hash": tx_hash,
            "nftoken_id": nftoken_id,
            "ledger_index": ledger_index,
            "tx_date": tx_date,
            "issuer": decoded.issuer,
            "taxon": decoded.taxon,
            "seller": seller,
            "buyer": buyer,
            "broker": broker,
            "sell_price_drops": sell_price["drops"],
            "sell_price_currency": sell_price["currency"],
            "sell_price_value": sell_price["value"],
            "sell_price_issuer": sell_price["issuer"],
            "buy_price_drops": buy_price["drops"],
            "buy_price_currency": buy_price["currency"],
            "buy_price_value": buy_price["value"],
            "buy_price_issuer": buy_price["issuer"],
            "uri": uri,
            "sale_type": sale_type,
        },
    )
    if response.status_code == 409:
        return
    response.raise_for_status()


# ── XRPL helpers ──

def search_accept_offers(gte_ledger_index: int, size: int = 100) -> list:
    query = {
        "bool": {
            "must": [
                {"term": {"TransactionType": "NFTokenAcceptOffer"}},
                {"term": {"meta.TransactionResult": "tesSUCCESS"}},
                {"range": {"ledger_index": {"gte": gte_ledger_index}}},
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
    hits.sort(key=lambda x: x.get("_source", {}).get("ledger_index", 0))
    return hits[:size]


def get_tx_details(tx_hash: str) -> dict:
    response = requests.post(
        XRPL_RPC,
        json={
            "method": "tx",
            "params": [{"transaction": tx_hash, "binary": False, "api_version": 2}],
        },
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json().get("result", {})


def extract_sale_info(tx_result: dict) -> dict:
    """
    NFTokenAcceptOffer から売買情報を抽出

    取引パターン:
      1. sell offer のみ Accept → Account=buyer, offer.Owner=seller
      2. buy offer のみ Accept  → Account=seller, offer.Owner=buyer
      3. 両方指定（ブローカー） → Account=broker, sell.Owner=seller, buy.Owner=buyer

    ブローカー取引では売り手の受取額と買い手の支払額が異なる。差額がブローカー利益。
    """
    tx = tx_result.get("tx_json", tx_result.get("tx", {}))
    meta = tx_result.get("meta", {})
    account = tx.get("Account", "")

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
        # ブローカー取引: Account は仲介者
        info["seller"] = sell_offer["owner"]
        info["buyer"] = buy_offer["owner"]
        info["broker"] = account
        info["sell_price"] = parse_amount(sell_offer["amount"])
        info["buy_price"] = parse_amount(buy_offer["amount"])
    elif sell_offer:
        # sell offer のみ: Account が買い手
        info["seller"] = sell_offer["owner"]
        info["buyer"] = account
        info["sell_price"] = parse_amount(sell_offer["amount"])
        info["buy_price"] = info["sell_price"]  # 直接取引 = 同額
    elif buy_offer:
        # buy offer のみ: Account が売り手
        info["seller"] = account
        info["buyer"] = buy_offer["owner"]
        info["buy_price"] = parse_amount(buy_offer["amount"])
        info["sell_price"] = info["buy_price"]  # 直接取引 = 同額
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


def send_to_cache_api(hex_uri: str) -> dict | None:
    if not hex_uri or not hex_uri.strip():
        return None

    try:
        decoded = bytes.fromhex(hex_uri).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None

    valid_schemes = ("ipfs://", "ar://", "http://", "https://")
    if not decoded.startswith(valid_schemes):
        return None

    response = requests.post(
        f"{CACHE_API_URL}/api/cache",
        json={"hex_uri": hex_uri},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def main():
    print("=== NFT SALE Monitor Start ===")

    last_ledger_index = get_last_ledger_index()
    print(f"Last ledger index: {last_ledger_index}")

    hits = search_accept_offers(last_ledger_index + 1, MAX_BATCH_SIZE)
    print(f"Found {len(hits)} NFTokenAcceptOffer transactions")

    if not hits:
        print("No new transactions")
        return

    max_ledger_index = last_ledger_index
    processed = 0
    errors = 0
    type_counts = {"sale": 0, "distribution": 0, "transfer": 0}

    for hit in hits:
        source = hit.get("_source", {})
        tx_hash = source.get("hash", "")
        ledger_index = source.get("ledger_index", 0)
        tx_date = source.get("_date", "")

        if not tx_hash:
            continue

        try:
            tx_result = get_tx_details(tx_hash)
            sale = extract_sale_info(tx_result)

            nftoken_id = sale["nftoken_id"]
            if not nftoken_id:
                print(f"  [{tx_hash[:8]}...] Skipped: no NFTokenID found")
                continue

            decoded = decode_nftoken_id(nftoken_id)

            # 取引タイプを分類
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
                print(f"  {type_icon} [{tx_hash}] {seller_short}... -> {buyer_short}... @ {buy_str} ({sale_type})")

            # URIをキャッシュ
            if sale["uri"]:
                result = send_to_cache_api(sale["uri"])
                if result:
                    print(f"    Cache: {result.get('status')}")

            save_sale_history(
                tx_hash=tx_hash,
                nftoken_id=nftoken_id,
                ledger_index=ledger_index,
                tx_date=tx_date,
                decoded=decoded,
                seller=sale["seller"] or "",
                buyer=sale["buyer"] or "",
                broker=sale["broker"],
                sell_price=sale["sell_price"],
                buy_price=sale["buy_price"],
                uri=sale["uri"],
                sale_type=sale_type,
            )

            processed += 1
            type_counts[sale_type] = type_counts.get(sale_type, 0) + 1

        except Exception as e:
            print(f"  [{tx_hash[:8]}...] Error: {e}")
            errors += 1

        max_ledger_index = max(max_ledger_index, ledger_index)

    if max_ledger_index > last_ledger_index:
        update_last_ledger_index(max_ledger_index)
        print(f"Updated last ledger index: {max_ledger_index}")

    print(f"=== Done: {processed} processed, {errors} errors ===")
    print(f"    💰 sales: {type_counts['sale']} | 🎁 distributions: {type_counts['distribution']} | 🔄 transfers: {type_counts['transfer']}")

if __name__ == "__main__":
    main()