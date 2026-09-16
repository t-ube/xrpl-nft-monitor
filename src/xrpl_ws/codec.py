"""XRPL のデータ形式を Python の値に直す。外部I/Oは持たない。"""

import binascii
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from xrpl.core import addresscodec

MASK32 = 0xFFFFFFFF
RIPPLE_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

CACHEABLE_SCHEMES = ("ipfs://", "ar://", "http://", "https://", "data:")


@dataclass(frozen=True)
class NFTokenIDDecoded:
    nftoken_id: str
    flags: int
    transfer_fee: int
    issuer: str
    taxon: int
    sequence: int


def decode_nftoken_id(nft_id_hex: str) -> NFTokenIDDecoded:
    """NFTokenID(32バイト)から issuer / taxon / sequence を取り出す。"""
    s = nft_id_hex.strip().upper()
    if s.startswith("0X"):
        s = s[2:]

    b = binascii.unhexlify(s)
    if len(b) != 32:
        raise ValueError("NFTokenID must be exactly 32 bytes")

    flags = (b[0] << 8) | b[1]
    transfer_fee = (b[2] << 8) | b[3]
    issuer = addresscodec.encode_classic_address(b[4:24])

    scrambled_taxon = int.from_bytes(b[24:28], "big")
    sequence = int.from_bytes(b[28:32], "big")

    # taxon は sequence をシードにスクランブルされている
    scramble = (384160001 * sequence + 2459) & MASK32
    taxon = (scrambled_taxon ^ scramble) & MASK32

    return NFTokenIDDecoded(
        nftoken_id=s,
        flags=flags,
        transfer_fee=transfer_fee,
        issuer=issuer,
        taxon=taxon,
        sequence=sequence,
    )


def ripple_time_to_iso(ripple_time: int) -> str:
    """Ripple time(2000-01-01基点の秒)を ISO8601 に。既存バッチと同じ形式。"""
    return (RIPPLE_EPOCH + timedelta(seconds=ripple_time)).isoformat()


def hex_to_text(hex_str: str) -> str | None:
    """16進のURIを文字列に。壊れていれば None。"""
    if not hex_str:
        return None
    try:
        return bytes.fromhex(hex_str).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None


def is_cacheable_uri(hex_uri: str) -> bool:
    """キャッシュAPIに投げる価値があるURIか。"""
    text = hex_to_text((hex_uri or "").strip())
    if text is None:
        return False

    text = text.strip()
    if text.startswith("{"):  # オンチェーン埋め込みJSON
        return True
    return text.lower().startswith(CACHEABLE_SCHEMES)
