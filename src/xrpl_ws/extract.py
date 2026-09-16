"""
ストリームのメッセージを、既存テーブルに入れるレコードへ変換する。

この層は純粋関数だけで構成する。ネットワークもDBも触らない。
サンプルJSONを食わせればそのままテスト可能。

新しいトランザクション種別を足すときは
  1. extract_xxx() を書く
  2. HANDLERS に1行足す
の2つだけで済む。
"""

from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from .codec import (
    NFTokenIDDecoded,
    decode_nftoken_id,
    hex_to_text,
    ripple_time_to_iso,
)

# 変換結果。table にそのまま INSERT し、uri はキャッシュAPIへ回す。
Extracted = tuple[str, dict[str, Any], str | None]


class _Skip:
    """対象外。抽出に失敗したわけではない。

    None を返すと「NFTokenID を解決できなかった」と区別がつかず、
    正常な取りこぼしがエラーとして記録されてしまう。
    Payment のように大半が対象外になる種別では、ログが埋まる。
    """
    __slots__ = ()

    def __repr__(self):
        return "SKIP"


SKIP = _Skip()


# =============================================================================
# 共通のヘルパ
# =============================================================================

def unwrap(msg: dict) -> tuple[dict, dict]:
    """ストリームのメッセージから (transaction, meta) を取り出す。

    API v1 は "transaction"、v2 は "tx_json"。接続先で変わりうるので両対応。
    """
    tx = msg.get("transaction") or msg.get("tx_json") or {}
    meta = msg.get("meta") or msg.get("metaData") or {}
    return tx, meta


def tx_hash_of(msg: dict, tx: dict) -> str:
    return tx.get("hash") or msg.get("hash") or ""


def tx_date_of(msg: dict, tx: dict) -> str | None:
    """既存バッチと同じ ISO 形式に揃える。"""
    if tx.get("date") is not None:
        return ripple_time_to_iso(tx["date"])
    return msg.get("close_time_iso")


def memo_of(tx: dict) -> str | None:
    """UTF-8でデコードできる最初の MemoData を返す。

    v2 では raw の Memos 配列も別途保存するため、ここは検索・分類用の
    代表テキストとして使う。
    """
    for m in tx.get("Memos") or []:
        data = (m.get("Memo") or {}).get("MemoData")
        if not data:
            continue
        text = hex_to_text(data)
        if text is not None:
            # PostgreSQL text / jsonb は U+0000 を保持できない。
            # raw Memos は16進文字列のまま memos(JSONB) に残るため、
            # 検索・分類用の decoded memo だけ NUL を除去する。
            return text.replace("\x00", "")
    return None


def memos_of(tx: dict) -> list:
    """tx.Memos をそのまま JSONB へ保存できる形で返す。"""
    memos = tx.get("Memos")
    return memos if isinstance(memos, list) else []


def tx_index_of(meta: dict) -> int:
    """meta.TransactionIndex を必須値として取り出す。"""
    value = meta.get("TransactionIndex")
    if value is None:
        value = meta.get("transaction_index")
    if value is None:
        raise ValueError("metadata has no TransactionIndex")
    return int(value)


def fee_drops_of(tx: dict) -> int:
    value = tx.get("Fee")
    if value is None:
        raise ValueError("transaction has no Fee")
    return int(value)


def iter_page_nodes(meta: dict, *, include_deleted: bool = False):
    """AffectedNodes のうち NFTokenPage のものだけを返す。

    バーンでページの最後の1件が消えるとページごと削除されるので、
    その場合だけ DeletedNode も見る。ミントの差分計算では逆に
    邪魔になるため、既定では含めない。
    """
    for node in meta.get("AffectedNodes", []):
        item = node.get("CreatedNode") or node.get("ModifiedNode")
        if item is None and include_deleted:
            item = node.get("DeletedNode")
        if item and item.get("LedgerEntryType") == "NFTokenPage":
            yield item


def find_uri(
    meta: dict,
    nftoken_id: str,
    *,
    keys: tuple[str, ...] = ("NewFields", "FinalFields"),
    include_deleted: bool = False,
) -> str | None:
    """NFTokenPage 群から対象NFTのURIを探す。

    どのフィールドを見るかは操作によって違う。
      ミント/修正後  NewFields か FinalFields（処理後の状態）
      修正前         PreviousFields
      バーン         PreviousFields（消える前の状態）。ページごと
                     削除された場合は DeletedNode.FinalFields
    """
    for item in iter_page_nodes(meta, include_deleted=include_deleted):
        for key in keys:
            fields = item.get(key) or {}
            for token in fields.get("NFTokens", []):
                nft = token.get("NFToken", {})
                if nft.get("NFTokenID") == nftoken_id:
                    return nft.get("URI")
    return None


def diff_minted_id(meta: dict) -> str | None:
    """
    AffectedNodes の差分からミントされた NFTokenID を求める。

    meta.nftoken_id が無い場合の保険。ledger RPC 経由だと入らないので、
    バッチ側ではこちらが唯一の手段になる。

    要点が2つある。
      - 全ページを横断して差集合を取る。ノード単位で判定すると
        ページ分割時に既存NFTを拾ってしまう。
      - ページ分割では隣接ページのリンクだけが更新される。そのノードは
        NFTokens が変わらないので、数えると既存NFTが新規扱いになる。
    """
    final_ids, previous_ids = set(), set()

    for item in iter_page_nodes(meta):
        prev_tokens = item.get("PreviousFields", {}).get("NFTokens")
        is_created = "NewFields" in item

        if not is_created and prev_tokens is None:
            continue  # リンク更新のみ

        fields = item.get("NewFields") or item.get("FinalFields") or {}
        for t in fields.get("NFTokens", []):
            final_ids.add(t["NFToken"]["NFTokenID"])
        for t in (prev_tokens or []):
            previous_ids.add(t["NFToken"]["NFTokenID"])

    new_ids = final_ids - previous_ids
    return new_ids.pop() if len(new_ids) == 1 else None


def parse_amount(amount: Any) -> dict[str, Any]:
    """XRPL Currency Amount を v2 の共通5列へ正規化する。"""
    result = {
        "drops": None,
        "currency": None,
        "value": None,
        "issuer": None,
        "mpt_issuance_id": None,
    }

    if amount is None:
        return result

    if isinstance(amount, str):
        try:
            result["drops"] = int(amount)
        except ValueError as exc:
            raise ValueError(f"invalid XRP amount: {amount!r}") from exc
        return result

    if not isinstance(amount, dict):
        raise ValueError(f"unsupported amount type: {type(amount).__name__}")

    if amount.get("mpt_issuance_id") is not None:
        result["mpt_issuance_id"] = amount.get("mpt_issuance_id")
        result["value"] = amount.get("value")
        return result

    result["currency"] = amount.get("currency")
    result["value"] = amount.get("value")
    result["issuer"] = amount.get("issuer")
    return result


def amount_present(amount: dict) -> bool:
    return any(
        amount.get(k) is not None
        for k in ("drops", "currency", "value", "issuer", "mpt_issuance_id")
    )


def amount_kind(amount: dict) -> str | None:
    if amount.get("drops") is not None:
        return "xrp"
    if amount.get("mpt_issuance_id") is not None:
        return "mpt"
    if (
        amount.get("currency") is not None
        and amount.get("value") is not None
        and amount.get("issuer") is not None
    ):
        return "iou"
    return None


def amount_is_zero(amount: dict) -> bool:
    kind = amount_kind(amount)
    if kind == "xrp":
        return amount["drops"] == 0
    if kind in ("iou", "mpt"):
        try:
            return Decimal(str(amount["value"])) == 0
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"invalid token amount value: {amount.get('value')!r}") from exc
    raise ValueError(f"invalid/absent amount: {amount!r}")


def amount_columns(prefix: str, amount: dict) -> dict[str, Any]:
    return {
        f"{prefix}_drops": amount["drops"],
        f"{prefix}_currency": amount["currency"],
        f"{prefix}_value": amount["value"],
        f"{prefix}_issuer": amount["issuer"],
        f"{prefix}_mpt_issuance_id": amount["mpt_issuance_id"],
    }


def classify_sale(seller: str, consideration: dict, decoded: NFTokenIDDecoded) -> str:
    """買い手側 consideration を基準に sale / free transfer を分類する。"""
    if not amount_is_zero(consideration):
        return "sale"
    return "distribution" if seller == decoded.issuer else "transfer"


def _accountroot_xrp_deltas(meta: dict) -> dict[str, int]:
    """AccountRoot.Balance の変化量(final - previous)をdropsで返す。"""
    deltas: dict[str, int] = {}
    for wrapper in meta.get("AffectedNodes", []):
        node = wrapper.get("ModifiedNode")
        if not node or node.get("LedgerEntryType") != "AccountRoot":
            continue
        final = node.get("FinalFields") or {}
        previous = node.get("PreviousFields") or {}
        if "Balance" not in previous:
            continue
        account = final.get("Account")
        final_balance = final.get("Balance")
        previous_balance = previous.get("Balance")
        if account is None or final_balance is None or previous_balance is None:
            continue
        deltas[account] = int(final_balance) - int(previous_balance)
    return deltas


def _economic_xrp_delta(
    deltas: dict[str, int],
    account: str,
    tx_account: str,
    fee_drops: int,
) -> int:
    """network feeを除外したAccountのXRP増減。"""
    delta = deltas.get(account, 0)
    if account == tx_account:
        delta += fee_drops
    return delta


def _decimal_text(value: Decimal) -> str:
    """DecimalをXRPL Amount.value向けの非指数表記文字列へ戻す。"""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("", "-0") else text


def _iou_amount(currency: str, issuer: str, value: Decimal) -> dict[str, Any]:
    return parse_amount(
        {
            "currency": currency,
            "issuer": issuer,
            "value": _decimal_text(value),
        }
    )


def _merge_previous_fields(final_fields: dict, previous_fields: dict) -> dict:
    """ModifiedNodeのPreviousFieldsをFinalFieldsへ重ねて変更前snapshotを作る。"""
    before = dict(final_fields)
    for key, value in (previous_fields or {}).items():
        before[key] = value
    return before


def _ripplestate_balance(fields: dict) -> Decimal | None:
    balance = (fields or {}).get("Balance")
    if not isinstance(balance, dict):
        return None
    try:
        return Decimal(str(balance.get("value")))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _ripplestate_accounts(fields: dict) -> tuple[str | None, str | None, str | None]:
    low = (fields or {}).get("LowLimit") or {}
    high = (fields or {}).get("HighLimit") or {}
    balance = (fields or {}).get("Balance") or {}
    currency = balance.get("currency") or low.get("currency") or high.get("currency")
    return low.get("issuer"), high.get("issuer"), currency


def _iou_holder_deltas(
    meta: dict,
    *,
    currency: str,
    iou_issuer: str,
) -> tuple[dict[str, Decimal], Decimal, list[str]]:
    """RippleStateから対象IOUのholder実残高差分を復元する。

    RippleState.Balance は low account 視点。
      issuer == high -> low(holder) delta = delta_low
      issuer == low  -> high(holder) delta = -delta_low

    戻り値:
      holder_deltas:
        issuer以外の各accountについて actual token balance change
      issuer_effect:
        -sum(holder_deltas)。TransferRate / Quality / rounding等を含む
        発行残高全体へのsigned effect。これはrole payoutではない。
      errors:
        復元不能node等。空ならexact reconstruction可能。

    CreatedNodeはbefore balance=0として扱う。
    DeletedNodeは将来のedge caseを推測しないためerrorにして、
    settlementをunresolved/partial側へ落とす。
    """
    holder_deltas: dict[str, Decimal] = {}
    errors: list[str] = []

    for wrapper in meta.get("AffectedNodes") or []:
        node_kind = None
        node = None

        if "ModifiedNode" in wrapper:
            node_kind = "modified"
            node = wrapper["ModifiedNode"]
        elif "CreatedNode" in wrapper:
            node_kind = "created"
            node = wrapper["CreatedNode"]
        elif "DeletedNode" in wrapper:
            node_kind = "deleted"
            node = wrapper["DeletedNode"]

        if not node or node.get("LedgerEntryType") != "RippleState":
            continue

        if node_kind == "modified":
            after = node.get("FinalFields") or {}
            before = _merge_previous_fields(
                after,
                node.get("PreviousFields") or {},
            )
        elif node_kind == "created":
            after = node.get("NewFields") or {}
            before = dict(after)
            if isinstance(before.get("Balance"), dict):
                before["Balance"] = dict(before["Balance"])
                before["Balance"]["value"] = "0"
        else:
            # 今回の全履歴111件では0件。削除時のFinalFieldsだけから
            # settlement途中の最終balanceを決め打ちしない。
            final = node.get("FinalFields") or {}
            low, high, node_currency = _ripplestate_accounts(final)
            if node_currency == currency and iou_issuer in {low, high}:
                errors.append(
                    f"deleted RippleState not supported:{node.get('LedgerIndex')}"
                )
            continue

        low, high, node_currency = _ripplestate_accounts(after)
        if not low or not high:
            low2, high2, currency2 = _ripplestate_accounts(before)
            low = low or low2
            high = high or high2
            node_currency = node_currency or currency2

        if node_currency != currency or iou_issuer not in {low, high}:
            continue

        before_balance = _ripplestate_balance(before)
        after_balance = _ripplestate_balance(after)
        if before_balance is None or after_balance is None:
            errors.append(
                f"RippleState balance unavailable:{node.get('LedgerIndex')}"
            )
            continue

        delta_low = after_balance - before_balance

        if iou_issuer == high:
            holder = low
            holder_delta = delta_low
        elif iou_issuer == low:
            holder = high
            holder_delta = -delta_low
        else:
            continue

        if not holder:
            errors.append(
                f"RippleState holder unavailable:{node.get('LedgerIndex')}"
            )
            continue

        holder_deltas[holder] = holder_deltas.get(holder, Decimal(0)) + holder_delta

    issuer_effect = -sum(holder_deltas.values(), Decimal(0))
    return holder_deltas, issuer_effect, errors


def _sale_settlement_iou(
    *,
    meta: dict,
    seller: str,
    buyer: str,
    broker: str | None,
    nft_issuer: str,
    transfer_fee: int,
    accept_mode: str,
    consideration: dict,
) -> dict[str, Any]:
    """IOU建てNFTokenAcceptOfferをRippleState actual deltaから復元する。

    Offer Amountは「actual debit/credit」とは限らない。
    fungible-token TransferRate、trustline QualityIn/QualityOut、丸め等で
    buyer debit / seller creditがoffer額と異なることがあるため、
    exact settlementではnominal計算を使わずmetadataをSource of Truthにする。

    現在の全履歴111件で検証済みの形:
      - IOU issuerはseller/buyer/broker/NFT issuerの外部
      - role overlapは none または seller == NFT issuer
      - relevant RippleStateはすべてModifiedNode
      - buyer debit = seller + royalty + broker + issuer_effect が全111件で成立

    将来これを外れるrole overlapは推測せずunresolvedへ落とす。
    """
    currency = consideration.get("currency")
    iou_issuer = consideration.get("issuer")

    if not currency or not iou_issuer or consideration.get("value") is None:
        return _unresolved_sale_settlement("invalid IOU consideration")

    settlement_roles = {seller, buyer, broker, nft_issuer}
    settlement_roles.discard(None)

    # IOU issuer自身がsettlement roleならtrustline holder deltaだけでは
    # そのrole flowを直接観測できない。未検証なので推測しない。
    if iou_issuer in settlement_roles:
        return _unresolved_sale_settlement(
            "IOU issuer overlaps NFT settlement role"
        )

    royalty_applies = (
        transfer_fee != 0
        and seller != nft_issuer
        and buyer != nft_issuer
    )

    # 同一accountに複数logical rolesが重なるとaccount netから分離不能。
    # seller==NFT issuer / buyer==NFT issuer はroyalty自体が適用されないため安全。
    role_pairs = [
        ("seller", seller),
        ("buyer", buyer),
        ("broker", broker),
        ("nft_issuer", nft_issuer),
    ]
    ambiguous: list[str] = []
    for i, (name_a, account_a) in enumerate(role_pairs):
        if not account_a:
            continue
        for name_b, account_b in role_pairs[i + 1:]:
            if not account_b or account_a != account_b:
                continue
            pair = {name_a, name_b}
            if pair in (
                {"seller", "nft_issuer"},
                {"buyer", "nft_issuer"},
            ) and not royalty_applies:
                continue
            ambiguous.append(f"{name_a}={name_b}")

    if ambiguous:
        return _unresolved_sale_settlement(
            "ambiguous IOU settlement role overlap:" + ",".join(ambiguous)
        )

    deltas, issuer_effect, errors = _iou_holder_deltas(
        meta,
        currency=currency,
        iou_issuer=iou_issuer,
    )

    buyer_paid = -deltas.get(buyer, Decimal(0))
    seller_received = deltas.get(seller, Decimal(0))
    royalty_received = (
        deltas.get(nft_issuer, Decimal(0))
        if royalty_applies
        else Decimal(0)
    )
    broker_received = (
        deltas.get(broker, Decimal(0))
        if accept_mode == "brokered" and broker
        else Decimal(0)
    )

    flows = {
        "buyer_paid": buyer_paid,
        "seller_received": seller_received,
        "royalty_received": royalty_received,
        "broker_received": broker_received,
    }

    for name, value in flows.items():
        if value < 0:
            errors.append(f"{name}<0:{_decimal_text(value)}")

    # Metadata上にsettlement role以外のholder movementが混ざっていないことを
    # conservationで確認する。TransferRate等による差はissuer_effectへ入るので
    # offer nominal amountとの一致は要求しない。
    payout_total = (
        seller_received
        + royalty_received
        + broker_received
    )
    residual = buyer_paid - payout_total

    if residual != issuer_effect:
        errors.append(
            "IOU conservation mismatch:"
            f"{_decimal_text(residual)}!={_decimal_text(issuer_effect)}"
        )

    record: dict[str, Any] = {
        "settlement_status": "exact" if not errors else "partial",
        "settlement_error": "; ".join(errors) if errors else None,
    }

    for prefix, value in flows.items():
        if value >= 0:
            record.update(
                amount_columns(
                    prefix,
                    _iou_amount(currency, iou_issuer, value),
                )
            )
        else:
            record.update(amount_columns(prefix, parse_amount(None)))

    return record


def _sale_settlement_xrp(
    *,
    tx: dict,
    meta: dict,
    seller: str,
    buyer: str,
    broker: str | None,
    issuer: str,
    transfer_fee: int,
    accept_mode: str,
    consideration: dict,
    broker_fee: dict,
) -> dict[str, Any]:
    """XRP建てNFTokenAcceptOfferのactual logical flowsをmetaから復元する。

    通常は AccountRoot の実残高差分を使う。

    ただし seller == buyer の歴史的な自己マッチでは、同じAccountRootに
    「buyerとしての支払」と「sellerとしての受取」が相殺されて現れるため、
    net delta だけから2つのlogical flowを別々には復元できない。

    そのケースだけは:
      buyer_paid      = buy-side consideration
      broker_received = declared NFTokenBrokerFee
      royalty_received= issuerのactual delta（broker==issuerならbroker netから分離）
      seller_received = buyer_paid - broker_received - royalty_received
    と復元し、最後にroleごとのlogical flowを同一口座ごとに合算して
    AccountRootのactual net deltaと照合する。
    """
    fee_drops = fee_drops_of(tx)
    tx_account = tx.get("Account", "")
    deltas = _accountroot_xrp_deltas(meta)
    expected_buyer = consideration.get("drops")

    royalty_applies = transfer_fee != 0 and seller != issuer and buyer != issuer

    # ------------------------------------------------------------------
    # seller == buyer:
    # historical self-match / self-trade.
    #
    # 同一口座のnet deltaから buyer_paid / seller_received をそれぞれ
    # 取り出すことはできないので、offer上のconsiderationと実際の
    # broker/royalty受取からlogical flowを復元する。
    # ------------------------------------------------------------------
    if seller == buyer and expected_buyer is not None:
        errors: list[str] = []

        if accept_mode == "brokered":
            declared_broker = broker_fee.get("drops")
            if declared_broker is None:
                # broker fee omitted == logical 0
                if amount_present(broker_fee):
                    errors.append("broker_fee_not_xrp")
                declared_broker = 0

            broker_received = declared_broker

            if royalty_applies:
                if issuer == broker:
                    broker_net = _economic_xrp_delta(
                        deltas, broker or "", tx_account, fee_drops
                    )
                    royalty_received = broker_net - broker_received
                else:
                    royalty_received = _economic_xrp_delta(
                        deltas, issuer, tx_account, fee_drops
                    )
            else:
                royalty_received = 0
        else:
            broker_received = 0
            royalty_received = (
                _economic_xrp_delta(deltas, issuer, tx_account, fee_drops)
                if royalty_applies
                else 0
            )

        buyer_paid = expected_buyer
        seller_received = buyer_paid - broker_received - royalty_received

        flows = {
            "buyer_paid": buyer_paid,
            "seller_received": seller_received,
            "royalty_received": royalty_received,
            "broker_received": broker_received,
        }

        for name, value in flows.items():
            if value < 0:
                errors.append(f"{name}<0:{value}")

        payout_total = seller_received + royalty_received + broker_received
        if buyer_paid != payout_total:
            errors.append(f"flow_mismatch:{buyer_paid}!={payout_total}")

        # Role overlapを考慮して、logical flowを口座単位の期待netへ畳み込む。
        expected_net: dict[str, int] = {}

        def add_expected(account: str | None, amount: int) -> None:
            if not account:
                return
            expected_net[account] = expected_net.get(account, 0) + amount

        add_expected(buyer, -buyer_paid)
        add_expected(seller, seller_received)
        if royalty_applies:
            add_expected(issuer, royalty_received)
        if accept_mode == "brokered":
            add_expected(broker, broker_received)

        for account, expected_delta in expected_net.items():
            actual_delta = _economic_xrp_delta(
                deltas, account, tx_account, fee_drops
            )
            if actual_delta != expected_delta:
                errors.append(
                    f"role_net_mismatch:{account}:"
                    f"{actual_delta}!={expected_delta}"
                )

        record: dict[str, Any] = {
            "settlement_status": "exact" if not errors else "partial",
            "settlement_error": "; ".join(errors) if errors else None,
        }

        for prefix, value in flows.items():
            if value >= 0:
                record.update(amount_columns(prefix, parse_amount(str(value))))
            else:
                record.update(amount_columns(prefix, parse_amount(None)))
        return record

    # ------------------------------------------------------------------
    # Normal path: distinct seller / buyer.
    # Existing meta-first reconstruction is kept unchanged.
    # ------------------------------------------------------------------
    buyer_paid = -_economic_xrp_delta(deltas, buyer, tx_account, fee_drops)
    seller_received = _economic_xrp_delta(deltas, seller, tx_account, fee_drops)

    if accept_mode == "brokered":
        broker_net = _economic_xrp_delta(deltas, broker or "", tx_account, fee_drops)
        declared_broker = broker_fee.get("drops") or 0
        if issuer == broker and royalty_applies:
            broker_received = declared_broker
            royalty_received = broker_net - broker_received
        else:
            broker_received = broker_net
            royalty_received = (
                _economic_xrp_delta(deltas, issuer, tx_account, fee_drops)
                if royalty_applies
                else 0
            )
    else:
        broker_received = 0
        royalty_received = (
            _economic_xrp_delta(deltas, issuer, tx_account, fee_drops)
            if royalty_applies
            else 0
        )

    errors: list[str] = []
    flows = {
        "buyer_paid": buyer_paid,
        "seller_received": seller_received,
        "royalty_received": royalty_received,
        "broker_received": broker_received,
    }
    for name, value in flows.items():
        if value < 0:
            errors.append(f"{name}<0:{value}")

    if expected_buyer is None:
        errors.append("consideration_not_xrp")
    elif buyer_paid != expected_buyer:
        errors.append(f"buyer_paid_mismatch:{buyer_paid}!={expected_buyer}")

    payout_total = seller_received + royalty_received + broker_received
    if buyer_paid != payout_total:
        errors.append(f"flow_mismatch:{buyer_paid}!={payout_total}")

    record: dict[str, Any] = {
        "settlement_status": "exact" if not errors else "partial",
        "settlement_error": "; ".join(errors) if errors else None,
    }

    # partialでもmetaから観測できた値は残す。負値だけ0へ丸めず、
    # shape制約を壊す値はNULLにしてエラー情報を残す。
    for prefix, value in flows.items():
        if value >= 0:
            record.update(amount_columns(prefix, parse_amount(str(value))))
        else:
            record.update(amount_columns(prefix, parse_amount(None)))
    return record


def _unresolved_sale_settlement(reason: str) -> dict[str, Any]:
    record: dict[str, Any] = {
        "settlement_status": "unresolved",
        "settlement_error": reason,
    }
    absent = parse_amount(None)
    for prefix in (
        "buyer_paid",
        "seller_received",
        "royalty_received",
        "broker_received",
    ):
        record.update(amount_columns(prefix, absent))
    return record


# =============================================================================
# NFTokenMint
# =============================================================================

def extract_mint(msg: dict) -> Extracted | None:
    tx, meta = unwrap(msg)

    # ストリームには meta.nftoken_id が入る。無ければ差分計算に落とす。
    nftoken_id = meta.get("nftoken_id") or diff_minted_id(meta)
    if not nftoken_id:
        return None

    decoded = decode_nftoken_id(nftoken_id)
    uri = tx.get("URI") or find_uri(meta, nftoken_id)

    # XLS-52: Amount を付けるとミントと同時に売りオファーが作られる。
    # Destination があればその口座だけが受け入れられる = 一次販売の相手。
    # NFT 自体は Account が保有するので owner は Account のままで正しい。
    # 実測で 93.6% のミントに Amount が入っていた。
    offer = parse_amount(tx.get("Amount"))

    record = {
        "tx_hash": tx_hash_of(msg, tx),
        "nftoken_id": nftoken_id,
        "ledger_index": msg.get("ledger_index"),
        "tx_date": tx_date_of(msg, tx),
        "issuer": decoded.issuer,
        "owner": tx.get("Account", ""),
        "taxon": decoded.taxon,
        "sequence": decoded.sequence,
        "uri": uri,

        # ミント同時オファー（XLS-52）
        "destination": tx.get("Destination"),
        "offer_amount_drops": offer["drops"],
        "offer_amount_currency": offer["currency"],
        "offer_amount_value": offer["value"],
        "offer_amount_issuer": offer["issuer"],

        # NFTokenID にエンコードされているが従来は捨てていた。
        # 実測でプロジェクトごとに 0〜10% とばらつく
        "transfer_fee": decoded.transfer_fee,

        # プラットフォーム識別。Issuer の有無では判定できない
        # （発行者が xrp.cafe の画面から自前ミントするケースがある）
        "source_tag": tx.get("SourceTag"),
        "memo": memo_of(tx),
    }
    return "nft_mint_history", record, uri


# =============================================================================
# NFTokenModify
# =============================================================================

def extract_modify(msg: dict) -> Extracted | None:
    tx, meta = unwrap(msg)

    # Modify は対象IDがトランザクション本体にある。推測が要らない。
    nftoken_id = tx.get("NFTokenID")
    if not nftoken_id:
        return None

    decoded = decode_nftoken_id(nftoken_id)
    current_uri = find_uri(meta, nftoken_id)
    previous_uri = find_uri(meta, nftoken_id, keys=("PreviousFields",))

    record = {
        "tx_hash": tx_hash_of(msg, tx),
        "nftoken_id": nftoken_id,
        "ledger_index": msg.get("ledger_index"),
        "tx_date": tx_date_of(msg, tx),
        "issuer": decoded.issuer,
        "owner": tx.get("Owner") or tx.get("Account", ""),
        "taxon": decoded.taxon,
        "sequence": decoded.sequence,
        "previous_uri": previous_uri,
        "current_uri": current_uri,
    }
    return "nft_modify_history", record, current_uri


# =============================================================================
# NFTokenAcceptOffer -> nft_sale_history_v2
# =============================================================================

def _collect_offers(meta: dict) -> tuple[dict | None, dict | None, str | None]:
    """削除されたNFTokenOfferから実際にAcceptされたsell/buy offerを拾う。"""
    sell_offer = buy_offer = None
    nftoken_id = None

    for node in meta.get("AffectedNodes", []):
        deleted = node.get("DeletedNode", {})
        if deleted.get("LedgerEntryType") != "NFTokenOffer":
            continue

        fields = deleted.get("FinalFields", {})
        offer = {
            "id": deleted.get("LedgerIndex"),
            "owner": fields.get("Owner", ""),
            "amount": fields.get("Amount"),
            "destination": fields.get("Destination"),
            "flags": fields.get("Flags", 0),
        }
        nftoken_id = nftoken_id or fields.get("NFTokenID")

        if fields.get("Flags", 0) & 1:  # lsfSellNFToken
            sell_offer = offer
        else:
            buy_offer = offer

    return sell_offer, buy_offer, nftoken_id


def extract_sale(msg: dict) -> Extracted | None:
    tx, meta = unwrap(msg)
    account = tx.get("Account", "")

    sell_offer, buy_offer, offer_nft_id = _collect_offers(meta)
    nftoken_id = meta.get("nftoken_id") or offer_nft_id
    if not nftoken_id:
        return None
    if not sell_offer and not buy_offer:
        return None

    sell_amount = parse_amount(sell_offer["amount"]) if sell_offer else parse_amount(None)
    buy_amount = parse_amount(buy_offer["amount"]) if buy_offer else parse_amount(None)

    if sell_offer and buy_offer:
        accept_mode = "brokered"
        seller = sell_offer["owner"]
        buyer = buy_offer["owner"]
        broker = account
        consideration = buy_amount
    elif sell_offer:
        accept_mode = "direct_sell"
        seller = sell_offer["owner"]
        buyer = account
        broker = None
        consideration = sell_amount
    else:
        accept_mode = "direct_buy"
        seller = account
        buyer = buy_offer["owner"]
        broker = None
        consideration = buy_amount

    decoded = decode_nftoken_id(nftoken_id)
    uri = find_uri(meta, nftoken_id)
    broker_fee = parse_amount(tx.get("NFTokenBrokerFee"))

    record: dict[str, Any] = {
        "tx_hash": tx_hash_of(msg, tx),
        "ledger_index": int(msg.get("ledger_index")),
        "tx_index": tx_index_of(meta),
        "tx_date": tx_date_of(msg, tx),
        "tx_result": meta.get("TransactionResult"),
        "tx_account": account,
        "tx_sequence": tx.get("Sequence"),
        "ticket_sequence": tx.get("TicketSequence"),
        "fee_drops": fee_drops_of(tx),
        "tx_flags": int(tx.get("Flags") or 0),

        "nftoken_id": nftoken_id,
        "issuer": decoded.issuer,
        "taxon": decoded.taxon,
        "nft_sequence": decoded.sequence,
        "nft_flags": decoded.flags,
        "transfer_fee": decoded.transfer_fee,
        "uri": uri,

        "seller": seller,
        "buyer": buyer,
        "broker": broker,
        "accept_mode": accept_mode,
        "sale_type": classify_sale(seller, consideration, decoded),

        "sell_offer_id": sell_offer["id"] if sell_offer else None,
        "sell_offer_destination": sell_offer["destination"] if sell_offer else None,
        "buy_offer_id": buy_offer["id"] if buy_offer else None,
        "buy_offer_destination": buy_offer["destination"] if buy_offer else None,

        "source_tag": tx.get("SourceTag"),
        "memo": memo_of(tx),
        "memos": memos_of(tx),
        "parser_version": 4,
    }

    record.update(amount_columns("sell_offer_amount", sell_amount))
    record.update(amount_columns("buy_offer_amount", buy_amount))
    record.update(amount_columns("broker_fee", broker_fee))

    consideration_kind = amount_kind(consideration)

    if consideration_kind == "xrp":
        record.update(
            _sale_settlement_xrp(
                tx=tx,
                meta=meta,
                seller=seller,
                buyer=buyer,
                broker=broker,
                issuer=decoded.issuer,
                transfer_fee=decoded.transfer_fee,
                accept_mode=accept_mode,
                consideration=consideration,
                broker_fee=broker_fee,
            )
        )
    elif consideration_kind == "iou":
        record.update(
            _sale_settlement_iou(
                meta=meta,
                seller=seller,
                buyer=buyer,
                broker=broker,
                nft_issuer=decoded.issuer,
                transfer_fee=decoded.transfer_fee,
                accept_mode=accept_mode,
                consideration=consideration,
            )
        )
    else:
        record.update(
            _unresolved_sale_settlement(
                f"realtime settlement parser does not yet support "
                f"{consideration_kind or 'unknown'} NFT consideration"
            )
        )

    return "nft_sale_history_v2", record, uri


# =============================================================================
# NFTokenBurn
# =============================================================================

def extract_burn(msg: dict) -> Extracted | None:
    tx, meta = unwrap(msg)

    # Burn も対象IDがトランザクション本体にある。10件のサンプルすべてで
    # 入っていたので推測は要らない。
    nftoken_id = tx.get("NFTokenID")
    if not nftoken_id:
        return None

    decoded = decode_nftoken_id(nftoken_id)

    # 焼かれた後の FinalFields には残っていないので、消える前を見る。
    # ページごと削除されるケース（最後の1件を焼いた場合）に備えて
    # DeletedNode も対象にする。
    uri = find_uri(
        meta,
        nftoken_id,
        keys=("PreviousFields", "FinalFields"),
        include_deleted=True,
    )

    record = {
        "tx_hash": tx_hash_of(msg, tx),
        "nftoken_id": nftoken_id,
        "ledger_index": msg.get("ledger_index"),
        "tx_date": tx_date_of(msg, tx),
        "issuer": decoded.issuer,
        # 発行者が他人のNFTを焼く場合は Owner に所有者が入る。
        # 実測で10件中9件が委任バーンだった。
        "owner": tx.get("Owner") or tx.get("Account", ""),
        "taxon": decoded.taxon,
        "sequence": decoded.sequence,
        "uri": uri,
    }
    return "nft_burn_history", record, None


# =============================================================================
# Payment -> nft_payment_v2
# =============================================================================

WATCHED: set[str] = set()


def set_watched(addresses) -> None:
    """監視対象を差し替える。起動時と定期更新で呼ぶ。"""
    WATCHED.clear()
    WATCHED.update(addresses)


def _payment_sender_spent_xrp(tx: dict, meta: dict, account: str) -> dict:
    """Payment senderのXRP実支出をnetwork fee除外で復元する。"""
    fee_drops = fee_drops_of(tx)
    deltas = _accountroot_xrp_deltas(meta)
    economic_delta = _economic_xrp_delta(deltas, account, account, fee_drops)
    spent = -economic_delta
    if spent < 0:
        raise ValueError(f"payment sender XRP spend became negative: {spent}")
    return parse_amount(str(spent))


def extract_payment(msg: dict) -> Extracted | _Skip | None:
    tx, meta = unwrap(msg)

    account = tx.get("Account", "")
    destination = tx.get("Destination", "")
    if not WATCHED or not (account in WATCHED or destination in WATCHED):
        return SKIP

    # API v2ではDeliverMax、v1ではAmount。
    requested_raw = tx.get("DeliverMax") if "DeliverMax" in tx else tx.get("Amount")
    deliver_max = parse_amount(requested_raw)
    if not amount_present(deliver_max):
        raise ValueError("successful Payment has no Amount/DeliverMax")

    send_max = parse_amount(tx.get("SendMax"))
    deliver_min = parse_amount(tx.get("DeliverMin"))

    # 実着金をAmount/DeliverMaxへfallbackしない。
    delivered_raw = meta.get("delivered_amount")
    if delivered_raw is None:
        delivered_raw = meta.get("DeliveredAmount")

    delivered_available = delivered_raw not in (None, "unavailable")
    delivered = parse_amount(delivered_raw) if delivered_available else parse_amount(None)
    delivered_source = "meta" if delivered_available else "unavailable"

    # SendMaxがあればsource asset、無ければDeliverMaxと同じasset。
    source_asset = send_max if amount_present(send_max) else deliver_max

    sender_spent = parse_amount(None)
    sender_spent_known = False
    sender_error = None

    # 初版はAccountRootから安全に復元できるXRP sourceのみ対応。
    if amount_kind(source_asset) == "xrp":
        try:
            sender_spent = _payment_sender_spent_xrp(tx, meta, account)
            sender_spent_known = True
        except Exception as exc:
            sender_error = str(exc)

    if delivered_available and sender_spent_known:
        settlement_status = "exact"
        settlement_error = None

        # direct XRP -> XRPならfee除外後に一致するはず。
        if amount_kind(delivered) == "xrp":
            if sender_spent["drops"] != delivered["drops"]:
                settlement_status = "partial"
                settlement_error = (
                    f"xrp payment mismatch: sender_spent={sender_spent['drops']} "
                    f"delivered={delivered['drops']}"
                )

    elif delivered_available or sender_spent_known:
        settlement_status = "partial"
        settlement_error = sender_error
        if settlement_error is None and not sender_spent_known:
            settlement_error = (
                f"realtime sender-spend parser does not yet support "
                f"{amount_kind(source_asset) or 'unknown'} source asset"
            )
    else:
        settlement_status = "unresolved"
        settlement_error = sender_error or "delivered_amount unavailable"

    record: dict[str, Any] = {
        "tx_hash": tx_hash_of(msg, tx),
        "ledger_index": int(msg.get("ledger_index")),
        "tx_index": tx_index_of(meta),
        "tx_date": tx_date_of(msg, tx),
        "tx_result": meta.get("TransactionResult"),

        "account": account,
        "destination": destination,
        "tx_sequence": tx.get("Sequence"),
        "ticket_sequence": tx.get("TicketSequence"),
        "fee_drops": fee_drops_of(tx),
        "tx_flags": int(tx.get("Flags") or 0),

        "destination_tag": tx.get("DestinationTag"),
        "source_tag": tx.get("SourceTag"),
        "invoice_id": tx.get("InvoiceID"),
        "delivered_source": delivered_source,

        "memo": memo_of(tx),
        "memos": memos_of(tx),
        "settlement_status": settlement_status,
        "settlement_error": settlement_error,
        "parser_version": 4,
    }

    record.update(amount_columns("deliver_max", deliver_max))
    record.update(amount_columns("send_max", send_max))
    record.update(amount_columns("deliver_min", deliver_min))
    record.update(amount_columns("delivered", delivered))
    record.update(amount_columns("sender_spent", sender_spent))

    return "nft_payment_v2", record, None


# =============================================================================
# 登録表
# =============================================================================

HANDLERS: dict[str, Callable[[dict], Extracted | None]] = {
    "NFTokenMint": extract_mint,
    "NFTokenModify": extract_modify,
    "NFTokenAcceptOffer": extract_sale,
    "NFTokenBurn": extract_burn,
    "Payment": extract_payment,
}
