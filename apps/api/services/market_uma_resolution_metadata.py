from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _clean_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def upsert_market_uma_resolution_metadata(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = _json_safe(payload or {})

    market_id = _clean_str(payload.get("market_id"))
    if not market_id:
        raise ValueError("payload missing market_id")

    disputed = bool(payload.get("disputed")) or bool(payload.get("dispute_transaction")) or bool(payload.get("disputed_time"))
    settled = bool(payload.get("settled")) or bool(payload.get("settlement_transaction")) or bool(payload.get("settled_time"))

    q = """
    INSERT INTO public.market_uma_resolution_metadata (
        market_id,
        oracle_family,
        oracle_type,
        oracle_contract,
        identifier,
        umip,
        requester,
        request_transaction,
        proposer,
        proposal_transaction,
        disputer,
        dispute_transaction,
        settlement_recipient,
        settlement_transaction,
        requested_time,
        proposed_time,
        disputed_time,
        settled_time,
        title,
        description,
        additional_text_data,
        bulletin_board_text,
        res_data,
        initializer,
        chain,
        expiry_type,
        disputed,
        settled,
        outcome_proposed,
        outcome_settled,
        raw_payload_json,
        last_seen_at,
        updated_at
    )
    VALUES (
        %(market_id)s,
        'uma_oo',
        %(oracle_type)s,
        %(oracle_contract)s,
        %(identifier)s,
        %(umip)s,
        %(requester)s,
        %(request_transaction)s,
        %(proposer)s,
        %(proposal_transaction)s,
        %(disputer)s,
        %(dispute_transaction)s,
        %(settlement_recipient)s,
        %(settlement_transaction)s,
        %(requested_time)s,
        %(proposed_time)s,
        %(disputed_time)s,
        %(settled_time)s,
        %(title)s,
        %(description)s,
        %(additional_text_data)s,
        %(bulletin_board_text)s,
        %(res_data)s,
        %(initializer)s,
        %(chain)s,
        %(expiry_type)s,
        %(disputed)s,
        %(settled)s,
        %(outcome_proposed)s,
        %(outcome_settled)s,
        %(raw_payload_json)s,
        NOW(),
        NOW()
    )
    ON CONFLICT (market_id)
    DO UPDATE SET
        oracle_type = EXCLUDED.oracle_type,
        oracle_contract = EXCLUDED.oracle_contract,
        identifier = EXCLUDED.identifier,
        umip = EXCLUDED.umip,
        requester = EXCLUDED.requester,
        request_transaction = EXCLUDED.request_transaction,
        proposer = EXCLUDED.proposer,
        proposal_transaction = EXCLUDED.proposal_transaction,
        disputer = EXCLUDED.disputer,
        dispute_transaction = EXCLUDED.dispute_transaction,
        settlement_recipient = EXCLUDED.settlement_recipient,
        settlement_transaction = EXCLUDED.settlement_transaction,
        requested_time = EXCLUDED.requested_time,
        proposed_time = EXCLUDED.proposed_time,
        disputed_time = EXCLUDED.disputed_time,
        settled_time = EXCLUDED.settled_time,
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        additional_text_data = EXCLUDED.additional_text_data,
        bulletin_board_text = EXCLUDED.bulletin_board_text,
        res_data = EXCLUDED.res_data,
        initializer = EXCLUDED.initializer,
        chain = EXCLUDED.chain,
        expiry_type = EXCLUDED.expiry_type,
        disputed = EXCLUDED.disputed,
        settled = EXCLUDED.settled,
        outcome_proposed = EXCLUDED.outcome_proposed,
        outcome_settled = EXCLUDED.outcome_settled,
        raw_payload_json = EXCLUDED.raw_payload_json,
        last_seen_at = NOW(),
        updated_at = NOW()
    RETURNING id, market_id, disputed, settled, updated_at;
    """

    params = {
        "market_id": market_id,
        "oracle_type": _clean_str(payload.get("oracle_type")),
        "oracle_contract": _clean_str(payload.get("oracle_contract")),
        "identifier": _clean_str(payload.get("identifier")),
        "umip": _clean_str(payload.get("umip")),
        "requester": _clean_str(payload.get("requester")),
        "request_transaction": _clean_str(payload.get("request_transaction")),
        "proposer": _clean_str(payload.get("proposer")),
        "proposal_transaction": _clean_str(payload.get("proposal_transaction")),
        "disputer": _clean_str(payload.get("disputer")),
        "dispute_transaction": _clean_str(payload.get("dispute_transaction")),
        "settlement_recipient": _clean_str(payload.get("settlement_recipient")),
        "settlement_transaction": _clean_str(payload.get("settlement_transaction")),
        "requested_time": payload.get("requested_time"),
        "proposed_time": payload.get("proposed_time"),
        "disputed_time": payload.get("disputed_time"),
        "settled_time": payload.get("settled_time"),
        "title": _clean_str(payload.get("title")),
        "description": _clean_str(payload.get("description")),
        "additional_text_data": _clean_str(payload.get("additional_text_data")),
        "bulletin_board_text": _clean_str(payload.get("bulletin_board_text")),
        "res_data": _clean_str(payload.get("res_data")),
        "initializer": _clean_str(payload.get("initializer")),
        "chain": _clean_str(payload.get("chain")),
        "expiry_type": _clean_str(payload.get("expiry_type")),
        "disputed": disputed,
        "settled": settled,
        "outcome_proposed": _clean_str(payload.get("outcome_proposed")),
        "outcome_settled": _clean_str(payload.get("outcome_settled")),
        "raw_payload_json": Json(payload),
    }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            row = cur.fetchone()
            conn.commit()

    return {
        "status": "ok",
        "id": row[0],
        "market_id": row[1],
        "disputed": row[2],
        "settled": row[3],
        "updated_at": row[4].isoformat() if row and row[4] else None,
    }