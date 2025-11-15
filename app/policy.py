from __future__ import annotations

import datetime
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from database import get_active_policy


UTC = datetime.timezone.utc
WEEKDAY_TOKENS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def load_active_policy(conn) -> Dict:
    """Return the active policy payload as a dict."""
    if conn is None:
        return {}
    if callable(conn):
        with conn() as session:
            policy = get_active_policy(session)
            return policy.params_dict() if policy else {}
    policy = get_active_policy(conn)
    return policy.params_dict() if policy else {}


def role_catalog(policy: Dict) -> Set[str]:
    roles = policy.get("roles") if isinstance(policy, dict) else {}
    if not isinstance(roles, dict):
        return set()
    return {name for name in roles.keys()}


def role_definition(policy: Dict, role: str) -> Dict:
    roles = policy.get("roles") if isinstance(policy, dict) else {}
    if not isinstance(roles, dict):
        return {}
    details = roles.get(role) or {}
    return details if isinstance(details, dict) else {}


def hourly_wage(policy: Dict, role: str, default: float = 0.0) -> float:
    details = role_definition(policy, role)
    value = details.get("hourly_wage") if isinstance(details, dict) else None
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def weekday_token(date_: datetime.date) -> str:
    return WEEKDAY_TOKENS[date_.weekday()]


def parse_time_label(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    label = value.strip()
    if not label:
        return None
    if label.startswith("@"):
        return None
    if ":" not in label:
        return None
    hour_str, minute_str = label.split(":", 1)
    try:
        hours = int(hour_str)
        minutes = int(minute_str)
    except ValueError:
        return None
    total_minutes = max(0, hours) * 60 + max(0, minutes)
    return total_minutes


def minutes_to_datetime(date_: datetime.date, minutes: int) -> datetime.datetime:
    day_offset = minutes // (24 * 60)
    minute_offset = minutes % (24 * 60)
    base = datetime.datetime.combine(date_, datetime.time.min, tzinfo=UTC)
    return base + datetime.timedelta(days=day_offset, minutes=minute_offset)


def _hours_entry(policy: Dict, date_: datetime.date) -> Dict[str, str]:
    hours_cfg = policy.get("business_hours") or {}
    weekday = weekday_token(date_)
    entry = hours_cfg.get(weekday) or hours_cfg.get(weekday.lower()) or hours_cfg.get(weekday.capitalize())
    return entry if isinstance(entry, dict) else {}


def open_minutes(policy: Dict, date_: datetime.date) -> int:
    entry = _hours_entry(policy, date_)
    label = entry.get("open") if entry else None
    parsed = parse_time_label(label)
    if parsed is not None:
        return parsed
    return parse_time_label("10:00") or 10 * 60


def close_minutes(policy: Dict, date_: datetime.date) -> int:
    entry = _hours_entry(policy, date_)
    label = entry.get("close") if entry else None
    parsed = parse_time_label(label)
    if parsed is not None:
        return parsed
    timeblocks = policy.get("timeblocks") or {}
    close_spec = timeblocks.get("close", {})
    weekday = weekday_token(date_)
    by_weekday = close_spec.get("byWeekdayEnd") or {}
    value = (
        by_weekday.get(weekday)
        or by_weekday.get(weekday.lower())
        or by_weekday.get(weekday.capitalize())
        or close_spec.get("end")
        or "24:00"
    )
    parsed = parse_time_label(value)
    return parsed if parsed is not None else 24 * 60


ANCHOR_PATTERN = re.compile(r"^@(?P<anchor>open|close)(?P<offset>[+-]\d+)?$", re.IGNORECASE)


def _parse_time_expression(
    policy: Dict,
    date_: datetime.date,
    label: Optional[str],
    *,
    close_min: int,
) -> Optional[int]:
    if label is None:
        return None
    raw = label.strip()
    if not raw:
        return None
    match = ANCHOR_PATTERN.match(raw)
    if match:
        anchor = match.group("anchor").lower()
        offset_raw = match.group("offset")
        offset = int(offset_raw) if offset_raw else 0
        if anchor == "open":
            base = open_minutes(policy, date_)
        else:
            base = close_min
        return base + offset
    return parse_time_label(raw)


def _resolve_block_window(
    policy: Dict,
    date_: datetime.date,
    block_spec: Dict[str, Any],
    *,
    close_min: int,
) -> Optional[Tuple[datetime.datetime, datetime.datetime]]:
    if not isinstance(block_spec, dict):
        return None
    weekday = weekday_token(date_)
    start_label = block_spec.get("start")
    end_label = block_spec.get("end")
    if not end_label and isinstance(block_spec.get("byWeekdayEnd"), dict):
        options = block_spec["byWeekdayEnd"]
        end_label = options.get(weekday) or options.get(weekday.lower()) or options.get(weekday.capitalize())
    start_minutes = _parse_time_expression(policy, date_, start_label, close_min=close_min)
    if start_minutes is None:
        start_minutes = parse_time_label("09:00") or 0
    end_minutes = _parse_time_expression(policy, date_, end_label, close_min=close_min)
    if end_minutes is None:
        end_minutes = close_min
    if end_minutes <= start_minutes:
        end_minutes = max(start_minutes + 60, end_minutes)
    start_dt = minutes_to_datetime(date_, start_minutes)
    end_dt = minutes_to_datetime(date_, end_minutes)
    return (start_dt, end_dt)


def resolve_policy_block(
    policy: Dict,
    block_name: str,
    date_: datetime.date,
    *,
    close_min: Optional[int] = None,
) -> Optional[Tuple[str, datetime.datetime, datetime.datetime]]:
    timeblocks = policy.get("timeblocks") or {}
    block_spec = timeblocks.get(block_name)
    if not isinstance(block_spec, dict):
        return None
    close_value = close_min if close_min is not None else close_minutes(policy, date_)
    window = _resolve_block_window(policy, date_, block_spec, close_min=close_value)
    if not window:
        return None
    return (block_name, window[0], window[1])


def resolve_role_blocks(policy: Dict, role_cfg: Dict, date_: datetime.date) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    if not isinstance(role_cfg, dict):
        return blocks
    block_specs = role_cfg.get("blocks") or []
    if not isinstance(block_specs, list):
        return blocks
    close_min = close_minutes(policy, date_)
    for entry in block_specs:
        if isinstance(entry, str):
            resolved = resolve_policy_block(policy, entry, date_, close_min=close_min)
            if not resolved:
                continue
            name, start_dt, end_dt = resolved
            blocks.append({"name": name, "start": start_dt, "end": end_dt})
            continue
        if isinstance(entry, dict):
            name = entry.get("name") or entry.get("label") or entry.get("id") or "block"
            window = _resolve_block_window(date_, entry, close_min=close_min)
            if not window:
                continue
            blocks.append({"name": name, "start": window[0], "end": window[1]})
    return blocks


def shift_length_rule(role_cfg: Dict) -> Dict[str, Any]:
    if not isinstance(role_cfg, dict):
        return {}
    rule = role_cfg.get("shift_length_rule") or {}
    return rule if isinstance(rule, dict) else {}
