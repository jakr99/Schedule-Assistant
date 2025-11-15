from __future__ import annotations

import copy
from typing import Any, Dict, List


def _block_config(
    base: int,
    *,
    min_staff: int | None = None,
    max_staff: int | None = None,
    per_sales: float = 0.0,
    per_modifier: float = 0.0,
) -> Dict[str, float | int]:
    minimum = 0 if min_staff is None else min_staff
    maximum = max(base, minimum) if max_staff is None else max_staff
    normalized_per_sales = max(0.0, min(per_sales, 0.05))
    normalized_per_modifier = max(0.0, min(per_modifier, 0.5))
    return {
        "base": max(0, base),
        "min": max(0, minimum),
        "max": max(0, maximum),
        "per_1000_sales": normalized_per_sales,
        "per_modifier": normalized_per_modifier,
    }


def _role_config(
    *,
    wage: float,
    priority: float,
    max_weekly: int,
    blocks: Dict[str, Dict[str, float | int]],
    daily_boost: Dict[str, int] | None = None,
    enabled: bool = True,
    thresholds: List[Dict[str, float | int | str]] | None = None,
) -> Dict[str, Any]:
    return {
        "enabled": enabled,
        "hourly_wage": wage,
        "priority": priority,
        "max_weekly_hours": max_weekly,
        "daily_boost": daily_boost or {},
        "shift_length_rule": {"minHrs": 5, "maxHrs": 8, "preferBlocks": True},
        "thresholds": thresholds or [],
        "blocks": blocks,
    }


DEFAULT_TIMEBLOCKS: Dict[str, Dict[str, str]] = {
    "Open": {"start": "@open-30", "end": "@open"},
    "Mid": {"start": "@open", "end": "16:00"},
    "PM": {"start": "16:00", "end": "@close"},
    "Close": {"start": "16:00", "end": "@close+35"},
}

BUSINESS_HOURS: Dict[str, Dict[str, str]] = {
    "Mon": {"open": "11:00", "close": "24:00"},
    "Tue": {"open": "11:00", "close": "24:00"},
    "Wed": {"open": "11:00", "close": "24:00"},
    "Thu": {"open": "11:00", "close": "24:00"},
    "Fri": {"open": "11:00", "close": "25:00"},
    "Sat": {"open": "11:00", "close": "25:00"},
    "Sun": {"open": "11:00", "close": "23:00"},
}


ROLES: Dict[str, Dict[str, Any]] = {
    "Server - Dining": _role_config(
        wage=6.25,
        priority=1.0,
        max_weekly=38,
        daily_boost={"Fri": 1, "Sat": 1, "Sun": -3},
        thresholds=[
            {"metric": "demand_index", "gte": 0.65, "add": 1},
            {"metric": "demand_index", "gte": 1.0, "add": 1},
        ],
        blocks={
            "Open": _block_config(2, max_staff=3, per_sales=0.2, per_modifier=0.4),
            "Mid": _block_config(2, max_staff=4, per_sales=0.25, per_modifier=0.4),
            "PM": _block_config(3, max_staff=4, per_sales=0.3, per_modifier=0.5),
            "Close": _block_config(1, max_staff=3, per_sales=0.15, per_modifier=0.3),
        },
    ),
    "Server - Cocktail": _role_config(
        wage=6.75,
        priority=0.95,
        max_weekly=36,
        daily_boost={"Thu": 1, "Fri": 1, "Sat": 1, "Sun": -3},
        thresholds=[
            {"metric": "demand_index", "gte": 0.6, "add": 1},
            {"metric": "demand_index", "gte": 0.95, "add": 1},
        ],
        blocks={
            "Open": _block_config(1, max_staff=2, per_sales=0.1, per_modifier=0.2),
            "Mid": _block_config(1, max_staff=2, per_sales=0.15, per_modifier=0.3),
            "PM": _block_config(2, max_staff=3, per_sales=0.2, per_modifier=0.4),
            "Close": _block_config(1, max_staff=2, per_sales=0.15, per_modifier=0.3),
        },
    ),
    "Server - Patio": _role_config(
        wage=6.0,
        priority=0.8,
        max_weekly=32,
        daily_boost={"Fri": 1, "Sat": 1, "Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.7, "add": 1}],
        blocks={
            "Mid": _block_config(1, max_staff=2, per_sales=0.1, per_modifier=0.2),
            "PM": _block_config(1, max_staff=2, per_sales=0.15, per_modifier=0.3),
        },
    ),
    "Bartender": _role_config(
        wage=10.0,
        priority=0.98,
        max_weekly=40,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.75, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=2, per_sales=0.05, per_modifier=0.1),
            "Mid": _block_config(1, max_staff=2, per_sales=0.1, per_modifier=0.2),
            "PM": _block_config(1, max_staff=2, per_sales=0.15, per_modifier=0.3),
            "Close": _block_config(1, max_staff=2, per_sales=0.1, per_modifier=0.2),
        },
    ),
    "Cashier": _role_config(
        wage=15.0,
        priority=0.9,
        max_weekly=35,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.7, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=2, per_sales=0.05),
            "Mid": _block_config(1, max_staff=2, per_sales=0.1),
            "PM": _block_config(1, max_staff=2, per_sales=0.15),
        },
    ),
    "Cashier - To-Go Specialist": _role_config(
        wage=15.0,
        priority=0.88,
        max_weekly=35,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.75, "add": 1}],
        blocks={
            "Mid": _block_config(1, max_staff=2, per_sales=0.1, per_modifier=0.2),
            "PM": _block_config(1, max_staff=2, per_sales=0.15, per_modifier=0.25),
            "Close": _block_config(1, max_staff=2, per_sales=0.1, per_modifier=0.2),
        },
    ),
    "Host": _role_config(
        wage=14.0,
        priority=0.75,
        max_weekly=32,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.75, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=1),
            "Mid": _block_config(1, max_staff=1),
        },
    ),
    "Expo": _role_config(
        wage=17.5,
        priority=0.92,
        max_weekly=40,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.8, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=1),
            "Mid": _block_config(1, max_staff=2),
            "PM": _block_config(1, max_staff=2, per_sales=0.1),
            "Close": _block_config(1, max_staff=2),
        },
    ),
    "Grill": _role_config(
        wage=18.0,
        priority=0.94,
        max_weekly=40,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.85, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=2),
            "Mid": _block_config(1, max_staff=2, per_sales=0.1),
            "PM": _block_config(1, max_staff=2, per_sales=0.15),
            "Close": _block_config(1, max_staff=2),
        },
    ),
    "Cook": _role_config(
        wage=17.0,
        priority=0.9,
        max_weekly=40,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.85, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=2),
            "Mid": _block_config(1, max_staff=2, per_sales=0.1),
            "PM": _block_config(1, max_staff=2, per_sales=0.15),
            "Close": _block_config(1, max_staff=2),
        },
    ),
    "Prep": _role_config(
        wage=16.0,
        priority=0.8,
        max_weekly=34,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.75, "add": 1}],
        blocks={
            "Open": _block_config(1, max_staff=1),
            "Mid": _block_config(1, max_staff=2),
        },
    ),
    "Chip": _role_config(
        wage=15.5,
        priority=0.78,
        max_weekly=34,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.7, "add": 1}],
        blocks={
            "Mid": _block_config(1, max_staff=2),
            "PM": _block_config(1, max_staff=2, per_sales=0.1),
        },
    ),
    "Shake": _role_config(
        wage=15.5,
        priority=0.78,
        max_weekly=34,
        daily_boost={"Sun": -2},
        thresholds=[{"metric": "demand_index", "gte": 0.7, "add": 1}],
        blocks={
            "Mid": _block_config(1, max_staff=2),
            "PM": _block_config(1, max_staff=2, per_sales=0.1),
        },
    ),
    "Kitchen Opener": _role_config(
        wage=18.5,
        priority=0.85,
        max_weekly=38,
        daily_boost={"Sun": -2},
        blocks={"Open": _block_config(1, max_staff=2)},
    ),
    "Kitchen Closer": _role_config(
        wage=19.0,
        priority=0.87,
        max_weekly=38,
        daily_boost={"Sun": -2},
        blocks={"Close": _block_config(1, max_staff=2)},
    ),
}


BASELINE_POLICY: Dict[str, Any] = {
    "name": "Baseline Coverage",
    "description": "Seeded policy that balances FOH/BOH coverage for the automation workflow.",
    "global": {
        "max_hours_week": 48,
        "min_rest_hours": 10,
        "max_consecutive_days": 7,
        "round_to_minutes": 15,
        "allow_split_shifts": True,
        "overtime_penalty": 1.5,
        "desired_hours_floor_pct": 0.85,
        "desired_hours_ceiling_pct": 1.15,
        "open_buffer_minutes": 30,
        "close_buffer_minutes": 35,
    },
    "timeblocks": DEFAULT_TIMEBLOCKS,
    "business_hours": BUSINESS_HOURS,
    "roles": ROLES,
}


def build_default_policy() -> Dict[str, Any]:
    """Return a deepcopy so callers can mutate the policy safely."""
    return copy.deepcopy(BASELINE_POLICY)


def ensure_default_policy(session_factory) -> None:
    """Seed the baseline policy exactly once so the generator can run end-to-end."""
    from database import get_active_policy, upsert_policy  # late import to avoid circular deps

    with session_factory() as session:
        if get_active_policy(session):
            return
        spec = build_default_policy()
        name = spec.get("name", "Baseline Coverage")
        params = {key: value for key, value in spec.items() if key != "name"}
        upsert_policy(session, name, params, edited_by="system")
