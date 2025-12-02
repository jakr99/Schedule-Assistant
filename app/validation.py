from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from database import Employee, Shift, WeekSchedule, get_active_policy
from policy import anchor_rules, build_default_policy, close_minutes, open_minutes
from roles import normalize_role, role_matches

UTC = datetime.timezone.utc
WEEKDAY_TOKENS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
ROLE_CONCURRENCY_LIMITS = {
    "Server - Dining": 6,
    "Server - Cocktail": 4,
}
NORMALIZED_ROLE_LIMITS = {
    normalize_role(name): (name, limit) for name, limit in ROLE_CONCURRENCY_LIMITS.items()
}


def validate_week_schedule(session, week_start: datetime.date, *, employee_session=None) -> Dict[str, Any]:
    """Return validation findings for the requested week."""
    normalized_start = _normalize_week_start(week_start)
    week = session.execute(
        select(WeekSchedule).where(WeekSchedule.week_start_date == normalized_start)
    ).scalar_one_or_none()
    if not week:
        return {
            "week_start": normalized_start.isoformat(),
            "week_id": None,
            "issues": [
                {
                    "type": "missing_schedule",
                    "severity": "error",
                    "message": "No schedule exists for the requested week.",
                }
            ],
            "warnings": [],
        }

    shifts = list(session.scalars(select(Shift).where(Shift.week_id == week.id)))
    policy_model = get_active_policy(session)
    if policy_model:
        policy_payload = policy_model.params_dict()
    else:
        policy_payload = build_default_policy()

    employee_ids = {shift.employee_id for shift in shifts if shift.employee_id}
    employee_map: Dict[int, Employee] = {}
    if employee_ids and employee_session:
        employees = employee_session.scalars(
            select(Employee)
            .options(selectinload(Employee.unavailability))
            .where(Employee.id.in_(employee_ids))
        )
        for employee in employees:
            employee_map[employee.id] = employee

    issues: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    issues.extend(_availability_issues(shifts, employee_map))
    issues.extend(_coverage_issues(shifts, policy_payload))
    issues.extend(_open_close_issues(shifts, policy_payload))
    warnings.extend(_concurrency_warnings(shifts))
    return {
        "week_start": normalized_start.isoformat(),
        "week_id": week.id,
        "issues": issues,
        "warnings": warnings,
    }


def _normalize_week_start(date_value: datetime.date | datetime.datetime) -> datetime.date:
    if isinstance(date_value, datetime.datetime):
        date_value = date_value.date()
    weekday = date_value.weekday()
    if weekday == 0:
        return date_value
    return date_value - datetime.timedelta(days=weekday)


def _availability_issues(shifts: List[Shift], employees: Dict[int, Employee]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    unavailability = {emp_id: _unavailability_windows(employee) for emp_id, employee in employees.items()}
    for shift in shifts:
        if not shift.employee_id or shift.employee_id not in unavailability:
            continue
        segments = _shift_segments(shift)
        windows = unavailability[shift.employee_id]
        employee = employees[shift.employee_id]
        for day_index, day_start, seg_start, seg_end in segments:
            start_minutes = int((seg_start - day_start).total_seconds() // 60)
            end_minutes = int((seg_end - day_start).total_seconds() // 60)
            for window_start, window_end in windows.get(day_index, []):
                if start_minutes < window_end and end_minutes > window_start:
                    issues.append(
                        {
                            "type": "availability",
                            "severity": "error",
                            "shift_id": shift.id,
                            "employee_id": shift.employee_id,
                            "employee": employee.full_name,
                            "day": WEEKDAY_TOKENS[day_index],
                            "message": f"{employee.full_name} is unavailable between "
                            f"{_format_minutes(window_start)}-{_format_minutes(window_end)} "
                            f"on {WEEKDAY_TOKENS[day_index]}.",
                        }
                    )
                    break
    return issues


def _coverage_issues(shifts: List[Shift], policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    anchors = anchor_rules(policy)
    opener_requirements = anchors.get("openers", {})
    closer_requirements = anchors.get("closers", {})
    opener_roles = anchors.get("opener_roles", {})
    closer_roles = anchors.get("closer_roles", {})
    opener_counts: Dict[int, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    closer_counts: Dict[int, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for shift in shifts:
        start = shift.start.astimezone(UTC)
        day_index = start.weekday()
        for group, roles in opener_roles.items():
            if _matches_role(shift.role, roles):
                opener_counts[day_index][group] += 1
        for group, roles in closer_roles.items():
            if _matches_role(shift.role, roles):
                closer_counts[day_index][group] += 1
    issues.extend(
        _compare_anchor_counts(
            opener_counts,
            opener_requirements,
            label="openers",
        )
    )
    issues.extend(
        _compare_anchor_counts(
            closer_counts,
            closer_requirements,
            label="closers",
        )
    )
    return issues


def _concurrency_warnings(shifts: List[Shift]) -> List[Dict[str, Any]]:
    warnings: List[Dict[str, Any]] = []
    if not NORMALIZED_ROLE_LIMITS:
        return warnings
    day_windows: Dict[str, Dict[int, List[Tuple[datetime.datetime, datetime.datetime]]]] = {}
    for normalized_role, (label, _limit) in NORMALIZED_ROLE_LIMITS.items():
        day_windows[label] = defaultdict(list)
    for shift in shifts:
        normalized = normalize_role(shift.role)
        if normalized not in NORMALIZED_ROLE_LIMITS:
            continue
        label, _limit = NORMALIZED_ROLE_LIMITS[normalized]
        segments = _shift_segments(shift)
        for day_index, _day_start, seg_start, seg_end in segments:
            day_windows[label][day_index].append((seg_start, seg_end))
    for label, by_day in day_windows.items():
        limit = ROLE_CONCURRENCY_LIMITS[label]
        for day_index in range(7):
            windows = by_day.get(day_index, [])
            if not windows:
                continue
            overlap = _max_overlap(windows)
            if overlap > limit:
                warnings.append(
                    {
                        "type": "concurrency",
                        "severity": "warning",
                        "role": label,
                        "day": WEEKDAY_TOKENS[day_index],
                        "allowed": limit,
                        "actual": overlap,
                        "message": f"{label} scheduled {overlap} concurrent shifts on "
                        f"{WEEKDAY_TOKENS[day_index]} (limit {limit}).",
                    }
                )
    return warnings


def _open_close_issues(shifts: List[Shift], policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Reject shifts that start before open (unless opener) or end after close (unless closer)."""
    issues: List[Dict[str, Any]] = []
    for shift in shifts:
        loc = (shift.location or "").strip().lower()
        segments = _shift_segments(shift)
        if loc == "close":
            continue
        for day_index, day_start, seg_start, seg_end in segments:
            date_value = day_start.date()
            op_day_start = day_start
            op_date = date_value
            if seg_start.time() < datetime.time(6, 0):
                op_day_start = day_start - datetime.timedelta(days=1)
                op_date = op_day_start.date()
            open_min = open_minutes(policy, op_date)
            close_min = close_minutes(policy, op_date)
            open_dt = op_day_start + datetime.timedelta(minutes=open_min)
            close_dt = op_day_start + datetime.timedelta(minutes=close_min)
            if loc not in {"open", "close"} and seg_start < open_dt:
                issues.append(
                    {
                        "type": "hours",
                        "severity": "error",
                        "shift_id": shift.id,
                        "role": shift.role,
                        "day": WEEKDAY_TOKENS[day_index],
                        "message": f"{shift.role} shift starts before open ({seg_start.strftime('%H:%M')} < {open_dt.strftime('%H:%M')})",
                    }
                )
                break
            if loc != "close" and seg_end > close_dt:
                issues.append(
                    {
                        "type": "hours",
                        "severity": "error",
                        "shift_id": shift.id,
                        "role": shift.role,
                        "day": WEEKDAY_TOKENS[day_index],
                        "message": f"{shift.role} shift ends after close ({seg_end.strftime('%H:%M')} > {close_dt.strftime('%H:%M')})",
                    }
                )
                break
    return issues


def _compare_anchor_counts(
    counts: Dict[int, Dict[str, int]],
    requirements: Dict[str, int],
    *,
    label: str,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for day_index in range(7):
        day_counts = counts.get(day_index, {})
        for group, required in requirements.items():
            actual = day_counts.get(group, 0)
            if actual < required:
                issues.append(
                    {
                        "type": "coverage",
                        "severity": "error",
                        "day": WEEKDAY_TOKENS[day_index],
                        "group": group,
                        "required": required,
                        "actual": actual,
                        "message": f"{group} requires {required} {label} on {WEEKDAY_TOKENS[day_index]}, "
                        f"but only {actual} were scheduled.",
                    }
                )
    return issues


def _shift_segments(shift: Shift) -> List[Tuple[int, datetime.datetime, datetime.datetime, datetime.datetime]]:
    start = _as_local_datetime(shift.start)
    end = _as_local_datetime(shift.end)
    if end < start:
        end = start
    segments: List[Tuple[int, datetime.datetime, datetime.datetime, datetime.datetime]] = []
    cursor = start
    while cursor < end:
        day_start = datetime.datetime.combine(cursor.date(), datetime.time.min)
        day_end = day_start + datetime.timedelta(days=1)
        segment_end = min(end, day_end)
        segments.append((cursor.weekday(), day_start, cursor, segment_end))
        cursor = segment_end
    return segments


def _max_overlap(windows: List[Tuple[datetime.datetime, datetime.datetime]]) -> int:
    events: List[Tuple[datetime.datetime, int]] = []
    for start, end in windows:
        events.append((start, 1))
        events.append((end, -1))
    events.sort()
    running = 0
    best = 0
    for _, delta in events:
        running += delta
        if running > best:
            best = running
    return best


def _as_local_datetime(value: datetime.datetime) -> datetime.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(UTC).replace(tzinfo=None)


def _unavailability_windows(employee: Employee) -> Dict[int, List[Tuple[int, int]]]:
    mapping: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for entry in employee.unavailability:
        window = (
            entry.start_time.hour * 60 + entry.start_time.minute,
            entry.end_time.hour * 60 + entry.end_time.minute,
        )
        mapping[entry.day_of_week].append(window)
    for day in mapping:
        mapping[day].sort()
    return mapping


def _matches_role(role_name: str, targets: Iterable[str]) -> bool:
    for target in targets:
        if role_matches(role_name, target):
            return True
    return False


def _format_minutes(value: int) -> str:
    hours = value // 60
    minutes = value % 60
    return f"{hours:02d}:{minutes:02d}"
