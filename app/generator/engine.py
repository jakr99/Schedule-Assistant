from __future__ import annotations

import datetime
import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

from database import (
    Employee,
    Shift,
    WeekSchedule,
    get_or_create_week,
    get_or_create_week_context,
    get_week_daily_projections,
    list_modifiers_for_week,
    record_audit_log,
    upsert_shift,
)
from policy import (
    PATTERN_TEMPLATES,
    anchor_rules,
    build_default_policy,
    close_minutes,
    hourly_wage,
    open_minutes,
    resolve_policy_block,
    role_definition,
    parse_time_label,
    shift_length_limits,
    shift_length_rule,
)
from roles import is_manager_role, normalize_role, role_matches, role_group

UTC = datetime.timezone.utc
WEEKDAY_TOKENS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


@dataclass
class BlockDemand:
    day_index: int
    date: datetime.date
    start: datetime.datetime
    end: datetime.datetime
    role: str
    block_name: str
    labels: List[str]
    need: int
    priority: float = 1.0
    minimum: int = 0
    allow_cuts: bool = True
    always_on: bool = False
    role_group: str = "Other"
    hourly_rate: float = 0.0
    recommended_cut: Optional[datetime.datetime] = None
    max_capacity: int = 0

    @property
    def duration_hours(self) -> float:
        return max(0.0, (self.end - self.start).total_seconds() / 3600)


class ScheduleGenerator:
    def __init__(
        self,
        session,
        policy: Dict,
        actor: str = "system",
        wage_overrides: Optional[Dict[str, float]] = None,
        *,
        cut_relax_level: int = 0,
    ) -> None:
        self.session = session
        self.policy = policy or {}
        self.actor = actor or "system"
        self.wage_overrides = wage_overrides or {}
        try:
            self.cut_relax_level: int = max(0, int(cut_relax_level))
        except (TypeError, ValueError):
            self.cut_relax_level = 0
        raw_roles = self.policy.get("roles") if isinstance(self.policy.get("roles"), dict) else {}
        self.roles_config: Dict[str, Dict] = {
            role: config for role, config in raw_roles.items() if not is_manager_role(role)
        }
        global_cfg = self.policy.get("global") or {}
        self.max_hours_per_week: float = float(global_cfg.get("max_hours_week", 40) or 40)
        self.max_consecutive_days: int = int(global_cfg.get("max_consecutive_days", 6) or 6)
        self.round_to_minutes: int = int(global_cfg.get("round_to_minutes", 15) or 15)
        self.allow_split_shifts: bool = bool(global_cfg.get("allow_split_shifts", True))
        self.overtime_penalty: float = float(global_cfg.get("overtime_penalty", 1.5) or 1.5)
        desired_floor = float(global_cfg.get("desired_hours_floor_pct", 0.85) or 0.0)
        desired_ceiling = float(global_cfg.get("desired_hours_ceiling_pct", 1.15) or 0.0)
        self.desired_hours_floor_pct: float = self._clamp(desired_floor, 0.0, 1.0)
        min_ceiling = max(self.desired_hours_floor_pct + 0.05, 0.1)
        self.desired_hours_ceiling_pct: float = self._clamp(max(desired_ceiling, min_ceiling), min_ceiling, 2.0)
        self.open_buffer_minutes: int = int(global_cfg.get("open_buffer_minutes", 30) or 0)
        self.close_buffer_minutes: int = int(global_cfg.get("close_buffer_minutes", 35) or 0)
        labor_pct = float(global_cfg.get("labor_budget_pct", 0.27) or 0.0)
        if labor_pct > 1.0:
            labor_pct /= 100.0
        self.labor_budget_pct = self._clamp(labor_pct, 0.05, 0.9)
        labor_tol = float(global_cfg.get("labor_budget_tolerance_pct", 0.08) or 0.0)
        if labor_tol > 1.0:
            labor_tol /= 100.0
        self.labor_budget_tolerance = self._clamp(labor_tol, 0.0, 0.5)

        self.employees: List[Dict[str, Any]] = []
        self.modifiers_by_day: Dict[int, List[Dict[str, Any]]] = {}
        self.day_contexts: List[Dict[str, Any]] = []
        self.role_group_settings: Dict[str, Dict[str, Any]] = self._load_role_group_settings()
        self.group_budget_by_day: List[Dict[str, float]] = []
        self.warnings: List[str] = []
        self.interchangeable_groups: Set[str] = {"Cashier"}
        self.random = random.Random()
        self.group_pressure: Dict[int, Dict[str, float]] = {}
        self.cut_priority_rank: Dict[str, int] = {
            "Cashier": 0,
            "Servers": 1,
            "Kitchen": 2,
            "Bartenders": 3,
            "Other": 2,
        }
        self.trim_aggressive_ratio: float = float(global_cfg.get("trim_aggressive_ratio", 1.0) or 1.0)
        self.anchors = anchor_rules(self.policy)
        order_mode = (self.anchors.get("open_close_order") or "prefer").strip().lower()
        self.open_close_order_mode = order_mode if order_mode in {"off", "prefer", "enforce"} else "prefer"
        self.group_aliases = {"heart of house": "Kitchen", "cashier & takeout": "Cashier"}
        self.non_cuttable_roles: Set[str] = {
            normalize_role(role) for role in (self.anchors.get("non_cuttable_roles") or [])
        }
        self.pattern_templates: Dict[str, Any] = {}
        raw_patterns = self.policy.get("pattern_templates") if isinstance(self.policy, dict) else {}
        if isinstance(raw_patterns, dict) and raw_patterns:
            self.pattern_templates = raw_patterns
        else:
            self.pattern_templates = PATTERN_TEMPLATES

    @staticmethod
    def _clamp(value: float, minimum: float, maximum: float) -> float:
        return max(minimum, min(maximum, value))

    @staticmethod
    def _to_float(value: Any, *, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_int(value: Any, *, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _round_minutes(self, minutes: float) -> int:
        """Round a minute value to the nearest configured step."""
        step = max(1, self.round_to_minutes)
        return int(round(minutes / step) * step)

    def generate(self, week_start_date: datetime.date) -> Dict[str, Any]:
        if not self.roles_config:
            return {
                "week_id": None,
                "days": [],
                "total_cost": 0.0,
                "warnings": ["Active policy does not define any eligible roles. Unable to generate schedule."],
            }

        week = get_or_create_week(self.session, week_start_date)
        context = get_or_create_week_context(
            self.session,
            week.iso_year,
            week.iso_week,
            week.label,
        )
        week.context_id = context.id
        self.session.commit()

        self._reset_week(week)
        self.employees = self._load_employee_profiles()
        self.random.shuffle(self.employees)
        self.modifiers_by_day = self._load_modifiers(week.week_start_date)
        self.day_contexts = self._build_day_contexts(context, week.week_start_date)
        self.group_budget_by_day = self._build_group_budgets()
        demands = self._compute_block_demands(week.week_start_date)
        assignments = self._assign(demands)
        self._enforce_shift_continuity(assignments, week.week_start_date)
        self._warn_unpaired_openers()

        created_ids: List[int] = []
        for payload in assignments:
            payload.update({"week_id": week.id, "status": "draft", "week_start": week.week_start_date})
            created_ids.append(upsert_shift(self.session, payload))

        summary = self._build_summary(week, assignments)
        summary["warnings"].extend(self.warnings)
        summary["shifts_created"] = len(created_ids)
        record_audit_log(
            self.session,
            self.actor,
            "schedule_generate",
            target_type="WeekSchedule",
            target_id=week.id,
            payload={"shifts_created": len(created_ids)},
        )
        return summary

    def _reset_week(self, week: WeekSchedule) -> None:
        self.session.execute(delete(Shift).where(Shift.week_id == week.id))
        week.status = "draft"
        self.session.commit()

    def _load_employee_profiles(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Employee)
            .where(Employee.status == "active")
            .options(selectinload(Employee.unavailability))
            .order_by(Employee.full_name.asc())
        )
        employees: List[Dict[str, Any]] = []
        self.employee_lookup: Dict[int, Dict[str, Any]] = {}
        for employee in self.session.scalars(stmt):
            role_set = {role.strip() for role in employee.role_list if role.strip()}
            if not role_set:
                continue
            unavailability = {index: [] for index in range(7)}
            for entry in employee.unavailability:
                start_minutes = entry.start_time.hour * 60 + entry.start_time.minute
                end_minutes = entry.end_time.hour * 60 + entry.end_time.minute
                unavailability.setdefault(entry.day_of_week, []).append((start_minutes, end_minutes))
            desired_hours = max(0.0, float(employee.desired_hours or 0))
            desired_floor = desired_hours * self.desired_hours_floor_pct if desired_hours else 0.0
            desired_ceiling = desired_hours * self.desired_hours_ceiling_pct if desired_hours else self.max_hours_per_week
            desired_ceiling = min(self.max_hours_per_week, desired_ceiling or self.max_hours_per_week)
            if desired_ceiling < desired_floor:
                desired_ceiling = desired_floor
            record = {
                "id": employee.id,
                "name": employee.full_name,
                "roles": role_set,
                "desired_hours": desired_hours,
                "desired_floor": desired_floor,
                "desired_ceiling": desired_ceiling,
                "total_hours": 0.0,
                "assignments": {idx: [] for idx in range(7)},
                "day_last_block_end": {idx: None for idx in range(7)},
                "last_assignment_end": None,
                "unavailability": unavailability,
                "days_with_assignments": set(),
                "last_day_index": None,
                "consecutive_days": 0,
                "pending_open_links": {idx: [] for idx in range(7)},
            }
            employees.append(record)
            if employee.id is not None:
                self.employee_lookup[employee.id] = record
        return employees

    def _load_modifiers(self, week_start: datetime.date) -> Dict[int, List[Dict[str, Any]]]:
        modifiers = list_modifiers_for_week(self.session, week_start)
        mapping: Dict[int, List[Dict[str, Any]]] = {idx: [] for idx in range(7)}
        for modifier in modifiers:
            day_idx = int(modifier.get("day_of_week", 0))
            start_time = modifier.get("start_time")
            end_time = modifier.get("end_time")
            pct = float(modifier.get("pct_change", 0) or 0)
            start_minutes = start_time.hour * 60 + start_time.minute if start_time else 0
            end_minutes = end_time.hour * 60 + end_time.minute if end_time else 24 * 60
            mapping.setdefault(day_idx, []).append(
                {
                    "start": start_minutes,
                    "end": end_minutes,
                    "pct": pct,
                    "multiplier": 1.0 + (pct / 100.0),
                }
            )
        return mapping

    def _load_role_group_settings(self) -> Dict[str, Dict[str, Any]]:
        specs = self.policy.get("role_groups") if isinstance(self.policy, dict) else {}
        mapping: Dict[str, Dict[str, Any]] = {}
        if not isinstance(specs, dict):
            return mapping
        for name, raw in specs.items():
            if not isinstance(raw, dict):
                continue
            label = (name or "Group").strip() or "Group"
            allocation = self._parse_allocation_pct(raw.get("allocation_pct"))
            mapping[label] = {
                "allocation_pct": allocation,
                "allow_cuts": bool(raw.get("allow_cuts", True)),
                "always_on": bool(raw.get("always_on", False)),
                "cut_buffer_minutes": int(raw.get("cut_buffer_minutes", 30) or 0),
            }
        if not mapping:
            mapping = build_default_policy().get("role_groups", {})
        return mapping

    @staticmethod
    def _parse_allocation_pct(value: Any) -> float:
        try:
            pct = float(value)
        except (TypeError, ValueError):
            return 0.0
        if pct > 1.0:
            pct = pct / 100.0
        return max(0.0, min(1.0, pct))

    def _build_day_contexts(self, context, week_start: datetime.date) -> List[Dict[str, Any]]:
        projections = get_week_daily_projections(self.session, context.id)
        projection_map = {projection.day_of_week: projection for projection in projections}
        contexts: List[Dict[str, Any]] = []
        adjusted_values: List[float] = []

        for day_index in range(7):
            projection = projection_map.get(day_index)
            sales = float(projection.projected_sales_amount) if projection else 0.0
            notes_payload = self._parse_projection_notes(projection.projected_notes if projection else "")
            modifier_multiplier = self._day_modifier_multiplier(day_index)
            adjusted_sales = sales * modifier_multiplier
            adjusted_values.append(adjusted_sales)
            contexts.append(
                {
                    "day_index": day_index,
                    "weekday_token": WEEKDAY_TOKENS[day_index],
                    "sales": sales,
                    "notes": notes_payload,
                    "modifier_multiplier": modifier_multiplier,
                    "indices": {},
                }
            )

        max_sales = max(adjusted_values) if adjusted_values else 0.0
        if max_sales <= 0:
            max_sales = 1.0

        for ctx, adjusted in zip(contexts, adjusted_values):
            ctx["indices"] = self._compute_indices(ctx, adjusted, max_sales)
        return contexts

    def _build_group_budgets(self) -> List[Dict[str, float]]:
        budgets: List[Dict[str, float]] = []
        if not self.day_contexts:
            return budgets
        if not self.role_group_settings:
            return [{} for _ in self.day_contexts]
        for ctx in self.day_contexts:
            sales = float(ctx.get("sales", 0.0) or 0.0)
            modifier_multiplier = float(ctx.get("modifier_multiplier", 1.0) or 1.0)
            adjusted_sales = sales * modifier_multiplier
            total_budget = adjusted_sales * self.labor_budget_pct
            day_budget: Dict[str, float] = {}
            for group, spec in self.role_group_settings.items():
                pct = float(spec.get("allocation_pct", 0.0) or 0.0)
                if pct > 1.0:
                    pct /= 100.0
                pct = max(0.0, min(1.0, pct))
                if pct <= 0.0:
                    continue
                day_budget[group] = round(total_budget * pct, 2)
            budgets.append(day_budget)
        if not budgets:
            budgets = [{} for _ in self.day_contexts]
        return budgets

    def _compute_indices(self, day_ctx: Dict[str, Any], adjusted_sales: float, max_sales: float) -> Dict[str, float]:
        indices: Dict[str, float] = {}
        mapping = self.policy.get("demand_mapping") or {}
        base_index = adjusted_sales / max_sales if max_sales > 0 else 0.0
        base_index = max(0.0, min(1.5, base_index))
        indices["demand_index"] = round(base_index, 3)

        extra_data = day_ctx.get("notes", {})
        for name, spec in (mapping.get("indices") or {}).items():
            if name == "demand_index":
                continue
            value = None
            if isinstance(spec, dict):
                source_key = spec.get("source")
                if source_key and source_key in extra_data:
                    try:
                        value = float(extra_data[source_key])
                    except (TypeError, ValueError):
                        value = None
                role_weight = spec.get("roleWeight")
                if value is None and isinstance(role_weight, dict):
                    if role_weight:
                        value = base_index * max(role_weight.values())
            if value is None:
                value = base_index
            indices[name] = round(max(0.0, min(2.0, value)), 3)
        return indices

    def _day_modifier_multiplier(self, day_index: int) -> float:
        windows = self.modifiers_by_day.get(day_index, [])
        if not windows:
            return 1.0
        total = 0.0
        for window in windows:
            span = max(0.0, window["end"] - window["start"])
            fraction = span / (24 * 60)
            total += (window["pct"] / 100.0) * max(fraction, 0.1)
        return max(0.5, 1.0 + total)

    def _compute_block_demands(self, week_start: datetime.date) -> List[BlockDemand]:
        demands: List[BlockDemand] = []
        for day_index in range(7):
            date_value = week_start + datetime.timedelta(days=day_index)
            for role_name, role_cfg in self.roles_config.items():
                if not role_cfg.get("enabled", True):
                    continue
                block_targets = role_cfg.get("blocks") or {}
                group_name = self._role_group_name(role_name, role_cfg)
                group_defaults = self.role_group_settings.get(group_name, {})
                allow_cuts = bool(role_cfg.get("allow_cuts", group_defaults.get("allow_cuts", True)))
                always_on = bool(role_cfg.get("always_on", group_defaults.get("always_on", False)))
                for block_name, block_cfg in block_targets.items():
                    block_label = (block_name or "").strip().lower()
                    if block_label == "open" and not self._role_allows_open_shift(role_name):
                        continue
                    overrides: Dict[str, str] = {}
                    if isinstance(block_cfg, dict):
                        custom_start = block_cfg.get("start")
                        custom_end = block_cfg.get("end")
                        if custom_start:
                            overrides["start"] = str(custom_start)
                        if custom_end:
                            overrides["end"] = str(custom_end)
                    resolved = resolve_policy_block(
                        self.policy,
                        block_name,
                        date_value,
                        overrides=overrides or None,
                    )
                    if not resolved:
                        continue
                    _, start_dt, end_dt = resolved
                    start_dt, end_dt = self._adjust_block_window(role_name, block_name, date_value, start_dt, end_dt)
                    pattern_windows = self._pattern_windows(
                        role_name,
                        date_value,
                        block_label,
                        anchor_start=start_dt,
                        anchor_end=end_dt,
                    )
                    need, minimum, max_staff = self._calculate_block_need(
                        role_name, role_cfg, block_cfg, block_name, day_index
                    )
                    if need <= 0:
                        continue
                    rate = self._role_wage(role_name)
                    labels = [block_name]
                    if pattern_windows:
                        windows = pattern_windows
                        slots = need
                        mins = minimum
                        max_slots = max(max_staff, need)
                        count = len(windows)
                        base_each = slots // count
                        remainder = slots % count
                        min_each = mins // count
                        min_rem = mins % count
                        max_each = max_slots // count
                        max_rem = max_slots % count
                        for idx, (p_start, p_end) in enumerate(windows):
                            slot_need = base_each + (1 if idx < remainder else 0)
                            slot_min = min_each + (1 if idx < min_rem else 0)
                            slot_max = max_each + (1 if idx < max_rem else 0)
                            if slot_need <= 0 and slot_min <= 0:
                                continue
                            slot_need = max(slot_need, slot_min)
                            slot_max = max(slot_need, slot_max)
                            demand_labels = list(labels)
                            demands.append(
                                BlockDemand(
                                    day_index=day_index,
                                    date=date_value,
                                    start=p_start,
                                    end=p_end,
                                    role=role_name,
                                    block_name=block_name,
                                    labels=demand_labels,
                                    need=slot_need,
                                    priority=float(role_cfg.get("priority", 1.0)),
                                    minimum=slot_min,
                                    allow_cuts=allow_cuts,
                                    always_on=always_on,
                                    role_group=group_name,
                                    hourly_rate=rate,
                                    max_capacity=slot_max,
                                )
                            )
                    else:
                        demands.append(
                            BlockDemand(
                                day_index=day_index,
                                date=date_value,
                                start=start_dt,
                                end=end_dt,
                                role=role_name,
                                block_name=block_name,
                                labels=labels,
                                need=need,
                                priority=float(role_cfg.get("priority", 1.0)),
                                minimum=minimum,
                                allow_cuts=allow_cuts,
                                always_on=always_on,
                                role_group=group_name,
                                hourly_rate=rate,
                                max_capacity=max_staff,
                            )
                        )
        self._enforce_anchor_shift_caps(demands)
        self._apply_labor_allocations(demands)
        self._boost_under_budget_groups(demands)
        self._record_group_pressure(demands)
        self._annotate_cut_windows(demands)
        return demands

    def _role_wage(self, role_name: str) -> float:
        if role_name in self.wage_overrides:
            try:
                override = float(self.wage_overrides[role_name])
                if override > 0:
                    return override
            except (TypeError, ValueError):
                pass
        return hourly_wage(self.policy, role_name, 0.0)

    def _adjust_block_window(
        self,
        role_name: str,
        block_name: str,
        date_value: datetime.date,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime,
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        normalized_role = normalize_role(role_name)
        block_label = (block_name or "").strip().lower()
        is_cashier = any(keyword in normalized_role for keyword in ("cashier", "takeout", "to-go"))
        opener_keywords = ("opener",)
        closer_keywords = ("closer",)
        if self.open_buffer_minutes and any(keyword in normalized_role for keyword in opener_keywords):
            buffer_minutes = self._round_minutes(max(0, self.open_buffer_minutes))
            open_min = open_minutes(self.policy, date_value)
            day_start = datetime.datetime.combine(date_value, datetime.time.min, tzinfo=UTC)
            open_dt = day_start + datetime.timedelta(minutes=open_min)
            buffered_start = open_dt - datetime.timedelta(minutes=buffer_minutes)
            if buffered_start < day_start:
                buffered_start = day_start
            if start_dt > buffered_start:
                start_dt = buffered_start
        # Apply pattern templates for Open/Mid/PM blocks to align with user-provided shift shapes
        # without moving the policy-driven start time.
        window_override = self._pattern_window(
            role_name,
            date_value,
            block_label,
            anchor_start=start_dt,
            anchor_end=end_dt,
        )
        if window_override:
            _anchored_start, end_dt = window_override
        return start_dt, end_dt


    def _day_sales_value(self, day_index: int) -> float:
        if 0 <= day_index < len(self.day_contexts):
            ctx = self.day_contexts[day_index]
            sales = float(ctx.get("sales", 0.0))
            modifier_multiplier = float(ctx.get("modifier_multiplier", 1.0))
            return sales * modifier_multiplier
        return 0.0

    def _pattern_window(
        self,
        role_name: str,
        date_value: datetime.date,
        block_label: str,
        *,
        anchor_start: Optional[datetime.datetime] = None,
        anchor_end: Optional[datetime.datetime] = None,
    ) -> Optional[Tuple[datetime.datetime, datetime.datetime]]:
        windows = self._pattern_windows(
            role_name,
            date_value,
            block_label,
            anchor_start=anchor_start,
            anchor_end=anchor_end,
        )
        return windows[0] if windows else None

    def _pattern_windows(
        self,
        role_name: str,
        date_value: datetime.date,
        block_label: str,
        *,
        anchor_start: Optional[datetime.datetime] = None,
        anchor_end: Optional[datetime.datetime] = None,
    ) -> List[Tuple[datetime.datetime, datetime.datetime]]:
        if block_label not in {"open", "mid", "pm"}:
            return []
        day_token = WEEKDAY_TOKENS[date_value.weekday()]
        group_name = self._canonical_group(role_group(role_name))
        templates = self.pattern_templates.get(group_name) or self.pattern_templates.get(role_name)
        if not isinstance(templates, dict):
            return []
        day_spec = templates.get(day_token) or templates.get("default")
        if not isinstance(day_spec, dict):
            return []
        block_key = "am" if block_label in {"open", "mid"} else "pm"
        windows = day_spec.get(block_key)
        parsed: List[Tuple[datetime.datetime, datetime.datetime]] = []
        if not isinstance(windows, list):
            return parsed
        for window in windows:
            parsed_window = self._parse_pattern_window(date_value, window)
            if not parsed_window:
                continue
            start_dt, end_dt = parsed_window
            if anchor_start:
                duration = end_dt - start_dt
                if duration.total_seconds() <= 0:
                    continue
                anchored_start = anchor_start
                anchored_end = anchor_start + duration
                if anchor_end and anchored_end > anchor_end:
                    anchored_end = anchor_end
                if anchored_end <= anchored_start:
                    continue
                parsed_window = (anchored_start, anchored_end)
            parsed.append(parsed_window)
        return parsed

    def _parse_pattern_window(
        self, date_value: datetime.date, window: Dict[str, Any]
    ) -> Optional[Tuple[datetime.datetime, datetime.datetime]]:
        if not isinstance(window, dict):
            return None
        start_label = window.get("start")
        end_label = window.get("end")
        start_minutes = parse_time_label(str(start_label)) if start_label is not None else None
        end_minutes = parse_time_label(str(end_label)) if end_label is not None else None
        if start_minutes is None or end_minutes is None:
            return None
        base = datetime.datetime.combine(date_value, datetime.time.min, tzinfo=UTC)
        start_dt = base + datetime.timedelta(minutes=int(start_minutes))
        end_dt = base + datetime.timedelta(minutes=int(end_minutes))
        return start_dt, end_dt

    def _calculate_block_need(
        self,
        role_name: str,
        role_cfg: Dict[str, Any],
        block_cfg: Dict[str, Any],
        block_name: str,
        day_index: int,
    ) -> Tuple[int, int, int]:
        block_label = (block_name or "").strip().lower()
        always_on = bool(role_cfg.get("always_on", False))
        base = int(block_cfg.get("base", block_cfg.get("min", 0)))
        min_staff = int(block_cfg.get("min", base))
        if min_staff <= 0 and base > 0:
            min_staff = base
        max_staff = max(int(block_cfg.get("max", max(base, min_staff))), min_staff)
        per_sales = float(block_cfg.get("per_1000_sales", 0.0))
        per_modifier = float(block_cfg.get("per_modifier", 0.0))
        sales = self._day_sales_value(day_index)
        sales_component = int(math.floor((sales / 1000.0) * per_sales))
        modifier_component = int(round(len(self.modifiers_by_day.get(day_index, [])) * per_modifier))
        boosts = role_cfg.get("daily_boost", {}) or {}
        day_token = WEEKDAY_TOKENS[day_index]
        daily_boost = int(boosts.get(day_token, 0))
        need = base + sales_component + modifier_component + daily_boost
        need += self._threshold_adjustment(role_cfg, block_cfg, day_index)
        if block_label == "open":
            if not self._role_allows_open_shift(role_name):
                return 0, 0, 0
            need = 1 if need > 0 else 0
        demand_index = 1.0
        if 0 <= day_index < len(self.day_contexts):
            demand_index = self.day_contexts[day_index].get("indices", {}).get("demand_index", 1.0)
        if not role_cfg.get("critical") and not always_on and block_label in {"mid", "pm"}:
            if demand_index < 0.3:
                need = min(need, min_staff)
        floor_rules = block_cfg.get("floor_by_demand", []) if isinstance(block_cfg, dict) else []
        if isinstance(floor_rules, list):
            for rule in floor_rules:
                if not isinstance(rule, dict):
                    continue
                gte = rule.get("gte")
                minimum_floor = rule.get("min")
                try:
                    gte_val = float(gte)
                    floor_val = int(minimum_floor)
                except (TypeError, ValueError):
                    continue
                if demand_index >= gte_val:
                    need = max(need, floor_val)
        need = max(min_staff, need)
        if max_staff > 0:
            need = min(max_staff, need)
        else:
            max_staff = need
        minimum = max(0, min_staff)
        if always_on:
            minimum = max(1, minimum)
        need = max(minimum, need)
        if self._is_opener_block(role_name, block_name):
            max_staff = 1
            need = min(1, need) if need > 0 else 0
            minimum = min(minimum, need)
        if self._is_closer_block(role_name, block_name):
            max_staff = 1
            need = min(1, need) if need > 0 else 0
            minimum = min(minimum, need)
        if max_staff <= 0:
            max_staff = need
        minimum = min(minimum, max_staff)
        return max(0, need), max(0, minimum), max_staff

    def _role_group_name(self, role_name: str, role_cfg: Optional[Dict[str, Any]]) -> str:
        explicit = ""
        if isinstance(role_cfg, dict):
            explicit = (role_cfg.get("group") or "").strip()
        if explicit:
            return self._canonical_group(explicit)
        inferred = role_group(role_name)
        return self._canonical_group(inferred if inferred else "Other")

    def _canonical_group(self, name: str) -> str:
        label = (name or "").strip()
        normalized = label.lower()
        if normalized in self.group_aliases:
            return self.group_aliases[normalized]
        if label:
            return label
        return "Other"

    def _apply_labor_allocations(self, demands: List[BlockDemand]) -> None:
        if not demands or not self.role_group_settings or not self.group_budget_by_day:
            return
        buckets: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for demand in demands:
            key = (demand.day_index, demand.role_group)
            bucket = buckets.setdefault(
                key,
                {"budget": self._group_budget_for_day(demand.day_index, demand.role_group), "demands": []},
            )
            bucket["demands"].append(demand)
        for (day_index, group_name), payload in buckets.items():
            budget = payload.get("budget")
            if budget is None or budget <= 0:
                continue
            total_cost = sum(self._slot_cost(demand) * demand.need for demand in payload["demands"])
            ratio = total_cost / budget if budget > 0 else 0.0
            if ratio <= 1.0:
                continue
            soft_mode = ratio <= (1.0 + self.labor_budget_tolerance + 1e-6)
            allowed_max = budget * (1 + self.labor_budget_tolerance)
            demand_index = 1.0
            if 0 <= day_index < len(self.day_contexts):
                demand_index = self.day_contexts[day_index].get("indices", {}).get("demand_index", 1.0)
            # Do not clamp budgets on "slow" days; allow full allocation + tolerance.
            # Trim-aggressive ratio is only used if explicitly > 1.0 (over-allocate); otherwise leave headroom intact.
            if ratio > 1.0:
                for demand in payload["demands"]:
                    if not self._is_anchor_demand(demand):
                        demand.minimum = min(demand.minimum, 0)
            removable: List[Tuple[float, float, BlockDemand]] = []
            for demand in payload["demands"]:
                if not demand.allow_cuts or demand.need <= demand.minimum:
                    continue
                slots = demand.need - demand.minimum
                if slots <= 0:
                    continue
                if self._is_anchor_demand(demand):
                    continue
                if soft_mode and demand.priority >= 1.0:
                    continue
                slot_cost = self._slot_cost(demand)
                removable.extend([(slot_cost, demand.priority, demand)] * slots)
            removable.sort(key=lambda entry: (entry[1], -entry[0]))
            idx = 0
            while total_cost > allowed_max + 0.01 and idx < len(removable):
                slot_cost, _priority, demand = removable[idx]
                if demand.need <= demand.minimum:
                    idx += 1
                    continue
                demand.need -= 1
                total_cost -= slot_cost
                if "trimmed by budget" not in demand.labels:
                    demand.labels.append("trimmed by budget")
                idx += 1
            if total_cost > allowed_max + 0.01:
                overage = max(0.0, total_cost - allowed_max)
                self.warnings.append(
                    f"Budget shortfall for {group_name} on {self._day_label(day_index)} (${overage:.2f})"
                )

    def _boost_under_budget_groups(self, demands: List[BlockDemand]) -> None:
        """Add incremental coverage when a group/day is materially under budget."""
        if not demands or not self.group_budget_by_day:
            return
        tolerance = max(0.0, min(0.9, self.labor_budget_tolerance))
        buckets: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for demand in demands:
            if demand.need <= 0:
                continue
            budget = self._group_budget_for_day(demand.day_index, demand.role_group)
            if not budget or budget <= 0:
                continue
            key = (demand.day_index, demand.role_group)
            bucket = buckets.setdefault(key, {"budget": budget, "demands": []})
            bucket["demands"].append(demand)
        for (day_index, group_name), payload in buckets.items():
            budget = payload["budget"]
            if budget <= 0:
                continue
            allowed_min = budget * max(0.0, 1.0 - tolerance)
            if allowed_min <= 0:
                continue
            current_cost = sum(self._slot_cost(demand) * demand.need for demand in payload["demands"])
            if current_cost >= allowed_min - 0.5:
                continue
            allowed_max = budget * (1.0 + tolerance)
            expandable = [
                demand
                for demand in payload["demands"]
                if demand.allow_cuts
                and getattr(demand, "max_capacity", demand.need) > demand.need
                and self._slot_cost(demand) > 0
            ]
            if not expandable:
                continue
            expandable.sort(key=self._budget_boost_rank)
            needed_extra = allowed_min - current_cost
            iterations = 0
            while needed_extra > 5.0 and expandable:
                progress = False
                for demand in list(expandable):
                    capacity = getattr(demand, "max_capacity", demand.need)
                    slack = max(0, capacity - demand.need)
                    if slack <= 0:
                        expandable.remove(demand)
                        continue
                    slot_cost = self._slot_cost(demand)
                    if slot_cost <= 0:
                        expandable.remove(demand)
                        continue
                    if current_cost + slot_cost > allowed_max + 0.5:
                        continue
                    demand.need += 1
                    demand.minimum = min(demand.need, demand.minimum + 1)
                    demand.allow_cuts = False
                    current_cost += slot_cost
                    needed_extra = max(0.0, allowed_min - current_cost)
                    if "budget boost" not in demand.labels:
                        demand.labels.append("budget boost")
                    progress = True
                    break
                if not progress:
                    break
                iterations += 1
                if iterations >= 500:
                    break

    def _budget_boost_rank(self, demand: BlockDemand) -> Tuple[int, float, float]:
        block_label = demand.block_name.strip().lower()
        block_order = {"pm": 0, "mid": 1, "open": 2, "close": 3}.get(block_label, 4)
        return (block_order, -demand.priority, -self._slot_cost(demand))

    def _record_group_pressure(self, demands: List[BlockDemand]) -> None:
        """Estimate group-level budget pressure for dynamic cut targeting."""
        pressure: Dict[int, Dict[str, float]] = {}
        if not demands:
            self.group_pressure = pressure
            return
        cost_totals: Dict[Tuple[int, str], float] = {}
        budget_totals: Dict[Tuple[int, str], float] = {}
        for demand in demands:
            if demand.need <= 0:
                continue
            key = (demand.day_index, demand.role_group)
            cost_totals[key] = cost_totals.get(key, 0.0) + (self._slot_cost(demand) * demand.need)
            budget = self._group_budget_for_day(demand.day_index, demand.role_group)
            if budget is not None:
                budget_totals[key] = budget
        for (day_idx, group_name), cost_value in cost_totals.items():
            budget = budget_totals.get((day_idx, group_name), 0.0)
            ratio = cost_value / budget if budget > 0 else 1.0
            day_map = pressure.setdefault(day_idx, {})
            day_map[group_name] = round(max(0.0, ratio), 3)
        self.group_pressure = pressure

    def _enforce_anchor_shift_caps(self, demands: List[BlockDemand]) -> None:
        """Ensure opener/closer counts match policy anchors."""
        if not demands:
            return
        opener_caps = {self._canonical_group(k): v for k, v in (self.anchors.get("openers") or {}).items()}
        closer_caps = {self._canonical_group(k): v for k, v in (self.anchors.get("closers") or {}).items()}
        opener_roles = {self._canonical_group(k): [normalize_role(r) for r in v] for k, v in (self.anchors.get("opener_roles") or {}).items()}
        closer_roles = {self._canonical_group(k): [normalize_role(r) for r in v] for k, v in (self.anchors.get("closer_roles") or {}).items()}
        allow_cashier_close = bool(self.anchors.get("allow_cashier_closer", False))

        by_day: Dict[int, List[BlockDemand]] = {}
        for demand in demands:
            by_day.setdefault(demand.day_index, []).append(demand)

        for day_demands in by_day.values():
            self._apply_anchor_caps(day_demands, opener_caps, opener_roles, block_label="open")
            blocked_groups = set()
            if not allow_cashier_close:
                blocked_groups.add("Cashier")
            self._apply_anchor_caps(day_demands, closer_caps, closer_roles, block_label="close", blocked_groups=blocked_groups)
            self._dedupe_anchor_assignments(day_demands, "open")

    def _apply_anchor_caps(
        self,
        day_demands: List[BlockDemand],
        caps: Dict[str, int],
        role_preferences: Dict[str, List[str]],
        *,
        block_label: str,
        blocked_groups: Optional[Set[str]] = None,
    ) -> None:
        targets = [d for d in day_demands if d.block_name.strip().lower() == block_label]
        if not targets:
            return
        blocked_groups = blocked_groups or set()
        for demand in list(targets):
            if demand.role_group in blocked_groups:
                demand.need = 0
                demand.minimum = 0
        for demand in list(targets):
            if demand.role_group not in caps:
                demand.need = 0
                demand.minimum = 0
        for group_name, count in caps.items():
            if count <= 0:
                for demand in [d for d in targets if d.role_group == group_name]:
                    demand.need = 0
                    demand.minimum = 0
                continue
            candidates = [d for d in targets if d.role_group == group_name and d.need >= 0]
            if not candidates:
                continue
            prefs = role_preferences.get(group_name, [])
            chosen_list = self._pick_preferred_candidates(candidates, prefs, count)
            chosen_set = {id(item) for item in chosen_list}
            for demand in candidates:
                if id(demand) in chosen_set:
                    demand.need = 1
                    demand.minimum = max(1, demand.minimum)
                    demand.allow_cuts = False
                else:
                    demand.need = 0
                    demand.minimum = 0

    @staticmethod
    def _pick_preferred_candidates(
        candidates: List[BlockDemand], preferred_roles: List[str], target_count: int
    ) -> List[BlockDemand]:
        if not candidates or target_count <= 0:
            return []
        normalized_preferences = [normalize_role(role) for role in preferred_roles]

        def rank(demand: BlockDemand) -> Tuple[int, float]:
            normalized = normalize_role(demand.role)
            try:
                preferred_index = normalized_preferences.index(normalized)
            except ValueError:
                preferred_index = len(normalized_preferences)
            return (preferred_index, -demand.priority)

        return sorted(candidates, key=rank)[:target_count]

    @staticmethod
    def _dedupe_anchor_assignments(day_demands: List[BlockDemand], block_label: str) -> None:
        seen: Set[Tuple[str, str]] = set()
        for demand in sorted(
            [d for d in day_demands if d.block_name.strip().lower() == block_label],
            key=lambda d: (-d.priority, d.start),
        ):
            key = (demand.role_group, demand.block_name.strip().lower())
            if demand.need > 0 and key not in seen:
                seen.add(key)
                continue
            if demand.need > 0:
                demand.need = 0
                demand.minimum = 0

    @staticmethod
    def _server_cut_preference() -> List[str]:
        """Server roles cut order: patio -> dining -> cocktail."""
        return ["server - patio", "server - dining", "server - cocktail"]

    def _annotate_cut_windows(self, demands: List[BlockDemand]) -> None:
        if not demands:
            return
        # First pass: generate base cut candidates.
        candidates: List[BlockDemand] = []
        for demand in demands:
            if demand.need <= 0 or not demand.allow_cuts:
                continue
            cut_time = self._recommend_cut_time(demand)
            if not cut_time:
                continue
            demand.recommended_cut = cut_time
            candidates.append(demand)

        if not candidates:
            return

        # Second pass: stagger within each day/group to avoid all cuts firing at once
        # and pull harder when a group is over budget for that day.
        by_day_group: Dict[Tuple[int, str], List[BlockDemand]] = {}
        for d in candidates:
            key = (d.day_index, d.role_group)
            by_day_group.setdefault(key, []).append(d)

        for (day_idx, group), bucket in by_day_group.items():
            pressure = self.group_pressure.get(day_idx, {}).get(group, 1.0)
            stagger_step = 10
            if pressure >= 1.1:
                stagger_step = 15
            if pressure >= 1.3:
                stagger_step = 20

            if group == "Servers":
                preferences = self._server_cut_preference()

                def sort_key(demand: BlockDemand) -> Tuple[Any, ...]:
                    normalized = normalize_role(demand.role)
                    try:
                        pref_index = preferences.index(normalized)
                    except ValueError:
                        pref_index = len(preferences)
                    return (pref_index, demand.start, -demand.priority, demand.recommended_cut or demand.end)

            else:

                def sort_key(demand: BlockDemand) -> Tuple[Any, ...]:
                    return (demand.start, -demand.priority, demand.recommended_cut or demand.end)

            bucket.sort(key=sort_key)
            for offset, demand in enumerate(bucket):
                slot_cut = demand.recommended_cut or demand.end
                # Pull earlier if budget pressure is high for that group/day.
                if pressure > 1.0:
                    extra_pull = int(min(120, 30 + (pressure - 1.0) * 90))
                    slot_cut = slot_cut - datetime.timedelta(minutes=extra_pull)
                # Stagger to avoid simultaneous releases.
                slot_cut = slot_cut - datetime.timedelta(minutes=stagger_step * offset)
                # Enforce minimum shift length and not after original end.
                min_hours, max_hours = shift_length_limits(self.policy, demand.role, demand.role_group)
                min_duration = datetime.timedelta(minutes=int(min_hours * 60))
                if slot_cut < demand.start + min_duration:
                    slot_cut = demand.start + min_duration
                if slot_cut >= demand.end:
                    continue
                demand.recommended_cut = slot_cut
                label = f"cut around {slot_cut.strftime('%H:%M')}"
                if label not in demand.labels:
                    demand.labels.append(label)

    def _recommend_cut_time(self, demand: BlockDemand) -> Optional[datetime.datetime]:
        if self._is_closer_block(demand.role, demand.block_name):
            return None
        if demand.block_name.strip().lower() == "open":
            return None

        role_cfg = self.roles_config.get(demand.role, {})
        group_cfg = self.role_group_settings.get(demand.role_group, {})
        group_name = demand.role_group
        base_buffer = role_cfg.get("cut_buffer_minutes", group_cfg.get("cut_buffer_minutes", 30))
        try:
            buffer_minutes = int(base_buffer)
        except (TypeError, ValueError):
            buffer_minutes = 30

        demand_index = 1.0
        if 0 <= demand.day_index < len(self.day_contexts):
            demand_index = self.day_contexts[demand.day_index].get("indices", {}).get("demand_index", 1.0)

        pressure_ratio = self.group_pressure.get(demand.day_index, {}).get(demand.role_group, 1.0)
        priority_rank = self.cut_priority_rank.get(demand.role_group, 2)
        min_hours, max_hours = shift_length_limits(self.policy, demand.role, demand.role_group)
        # Allow faster releases than the nominal minimum when trimming: 1.5h floor for non-closers.
        if not self._is_closer_block(demand.role, demand.block_name):
            min_hours = min(min_hours, 1.5)

        normalized_role = normalize_role(demand.role)
        demand_softness = max(0.0, 1.0 - demand_index)  # softer = closer to 1
        pressure_factor = max(0.0, pressure_ratio - 1.0)
        cashier_bias = 1.0 if ("cashier" in normalized_role or "takeout" in normalized_role or "to-go" in normalized_role) else 0.0

        group_bias = 0
        if group_name in {"Cashier"}:
            group_bias = 90
        elif group_name in {"Servers"}:
            group_bias = 70
        elif group_name in {"Kitchen"}:
            group_bias = 55
        else:
            group_bias = 35

        # Pull earlier when demand is soft or the group is over budget.
        early_pull_minutes = int(demand_softness * 150) + int(
            min(320, (pressure_factor * 260) + priority_rank * 24 + group_bias)
        )
        if pressure_ratio >= 1.2:
            early_pull_minutes += 30
        if pressure_ratio >= 1.4:
            early_pull_minutes += 45
        if pressure_ratio >= 1.7:
            early_pull_minutes += 55
        if pressure_ratio >= 2.0:
            early_pull_minutes += 70
        if cashier_bias:
            early_pull_minutes += 45

        relax_level = max(0, self.cut_relax_level)
        if relax_level >= 1:
            early_pull_minutes = max(0, early_pull_minutes - relax_level * 45)

        buffer_minutes = max(0, buffer_minutes + early_pull_minutes)
        candidate = demand.end - datetime.timedelta(minutes=buffer_minutes)

        block_lower = demand.block_name.strip().lower()
        block_len = demand.end - demand.start
        if block_len.total_seconds() > 0:
            softness = max(0.0, 1.0 - demand_index)
            if block_lower == "mid":
                target_frac = 0.5 - 0.25 * softness
            elif block_lower == "pm":
                target_frac = 0.6 - 0.22 * softness
            else:
                target_frac = 0.7 - 0.18 * softness
            target_frac = max(0.3, min(0.85, target_frac))
            if relax_level >= 1:
                target_frac = min(0.98, target_frac + 0.05 * relax_level)
            duration_target = block_len.total_seconds() * target_frac
            # Scale harder when the group is over budget to hit closer to 100% of allocation.
            if pressure_ratio > 1.0:
                budget_scale = max(0.25, 1.0 / min(2.5, pressure_ratio + 0.1))
                duration_target = min(duration_target, block_len.total_seconds() * budget_scale)
            target_time = demand.start + datetime.timedelta(seconds=duration_target)
            if target_time < candidate:
                candidate = target_time

        min_duration = datetime.timedelta(minutes=int(min_hours * 60))
        max_span = datetime.timedelta(minutes=int(max_hours * 60))
        if candidate < demand.start + min_duration:
            candidate = demand.start + min_duration
        if demand.end - demand.start > max_span:
            # Encourage earlier cuts to respect max shift length.
            max_candidate = demand.start + max_span
            if candidate > max_candidate:
                candidate = max_candidate
        if candidate >= demand.end:
            return None
        return candidate

    def _group_budget_for_day(self, day_index: int, group_name: str) -> Optional[float]:
        if not self.group_budget_by_day:
            return None
        if 0 <= day_index < len(self.group_budget_by_day):
            return self.group_budget_by_day[day_index].get(group_name)
        return None

    @staticmethod
    def _slot_cost(demand: BlockDemand) -> float:
        return max(0.0, demand.duration_hours * max(0.0, demand.hourly_rate or 0.0))

    @staticmethod
    def _compute_cost(start: datetime.datetime, end: datetime.datetime, rate: float) -> float:
        hours = max(0.0, (end - start).total_seconds() / 3600)
        return round(hours * max(0.0, rate or 0.0), 2)

    def _day_label(self, day_index: int) -> str:
        if 0 <= day_index < len(self.day_contexts):
            return self.day_contexts[day_index].get("weekday_token") or f"Day {day_index + 1}"
        return f"Day {day_index + 1}"

    def _is_opener_block(self, role_name: str, block_name: str) -> bool:
        normalized_role = normalize_role(role_name)
        block_label = (block_name or "").strip().lower()
        if block_label == "open":
            return True
        if not normalized_role:
            return False
        return "opener" in normalized_role

    def _role_allows_open_shift(self, role_name: str) -> bool:
        normalized_role = normalize_role(role_name)
        if not normalized_role:
            return False
        allowed = {
            "kitchen opener",
            "bartender",
            "bartender - opener",
            "server",
            "server - dining",
            "server - dining opener",
            "server - cocktail",
            "server - cocktail opener",
        }
        return normalized_role in allowed

    def _is_closer_block(self, role_name: str, block_name: str) -> bool:
        normalized_role = normalize_role(role_name)
        if not normalized_role:
            return False
        return "closer" in normalized_role and block_name.strip().lower() == "close"

    def _is_anchor_demand(self, demand: BlockDemand) -> bool:
        normalized_role = normalize_role(demand.role)
        role_cfg = role_definition(self.policy, demand.role)
        return (
            normalized_role in self.non_cuttable_roles
            or not demand.allow_cuts
            or self._is_closer_block(demand.role, demand.block_name)
            or self._is_opener_block(demand.role, demand.block_name)
            or demand.always_on
            or bool(role_cfg.get("critical"))
        )

    def _demand_window_minutes(self, demand: BlockDemand) -> Tuple[int, int]:
        start_delta_days = (demand.start.date() - demand.date).days
        end_delta_days = (demand.end.date() - demand.date).days
        start_minutes = demand.start.hour * 60 + demand.start.minute + start_delta_days * 24 * 60
        end_minutes = demand.end.hour * 60 + demand.end.minute + end_delta_days * 24 * 60
        if end_minutes < start_minutes:
            end_minutes = start_minutes
        return start_minutes, end_minutes

    def _demand_day_segments(self, demand: BlockDemand) -> List[Tuple[int, int, int]]:
        start_minutes, end_minutes = self._demand_window_minutes(demand)
        segments: List[Tuple[int, int, int]] = []
        cursor = start_minutes
        minutes_per_day = 24 * 60
        while cursor < end_minutes:
            day_offset = cursor // minutes_per_day
            day_start = day_offset * minutes_per_day
            day_end = day_start + minutes_per_day
            segment_end = min(end_minutes, day_end)
            segments.append(
                (
                    int(day_offset),
                    int(cursor - day_start),
                    int(segment_end - day_start),
                )
            )
            cursor = segment_end
        return segments or [(0, 0, 0)]

    def _threshold_adjustment(self, role_cfg: Dict[str, Any], block_cfg: Dict[str, Any], day_index: int) -> int:
        thresholds = []
        if isinstance(block_cfg, dict):
            thresholds = block_cfg.get("thresholds") or []
        if not thresholds and isinstance(role_cfg, dict):
            thresholds = role_cfg.get("thresholds") or []
        if not thresholds:
            return 0
        indices: Dict[str, float] = {}
        if 0 <= day_index < len(self.day_contexts):
            indices = self.day_contexts[day_index].get("indices", {})
        adjustment = 0
        for rule in thresholds:
            if not isinstance(rule, dict):
                continue
            metric = (rule.get("metric") or "demand_index").strip()
            if not metric:
                continue
            value = indices.get(metric)
            if value is None:
                continue
            gte = self._to_float(rule.get("gte"), default=0.0)
            lte = rule.get("lte")
            if value < gte:
                continue
            if lte is not None and value > self._to_float(lte, default=value):
                continue
            add = self._to_int(rule.get("add"), default=0)
            adjustment += add
        return adjustment

    def _assign(self, demands: List[BlockDemand]) -> List[Dict[str, Any]]:
        assignments: List[Dict[str, Any]] = []
        essential_batch: List[Tuple[BlockDemand, int]] = []
        extra_batch: List[Tuple[BlockDemand, int]] = []
        for demand in demands:
            required = min(max(0, demand.minimum), demand.need)
            remaining = max(0, demand.need - required)
            if required > 0:
                essential_batch.append((demand, required))
            if remaining > 0:
                extra_batch.append((demand, remaining))
        essential_key: Callable[[Tuple[BlockDemand, int]], Tuple[Any, ...]] = lambda entry: (
            entry[0].day_index,
            entry[0].start,
            -entry[0].priority,
            entry[0].role,
        )
        extra_key: Callable[[Tuple[BlockDemand, int]], Tuple[Any, ...]] = lambda entry: (
            -entry[0].priority,
            entry[0].day_index,
            entry[0].start,
            entry[0].role,
        )
        assignments.extend(self._process_demand_batch(essential_batch, order_key=essential_key))
        assignments.extend(self._process_demand_batch(extra_batch, order_key=extra_key))
        return assignments

    def _process_demand_batch(
        self,
        batch: List[Tuple[BlockDemand, int]],
        *,
        order_key: Callable[[Tuple[BlockDemand, int]], Tuple[Any, ...]],
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        if not batch:
            return results
        for demand, count in sorted(batch, key=order_key):
            entries: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []
            for _ in range(count):
                candidate = self._select_employee(demand)
                if not candidate:
                    entries.append((self._build_assignment_payload(None, demand, override_end=demand.end), None))
                    self.warnings.append(
                        f"No coverage for {demand.role} on {demand.date.isoformat()} "
                        f"{demand.start.strftime('%H:%M')} - {demand.end.strftime('%H:%M')} ({demand.block_name})"
                    )
                    continue
                self._register_assignment(candidate, demand)
                entries.append((self._build_assignment_payload(candidate, demand, override_end=demand.end), candidate))
            self._apply_staggered_cuts_for_demand(demand, entries)
            results.extend(payload for payload, _ in entries)
        return results

    def _apply_staggered_cuts_for_demand(
        self,
        demand: BlockDemand,
        entries: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]],
    ) -> None:
        """Stagger cut times within a block, prioritizing longest-worked staff first."""
        if not entries:
            return

        base_labels = [
            label
            for label in demand.labels
            if not label.lower().startswith("cut around") and label.lower() != "pattern"
        ]
        if not base_labels:
            base_labels = [demand.block_name]

        block_is_close = demand.block_name.strip().lower() == "close"
        is_closer = self._is_closer_block(demand.role, demand.block_name) or block_is_close

        min_hours, _ = shift_length_limits(self.policy, demand.role, demand.role_group)
        if not is_closer:
            # Allow shorter-than-policy shifts *a little* for non-closers,
            # but don't go all the way to zero.
            min_hours = min(min_hours, 1.5)
        min_duration = datetime.timedelta(minutes=int(min_hours * 60)) if not is_closer else datetime.timedelta()
        block_len = demand.end - demand.start
        if min_duration > block_len:
            min_duration = block_len

        latest_cut = demand.recommended_cut
        if not demand.allow_cuts or not latest_cut:
        # No cuts allowed or no recommended cut -> everyone works full block.
            for payload, employee in entries:
                end_time = demand.end
                if end_time < demand.start + min_duration:
                    end_time = demand.start + min_duration
                if end_time > demand.end:
                    end_time = demand.end
                self._finalize_assignment_payload(payload, employee, end_time, base_labels, demand)
            return

        total_slots = len(entries)
        if total_slots <= 0:
            return

        core_slots = min(total_slots, max(0, demand.minimum))
        cuttable_slots = max(0, total_slots - core_slots)

        if latest_cut > demand.end:
            latest_cut = demand.end
        if latest_cut < demand.start + min_duration:
            latest_cut = demand.start + min_duration

        start_minutes, _ = self._demand_window_minutes(demand)
        pressure = self.group_pressure.get(demand.day_index, {}).get(demand.role_group, 1.0)
        demand_index = 1.0
        if 0 <= demand.day_index < len(self.day_contexts):
            demand_index = self.day_contexts[demand.day_index].get("indices", {}).get("demand_index", 1.0)

        stagger_step = 12
        if pressure >= 1.15:
            stagger_step = 10
        if pressure >= 1.35:
            stagger_step = 8
        if pressure >= 1.6:
            stagger_step = 6
        if demand_index < 0.9:
            stagger_step += 3
        stagger_step = max(4, min(20, stagger_step))

        softness = max(0.0, 1.0 - demand_index)
        jitter_range = int(round(softness * 4 + max(0.0, pressure - 1.0) * 3))

        # Rank potential cuts.
        scored: List[Tuple[float, int]] = []
        fifo_mode = self.open_close_order_mode
        for idx, (_payload, employee) in enumerate(entries):
            base_score = self._cut_priority_score(employee, demand, start_minutes, pressure)
            fifo_bonus = 0.0
            if fifo_mode in {"prefer", "enforce"}:
                fifo_bonus = self._fifo_order_weight(employee, demand, start_minutes)
            if fifo_mode == "enforce":
                # Strong FIFO: base score secondary to start order.
                score = (fifo_bonus * 5.0) + (base_score * 0.25)
            elif fifo_mode == "prefer":
                score = base_score + fifo_bonus
            else:
                score = base_score
            scored.append((score, idx))
        scored.sort(key=lambda item: item[0], reverse=True)

        early_indices = [idx for _score, idx in scored[:cuttable_slots]]
        planned_end_times: List[datetime.datetime] = [latest_cut for _ in entries]
        planned_labels: List[List[str]] = [list(base_labels) for _ in entries]

        span = max(datetime.timedelta(), latest_cut - (demand.start + min_duration))
        span_minutes = max(0, int(span.total_seconds() // 60))
        linear_step = span_minutes / max(1, len(early_indices) - 1) if len(early_indices) > 1 else span_minutes
        linear_step = max(4, min(stagger_step, int(linear_step))) if linear_step > 0 else stagger_step

        for order, idx in enumerate(early_indices):
            payload, employee = entries[idx]
            jitter = self.random.randint(-jitter_range, jitter_range) if jitter_range else 0
            cut_time = latest_cut - datetime.timedelta(minutes=(linear_step * order) + jitter)
            floor_time = demand.start + min_duration
            if cut_time < floor_time:
                cut_time = floor_time
            if cut_time > latest_cut:
                cut_time = latest_cut
            planned_end_times[idx] = cut_time
            # NOTE: do not label here; labels are assigned after chronological sort.

        remaining_indices = [idx for _score, idx in scored if idx not in early_indices]
        trailing_step = max(3, min(10, (stagger_step // 2) + 2))
        for order, idx in enumerate(remaining_indices):
            payload, employee = entries[idx]
            back_offset = max(len(remaining_indices) - order - 1, 0)
            cut_time = latest_cut - datetime.timedelta(minutes=trailing_step * back_offset)
            if jitter_range and len(remaining_indices) > 1:
                jitter = self.random.randint(0, jitter_range)
                cut_time = cut_time - datetime.timedelta(minutes=jitter)
            if cut_time < demand.start + min_duration:
                cut_time = demand.start + min_duration
            if cut_time > latest_cut:
                cut_time = latest_cut
            planned_end_times[idx] = cut_time
            planned_labels[idx] = base_labels + [f"final cut around {cut_time.strftime('%H:%M')}"]

        if self.open_close_order_mode != "off" and len(entries) > 1:
            ordering: List[Tuple[int, datetime.datetime]] = []
            for idx, (_payload, employee) in enumerate(entries):
                earliest = start_minutes
                if employee:
                    day_assignments = employee["assignments"].get(demand.day_index, [])
                    if day_assignments:
                        earliest = min([start for start, _ in day_assignments] + [start_minutes])
                ordering.append((earliest, planned_end_times[idx]))
            ordering.sort(key=lambda item: item[0])
            violation = False
            for first, second in zip(ordering, ordering[1:]):
                if first[1] > second[1] + datetime.timedelta(minutes=2):
                    violation = True
                    break
            if violation:
                self.warnings.append(
                    f"Could not fully honor opener/closer order for {demand.role_group} on {self._day_label(demand.day_index)}; review cuts."
                )

        # Now assign "1st/2nd/3rd cut" based on actual chronological cut times.
        if early_indices:
            early_indices_by_time = sorted(early_indices, key=lambda i: planned_end_times[i])
            for ordinal, idx in enumerate(early_indices_by_time, start=1):
                cut_time = planned_end_times[idx]
                planned_labels[idx] = base_labels + [
                    f"{self._ordinal_label(ordinal)} cut around {cut_time.strftime('%H:%M')}"
                ]

        for idx, (payload, employee) in enumerate(entries):
            self._finalize_assignment_payload(payload, employee, planned_end_times[idx], planned_labels[idx], demand)

    def _finalize_assignment_payload(
        self,
        payload: Dict[str, Any],
        employee: Optional[Dict[str, Any]],
        end_time: datetime.datetime,
        labels: List[str],
        demand: BlockDemand,
    ) -> None:
        payload["end"] = end_time
        payload["labor_cost"] = self._compute_cost(payload["start"], end_time, payload.get("labor_rate", 0.0))
        payload["notes"] = ", ".join(labels)
        if employee and end_time < demand.end:
            delta_hours = max(0.0, (demand.end - end_time).total_seconds() / 3600)
            employee["total_hours"] = max(0.0, employee.get("total_hours", 0.0) - delta_hours)

    def _cut_priority_score(
        self,
        employee: Optional[Dict[str, Any]],
        demand: BlockDemand,
        start_minutes: int,
        pressure: float,
    ) -> float:
        """Higher scores mean the employee should be released earlier."""
        if not employee:
            return float("-inf")
        day_assignments = employee["assignments"].get(demand.day_index, [])
        day_minutes = sum(end - start for start, end in day_assignments)
        day_hours = day_minutes / 60.0
        earliest_start = min((start for start, _ in day_assignments), default=start_minutes)
        started_early = earliest_start < start_minutes
        long_day_bonus = 1.5 if day_hours >= 7 else (0.75 if day_hours >= 5 else 0.0)
        weekly_hours = employee.get("total_hours", 0.0)
        pressure_bonus = max(0.0, pressure - 1.0) * 2.0
        return (weekly_hours * 0.7) + (day_hours * 1.6) + (2.0 if started_early else 0.0) + long_day_bonus + pressure_bonus

    def _fifo_order_weight(self, employee: Optional[Dict[str, Any]], demand: BlockDemand, start_minutes: int) -> float:
        """Higher weight means this person should leave earlier based on earliest start (FIFO)."""
        if not employee:
            return 0.0
        day_assignments = employee["assignments"].get(demand.day_index, [])
        if not day_assignments:
            return 0.0
        earliest_start = min(start for start, _ in day_assignments)
        age_minutes = max(0, start_minutes - earliest_start)
        # 30 minute chunks, capped to avoid overpowering budget logic.
        return min(6.0, age_minutes / 30.0)

    @staticmethod
    def _ordinal_label(value: int) -> str:
        if 10 <= value % 100 <= 20:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
        return f"{value}{suffix}"

    def _select_employee(self, demand: BlockDemand) -> Optional[Dict[str, Any]]:
        candidate_sets: List[List[Dict[str, Any]]] = []
        pending_openers = self._pending_opener_candidates(demand)
        if pending_openers:
            candidate_sets.append(pending_openers)
        candidate_sets.append(self.employees)

        for allow_overflow in (False, True):
            for pool in candidate_sets:
                if not pool:
                    continue
                exact_pool = [candidate for candidate in pool if demand.role in candidate.get("roles", set())]
                candidates = exact_pool if exact_pool else pool
                best_candidate = None
                best_score = float("-inf")
                for employee in candidates:
                    if not self._employee_can_cover_role(employee, demand.role):
                        continue
                    if not self._employee_available(employee, demand, allow_desired_overflow=allow_overflow):
                        continue
                    score = self._score_candidate(employee, demand, allow_overflow=allow_overflow)
                    if score > best_score:
                        best_score = score
                        best_candidate = employee
                if best_candidate:
                    return best_candidate
        return None

    def _pending_opener_candidates(self, demand: BlockDemand) -> List[Dict[str, Any]]:
        start_minutes, _ = self._demand_window_minutes(demand)
        tolerance = max(5, self.round_to_minutes)
        normalized_role = normalize_role(demand.role)
        matches: Dict[int, Tuple[int, Dict[str, Any]]] = {}
        for employee in self.employees:
            queue = employee.get("pending_open_links", {}).get(demand.day_index, [])
            if not queue:
                continue
            for link in queue:
                if link.get("fulfilled"):
                    continue
                target_start = link.get("target_start")
                deadline = link.get("deadline", target_start)
                if target_start is None:
                    continue
                if start_minutes < target_start - tolerance or start_minutes > deadline + tolerance:
                    continue
                covers = link.get("covers") or set()
                role_group = link.get("role_group")
                if normalized_role not in covers and demand.role_group != role_group:
                    continue
                existing = matches.get(employee["id"])
                if existing is None or target_start < existing[0]:
                    matches[employee["id"]] = (target_start, employee)
                break
        ordered = sorted(matches.values(), key=lambda entry: (entry[0], entry[1]["id"]))
        return [entry[1] for entry in ordered]

    def _employee_can_cover_role(self, employee: Dict[str, Any], role_name: str) -> bool:
        if not role_name:
            return False
        candidate_roles = employee.get("roles") or set()
        for candidate in candidate_roles:
            if role_matches(candidate, role_name):
                return True
            candidate_cfg = role_definition(self.policy, candidate)
            covers = candidate_cfg.get("covers") if isinstance(candidate_cfg, dict) else []
            if covers and role_name in covers:
                return True
        target_group = role_group(role_name)
        if target_group and target_group in self.interchangeable_groups:
            for candidate in candidate_roles:
                if role_group(candidate) == target_group:
                    return True
        role_cfg = role_definition(self.policy, role_name)
        qualifiers = role_cfg.get("qualifiers") if isinstance(role_cfg, dict) else None
        if qualifiers:
            for qualifier in qualifiers:
                for candidate in candidate_roles:
                    if role_matches(candidate, qualifier):
                        return True
        return False

    def _employee_available(
        self,
        employee: Dict[str, Any],
        demand: BlockDemand,
        *,
        allow_desired_overflow: bool = False,
    ) -> bool:
        assignments = employee["assignments"][demand.day_index]
        demand_start_minutes, demand_end_minutes = self._demand_window_minutes(demand)
        for start_minute, end_minute in assignments:
            if demand_start_minutes < end_minute and demand_end_minutes > start_minute:
                return False
        if not self.allow_split_shifts and assignments:
            return False
        for offset, seg_start, seg_end in self._demand_day_segments(demand):
            day_idx = (demand.day_index + offset) % 7
            for window_start, window_end in employee["unavailability"].get(day_idx, []):
                if seg_start < window_end and seg_end > window_start:
                    return False
        same_day_assignment = demand.day_index in employee["days_with_assignments"]
        last_end = employee["last_assignment_end"]
        if last_end and not same_day_assignment:
            pass
        block_hours = demand.duration_hours
        projected_hours = employee["total_hours"] + block_hours
        if projected_hours > self.max_hours_per_week + 1e-6:
            return False
        role_cap = role_definition(self.policy, demand.role).get("max_weekly_hours")
        try:
            if role_cap and projected_hours > float(role_cap):
                return False
        except (TypeError, ValueError):
            pass
        desired_ceiling = employee.get("desired_ceiling")
        if (
            not allow_desired_overflow
            and desired_ceiling is not None
            and desired_ceiling > 0
            and projected_hours > desired_ceiling + 1e-6
        ):
            return False
        if self._would_violate_consecutive(employee, demand.day_index):
            return False
        return True

    def _would_violate_consecutive(self, employee: Dict[str, Any], day_index: int) -> bool:
        if self.max_consecutive_days <= 0:
            return False
        if day_index in employee["days_with_assignments"]:
            return False
        last_day = employee.get("last_day_index")
        consecutive = employee.get("consecutive_days", 0)
        if last_day is None:
            projected = 1
        elif day_index == last_day:
            projected = consecutive
        elif last_day is not None and day_index == last_day + 1:
            projected = consecutive + 1
        else:
            projected = 1
        return projected > self.max_consecutive_days

    def _score_candidate(
        self,
        employee: Dict[str, Any],
        demand: BlockDemand,
        *,
        allow_overflow: bool = False,
    ) -> float:
        role_cfg = role_definition(self.policy, demand.role)
        priority = 0.5
        try:
            priority = float(role_cfg.get("priority", 0.5))
        except (TypeError, ValueError):
            priority = 0.5
        block_hours = demand.duration_hours
        projected_hours = employee["total_hours"] + block_hours
        desired = employee["desired_hours"] or employee.get("desired_ceiling") or self.max_hours_per_week
        desired = desired or self.max_hours_per_week
        floor = max(0.0, employee.get("desired_floor", 0.0))
        ceiling = max(floor, employee.get("desired_ceiling", self.max_hours_per_week) or self.max_hours_per_week)
        window_span = max(1.0, ceiling - floor)
        if projected_hours < floor:
            coverage_focus = 1.0 + (floor - projected_hours) / max(1.0, desired)
        elif projected_hours <= ceiling:
            coverage_focus = 0.6 * (ceiling - projected_hours) / window_span
        else:
            overflow = projected_hours - ceiling
            coverage_focus = -overflow / max(1.0, desired)
            if not allow_overflow:
                coverage_focus -= 0.5
        day_minutes = sum(end - start for start, end in employee["assignments"][demand.day_index])
        day_hours = day_minutes / 60.0
        continuity = 0.2 if self._continues_assignment(employee, demand) else 0.0
        if role_group(demand.role) == "Servers":
            continuity *= 0.5
        is_closer = self._is_closer_block(demand.role, demand.block_name) or demand.block_name.strip().lower() == "close"
        if is_closer:
            if employee["assignments"][demand.day_index]:
                continuity += 0.4
            else:
                continuity -= 0.3
        day_load = len(employee["assignments"][demand.day_index])
        availability_bonus = max(-0.15, 0.3 - 0.1 * day_load)
        day_fairness = max(-0.4, 0.15 * (1 - (day_hours / 6.0)))  # prefer those working less today
        if day_hours >= 7.0:
            day_fairness -= 0.25
        wage_penalty = self._role_wage(demand.role) * 0.02
        overtime_penalty = self.overtime_penalty if projected_hours > self.max_hours_per_week else 0.0
        consecutive_penalty = 0.05 * max(0, employee.get("consecutive_days", 0) - 3)
        distribution_bonus = max(-0.2, 0.2 * (1 - (employee["total_hours"] / max(1.0, ceiling))))
        return (
            priority
            + coverage_focus
            + continuity
            + availability_bonus
            + day_fairness
            + distribution_bonus
            - wage_penalty
            - overtime_penalty
            - consecutive_penalty
            + self.random.uniform(-0.05, 0.05)
        )

    def _continues_assignment(self, employee: Dict[str, Any], demand: BlockDemand) -> bool:
        last_end = employee["day_last_block_end"][demand.day_index]
        start_minutes, _ = self._demand_window_minutes(demand)
        if last_end is None:
            return False
        tolerance = max(1, self.round_to_minutes)
        return abs(start_minutes - last_end) <= tolerance

    def _closer_has_continuity(self, assignments: List[Tuple[int, int]], demand_start_minutes: int) -> bool:
        if not assignments:
            return False
        tolerance = max(5, self.round_to_minutes)
        latest_end = max(end for _start, end in assignments)
        earliest_start = min(start for start, _end in assignments)
        return latest_end >= demand_start_minutes - tolerance and earliest_start < demand_start_minutes

    def _register_assignment(self, employee: Dict[str, Any], demand: BlockDemand) -> None:
        start_minutes, end_minutes = self._demand_window_minutes(demand)
        employee["assignments"][demand.day_index].append((start_minutes, end_minutes))
        employee["day_last_block_end"][demand.day_index] = end_minutes
        employee["total_hours"] += demand.duration_hours
        employee["last_assignment_end"] = demand.end
        if demand.day_index not in employee["days_with_assignments"]:
            last_day = employee.get("last_day_index")
            if last_day is not None and demand.day_index == last_day + 1:
                employee["consecutive_days"] = employee.get("consecutive_days", 0) + 1
            else:
                employee["consecutive_days"] = 1
            employee["last_day_index"] = demand.day_index
            employee["days_with_assignments"].add(demand.day_index)
        self._track_opener_continuity(employee, demand, start_minutes, end_minutes)

    def _track_opener_continuity(
        self,
        employee: Optional[Dict[str, Any]],
        demand: BlockDemand,
        start_minutes: int,
        end_minutes: int,
    ) -> None:
        if not employee:
            return
        if self._is_opener_block(demand.role, demand.block_name):
            self._create_open_link_requirement(employee, demand, end_minutes)
        else:
            self._fulfill_open_link_requirement(employee, demand, start_minutes)

    def _create_open_link_requirement(self, employee: Dict[str, Any], demand: BlockDemand, end_minutes: int) -> None:
        queue = employee["pending_open_links"].setdefault(demand.day_index, [])
        role_cfg = role_definition(self.policy, demand.role)
        covers = {normalize_role(demand.role)}
        for cover in role_cfg.get("covers", []) or []:
            normalized = normalize_role(cover)
            if normalized:
                covers.add(normalized)
        tolerance = max(5, self.round_to_minutes)
        queue.append(
            {
                "target_start": end_minutes,
                "deadline": end_minutes + tolerance,
                "covers": covers,
                "role_group": demand.role_group,
                "role": demand.role,
                "fulfilled": False,
            }
        )

    def _fulfill_open_link_requirement(self, employee: Dict[str, Any], demand: BlockDemand, start_minutes: int) -> None:
        queue = employee["pending_open_links"].get(demand.day_index)
        if not queue:
            return
        normalized_role = normalize_role(demand.role)
        tolerance = max(5, self.round_to_minutes)
        for idx, link in enumerate(list(queue)):
            if link.get("fulfilled"):
                continue
            target_start = link.get("target_start")
            if target_start is None:
                continue
            deadline = link.get("deadline", target_start)
            if start_minutes < target_start - tolerance or start_minutes > deadline + tolerance:
                continue
            covers = link.get("covers") or set()
            role_group = link.get("role_group")
            if normalized_role not in covers and demand.role_group != role_group:
                continue
            queue.pop(idx)
            break

    def _enforce_shift_continuity(self, assignments: List[Dict[str, Any]], week_start: datetime.date) -> None:
        if not assignments:
            return
        day_map = defaultdict(list)
        for payload in assignments:
            day_index = self._day_index_from_datetime(week_start, payload["start"])
            day_map[day_index].append(payload)
        self._ensure_opener_followups(day_map, assignments, week_start)
        self._ensure_closer_continuity(day_map, assignments, week_start)

    def _ensure_opener_followups(
        self,
        day_map: Dict[int, List[Dict[str, Any]]],
        assignments: List[Dict[str, Any]],
        week_start: datetime.date,
    ) -> None:
        tolerance = datetime.timedelta(minutes=max(5, self.round_to_minutes))
        for employee in self.employees:
            links_by_day = employee.get("pending_open_links") or {}
            for day_index, links in list(links_by_day.items()):
                if not links:
                    continue
                active_shifts = [
                    shift
                    for shift in day_map.get(day_index, [])
                    if (shift.get("location") or "").strip().lower() not in {"open", "close"}
                ]
                if not active_shifts:
                    links_by_day[day_index] = []
                    continue
                for link in list(links):
                    if link.get("fulfilled"):
                        continue
                    target_dt = self._day_datetime(week_start, day_index, link.get("target_start", 0))
                    if self._assign_existing_followup(active_shifts, employee, target_dt, link, tolerance):
                        links.remove(link)
                        continue
                    if self._create_open_followup_shift(
                        assignments, day_map, day_index, employee, target_dt, link, week_start, active_shifts
                    ):
                        links.remove(link)
                if not links:
                    links_by_day[day_index] = []

    def _assign_existing_followup(
        self,
        day_shifts: List[Dict[str, Any]],
        employee: Dict[str, Any],
        target_dt: datetime.datetime,
        link: Dict[str, Any],
        tolerance: datetime.timedelta,
    ) -> bool:
        if not day_shifts:
            return False
        emp_id = employee.get("id")
        if not emp_id:
            return True
        normalized_role = normalize_role(link.get("role") or "")
        candidates: List[Dict[str, Any]] = []
        for shift in day_shifts:
            loc = (shift.get("location") or "").strip().lower()
            if loc in {"open", "close"}:
                continue
            if shift.get("employee_id") == emp_id:
                if shift["start"] >= target_dt - tolerance:
                    return True
                continue
            if shift.get("_followup_locked"):
                continue
            if shift["start"] < target_dt - tolerance:
                continue
            if normalized_role and normalize_role(shift["role"]) != normalized_role:
                covers = link.get("covers") or set()
                candidate_group = role_group(shift["role"])
                if normalize_role(shift["role"]) not in covers and link.get("role_group") != candidate_group:
                    continue
            candidates.append(shift)
        if not candidates:
            return False
        chosen = sorted(candidates, key=lambda payload: (payload["start"], payload["end"]))[0]
        self._transfer_shift_employee(chosen, emp_id, tag="Opener follow-up")
        chosen["_followup_locked"] = True
        return True

    def _create_open_followup_shift(
        self,
        assignments: List[Dict[str, Any]],
        day_map: Dict[int, List[Dict[str, Any]]],
        day_index: int,
        employee: Dict[str, Any],
        target_dt: datetime.datetime,
        link: Dict[str, Any],
        week_start: datetime.date,
        day_shifts: List[Dict[str, Any]],
    ) -> bool:
        if not day_shifts or not self.allow_split_shifts:
            return False
        role_name = link.get("role") or next(iter(employee["roles"]), None)
        if not role_name:
            return False
        min_hours, _ = shift_length_limits(self.policy, role_name, link.get("role_group") or role_group(role_name))
        duration = datetime.timedelta(hours=max(2.5, min_hours))
        start_dt = target_dt
        end_dt = target_dt + duration
        payload = self._create_manual_shift(
            employee.get("id"),
            role_name,
            start_dt,
            end_dt,
            location="Follow-up",
            note="Auto follow-up for opener",
        )
        assignments.append(payload)
        day_map[day_index].append(payload)
        return True

    def _ensure_closer_continuity(
        self,
        day_map: Dict[int, List[Dict[str, Any]]],
        assignments: List[Dict[str, Any]],
        week_start: datetime.date,
    ) -> None:
        tolerance = datetime.timedelta(minutes=max(5, self.round_to_minutes))
        for day_index, shifts in list(day_map.items()):
            for shift in list(shifts):
                location = (shift.get("location") or "").strip().lower()
                if location != "close":
                    continue
                emp_id = shift.get("employee_id")
                if not emp_id:
                    continue
                op_day = self._closer_operational_day_index(week_start, shift["start"])
                if self._closer_has_prior_assignment(emp_id, shift["start"], day_map.get(op_day, []), tolerance):
                    continue
                if self._pair_closer_with_existing(emp_id, shift, day_map.get(op_day, []), tolerance):
                    continue
                self._create_closer_lead_in(assignments, day_map, op_day, shift, emp_id)

    def _closer_operational_day_index(self, week_start: datetime.date, start_dt: datetime.datetime) -> int:
        op_date = start_dt.date()
        if start_dt.time() < datetime.time(6, 0):
            op_date = start_dt.date() - datetime.timedelta(days=1)
        return (op_date - week_start).days

    def _closer_has_prior_assignment(
        self,
        employee_id: int,
        close_start: datetime.datetime,
        day_shifts: List[Dict[str, Any]],
        tolerance: datetime.timedelta,
    ) -> bool:
        if not day_shifts:
            return False
        for shift in day_shifts:
            if shift.get("employee_id") != employee_id:
                continue
            loc = (shift.get("location") or "").strip().lower()
            if loc in {"open", "close"}:
                continue
            if shift["end"] > close_start - tolerance:
                return True
        return False

    def _pair_closer_with_existing(
        self,
        employee_id: int,
        close_shift: Dict[str, Any],
        day_shifts: List[Dict[str, Any]],
        tolerance: datetime.timedelta,
    ) -> bool:
        if not day_shifts:
            return False
        candidates: List[Dict[str, Any]] = []
        for shift in day_shifts:
            loc = (shift.get("location") or "").strip().lower()
            if loc in {"open", "close"}:
                continue
            if shift.get("employee_id") == employee_id:
                if shift["end"] > close_shift["start"] - tolerance:
                    return True
                continue
            if shift.get("_followup_locked"):
                continue
            if shift["end"] <= close_shift["start"]:
                candidates.append(shift)
        if not candidates:
            return False
        chosen = sorted(candidates, key=lambda payload: payload["end"], reverse=True)[0]
        self._transfer_shift_employee(chosen, employee_id, tag="Closer lead-in")
        chosen["_followup_locked"] = True
        return True

    def _create_closer_lead_in(
        self,
        assignments: List[Dict[str, Any]],
        day_map: Dict[int, List[Dict[str, Any]]],
        op_day_index: int,
        close_shift: Dict[str, Any],
        employee_id: int,
    ) -> None:
        role_name = close_shift.get("role")
        if not role_name:
            return
        min_hours, _ = shift_length_limits(self.policy, role_name, role_group(role_name))
        duration = datetime.timedelta(hours=max(2.5, min_hours))
        start_dt = close_shift["start"] - duration
        payload = self._create_manual_shift(
            employee_id,
            role_name,
            start_dt,
            close_shift["start"],
            location="Close lead",
            note="Auto lead-in for closer",
        )
        payload["_followup_locked"] = True
        assignments.append(payload)
        day_map[op_day_index].append(payload)

    def _transfer_shift_employee(self, shift: Dict[str, Any], new_employee_id: Optional[int], *, tag: str) -> None:
        old_employee_id = shift.get("employee_id")
        if old_employee_id == new_employee_id or new_employee_id is None:
            return
        shift["employee_id"] = new_employee_id
        shift["notes"] = self._append_note(shift.get("notes"), tag)

    def _create_manual_shift(
        self,
        employee_id: Optional[int],
        role_name: str,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime,
        *,
        location: str,
        note: str,
    ) -> Dict[str, Any]:
        rate = self._role_wage(role_name)
        payload = {
            "employee_id": employee_id,
            "role": role_name,
            "start": start_dt,
            "end": end_dt,
            "labor_rate": rate,
            "labor_cost": self._compute_cost(start_dt, end_dt, rate),
            "location": location,
            "notes": note,
        }
        return payload

    def _append_note(self, existing: Optional[str], addition: str) -> str:
        if not existing:
            return addition
        if addition.lower() in existing.lower():
            return existing
        return f"{existing}, {addition}"

    def _day_datetime(self, week_start: datetime.date, day_index: int, minutes: int) -> datetime.datetime:
        day = week_start + datetime.timedelta(days=day_index)
        return datetime.datetime.combine(day, datetime.time.min, tzinfo=UTC) + datetime.timedelta(minutes=minutes)

    def _day_index_from_datetime(self, week_start: datetime.date, dt: datetime.datetime) -> int:
        return (dt.date() - week_start).days

    def _warn_unpaired_openers(self) -> None:
        for employee in self.employees:
            for day_index, links in (employee.get("pending_open_links") or {}).items():
                if not links:
                    continue
                for _link in links:
                    self.warnings.append(
                        f"Opener continuity missing for {employee['name']} on {self._day_label(day_index)}; add a follow-up shift."
                    )
                links.clear()

    def _build_assignment_payload(
        self,
        employee: Optional[Dict[str, Any]],
        demand: BlockDemand,
        *,
        override_end: Optional[datetime.datetime] = None,
    ) -> Dict[str, Any]:
        rate = self._role_wage(demand.role) if employee else 0.0
        start_time = demand.start
        end_time = override_end or demand.recommended_cut or demand.end
        if end_time <= start_time:
            end_time = demand.end
        hours = max(0.0, (end_time - start_time).total_seconds() / 3600)
        cost = round(hours * rate, 2)
        notes = ", ".join(demand.labels)
        return {
            "employee_id": employee["id"] if employee else None,
            "role": demand.role,
            "start": start_time,
            "end": end_time,
            "labor_rate": rate,
            "labor_cost": cost,
            "location": demand.block_name,
            "notes": notes,
        }

    def _build_summary(self, week: WeekSchedule, shifts: List[Dict[str, Any]]) -> Dict[str, Any]:
        totals = []
        total_cost = 0.0
        total_shifts = 0
        for day_offset in range(7):
            date_value = week.week_start_date + datetime.timedelta(days=day_offset)
            day_shifts = [shift for shift in shifts if shift["start"].date() == date_value]
            day_cost = sum(shift["labor_cost"] for shift in day_shifts if shift["labor_cost"])
            totals.append(
                {
                    "date": date_value.isoformat(),
                    "shifts_created": len(day_shifts),
                    "cost": round(day_cost, 2),
                }
            )
            total_cost += day_cost
            total_shifts += len(day_shifts)
        total_budget = 0.0
        if self.group_budget_by_day:
            for day_budget in self.group_budget_by_day:
                if not isinstance(day_budget, dict):
                    continue
                total_budget += sum(float(value) for value in day_budget.values())
        budget_ratio = total_cost / total_budget if total_budget > 1e-6 else None
        return {
            "week_id": week.id,
            "days": totals,
            "total_cost": round(total_cost, 2),
            "total_shifts": total_shifts,
            "projected_budget_total": round(total_budget, 2),
            "policy_budget_ratio": round(budget_ratio, 4) if budget_ratio is not None else None,
            "warnings": [],
        }

    @staticmethod
    def _parse_projection_notes(raw: Optional[str]) -> Dict[str, Any]:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
