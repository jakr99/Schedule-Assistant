from __future__ import annotations

import datetime
from typing import Callable, Dict

from .engine import ScheduleGenerator
from policy import load_active_policy
from wages import wage_amounts
from validation import validate_week_schedule
from database import EmployeeSessionLocal

DEFAULT_MAX_ATTEMPTS = 3


def generate_schedule_for_week(
    session_factory: Callable,
    week_start_date: datetime.date,
    actor: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    employee_session_factory: Callable = EmployeeSessionLocal,
) -> Dict:
    if week_start_date is None:
        raise ValueError("week_start_date is required.")
    attempts = max(1, int(max_attempts or 1))
    wages = wage_amounts()
    last_error: Exception | None = None
    best_summary: Dict | None = None
    best_ratio: float = -1.0
    policy_cache: Dict | None = None
    budget_target_ratio: float = 0.9
    for attempt in range(1, attempts + 1):
        try:
            with session_factory() as session, employee_session_factory() as employee_session:
                if policy_cache is None:
                    policy_cache = load_active_policy(session)
                    global_cfg = policy_cache.get("global", {}) if isinstance(policy_cache, dict) else {}
                    tolerance = float(global_cfg.get("labor_budget_tolerance_pct", 0.08) or 0.0)
                    if tolerance > 1.0:
                        tolerance /= 100.0
                    budget_target_ratio = max(0.75, 1.0 - (tolerance / 2.0))
                engine = ScheduleGenerator(
                    session,
                    policy_cache,
                    actor=actor or "system",
                    wage_overrides=wages,
                    employee_session=employee_session,
                    cut_relax_level=min(max(0, attempt - 1), 2),
                )
                summary = engine.generate(week_start_date)
                try:
                    validation_report = validate_week_schedule(
                        session,
                        week_start_date,
                        employee_session=employee_session,
                    )
                except Exception:  # noqa: BLE001
                    validation_report = {"issues": [], "warnings": [], "week_start": week_start_date.isoformat()}
                summary["validation"] = validation_report
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue
        ratio = summary.get("policy_budget_ratio")
        budget_total = summary.get("projected_budget_total", 0.0)
        if ratio is None:
            ratio = 1.0 if not budget_total else 0.0
        if ratio > best_ratio:
            best_ratio = ratio
            best_summary = summary
        if summary.get("shifts_created", 0) <= 0:
            last_error = RuntimeError("Generator produced no shifts; retrying with a fresh seed.")
            continue
        if not budget_total or ratio >= budget_target_ratio:
            summary["attempts"] = attempt
            summary["budget_target_ratio"] = round(budget_target_ratio, 4)
            return summary
        last_error = RuntimeError(
            f"Labor budget usage {ratio:.2f} below target {budget_target_ratio:.2f}; retrying."
        )
    if best_summary:
        warnings = best_summary.setdefault("warnings", [])
        warnings.append(
            f"Could not reach labor budget target after {attempts} attempts "
            f"(hit {best_ratio:.2f} vs target {budget_target_ratio:.2f})."
        )
        best_summary["attempts"] = attempts
        best_summary["budget_target_ratio"] = round(budget_target_ratio, 4)
        return best_summary
    raise RuntimeError(f"Schedule generation failed after {attempts} attempts.") from last_error
