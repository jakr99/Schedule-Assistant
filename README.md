# Schedule Assistant - CS 4090 Capstone

Schedule Assistant is a Buffalo Wild Wings planning tool built with PySide6. It helps the GM/SM team project sales, manage modifiers, define scheduling policies, and generate a week of coverage without touching raw JSON.

Latest feature work (2025-11-14): operating-hours inputs with open/close buffers, anchor-aware time blocks, and a cleaner swap/grant experience on the Week Schedule grid.

## Team
- Aaron Deken
- Jacob Lee

## What it does
- Role-aware login with session timeouts and audit logging.
- Week Preparation pulls projections, modifiers, and policy presets into one place before building the schedule.
- Week Schedule gives drag-friendly coverage editing plus swap/grant helpers.
- Validate / Import / Export moves week assets (employees, projections, modifiers, shifts) between files while tracking each transfer.
- Policy Composer lets GMs adjust global guardrails, time blocks, and per-role coverage without editing JSON.
- Account Manager and Employee Directory dialogs cover user onboarding and roster maintenance.

## Tech stack
- Python 3.10+
- PySide6 (Qt for Python)
- SQLite + SQLAlchemy ORM
- FastAPI scaffold in place for future remote API work
- Designed around Windows 11, but runs anywhere Qt has GUI support

## Project layout
```
Schedule-Assistant/
  app/
    main.py           PySide6 application shell
    database.py       SQLAlchemy models and persistence helpers
    data/             Local SQLite DB, logs, cached week state
    requirements.txt  Runtime dependencies
    ...
  launch_app.py       Cross-platform launcher (creates .venv, installs deps)
  launch.bat          Windows shortcut to the launcher
  history.txt         Release notes
  TODO.txt            Working task list
```

## Getting started
### Quick start
1. Clone the repo and `cd Schedule-Assistant`.
2. Run `python launch_app.py` (or double-click `launch.bat` on Windows).

The launcher creates `.venv`, installs `app/requirements.txt`, and starts the UI.

### Manual setup
```
python -m venv .venv
.\.venv\Scripts\activate          # bash/zsh: source .venv/bin/activate
pip install -r app/requirements.txt
python -m app.main
```

## Default login
The first boot seeds an IT Assistant account so you can sign in and add real users:
- Username: `it_assistant`
- Password: `letmein`

Change that password immediately from the Account Manager dialog once you are in.

## Daily workflow
- **Week Preparation** - pick the ISO week, enter projections, and layer modifiers/events.
- **Week Schedule** - build a draft schedule, tweak coverage, and run swap/grant actions.
- **Validate / Import / Export** - run coverage checks, move data between weeks, or export PDFs/CSVs.
- **Policies** - GM/IT roles can edit the active policy; SMs get read-only access when the flag is enabled.
- **Employee Directory / Account Manager** - maintain roster data and application accounts.

## Policy + automation
The Policy Composer exposes:
- Global guardrails (min rest, max hours, split shift toggle, overtime penalty).
- Anchor-aware time blocks that understand `@open` / `@close` plus buffer minutes.

The generator consumes active policy, projections, modifiers, and availability to build a week plan. High-priority roles fill first, and every assignment honors rest windows plus desired-hour ranges.

## Sample data
Populate a starter roster and realistic availability:
```
python -m app.scripts.seed_employees
```
Run it once on a fresh database to get 30+ FOH/BOH records for demos and tests.

## Development notes & troubleshooting
- Data lives in `app/data/`. Delete `schedule.db` if you need a fresh environment (dev only).
- `python launch_app.py` reuses the last dependency hash; remove `.venv` if you want a clean reinstall.
- Smoke test the workflow:
  ```
  python app/scripts/workflow_smoke.py --week-start YYYY-MM-DD   # week arg optional
  ```
- If UI assets fail to load, confirm the app is running inside the repo root so paths resolve correctly.
