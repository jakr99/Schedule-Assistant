# Schedule Assistant

Lightweight desktop app for managing restaurant scheduling context with PySide6 and SQLite.

## Requirements

- Python 3.10+ (3.11 recommended)
- Windows/macOS/Linux with GUI support

## Quick Start

```bash
git clone <repo-url>
cd Schedule Assistant
python launch_app.py
```

`launch_app.py` creates `.venv`, installs `app/requirements.txt`, and starts the UI. On Windows you can run `launch.bat`.

## Default Login

The first launch seeds an IT account automatically when no accounts are present. Use it to create additional roles.

- Username: `it_assistant`
- Password: `letmein`

## Development Notes

- The SQLite database and logs live under `app/data/`.
- The launcher tracks dependency changes with `.venv/.requirements.applied`.
- Use the week selector to set context before editing data.

## Troubleshooting

- If dependencies change, delete `.venv/.requirements.applied` or rerun `python launch_app.py` to trigger a reinstall.
- Delete `app/data/schedule.db` if you need a clean database reset (local only).

