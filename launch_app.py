from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import venv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
VENV_DIR = PROJECT_ROOT / ".venv"
REQUIREMENTS_FILE = PROJECT_ROOT / "app" / "requirements.txt"
REQUIREMENTS_MARKER = VENV_DIR / ".requirements.applied"
APP_ENTRYPOINT = PROJECT_ROOT / "app" / "main.py"


def venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def ensure_virtualenv() -> None:
    if VENV_DIR.exists() and venv_python().exists():
        return

    print(f"[launcher] Creating virtual environment at {VENV_DIR}...")
    builder = venv.EnvBuilder(with_pip=True, upgrade=False, clear=False)
    builder.create(VENV_DIR)


def current_requirements_signature() -> str:
    if not REQUIREMENTS_FILE.exists():
        raise FileNotFoundError(f"Requirements file not found: {REQUIREMENTS_FILE}")
    content = REQUIREMENTS_FILE.read_bytes()
    return hashlib.sha256(content).hexdigest()


def ensure_requirements() -> None:
    python_exec = venv_python()
    signature = current_requirements_signature()

    if REQUIREMENTS_MARKER.exists() and REQUIREMENTS_MARKER.read_text().strip() == signature:
        print("[launcher] Dependencies satisfied.")
        return

    print("[launcher] Making sure pip is up to date...")
    subprocess.check_call(
        [
            str(python_exec),
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip",
        ]
    )

    print(f"[launcher] Applying dependencies from {REQUIREMENTS_FILE}...")
    subprocess.check_call(
        [
            str(python_exec),
            "-m",
            "pip",
            "install",
            "-r",
            str(REQUIREMENTS_FILE),
        ]
    )

    REQUIREMENTS_MARKER.write_text(signature)


def launch_app() -> int:
    ensure_virtualenv()
    ensure_requirements()

    python_exec = venv_python()
    if not APP_ENTRYPOINT.exists():
        raise FileNotFoundError(f"App entrypoint not found: {APP_ENTRYPOINT}")

    print("[launcher] Starting Schedule Assistant...")
    return subprocess.call([str(python_exec), str(APP_ENTRYPOINT)])


if __name__ == "__main__":
    try:
        exit_code = launch_app()
    except subprocess.CalledProcessError as exc:
        print(f"[launcher] Command failed with exit code {exc.returncode}", file=sys.stderr)
        sys.exit(exc.returncode)
    except Exception as exc: 
        print(f"[launcher] {exc}", file=sys.stderr)
        sys.exit(1)
    else:
        sys.exit(exit_code)
