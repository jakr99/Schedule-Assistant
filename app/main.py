from __future__ import annotations

import base64
import calendar
import datetime
import hashlib
import json
import secrets
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from PySide6.QtCore import Qt, QDate, QTime, QEvent, QTimer
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QDateEdit,
    QTimeEdit,
    QVBoxLayout,
    QWidget,
    QHeaderView,
)

from database import (
    Employee,
    EmployeeUnavailability,
    SessionLocal,
    get_all_employees,
    get_all_weeks,
    get_or_create_week,
    init_database,
)


DATA_DIR = Path(__file__).resolve().parent / "data"
ACCOUNTS_FILE = DATA_DIR / "accounts.json"
AUDIT_FILE = DATA_DIR / "audit.log"
WEEK_STATE_FILE = DATA_DIR / "week_state.json"
LEGACY_PASSWORD_SALT = "schedule-assistant-salt"
PBKDF2_ITERATIONS = 200_000
MIN_PASSWORD_LENGTH = 8
DEFAULT_IT_PASSWORD = "letmein"
MAX_FAILED_ATTEMPTS = 5
LOCKOUT_MINUTES = 15
SESSION_WARNING_SECONDS = 9 * 60
SESSION_TIMEOUT_SECONDS = 10 * 60


def week_start_date(iso_year: int, iso_week: int) -> datetime.date:
    return datetime.date.fromisocalendar(iso_year, iso_week, 1)


def week_label(iso_year: int, iso_week: int) -> str:
    start = week_start_date(iso_year, iso_week)
    end = start + datetime.timedelta(days=6)
    start_str = start.strftime("%b %d")
    end_str = end.strftime("%b %d")
    if start.year != end.year:
        start_str = start.strftime("%b %d %Y")
        end_str = end.strftime("%b %d %Y")
    return f"{iso_year} W{iso_week:02d} ({start_str} - {end_str})"


def load_active_week(session_factory) -> Dict[str, Any]:
    iso_year = iso_week = None
    if WEEK_STATE_FILE.exists():
        try:
            data = json.loads(WEEK_STATE_FILE.read_text(encoding="utf-8"))
            iso_year = int(data.get("iso_year"))
            iso_week = int(data.get("iso_week"))
        except (ValueError, TypeError, json.JSONDecodeError):
            iso_year = iso_week = None
    if not iso_year or not iso_week:
        today = datetime.date.today()
        iso_year, iso_week, _ = today.isocalendar()
    label = week_label(iso_year, iso_week)
    with session_factory() as session:
        week = get_or_create_week(session, iso_year, iso_week, label)
        label = week.label
    save_active_week(iso_year, iso_week)
    return {"iso_year": iso_year, "iso_week": iso_week, "label": label}


def save_active_week(iso_year: int, iso_week: int) -> None:
    WEEK_STATE_FILE.write_text(
        json.dumps({"iso_year": iso_year, "iso_week": iso_week}),
        encoding="utf-8",
    )
EMPLOYEE_ROLE_GROUPS = {
    "Managers": [
        "Manager FOH",
        "Manager HOH",
    ],
    "Bartenders": [
        "Bartender",
        "Bartender - Opener",
        "Bartender - Closer",
    ],
    "Cashier / Guest Services": [
        "Cashier",
        "Cashier - To-Go Specialist",
    ],
    "Servers - Dining": [
        "Server - Dining",
        "Server - Dining Opener",
        "Server - Dining Preclose",
        "Server - Dining Closer",
        "Server - Patio",
    ],
    "Servers - Cocktail": [
        "Server - Cocktail",
        "Server - Cocktail Opener",
        "Server - Cocktail Preclose",
        "Server - Cocktail Closer",
    ],
    "Kitchen": [
        "Kitchen Opener",
        "Kitchen Closer",
        "Expo",
        "Grill",
        "Chip",
        "Shake",
    ],
}
EMPLOYEE_ROLE_OPTIONS = [role for group in EMPLOYEE_ROLE_GROUPS.values() for role in group]
DAYS_OF_WEEK = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
THEME_STYLESHEET = """
QWidget {
    background-color: #0b0b0d;
    color: #f7f7f7;
    font-family: 'Segoe UI', sans-serif;
    font-size: 17px;
}

QPushButton {
    background-color: #b8860b;
    color: #0b0b0d;
    border-radius: 7px;
    padding: 10px 17px;
    font-weight: 600;
}

QPushButton:hover {
    background-color: #c99821;
}

QPushButton:disabled {
    background-color: #2a2a2a;
    color: #777777;
}

QLineEdit, QComboBox {
    background-color: #16161d;
    border: 1px solid #2c2c33;
    border-radius: 7px;
    padding: 7px 12px;
    color: #f7f7f7;
}

QLineEdit:focus, QComboBox:focus {
    border-color: #b8860b;
}

QTableWidget {
    background-color: #16161d;
    border: 1px solid #2c2c33;
    border-radius: 10px;
    gridline-color: #2c2c33;
}

QHeaderView::section {
    background-color: #141418;
    color: #f7f7f7;
    padding: 7px;
    border: none;
}
"""


def secure_hash_password(password: str, *, enforce_length: bool = True) -> tuple[str, str]:
    if enforce_length and len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters long.")
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return (
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(derived).decode("ascii"),
    )


def verify_secure_password(password: str, salt_b64: str, hash_b64: str) -> bool:
    try:
        salt = base64.b64decode(salt_b64.encode("ascii"))
        stored = base64.b64decode(hash_b64.encode("ascii"))
    except (base64.binascii.Error, ValueError):
        return False
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return secrets.compare_digest(derived, stored)


def legacy_hash_password(password: str) -> str:
    salted = f"{LEGACY_PASSWORD_SALT}:{password}"
    return hashlib.sha256(salted.encode("utf-8")).hexdigest()


class AccountLockedError(Exception):
    """Raised when an account is locked and cannot authenticate."""

    def __init__(self, until: datetime.datetime) -> None:
        super().__init__("Account locked")
        self.until = until


class AuditLogger:
    """Append-only JSON line logger for security-relevant events."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self.file_path.touch()

    def log(
        self,
        event: str,
        username: Optional[str],
        *,
        role: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        entry = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "event": event,
            "username": username,
        }
        if role:
            entry["role"] = role
        if details:
            entry["details"] = details

        with self.file_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry))
            handle.write("\n")


audit_logger = AuditLogger(AUDIT_FILE)


class AccountStore:
    """Simple JSON-backed account store with role-aware operations."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_seed()

    def _ensure_seed(self) -> None:
        if self.file_path.exists():
            try:
                with self.file_path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
            except json.JSONDecodeError:
                data = {}
        else:
            data = {}

        users = data.get("users") or []
        if users:
            return

        salt, password_hash = secure_hash_password(DEFAULT_IT_PASSWORD, enforce_length=False)
        data["users"] = [
            {
                "username": "it_assistant",
                "display_name": "IT Assistant",
                "role": "IT",
                "password_salt": salt,
                "password_hash": password_hash,
            }
        ]
        self._write(data)

    def _read(self) -> Dict[str, List[Dict[str, str]]]:
        if not self.file_path.exists():
            return {"users": []}
        with self.file_path.open("r", encoding="utf-8") as handle:
            try:
                data = json.load(handle)
            except json.JSONDecodeError:
                data = {}
        data.setdefault("users", [])
        return data

    def _write(self, data: Dict[str, List[Dict[str, str]]]) -> None:
        with self.file_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)

    def _normalize_record(self, record: Dict[str, Any], *, persist: bool = False) -> Dict[str, Any]:
        updated = False
        if "failed_attempts" not in record:
            record["failed_attempts"] = 0
            updated = True
        if "locked_until" not in record:
            record["locked_until"] = None
            updated = True
        if persist and updated:
            data = self._read()
            index, _, data = self._find_user(record["username"], data)
            if index is not None:
                data["users"][index] = record
                self._write(data)
        return record

    def _locked_until(self, record: Dict[str, Any]) -> Optional[datetime.datetime]:
        locked_until = record.get("locked_until")
        if not locked_until:
            return None
        try:
            value = datetime.datetime.fromisoformat(locked_until)
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.timezone.utc)
            return value
        except ValueError:
            return None

    def _find_user(
        self,
        username: str,
        data: Optional[Dict[str, List[Dict[str, str]]]] = None,
    ) -> tuple[Optional[int], Optional[Dict[str, str]], Dict[str, List[Dict[str, str]]]]:
        if data is None:
            data = self._read()
        username_lower = username.lower()
        for index, user in enumerate(data["users"]):
            if user["username"].lower() == username_lower:
                record = self._normalize_record(user)
                data["users"][index] = record
                return index, record, data
        return None, None, data

    def _password_matches(self, record: Dict[str, str], password: str) -> bool:
        password_hash = record.get("password_hash")
        if not password_hash:
            return False

        salt = record.get("password_salt")
        if salt:
            return verify_secure_password(password, salt, password_hash)
        return secrets.compare_digest(legacy_hash_password(password), password_hash)

    def get_user(self, username: str) -> Optional[Dict[str, str]]:
        _, record, _ = self._find_user(username)
        if not record:
            return None
        return {
            "username": record["username"],
            "role": record["role"],
            "display_name": record.get("display_name", record["username"]),
        }

    def list_users(self) -> List[Dict[str, Any]]:
        data = self._read()
        sanitized: List[Dict[str, Any]] = []
        for index, user in enumerate(data["users"]):
            record = self._normalize_record(user)
            data["users"][index] = record
            locked_until = self._locked_until(record)
            sanitized.append(
                {
                    "username": record["username"],
                    "role": record["role"],
                    "display_name": record.get("display_name", record["username"]),
                    "failed_attempts": record.get("failed_attempts", 0),
                    "locked_until": locked_until.isoformat() if locked_until else None,
                }
            )
        self._write(data)
        return sorted(sanitized, key=lambda entry: (entry["role"], entry["username"]))

    def create_user(
        self,
        creator_username: str,
        creator_role: str,
        username: str,
        password: str,
        role: str,
    ) -> None:
        username = username.strip()
        if not username or not password:
            raise ValueError("Username and password are required.")
        if role not in {"GM", "SM"}:
            raise ValueError("Only GM or SM accounts can be created.")
        if creator_role not in {"IT", "GM"}:
            raise PermissionError("You are not allowed to create accounts.")

        _, existing, data = self._find_user(username)
        if existing:
            raise ValueError("Username already exists.")

        salt, password_hash = secure_hash_password(password)
        new_user = {
            "username": username,
            "display_name": username.title(),
            "role": role,
            "password_salt": salt,
            "password_hash": password_hash,
            "failed_attempts": 0,
            "locked_until": None,
        }
        data["users"].append(new_user)
        self._write(data)
        audit_logger.log(
            "account_create",
            username,
            role=role,
            details={
                "created_by": creator_username,
                "created_by_role": creator_role,
            },
        )

    def delete_user(self, deleter_username: str, deleter_role: str, target_username: str) -> None:
        if deleter_role == "SM":
            raise PermissionError("Scheduling Managers cannot delete accounts.")
        index, target, data = self._find_user(target_username)

        if not target:
            raise ValueError("Account not found.")
        if target_username == deleter_username:
            raise PermissionError("You cannot delete your own account.")
        if target["role"] == "IT":
            if deleter_role != "IT":
                raise PermissionError("Only IT can delete IT accounts.")
        if target["role"] == "GM" and deleter_role == "GM":
            raise PermissionError("GMs cannot delete other GMs.")

        if index is not None:
            del data["users"][index]
            self._write(data)
            audit_logger.log(
                "account_delete",
                target_username,
                role=target["role"],
                details={
                    "deleted_by": deleter_username,
                    "deleted_by_role": deleter_role,
                },
            )

    def verify_credentials(self, username: str, password: str) -> Optional[Dict[str, str]]:
        data = self._read()
        index, record, data = self._find_user(username, data)
        if not record:
            return None

        now = datetime.datetime.now(datetime.timezone.utc)
        locked_until = self._locked_until(record)
        if locked_until and locked_until > now:
            raise AccountLockedError(locked_until)
        if locked_until and locked_until <= now:
            record["locked_until"] = None
            record["failed_attempts"] = 0

        if not self._password_matches(record, password):
            record["failed_attempts"] = record.get("failed_attempts", 0) + 1
            locked_time: Optional[datetime.datetime] = None
            if record["failed_attempts"] >= MAX_FAILED_ATTEMPTS:
                locked_time = now + datetime.timedelta(minutes=LOCKOUT_MINUTES)
                record["locked_until"] = locked_time.isoformat()
            data["users"][index] = record
            self._write(data)
            if locked_time:
                raise AccountLockedError(locked_time)
            return None

        if record.get("failed_attempts"):
            record["failed_attempts"] = 0
            record["locked_until"] = None
        if not record.get("password_salt"):
            salt, password_hash = secure_hash_password(password, enforce_length=False)
            record["password_salt"] = salt
            record["password_hash"] = password_hash
        data["users"][index] = record
        self._write(data)

        return {
            "username": record["username"],
            "role": record["role"],
            "display_name": record.get("display_name", record["username"]),
        }

    def change_password(self, username: str, current_password: str, new_password: str) -> None:
        data = self._read()
        index, record, data = self._find_user(username, data)
        if not record:
            raise ValueError("Account not found.")
        if not self._password_matches(record, current_password):
            raise PermissionError("Current password is incorrect.")

        salt, password_hash = secure_hash_password(new_password)
        record["password_salt"] = salt
        record["password_hash"] = password_hash
        record["failed_attempts"] = 0
        record["locked_until"] = None
        data["users"][index] = record
        self._write(data)
        audit_logger.log(
            "password_change",
            username,
            role=record.get("role"),
        )


class LoginDialog(QDialog):
    def __init__(self, store: AccountStore) -> None:
        super().__init__()
        self.store = store
        self.authenticated_user: Optional[Dict[str, str]] = None
        self.setWindowTitle("Schedule Assistant - Sign in")
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        heading = QLabel("<h2 style='color:#b8860b;'>Sign in to Schedule Assistant</h2>")
        subheading = QLabel("Access is limited to authorized staff.")
        subheading.setStyleSheet("color:#ddd;")

        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Username")
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Password")
        self.password_input.setEchoMode(QLineEdit.Password)

        form = QFormLayout()
        form.addRow("Username", self.username_input)
        form.addRow("Password", self.password_input)

        self.error_label = QLabel()
        self.error_label.setStyleSheet("color: #ff6b6b;")
        self.error_label.setWordWrap(True)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.attempt_login)
        button_box.rejected.connect(self.reject)

        layout.addWidget(heading)
        layout.addWidget(subheading)
        layout.addSpacing(10)
        layout.addLayout(form)
        layout.addWidget(self.error_label)
        layout.addWidget(button_box)

    def attempt_login(self) -> None:
        username = self.username_input.text().strip()
        password = self.password_input.text()
        try:
            account = self.store.verify_credentials(username, password)
        except AccountLockedError as exc:
            self.password_input.clear()
            locked_local = exc.until.astimezone()
            message = locked_local.strftime("Account locked until %Y-%m-%d %H:%M %Z.")
            self.error_label.setText(message)
            audit_logger.log(
                "login_failure",
                username,
                details={
                    "reason": "account_locked",
                    "locked_until": exc.until.isoformat(),
                },
            )
            return

        self.password_input.clear()

        if not account:
            self.error_label.setText("Invalid username or password.")
            audit_logger.log(
                "login_failure",
                username,
                details={"reason": "invalid_credentials"},
            )
            return

        self.error_label.setText("")
        self.username_input.setText(account["username"])
        audit_logger.log(
            "login_success",
            account["username"],
            role=account.get("role"),
        )
        self.authenticated_user = account
        self.accept()


class AccountManagerDialog(QDialog):
    def __init__(self, store: AccountStore, active_user: Dict[str, str]) -> None:
        super().__init__()
        self.store = store
        self.active_user = active_user
        self.setWindowTitle("Manage Accounts")
        self.resize(520, 360)
        self._build_ui()
        self.refresh_table()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        intro = QLabel(
            "<b>Provision new accounts or remove access.</b><br>"
            "IT and General Managers can create new GM or SM accounts."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Username", "Role", "Display name", "Status"])
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        layout.addWidget(self.table)

        form_container = QWidget()
        form_layout = QGridLayout(form_container)
        form_layout.addWidget(QLabel("Username"), 0, 0)
        self.new_username = QLineEdit()
        form_layout.addWidget(self.new_username, 0, 1)

        form_layout.addWidget(QLabel("Temporary password"), 1, 0)
        self.new_password = QLineEdit()
        self.new_password.setPlaceholderText(f"Provide starter password (min {MIN_PASSWORD_LENGTH} chars)")
        form_layout.addWidget(self.new_password, 1, 1)

        form_layout.addWidget(QLabel("Role"), 2, 0)
        self.new_role = QComboBox()
        self.new_role.addItems(["GM", "SM"])
        form_layout.addWidget(self.new_role, 2, 1)

        self.feedback_label = QLabel()
        self.feedback_label.setWordWrap(True)
        form_layout.addWidget(self.feedback_label, 3, 0, 1, 2)

        buttons_row = QHBoxLayout()
        self.create_button = QPushButton("Create account")
        self.create_button.clicked.connect(self.handle_create)
        buttons_row.addWidget(self.create_button)

        self.delete_button = QPushButton("Delete selected")
        self.delete_button.clicked.connect(self.handle_delete)
        buttons_row.addWidget(self.delete_button)
        buttons_row.addStretch()

        layout.addWidget(form_container)
        layout.addLayout(buttons_row)

        if self.active_user["role"] == "SM":
            self.create_button.setDisabled(True)
            self.delete_button.setDisabled(True)
            self.feedback_label.setText("Scheduling Managers cannot modify accounts.")

    def refresh_table(self) -> None:
        users = self.store.list_users()
        self.table.setRowCount(len(users))
        for row, user in enumerate(users):
            self.table.setItem(row, 0, QTableWidgetItem(user["username"]))
            self.table.setItem(row, 1, QTableWidgetItem(user["role"]))
            self.table.setItem(row, 2, QTableWidgetItem(user.get("display_name", "")))
            status_text = "Active"
            locked_until_str = user.get("locked_until")
            if locked_until_str:
                try:
                    lock_dt = datetime.datetime.fromisoformat(locked_until_str)
                    if lock_dt.tzinfo is None:
                        lock_dt = lock_dt.replace(tzinfo=datetime.timezone.utc)
                    local_time = lock_dt.astimezone()
                    status_text = f"Locked until {local_time.strftime('%Y-%m-%d %H:%M %Z')}"
                except ValueError:
                    status_text = "Locked"
            elif user.get("failed_attempts"):
                status_text = f"{user['failed_attempts']} failed attempts"
            self.table.setItem(row, 3, QTableWidgetItem(status_text))
        self.table.resizeColumnsToContents()
    def handle_create(self) -> None:
        if self.active_user["role"] not in {"IT", "GM"}:
            self.feedback_label.setText("You are not permitted to create accounts.")
            return

        username = self.new_username.text().strip()
        password = self.new_password.text()
        role = self.new_role.currentText()
        try:
            self.store.create_user(
                self.active_user["username"],
                self.active_user["role"],
                username,
                password,
                role,
            )
        except (ValueError, PermissionError) as exc:
            self.new_password.clear()
            self.feedback_label.setText(f"<span style='color:#ff6b6b;'>{exc}</span>")
            return

        self.feedback_label.setText(f"<span style='color:#3cb371;'>Created {role} account for {username}.</span>")
        self.new_username.clear()
        self.new_password.clear()
        self.refresh_table()

    def handle_delete(self) -> None:
        selected = self.table.selectedItems()
        if not selected:
            self.feedback_label.setText("<span style='color:#ff6b6b;'>Select an account to delete.</span>")
            return
        username = selected[0].text()
        target_role = selected[1].text()

        confirm = QMessageBox.question(
            self,
            "Confirm deletion",
            f"Remove account '{username}' ({target_role})?",
        )
        if confirm != QMessageBox.Yes:
            return

        try:
            self.store.delete_user(
                deleter_username=self.active_user["username"],
                deleter_role=self.active_user["role"],
                target_username=username,
            )
        except (ValueError, PermissionError) as exc:
            self.feedback_label.setText(f"<span style='color:#ff6b6b;'>{exc}</span>")
            return

        self.feedback_label.setText(f"<span style='color:#3cb371;'>Deleted account '{username}'.</span>")
        self.refresh_table()


class WeekSelectorWidget(QWidget):
    def __init__(self, session_factory, active_week: Dict[str, Any], on_change) -> None:
        super().__init__()
        self.session_factory = session_factory
        self.on_change = on_change
        self.active_week = active_week
        self._updating = False
        self._build_ui()
        self.set_active_week(active_week)

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        layout.addWidget(QLabel("Active week"))

        self.week_combo = QComboBox()
        self.week_combo.currentIndexChanged.connect(self._handle_combo_change)
        layout.addWidget(self.week_combo)

        self.week_date = QDateEdit()
        self.week_date.setCalendarPopup(True)
        self.week_date.setDisplayFormat("yyyy-MM-dd")
        layout.addWidget(self.week_date)

        self.apply_button = QPushButton("Select week")
        self.apply_button.clicked.connect(self._handle_apply_clicked)
        layout.addWidget(self.apply_button)

        layout.addStretch(1)

    def set_active_week(self, active_week: Dict[str, Any]) -> None:
        self.active_week = active_week
        with self.session_factory() as session:
            weeks = get_all_weeks(session)
            if not weeks:
                week = get_or_create_week(
                    session,
                    active_week["iso_year"],
                    active_week["iso_week"],
                    active_week["label"],
                )
                weeks = [week]
        self._updating = True
        self.week_combo.clear()
        selected_index = 0
        for idx, week in enumerate(weeks):
            self.week_combo.addItem(week.label, (week.iso_year, week.iso_week))
            if (
                week.iso_year == active_week["iso_year"]
                and week.iso_week == active_week["iso_week"]
            ):
                selected_index = idx
        self.week_combo.setCurrentIndex(selected_index)
        self._updating = False
        self._update_date_edit()

    def _update_date_edit(self) -> None:
        start = week_start_date(self.active_week["iso_year"], self.active_week["iso_week"])
        self.week_date.setDate(QDate(start.year, start.month, start.day))

    def _handle_combo_change(self) -> None:
        if self._updating:
            return
        data = self.week_combo.currentData()
        if not data:
            return
        iso_year, iso_week = data
        label = self.week_combo.currentText()
        self.active_week = {"iso_year": iso_year, "iso_week": iso_week, "label": label}
        self._update_date_edit()
        if self.on_change:
            self.on_change(iso_year, iso_week, label)

    def _handle_apply_clicked(self) -> None:
        qdate = self.week_date.date()
        selected_date = datetime.date(qdate.year(), qdate.month(), qdate.day())
        iso_year, iso_week, _ = selected_date.isocalendar()
        label = week_label(iso_year, iso_week)
        with self.session_factory() as session:
            week = get_or_create_week(session, iso_year, iso_week, label)
            label = week.label
        self.set_active_week({"iso_year": iso_year, "iso_week": iso_week, "label": label})
        if self.on_change:
            self.on_change(iso_year, iso_week, label)


class EmployeeEditDialog(QDialog):
    def __init__(
        self,
        session_factory,
        actor: Dict[str, str],
        employee_id: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.session_factory = session_factory
        self.actor = actor
        self.employee_id = employee_id
        self.employee = None
        self.result_employee_id: Optional[int] = None
        self.result_action: Optional[str] = None
        self.result_snapshot: Dict[str, Any] = {}
        if self.employee_id is not None:
            with self.session_factory() as session:
                self.employee = session.get(Employee, self.employee_id)
        self.setWindowTitle("Edit employee" if self.employee else "Add employee")
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()
        self.name_input = QLineEdit()
        form.addRow("Full name", self.name_input)

        roles_container = QWidget()
        roles_layout = QVBoxLayout(roles_container)
        roles_layout.setContentsMargins(0, 0, 0, 0)

        selector_row = QHBoxLayout()
        self.role_selector = QComboBox()
        self.role_selector.addItem("Select role...", None)
        for group_label, roles in EMPLOYEE_ROLE_GROUPS.items():
            index = self.role_selector.count()
            self.role_selector.addItem(group_label, None)
            model_item = self.role_selector.model().item(index)
            if model_item is not None:
                model_item.setEnabled(False)
                model_item.setSelectable(False)
            for role in roles:
                self.role_selector.addItem(f"  {role}", role)
        selector_row.addWidget(self.role_selector)

        self.add_role_button = QPushButton("Add role")
        self.add_role_button.clicked.connect(self.handle_add_role)
        selector_row.addWidget(self.add_role_button)

        roles_layout.addLayout(selector_row)

        custom_row = QHBoxLayout()
        self.custom_role_input = QLineEdit()
        self.custom_role_input.setPlaceholderText("Custom role")
        self.custom_role_input.returnPressed.connect(self.handle_add_custom_role)
        custom_row.addWidget(self.custom_role_input)
        self.add_custom_role_button = QPushButton("Add custom")
        self.add_custom_role_button.clicked.connect(self.handle_add_custom_role)
        custom_row.addWidget(self.add_custom_role_button)
        roles_layout.addLayout(custom_row)

        self.role_list_widget = QListWidget()
        self.role_list_widget.setSelectionMode(QAbstractItemView.SingleSelection)
        self.role_list_widget.itemSelectionChanged.connect(self._update_role_buttons)
        self.role_list_widget.itemDoubleClicked.connect(lambda *_: self.remove_selected_role())
        roles_layout.addWidget(self.role_list_widget)

        list_button_row = QHBoxLayout()
        self.remove_role_button = QPushButton("Remove selected")
        self.remove_role_button.clicked.connect(self.remove_selected_role)
        list_button_row.addWidget(self.remove_role_button)
        list_button_row.addStretch()
        roles_layout.addLayout(list_button_row)

        form.addRow("Roles", roles_container)

        self.start_month_combo = QComboBox()
        self.start_month_combo.addItem("Not set", None)
        for month_index in range(1, 13):
            self.start_month_combo.addItem(calendar.month_name[month_index], month_index)
        form.addRow("Start month", self.start_month_combo)

        current_year = datetime.datetime.now().year
        self.start_year_combo = QComboBox()
        self.start_year_combo.addItem("Not set", None)
        for year in range(current_year + 2, current_year - 31, -1):
            self.start_year_combo.addItem(str(year), year)
        self._set_start_date_inputs(None, None)
        form.addRow("Start year", self.start_year_combo)

        self.desired_hours_input = QSpinBox()
        self.desired_hours_input.setRange(0, 168)
        self.desired_hours_input.setSuffix(" hrs")
        form.addRow("Desired weekly hours", self.desired_hours_input)

        self.status_combo = QComboBox()
        self.status_combo.addItems(["Active", "Inactive"])
        form.addRow("Status", self.status_combo)

        self.notes_input = QPlainTextEdit()
        self.notes_input.setPlaceholderText("Optional notes")
        self.notes_input.setFixedHeight(80)
        form.addRow("Notes", self.notes_input)

        layout.addLayout(form)

        self.feedback_label = QLabel()
        self.feedback_label.setStyleSheet("color: #ff6b6b;")
        layout.addWidget(self.feedback_label)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self._load_employee_into_form()
        self._update_role_buttons()

    def _load_employee_into_form(self) -> None:
        if not self.employee:
            return
        self._set_start_date_inputs(self.employee.start_month, self.employee.start_year)
        self.name_input.setText(self.employee.full_name)
        self.role_list_widget.clear()
        for role in self.employee.role_list:
            self._add_role_to_list(role, silent=True)
        self.desired_hours_input.setValue(self.employee.desired_hours or 0)
        self.status_combo.setCurrentIndex(0 if self.employee.status == "active" else 1)
        self.notes_input.setPlainText(self.employee.notes or "")

    def _collect_roles(self) -> List[str]:
        roles: List[str] = []
        for index in range(self.role_list_widget.count()):
            role = self.role_list_widget.item(index).text().strip()
            if role:
                roles.append(role)
        return roles

    def _set_start_date_inputs(self, month: Optional[int], year: Optional[int]) -> None:
        month_index = self.start_month_combo.findData(month) if month else 0
        if month_index == -1:
            month_index = 0
        self.start_month_combo.setCurrentIndex(month_index)

        year_index = self.start_year_combo.findData(year) if year else 0
        if year_index == -1:
            year_index = 0
        self.start_year_combo.setCurrentIndex(year_index)

    def handle_add_role(self) -> None:
        role = self.role_selector.currentData()
        if not role:
            return
        if self._add_role_to_list(str(role).strip()):
            self.role_selector.setCurrentIndex(0)
            self.feedback_label.setText("")

    def handle_add_custom_role(self) -> None:
        role = self.custom_role_input.text().strip()
        if not role:
            self.feedback_label.setText("Enter a custom role before adding.")
            return
        if self._add_role_to_list(role):
            self.custom_role_input.clear()
            self.feedback_label.setText("")

    def remove_selected_role(self) -> None:
        current_item = self.role_list_widget.currentItem()
        if not current_item:
            return
        row = self.role_list_widget.row(current_item)
        self.role_list_widget.takeItem(row)
        self.feedback_label.setText("")
        self._update_role_buttons()

    def _add_role_to_list(self, role: str, *, silent: bool = False) -> bool:
        normalized = role.strip()
        if not normalized:
            return False
        for index in range(self.role_list_widget.count()):
            existing = self.role_list_widget.item(index)
            if existing.text().strip().lower() == normalized.lower():
                self.role_list_widget.setCurrentRow(index)
                self._update_role_buttons()
                if not silent:
                    self.feedback_label.setText("Role already assigned.")
                return False
        item = QListWidgetItem(normalized)
        self.role_list_widget.addItem(item)
        self.role_list_widget.setCurrentItem(item)
        self._update_role_buttons()
        if not silent:
            self.feedback_label.setText("")
        return True

    def _update_role_buttons(self) -> None:
        has_selection = self.role_list_widget.currentItem() is not None
        self.remove_role_button.setEnabled(has_selection)

    def accept(self) -> None:  # type: ignore[override]
        full_name = self.name_input.text().strip()
        if not full_name:
            self.feedback_label.setText("Employee name is required.")
            self.name_input.setFocus()
            return
        roles = self._collect_roles()
        desired_hours = self.desired_hours_input.value()
        status = "active" if self.status_combo.currentIndex() == 0 else "inactive"
        notes = self.notes_input.toPlainText().strip()
        selected_month = self.start_month_combo.currentData()
        selected_year = self.start_year_combo.currentData()
        if (selected_month is None) != (selected_year is None):
            self.feedback_label.setText("Select both start month and start year, or leave both unset.")
            return
        start_month = int(selected_month) if selected_month is not None else None
        start_year = int(selected_year) if selected_year is not None else None

        with self.session_factory() as session:
            if self.employee_id is not None:
                employee = session.get(Employee, self.employee_id)
                if not employee:
                    self.feedback_label.setText("Employee record not found.")
                    return
                action = "update"
            else:
                employee = Employee()
                session.add(employee)
                action = "create"

            employee.full_name = full_name
            employee.role_list = roles
            employee.desired_hours = desired_hours
            employee.status = status
            employee.notes = notes
            employee.start_month = start_month
            employee.start_year = start_year
            employee.updated_at = datetime.datetime.now(datetime.timezone.utc)

            session.commit()
            session.refresh(employee)

        self.result_employee_id = employee.id
        self.result_action = action
        self.result_snapshot = {
            "full_name": employee.full_name,
            "roles": employee.role_list,
            "status": employee.status,
            "desired_hours": employee.desired_hours,
            "start_month": employee.start_month,
            "start_year": employee.start_year,
            "start_label": employee.start_date_label,
        }
        super().accept()


class UnavailabilityEntryDialog(QDialog):
    def __init__(
        self,
        day_of_week: Optional[int] = None,
        start_time: Optional[datetime.time] = None,
        end_time: Optional[datetime.time] = None,
    ) -> None:
        super().__init__()
        self.result_data: Optional[Dict[str, Any]] = None
        self.setWindowTitle("Unavailability window")
        self._build_ui(day_of_week, start_time, end_time)

    def _build_ui(
        self,
        day_of_week: Optional[int],
        start_time: Optional[datetime.time],
        end_time: Optional[datetime.time],
    ) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.day_combo = QComboBox()
        self.day_combo.addItems(DAYS_OF_WEEK)
        if day_of_week is not None:
            self.day_combo.setCurrentIndex(day_of_week)
        form.addRow("Day of week", self.day_combo)

        self.start_time_edit = QTimeEdit()
        self.start_time_edit.setDisplayFormat("HH:mm")
        self.start_time_edit.setTime(
            QTime(start_time.hour, start_time.minute) if start_time else QTime(8, 0)
        )
        form.addRow("Starts at", self.start_time_edit)

        self.end_time_edit = QTimeEdit()
        self.end_time_edit.setDisplayFormat("HH:mm")
        self.end_time_edit.setTime(
            QTime(end_time.hour, end_time.minute) if end_time else QTime(17, 0)
        )
        form.addRow("Ends at", self.end_time_edit)

        layout.addLayout(form)

        self.feedback_label = QLabel()
        self.feedback_label.setStyleSheet("color: #ff6b6b;")
        layout.addWidget(self.feedback_label)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self) -> None:  # type: ignore[override]
        start_time = self.start_time_edit.time()
        end_time = self.end_time_edit.time()
        if start_time >= end_time:
            self.feedback_label.setText("End time must be after start time.")
            return
        self.result_data = {
            "day_of_week": self.day_combo.currentIndex(),
            "start_time": datetime.time(start_time.hour(), start_time.minute()),
            "end_time": datetime.time(end_time.hour(), end_time.minute()),
        }
        super().accept()


class UnavailabilityDialog(QDialog):
    def __init__(self, session_factory, actor: Dict[str, str], employee_id: int) -> None:
        super().__init__()
        self.session_factory = session_factory
        self.actor = actor
        self.employee_id = employee_id
        self.entries: List[EmployeeUnavailability] = []
        with self.session_factory() as session:
            employee = session.get(Employee, self.employee_id)
            self.employee_name = employee.full_name if employee else "Employee"
        self.setWindowTitle(f"Unavailability — {self.employee_name}")
        self.resize(520, 360)
        self._build_ui()
        self.refresh_entries()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        hint = QLabel("Add time windows when the employee is unavailable for scheduling.")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Day", "Starts", "Ends"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)

        buttons = QHBoxLayout()
        self.add_button = QPushButton("Add window")
        self.add_button.clicked.connect(self.add_entry)
        buttons.addWidget(self.add_button)

        self.edit_button = QPushButton("Edit")
        self.edit_button.clicked.connect(self.edit_entry)
        buttons.addWidget(self.edit_button)

        self.delete_button = QPushButton("Remove")
        self.delete_button.clicked.connect(self.remove_entry)
        buttons.addWidget(self.delete_button)

        buttons.addStretch()
        layout.addLayout(buttons)

        self.table.itemSelectionChanged.connect(self.update_button_state)
        self.update_button_state()

    def refresh_entries(self) -> None:
        with self.session_factory() as session:
            stmt = (
                select(EmployeeUnavailability)
                .where(EmployeeUnavailability.employee_id == self.employee_id)
                .order_by(
                    EmployeeUnavailability.day_of_week,
                    EmployeeUnavailability.start_time,
                )
            )
            self.entries = list(session.scalars(stmt))

        self.table.setRowCount(len(self.entries))
        for row, entry in enumerate(self.entries):
            day_item = QTableWidgetItem(DAYS_OF_WEEK[entry.day_of_week])
            day_item.setData(Qt.UserRole, entry.id)
            self.table.setItem(row, 0, day_item)
            self.table.setItem(row, 1, QTableWidgetItem(entry.start_time.strftime("%H:%M")))
            self.table.setItem(row, 2, QTableWidgetItem(entry.end_time.strftime("%H:%M")))
        self.table.resizeColumnsToContents()
        self.update_button_state()

    def selected_entry(self) -> Optional[EmployeeUnavailability]:
        selected_rows = self.table.selectionModel().selectedRows() if self.table.selectionModel() else []
        if not selected_rows:
            return None
        row = selected_rows[0].row()
        if 0 <= row < len(self.entries):
            return self.entries[row]
        return None

    def update_button_state(self) -> None:
        has_selection = bool(self.table.selectionModel() and self.table.selectionModel().hasSelection())
        self.edit_button.setEnabled(has_selection)
        self.delete_button.setEnabled(has_selection)

    def add_entry(self) -> None:
        dialog = UnavailabilityEntryDialog()
        dialog.setStyleSheet(THEME_STYLESHEET)
        if dialog.exec() != QDialog.Accepted or not dialog.result_data:
            return
        result = dialog.result_data
        with self.session_factory() as session:
            entry = EmployeeUnavailability(
                employee_id=self.employee_id,
                day_of_week=result["day_of_week"],
                start_time=result["start_time"],
                end_time=result["end_time"],
            )
            session.add(entry)
            session.commit()
            session.refresh(entry)
            entry_id = entry.id

        audit_logger.log(
            "employee_unavailability_add",
            self.actor["username"],
            details={
                "employee_id": self.employee_id,
                "entry_id": entry_id,
                "day_of_week": result["day_of_week"],
                "start_time": result["start_time"].isoformat(),
                "end_time": result["end_time"].isoformat(),
            },
        )
        self.refresh_entries()

    def edit_entry(self) -> None:
        entry = self.selected_entry()
        if not entry:
            return
        dialog = UnavailabilityEntryDialog(
            entry.day_of_week,
            entry.start_time,
            entry.end_time,
        )
        dialog.setStyleSheet(THEME_STYLESHEET)
        if dialog.exec() != QDialog.Accepted or not dialog.result_data:
            return
        result = dialog.result_data
        with self.session_factory() as session:
            db_entry = session.get(EmployeeUnavailability, entry.id)
            if not db_entry:
                return
            db_entry.day_of_week = result["day_of_week"]
            db_entry.start_time = result["start_time"]
            db_entry.end_time = result["end_time"]
            session.commit()

        audit_logger.log(
            "employee_unavailability_update",
            self.actor["username"],
            details={
                "employee_id": self.employee_id,
                "entry_id": entry.id,
                "day_of_week": result["day_of_week"],
                "start_time": result["start_time"].isoformat(),
                "end_time": result["end_time"].isoformat(),
            },
        )
        self.refresh_entries()

    def remove_entry(self) -> None:
        entry = self.selected_entry()
        if not entry:
            return
        confirm = QMessageBox.question(
            self,
            "Remove unavailability",
            "Delete the selected unavailability window?",
        )
        if confirm != QMessageBox.Yes:
            return
        with self.session_factory() as session:
            db_entry = session.get(EmployeeUnavailability, entry.id)
            if not db_entry:
                return
            session.delete(db_entry)
            session.commit()

        audit_logger.log(
            "employee_unavailability_delete",
            self.actor["username"],
            details={
                "employee_id": self.employee_id,
                "entry_id": entry.id,
            },
        )
        self.refresh_entries()


class EmployeeDirectoryDialog(QDialog):
    def __init__(self, session_factory, actor: Dict[str, str]) -> None:
        super().__init__()
        self.session_factory = session_factory
        self.actor = actor
        self.employees: List[Employee] = []
        self.visible_employees: List[Employee] = []
        self.setWindowTitle("Employee directory")
        self.resize(780, 460)
        self._build_ui()
        self.refresh_table()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        intro = QLabel(
            "Maintain employee profiles, their role coverage, availability, and standing."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Show"))
        self.filter_combo = QComboBox()
        self.filter_combo.addItems(["All employees", "Active only", "Inactive only"])
        self.filter_combo.currentIndexChanged.connect(self.refresh_table)
        controls.addWidget(self.filter_combo)
        controls.addStretch()
        layout.addLayout(controls)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["Name", "Roles", "Desired hours", "Start date", "Status", "Notes"]
        )
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.itemSelectionChanged.connect(self.update_button_state)
        self.table.cellDoubleClicked.connect(lambda *_: self.edit_employee())
        layout.addWidget(self.table)

        buttons = QHBoxLayout()
        self.add_button = QPushButton("Add employee")
        self.add_button.clicked.connect(self.add_employee)
        buttons.addWidget(self.add_button)

        self.edit_button = QPushButton("Edit")
        self.edit_button.clicked.connect(self.edit_employee)
        buttons.addWidget(self.edit_button)

        self.toggle_button = QPushButton("Deactivate")
        self.toggle_button.clicked.connect(self.toggle_employee_status)
        buttons.addWidget(self.toggle_button)

        self.availability_button = QPushButton("Manage availability")
        self.availability_button.clicked.connect(self.manage_availability)
        buttons.addWidget(self.availability_button)

        buttons.addStretch()
        layout.addLayout(buttons)

        self.update_button_state()

    def refresh_table(self) -> None:
        with self.session_factory() as session:
            self.employees = get_all_employees(session)

        selection = self.filter_combo.currentIndex() if hasattr(self, "filter_combo") else 0
        if selection == 1:
            self.visible_employees = [emp for emp in self.employees if emp.status == "active"]
        elif selection == 2:
            self.visible_employees = [emp for emp in self.employees if emp.status == "inactive"]
        else:
            self.visible_employees = list(self.employees)

        self.table.setRowCount(len(self.visible_employees))
        for row, employee in enumerate(self.visible_employees):
            name_item = QTableWidgetItem(employee.full_name)
            name_item.setData(Qt.UserRole, employee.id)
            self.table.setItem(row, 0, name_item)
            roles_text = ", ".join(employee.role_list) if employee.role_list else "No roles"
            self.table.setItem(row, 1, QTableWidgetItem(roles_text))
            self.table.setItem(row, 2, QTableWidgetItem(str(employee.desired_hours)))
            self.table.setItem(row, 3, QTableWidgetItem(employee.start_date_label))
            status_text = "Active" if employee.status == "active" else "Inactive"
            self.table.setItem(row, 4, QTableWidgetItem(status_text))
            self.table.setItem(row, 5, QTableWidgetItem(employee.notes or ""))
        self.table.resizeRowsToContents()
        self.update_button_state()
    def selected_employee(self) -> Optional[Employee]:
        selection = self.table.selectionModel()
        if not selection or not selection.hasSelection():
            return None
        selected_rows = selection.selectedRows()
        if not selected_rows:
            return None
        row = selected_rows[0].row()
        if 0 <= row < len(self.visible_employees):
            return self.visible_employees[row]
        return None

    def update_button_state(self) -> None:
        employee = self.selected_employee()
        has_selection = employee is not None
        self.edit_button.setEnabled(has_selection)
        self.toggle_button.setEnabled(has_selection)
        self.availability_button.setEnabled(has_selection)
        if employee:
            self.toggle_button.setText("Deactivate" if employee.status == "active" else "Activate")
        else:
            self.toggle_button.setText("Deactivate")

    def add_employee(self) -> None:
        dialog = EmployeeEditDialog(self.session_factory, self.actor)
        dialog.setStyleSheet(THEME_STYLESHEET)
        if dialog.exec() == QDialog.Accepted and dialog.result_action:
            snapshot = dialog.result_snapshot
            audit_logger.log(
                "employee_create",
                self.actor["username"],
                role=self.actor.get("role"),
                details={
                    "employee_id": dialog.result_employee_id,
                    "full_name": snapshot.get("full_name"),
                    "roles": snapshot.get("roles"),
                    "desired_hours": snapshot.get("desired_hours"),
                    "start_date": snapshot.get("start_label"),
                },
            )
            self.refresh_table()

    def edit_employee(self) -> None:
        employee = self.selected_employee()
        if not employee:
            return
        dialog = EmployeeEditDialog(self.session_factory, self.actor, employee.id)
        dialog.setStyleSheet(THEME_STYLESHEET)
        if dialog.exec() == QDialog.Accepted and dialog.result_action:
            snapshot = dialog.result_snapshot
            audit_logger.log(
                "employee_update",
                self.actor["username"],
                role=self.actor.get("role"),
                details={
                    "employee_id": dialog.result_employee_id,
                    "full_name": snapshot.get("full_name"),
                    "roles": snapshot.get("roles"),
                    "desired_hours": snapshot.get("desired_hours"),
                    "status": snapshot.get("status"),
                    "start_date": snapshot.get("start_label"),
                },
            )
            self.refresh_table()

    def toggle_employee_status(self) -> None:
        employee = self.selected_employee()
        if not employee:
            return
        new_status = "inactive" if employee.status == "active" else "active"
        with self.session_factory() as session:
            db_employee = session.get(Employee, employee.id)
            if not db_employee:
                return
            db_employee.status = new_status
            db_employee.updated_at = datetime.datetime.now(datetime.timezone.utc)
            session.commit()
        audit_logger.log(
            "employee_deactivate" if new_status == "inactive" else "employee_activate",
            self.actor["username"],
            role=self.actor.get("role"),
            details={"employee_id": employee.id, "full_name": employee.full_name},
        )
        self.refresh_table()

    def manage_availability(self) -> None:
        employee = self.selected_employee()
        if not employee:
            return
        dialog = UnavailabilityDialog(self.session_factory, self.actor, employee.id)
        dialog.setStyleSheet(THEME_STYLESHEET)
        dialog.exec()


class ChangePasswordDialog(QDialog):
    def __init__(self, store: AccountStore, username: str) -> None:
        super().__init__()
        self.store = store
        self.username = username
        self.password_changed = False
        self.setWindowTitle("Change password")
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        heading = QLabel(
            f"<b>Update password for <span style='color:#b8860b;'>{self.username}</span></b>"
        )
        heading.setWordWrap(True)
        layout.addWidget(heading)

        form = QFormLayout()
        self.current_input = QLineEdit()
        self.current_input.setEchoMode(QLineEdit.Password)
        self.current_input.setPlaceholderText("Current password")
        form.addRow("Current password", self.current_input)

        self.new_input = QLineEdit()
        self.new_input.setEchoMode(QLineEdit.Password)
        self.new_input.setPlaceholderText(f"New password (min {MIN_PASSWORD_LENGTH} chars)")
        form.addRow("New password", self.new_input)

        self.confirm_input = QLineEdit()
        self.confirm_input.setEchoMode(QLineEdit.Password)
        self.confirm_input.setPlaceholderText("Confirm new password")
        form.addRow("Confirm password", self.confirm_input)

        self.feedback_label = QLabel()
        self.feedback_label.setStyleSheet("color: #ff6b6b;")
        self.feedback_label.setWordWrap(True)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.attempt_change)
        buttons.rejected.connect(self.reject)

        layout.addLayout(form)
        layout.addWidget(self.feedback_label)
        layout.addWidget(buttons)
        self.current_input.setFocus()

    def attempt_change(self) -> None:
        current_password = self.current_input.text()
        new_password = self.new_input.text()
        confirm_password = self.confirm_input.text()

        if new_password != confirm_password:
            self.feedback_label.setText("New passwords do not match.")
            self.new_input.clear()
            self.confirm_input.clear()
            return

        try:
            self.store.change_password(self.username, current_password, new_password)
        except PermissionError:
            self.feedback_label.setText("Current password is incorrect.")
            self.current_input.clear()
        except ValueError as exc:
            self.feedback_label.setText(str(exc))
            self.new_input.clear()
            self.confirm_input.clear()
        else:
            self.password_changed = True
            self.feedback_label.setText("")
            self.current_input.clear()
            self.new_input.clear()
            self.confirm_input.clear()
            self.accept()

class MainWindow(QMainWindow):
    def __init__(self, store: AccountStore, user: Dict[str, str], session_factory) -> None:
        super().__init__()
        self.store = store
        self.user = user
        self.session_factory = session_factory
        self.active_week = load_active_week(self.session_factory)
        self.setWindowTitle("Schedule Assistant")
        self.resize(720, 440)
        self._build_ui()
        self._init_session_timeout()

    def _build_ui(self) -> None:
        central = QWidget()
        layout = QVBoxLayout(central)
        layout.setAlignment(Qt.AlignTop)
        display_name = self.user.get("display_name", self.user["username"])
        welcome = QLabel(f"<h1 style='color:#b8860b;'>Welcome, {display_name}!</h1>")
        role_label = QLabel(f"Current role: <b>{self.user['role']}</b>")
        role_label.setStyleSheet("color:#ddd;")
        intro = QLabel(
            "The scheduling workspace will appear here. Use the controls below to continue. "
            "Authentication is required to reach this dashboard."
        )
        intro.setWordWrap(True)

        header_row = QHBoxLayout()
        header_titles = QVBoxLayout()
        header_titles.setSpacing(2)
        header_titles.addWidget(welcome)
        header_titles.addWidget(role_label)
        header_row.addLayout(header_titles)
        header_row.addStretch()
        logout_button = QPushButton("Sign out")
        logout_button.clicked.connect(self.handle_logout)
        header_row.addWidget(logout_button)
        layout.addLayout(header_row)
        layout.addSpacing(12)

        self.week_selector = WeekSelectorWidget(
            self.session_factory,
            self.active_week,
            self.on_week_changed,
        )
        layout.addWidget(self.week_selector)

        if self.user["role"] in {"IT", "GM", "SM"}:
            layout.addSpacing(12)
            top_actions = QHBoxLayout()
            top_actions.setSpacing(14)
            employees_button = QPushButton("Employee directory")
            employees_button.clicked.connect(self.open_employee_directory)
            top_actions.addWidget(employees_button)
            top_actions.addStretch()
            layout.addLayout(top_actions)

        layout.addSpacing(12)
        layout.addWidget(intro)
        layout.addSpacing(20)

        placeholder = QLabel(
            "<i>Placeholder: scheduling dashboards and data entry tools will live here.</i>"
        )
        placeholder.setStyleSheet("color:#9f9f9f;")
        layout.addSpacing(30)
        layout.addWidget(placeholder)
        layout.addStretch(1)

        footer_row = QHBoxLayout()
        footer_row.setSpacing(14)
        if self.user["role"] in {"IT", "GM"}:
            manage_accounts_button = QPushButton("Manage accounts")
            manage_accounts_button.clicked.connect(self.open_account_manager)
            footer_row.addWidget(manage_accounts_button)

        change_password_button = QPushButton("Change password")
        change_password_button.clicked.connect(self.open_change_password)
        footer_row.addWidget(change_password_button)
        footer_row.addStretch()
        layout.addLayout(footer_row)

        self.setCentralWidget(central)

    def on_week_changed(self, iso_year: int, iso_week: int, label: str) -> None:
        if (
            iso_year == self.active_week["iso_year"]
            and iso_week == self.active_week["iso_week"]
        ):
            return
        self.active_week = {"iso_year": iso_year, "iso_week": iso_week, "label": label}
        save_active_week(iso_year, iso_week)
        audit_logger.log(
            "week_context_change",
            self.user["username"],
            role=self.user.get("role"),
            details={"iso_year": iso_year, "iso_week": iso_week, "label": label},
        )
        self.reset_session_timers()

    def _init_session_timeout(self) -> None:
        self._warning_shown = False
        self.warning_timer = QTimer(self)
        self.warning_timer.setSingleShot(True)
        self.warning_timer.timeout.connect(self._show_timeout_warning)

        self.logout_timer = QTimer(self)
        self.logout_timer.setSingleShot(True)
        self.logout_timer.timeout.connect(self._handle_session_expired)

        self.reset_session_timers()
        self.installEventFilter(self)
        if self.centralWidget():
            self.centralWidget().installEventFilter(self)

    def reset_session_timers(self) -> None:
        if hasattr(self, "warning_timer") and hasattr(self, "logout_timer"):
            self._warning_shown = False
            self.warning_timer.start(SESSION_WARNING_SECONDS * 1000)
            self.logout_timer.start(SESSION_TIMEOUT_SECONDS * 1000)

    def _show_timeout_warning(self) -> None:
        if self._warning_shown:
            return
        self._warning_shown = True
        QMessageBox.warning(
            self,
            "Session timeout",
            "Inactivity detected. You will be signed out in 60 seconds unless you continue working.",
        )

    def _handle_session_expired(self) -> None:
        audit_logger.log(
            "session_timeout",
            self.user["username"],
            role=self.user.get("role"),
            details={
                "iso_year": self.active_week.get("iso_year"),
                "iso_week": self.active_week.get("iso_week"),
            },
        )
        QMessageBox.information(
            self,
            "Session timed out",
            "You were signed out due to inactivity.",
        )
        self.close()

    def eventFilter(self, source, event) -> bool:
        if event.type() in (
            QEvent.MouseButtonPress,
            QEvent.MouseButtonRelease,
            QEvent.KeyPress,
            QEvent.KeyRelease,
            QEvent.Wheel,
        ):
            self.reset_session_timers()
        return super().eventFilter(source, event)

    def open_account_manager(self) -> None:
        dialog = AccountManagerDialog(self.store, self.user)
        dialog.setStyleSheet(THEME_STYLESHEET)
        dialog.exec()

    def open_employee_directory(self) -> None:
        dialog = EmployeeDirectoryDialog(self.session_factory, self.user)
        dialog.setStyleSheet(THEME_STYLESHEET)
        dialog.exec()

    def open_change_password(self) -> None:
        dialog = ChangePasswordDialog(self.store, self.user["username"])
        dialog.setStyleSheet(THEME_STYLESHEET)
        if dialog.exec() == QDialog.Accepted and dialog.password_changed:
            QMessageBox.information(self, "Password updated", "Your password has been changed successfully.")

    def handle_logout(self) -> None:
        confirm = QMessageBox.question(self, "Sign out", "Return to sign-in screen?")
        if confirm == QMessageBox.Yes:
            self.close()

    def closeEvent(self, event: QCloseEvent) -> None:
        # On exit, we simply accept. Main application loop handles showing login again if needed.
        event.accept()


def launch_app() -> int:
    app = QApplication(sys.argv)
    app.setStyleSheet(THEME_STYLESHEET)

    store = AccountStore(ACCOUNTS_FILE)
    init_database()

    while True:
        login = LoginDialog(store)
        login.setStyleSheet(THEME_STYLESHEET)
        if login.exec() != QDialog.Accepted:
            break

        authenticated = login.authenticated_user
        if not authenticated:
            # Should not happen since dialog returns Accepted only on success, but guard anyway.
            continue

        window = MainWindow(store, authenticated, SessionLocal)
        window.setStyleSheet(THEME_STYLESHEET)
        window.show()
        app.exec()

        # After window closes (logout or quit), decide whether to prompt for login again.
        logout = QMessageBox.question(
            None,
            "Session ended",
            "Do you want to sign in again?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if logout != QMessageBox.Yes:
            break

    return 0


if __name__ == "__main__":
    sys.exit(launch_app())



