from __future__ import annotations

import datetime
from pathlib import Path
from typing import Iterable, List

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, create_engine, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.types import Time


DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{(DATA_DIR / 'schedule.db').as_posix()}"


class Base(DeclarativeBase):
    pass


class Employee(Base):
    __tablename__ = "employees"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(String(120), nullable=False)
    roles: Mapped[str] = mapped_column(String(120), default="", nullable=False)
    desired_hours: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    status: Mapped[str] = mapped_column(String(12), default="active", nullable=False)
    notes: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    start_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    start_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.now(datetime.timezone.utc)
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.datetime.now(datetime.timezone.utc),
        onupdate=datetime.datetime.now(datetime.timezone.utc),
    )

    unavailability: Mapped[List["EmployeeUnavailability"]] = relationship(
        back_populates="employee", cascade="all, delete-orphan"
    )

    @property
    def role_list(self) -> List[str]:
        return [role.strip() for role in self.roles.split(",") if role.strip()]

    @role_list.setter
    def role_list(self, roles: Iterable[str]) -> None:
        self.roles = ", ".join(sorted({role.strip() for role in roles if role.strip()}))

    @property
    def start_date_label(self) -> str:
        if self.start_month and self.start_year:
            month_name = datetime.date(1900, self.start_month, 1).strftime("%b")
            return f"{month_name} {self.start_year}"
        if self.start_year:
            return str(self.start_year)
        return "Not set"


class EmployeeUnavailability(Base):
    __tablename__ = "employee_unavailability"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id", ondelete="CASCADE"))
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)  # 0 = Monday
    start_time: Mapped[datetime.time] = mapped_column(Time, nullable=False)
    end_time: Mapped[datetime.time] = mapped_column(Time, nullable=False)

    employee: Mapped[Employee] = relationship(back_populates="unavailability")


class WeekContext(Base):
    __tablename__ = "week_context"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    iso_year: Mapped[int] = mapped_column(Integer, nullable=False)
    iso_week: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.now(datetime.timezone.utc)
    )

    __table_args__ = (UniqueConstraint("iso_year", "iso_week", name="uq_week_context_year_week"),)


engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_database() -> None:
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        columns = {
            row[1]: True
            for row in conn.execute(text("PRAGMA table_info(employees)"))
        }
        if "start_month" not in columns:
            conn.execute(text("ALTER TABLE employees ADD COLUMN start_month INTEGER"))
        if "start_year" not in columns:
            conn.execute(text("ALTER TABLE employees ADD COLUMN start_year INTEGER"))


def get_all_employees(session) -> List[Employee]:
    stmt = select(Employee).order_by(Employee.full_name)
    return list(session.scalars(stmt))


def get_all_weeks(session) -> List[WeekContext]:
    stmt = select(WeekContext).order_by(WeekContext.iso_year.desc(), WeekContext.iso_week.desc())
    return list(session.scalars(stmt))


def get_or_create_week(session, iso_year: int, iso_week: int, label: str) -> WeekContext:
    stmt = select(WeekContext).where(
        WeekContext.iso_year == iso_year,
        WeekContext.iso_week == iso_week,
    )
    existing = session.scalars(stmt).first()
    if existing:
        return existing
    week = WeekContext(iso_year=iso_year, iso_week=iso_week, label=label)
    session.add(week)
    session.commit()
    session.refresh(week)
    return week
