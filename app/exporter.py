from __future__ import annotations

from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent / "data" / "exports"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def export_week(week_id: int, format: str = "pdf") -> Path:
    """Stub exporter that writes a placeholder file and returns its path."""
    format = format.lower()
    if format not in {"pdf", "csv"}:
        raise ValueError("format must be 'pdf' or 'csv'")
    filename = DATA_DIR / f"week_{week_id}.{format}"
    filename.write_text(
        f"Placeholder export for week {week_id} ({format.upper()})",
        encoding="utf-8",
    )
    return filename
