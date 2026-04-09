from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = BASE_DIR / "results"

REQUIRED_COLUMNS = ("time", "open", "high", "low", "close")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    RESULTS_DIR.mkdir(exist_ok=True)


def list_datasets() -> list[dict[str, object]]:
    ensure_dirs()
    items: list[dict[str, object]] = []
    for path in sorted(DATA_DIR.glob("*.csv")):
        try:
            rows = sum(1 for _ in path.open("r", encoding="utf-8", errors="ignore")) - 1
        except OSError:
            rows = None
        items.append(
            {
                "name": path.name,
                "size_bytes": path.stat().st_size,
                "rows": max(rows or 0, 0),
            }
        )
    return items


def _coerce_time(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        max_abs = float(series.abs().max()) if len(series) else 0.0
        unit = "ms" if max_abs > 10_000_000_000 else "s"
        return pd.to_datetime(series, unit=unit, utc=True, errors="coerce")
    return pd.to_datetime(series, utc=True, errors="coerce")


def load_ohlcv_csv(name: str) -> pd.DataFrame:
    ensure_dirs()
    path = DATA_DIR / Path(name).name
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path.name}")

    df = pd.read_csv(path)
    columns = {column.lower().strip(): column for column in df.columns}

    remap = {}
    for required in REQUIRED_COLUMNS:
        if required in columns:
            remap[columns[required]] = required
    for alias in ("timestamp", "date", "open_time"):
        if alias in columns and "time" not in remap.values():
            remap[columns[alias]] = "time"

    df = df.rename(columns=remap)

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")

    df = df[list(REQUIRED_COLUMNS) + [column for column in df.columns if column not in REQUIRED_COLUMNS]]
    df["time"] = _coerce_time(df["time"])
    df = df.dropna(subset=["time", "open", "high", "low", "close"]).copy()

    for column in ("open", "high", "low", "close"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    else:
        df["volume"] = 0.0

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)
    return df


def save_uploaded_csv(filename: str, content: bytes) -> str:
    ensure_dirs()
    safe_name = Path(filename).name or "uploaded.csv"
    if not safe_name.lower().endswith(".csv"):
        safe_name = f"{safe_name}.csv"
    path = DATA_DIR / safe_name
    path.write_bytes(content)
    return path.name


def save_result(name: str, payload: dict) -> Path:
    ensure_dirs()
    path = RESULTS_DIR / name
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return path
