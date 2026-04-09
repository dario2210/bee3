from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import requests

from bee3_data import DATA_DIR, ensure_dirs


BINANCE_SPOT_URL = "https://api.binance.com/api/v3/klines"
BINANCE_FUTURES_URL = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_LIMIT = 1000
REQUEST_DELAY = 0.25

TF_MINUTES: dict[str, float] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
}


def interval_to_ms(interval: str) -> int:
    minutes = TF_MINUTES.get(interval)
    if minutes is None:
        raise ValueError(f"Unsupported interval '{interval}'")
    return int(minutes * 60 * 1000)


def _to_ms(date_value: str | None, *, end_of_day: bool = False) -> int | None:
    if not date_value or not str(date_value).strip():
        return None
    ts = pd.Timestamp(str(date_value).strip(), tz="UTC")
    if end_of_day and len(str(date_value).strip()) <= 10:
        ts = ts + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
    return int(ts.timestamp() * 1000)


def make_dataset_name(symbol: str, interval: str, market: str, start_date: str | None, end_date: str | None) -> str:
    start_part = (start_date or "start").replace(":", "-")
    end_part = (end_date or "latest").replace(":", "-")
    return f"{symbol.lower()}_{interval}_{market}_{start_part}_{end_part}.csv"


def fetch_klines_binance(
    symbol: str,
    interval: str,
    market: str = "spot",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    verbose: bool = False,
) -> pd.DataFrame:
    base_url = BINANCE_FUTURES_URL if market == "futures" else BINANCE_SPOT_URL
    bar_ms = interval_to_ms(interval)

    rows: list = []
    current_start = start_time_ms
    session = requests.Session()
    session.trust_env = False

    try:
        while True:
            params: dict[str, object] = {"symbol": symbol.upper(), "interval": interval, "limit": BINANCE_LIMIT}
            if current_start is not None:
                params["startTime"] = int(current_start)
            if end_time_ms is not None:
                params["endTime"] = int(end_time_ms)

            try:
                response = session.get(base_url, params=params, timeout=20)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as exc:
                raise RuntimeError(f"Binance request failed: {exc!r}") from exc

            if not data:
                break

            rows.extend(data)
            last_open_time_ms = int(data[-1][0])

            if verbose and len(rows) % (BINANCE_LIMIT * 5) == 0:
                print(f"[Binance] downloaded {len(rows)} candles up to {last_open_time_ms}")

            if len(data) < BINANCE_LIMIT:
                break
            if end_time_ms is not None and last_open_time_ms >= end_time_ms:
                break

            current_start = last_open_time_ms + bar_ms
            time.sleep(REQUEST_DELAY)
    finally:
        session.close()

    if not rows:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["open_time"] = df["open_time"].astype("int64")
    for column in ("open", "high", "low", "close", "volume"):
        df[column] = df[column].astype(float)
    return df[["open_time", "open", "high", "low", "close", "volume"]]


def download_binance_dataset(
    symbol: str,
    interval: str,
    market: str,
    start_date: str,
    end_date: str | None = None,
) -> dict[str, object]:
    ensure_dirs()

    start_ms = _to_ms(start_date)
    if start_ms is None:
        raise ValueError("start_date is required")
    end_ms = _to_ms(end_date, end_of_day=True)

    df = fetch_klines_binance(
        symbol=symbol,
        interval=interval,
        market=market,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
        verbose=False,
    )
    if df.empty:
        raise RuntimeError("No candles downloaded from Binance for the selected range")

    file_name = make_dataset_name(symbol, interval, market, start_date, end_date)
    path = Path(DATA_DIR) / file_name
    df.to_csv(path, index=False)

    return {
        "dataset": file_name,
        "rows": int(len(df)),
        "start_time": pd.Timestamp(df["open_time"].iloc[0], unit="ms", tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": pd.Timestamp(df["open_time"].iloc[-1], unit="ms", tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol": symbol.upper(),
        "interval": interval,
        "market": market,
    }
