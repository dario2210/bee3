from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from bee3_data import BASE_DIR, ensure_dirs, list_datasets, load_ohlcv_csv, save_result, save_uploaded_csv
from bee3_engine import SimulationResult, run_backtest
from bee3_market_data import TF_MINUTES, download_binance_dataset
from bee3_params import strategy_params_from_payload, wfo_config_from_payload
from bee3_wfo import WfoResult, run_wfo


ensure_dirs()

app = FastAPI(title="Bee3 Icarus MMS Lab")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


def _unix_seconds(value) -> int:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp())


def _serialize_dataframe(df: pd.DataFrame) -> list[dict[str, object]]:
    if df is None or df.empty:
        return []
    records = df.copy()
    for column in records.columns:
        if pd.api.types.is_datetime64_any_dtype(records[column]):
            records[column] = records[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return records.to_dict(orient="records")


def _filter_df_by_range(df: pd.DataFrame, date_range: dict | None) -> pd.DataFrame:
    if not date_range:
        return df.reset_index(drop=True)

    filtered = df.copy()
    start_date = str(date_range.get("start_date", "")).strip()
    end_date = str(date_range.get("end_date", "")).strip()

    if start_date:
        filtered = filtered[filtered["time"] >= pd.Timestamp(start_date, tz="UTC")]
    if end_date:
        filtered = filtered[filtered["time"] <= pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]

    filtered = filtered.reset_index(drop=True)
    if len(filtered) < 100:
        raise ValueError("Too few candles after applying the selected date range")
    return filtered


def _trades_for_ui(trades_df: pd.DataFrame, limit: int = 600) -> list[dict[str, object]]:
    if trades_df is None or trades_df.empty:
        return []
    return _serialize_dataframe(trades_df.tail(limit).reset_index(drop=True))


def _chart_payload(df: pd.DataFrame, result: SimulationResult) -> dict[str, object]:
    candles = [
        {
            "time": _unix_seconds(row.time),
            "open": round(float(row.open), 6),
            "high": round(float(row.high), 6),
            "low": round(float(row.low), 6),
            "close": round(float(row.close), 6),
        }
        for row in df.itertuples(index=False)
    ]

    upper = []
    lower = []
    for row in result.bands.itertuples(index=False):
        if pd.isna(row.upper) or pd.isna(row.lower):
            continue
        upper.append({"time": _unix_seconds(row.time), "value": round(float(row.upper), 6)})
        lower.append({"time": _unix_seconds(row.time), "value": round(float(row.lower), 6)})

    markers = []
    if result.trades is not None and not result.trades.empty:
        for trade in result.trades.itertuples(index=False):
            entry_color = "#0ea5a4" if trade.side == "long" else "#f97316"
            exit_color = "#22c55e" if trade.pnl >= 0 else "#ef4444"
            markers.append(
                {
                    "time": _unix_seconds(trade.entry_time),
                    "position": "belowBar" if trade.side == "long" else "aboveBar",
                    "shape": "arrowUp" if trade.side == "long" else "arrowDown",
                    "color": entry_color,
                    "text": f"IN {trade.side[0].upper()} {trade.volume:.2f}",
                }
            )
            markers.append(
                {
                    "time": _unix_seconds(trade.exit_time),
                    "position": "aboveBar" if trade.side == "long" else "belowBar",
                    "shape": "circle",
                    "color": exit_color,
                    "text": f"OUT {trade.pnl:+.2f}",
                }
            )

    return {"candles": candles, "upperBand": upper, "lowerBand": lower, "markers": markers}


def _backtest_payload(dataset_name: str, df: pd.DataFrame, result: SimulationResult) -> dict[str, object]:
    payload = {
        "mode": "backtest",
        "dataset": dataset_name,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": result.params,
        "summary": result.summary,
        "trades": _trades_for_ui(result.trades),
        "trade_count_total": int(len(result.trades)) if result.trades is not None else 0,
        "open_positions": result.open_positions,
        "chart": _chart_payload(df, result),
    }
    save_result("latest_backtest.json", payload)
    return payload


def _wfo_payload(dataset_name: str, df: pd.DataFrame, result: WfoResult) -> dict[str, object]:
    chart = {"candles": [], "upperBand": [], "lowerBand": [], "markers": []}
    if result.best_params:
        best_result = run_backtest(df, strategy_params_from_payload(result.best_params))
        chart = _chart_payload(df, best_result)

    payload = {
        "mode": "wfo",
        "dataset": dataset_name,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "best_params": result.best_params,
        "summary": result.summary,
        "windows": _serialize_dataframe(result.windows),
        "trades": _trades_for_ui(result.trades),
        "trade_count_total": int(len(result.trades)) if result.trades is not None else 0,
        "chart": chart,
    }
    save_result("latest_wfo.json", payload)
    save_result("latest_best_params.json", result.best_params)
    return payload


@app.get("/", response_class=FileResponse)
async def index() -> Path:
    return BASE_DIR / "static" / "index.html"


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/datasets")
async def datasets() -> dict[str, object]:
    return {"datasets": list_datasets()}


@app.get("/api/market-config")
async def market_config() -> dict[str, object]:
    return {
        "intervals": list(TF_MINUTES.keys()),
        "markets": ["spot", "futures"],
    }


@app.post("/api/upload")
async def upload_dataset(file: UploadFile = File(...)) -> dict[str, object]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    content = await file.read()
    name = save_uploaded_csv(file.filename, content)
    return {"dataset": name, "datasets": list_datasets()}


@app.post("/api/fetch-binance")
async def fetch_binance(payload: dict) -> JSONResponse:
    symbol = str(payload.get("symbol", "")).strip().upper()
    interval = str(payload.get("interval", "")).strip()
    market = str(payload.get("market", "spot")).strip().lower()
    start_date = str(payload.get("start_date", "")).strip()
    end_date = str(payload.get("end_date", "")).strip() or None

    if not symbol:
        raise HTTPException(status_code=400, detail="Missing symbol")
    if interval not in TF_MINUTES:
        raise HTTPException(status_code=400, detail="Unsupported interval")
    if market not in {"spot", "futures"}:
        raise HTTPException(status_code=400, detail="Unsupported market")
    if not start_date:
        raise HTTPException(status_code=400, detail="Missing start_date")

    try:
        result = download_binance_dataset(
            symbol=symbol,
            interval=interval,
            market=market,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse({"download": result, "datasets": list_datasets()})


@app.post("/api/backtest")
async def backtest(payload: dict) -> JSONResponse:
    dataset_name = str(payload.get("dataset", "")).strip()
    if not dataset_name:
        raise HTTPException(status_code=400, detail="Missing dataset")

    params_payload = dict(payload.get("params") or {})
    params_payload["force_close_on_end"] = bool(payload.get("force_close_on_end", False))
    params = strategy_params_from_payload(params_payload)

    try:
        df = load_ohlcv_csv(dataset_name)
        df = _filter_df_by_range(df, payload.get("range"))
        result = run_backtest(df, params)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(_backtest_payload(dataset_name, df, result))


@app.post("/api/wfo")
async def wfo(payload: dict) -> JSONResponse:
    dataset_name = str(payload.get("dataset", "")).strip()
    if not dataset_name:
        raise HTTPException(status_code=400, detail="Missing dataset")

    params = strategy_params_from_payload(payload.get("params"))
    config = wfo_config_from_payload(payload.get("wfo"))

    try:
        df = load_ohlcv_csv(dataset_name)
        df = _filter_df_by_range(df, payload.get("range"))
        result = run_wfo(df, params, config)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(_wfo_payload(dataset_name, df, result))
