from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from time import monotonic

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from bee3_data import BASE_DIR, ensure_dirs, list_datasets, load_ohlcv_csv, save_result, save_uploaded_csv
from bee3_engine import RunCancelled, SimulationResult, run_backtest
from bee3_market_data import TF_MINUTES, download_binance_dataset
from bee3_params import strategy_params_from_payload, wfo_config_from_payload
from bee3_wfo import WfoResult, run_wfo, wfo_candidate_count, wfo_total_work_units, wfo_window_count


ensure_dirs()

app = FastAPI(title="Bee3 Icarus MMS Lab")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


def _unix_seconds(value) -> int:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp())


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        "generated_at": _now_utc(),
        "params": result.params,
        "summary": result.summary,
        "trades": _trades_for_ui(result.trades),
        "trade_count_total": int(len(result.trades)) if result.trades is not None else 0,
        "open_positions": result.open_positions,
        "chart": _chart_payload(df, result),
    }
    save_result("latest_backtest.json", payload)
    return payload


def _wfo_payload(
    dataset_name: str,
    df: pd.DataFrame,
    result: WfoResult,
    *,
    chart_result: SimulationResult | None = None,
    include_chart: bool = True,
) -> dict[str, object]:
    chart = {"candles": [], "upperBand": [], "lowerBand": [], "markers": []}
    if include_chart and result.best_params:
        best_result = chart_result or run_backtest(df, strategy_params_from_payload(result.best_params))
        chart = _chart_payload(df, best_result)

    payload = {
        "mode": "wfo",
        "dataset": dataset_name,
        "generated_at": _now_utc(),
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


def _default_progress() -> dict[str, object]:
    return {
        "processed_units": 0,
        "total_units": 0,
        "current_bar": 0,
        "total_bars": 0,
        "current_window": 0,
        "total_windows": 0,
        "current_candidate": None,
        "total_candidates": 0,
        "phase": "idle",
        "phase_label": "Brak aktywnego runu.",
    }


_RUN_LOCK = Lock()
_RUN_STATE = {
    "job_id": 0,
    "running": False,
    "stop_requested": False,
    "status": "Czekam na pierwszy run.",
    "mode": None,
    "dataset": None,
    "started_at": None,
    "started_monotonic": None,
    "finished_at": None,
    "result": None,
    "error": None,
    "progress": _default_progress(),
    "live_trades": [],
    "live_windows": [],
    "trade_seq": 0,
    "window_seq": 0,
}


def _job_should_stop(job_id: int) -> bool:
    with _RUN_LOCK:
        return _RUN_STATE["job_id"] != job_id or bool(_RUN_STATE["stop_requested"])


def _mutate_job(job_id: int, mutator) -> bool:
    with _RUN_LOCK:
        if _RUN_STATE["job_id"] != job_id:
            return False
        mutator(_RUN_STATE)
        return True


def _set_status(job_id: int, message: str) -> None:
    _mutate_job(job_id, lambda state: state.update({"status": message}))


def _set_progress(job_id: int, **updates) -> None:
    def mutator(state: dict) -> None:
        state["progress"].update(updates)

    _mutate_job(job_id, mutator)


def _append_trade(job_id: int, trade: dict[str, object]) -> None:
    def mutator(state: dict) -> None:
        state["trade_seq"] += 1
        state["live_trades"].append({"seq": state["trade_seq"], **trade})

    _mutate_job(job_id, mutator)


def _append_window(job_id: int, window: dict[str, object]) -> None:
    def mutator(state: dict) -> None:
        state["window_seq"] += 1
        state["live_windows"].append({"seq": state["window_seq"], **window})

    _mutate_job(job_id, mutator)


def _start_run(mode: str, dataset_name: str) -> int:
    with _RUN_LOCK:
        if _RUN_STATE["running"]:
            raise RuntimeError("Another simulation is already running")
        job_id = int(_RUN_STATE["job_id"]) + 1
        progress = _default_progress()
        progress.update({"phase": "starting", "phase_label": "Uruchamiam symulację..."})
        _RUN_STATE.update(
            {
                "job_id": job_id,
                "running": True,
                "stop_requested": False,
                "status": "Uruchamiam symulację...",
                "mode": mode,
                "dataset": dataset_name,
                "started_at": _now_utc(),
                "started_monotonic": monotonic(),
                "finished_at": None,
                "result": None,
                "error": None,
                "progress": progress,
                "live_trades": [],
                "live_windows": [],
                "trade_seq": 0,
                "window_seq": 0,
            }
        )
        return job_id


def _finish_success(job_id: int, message: str, result: dict[str, object]) -> None:
    def mutator(state: dict) -> None:
        progress = state["progress"]
        total_units = int(progress.get("total_units") or 0)
        progress.update(
            {
                "processed_units": total_units,
                "phase": "completed",
                "phase_label": "Symulacja zakończona.",
            }
        )
        state.update(
            {
                "running": False,
                "stop_requested": False,
                "status": message,
                "finished_at": _now_utc(),
                "result": result,
                "error": None,
            }
        )

    _mutate_job(job_id, mutator)


def _finish_cancelled(job_id: int, message: str) -> None:
    def mutator(state: dict) -> None:
        state["progress"].update({"phase": "stopped", "phase_label": "Symulacja zatrzymana."})
        state.update(
            {
                "running": False,
                "stop_requested": False,
                "status": message,
                "finished_at": _now_utc(),
                "result": None,
                "error": None,
            }
        )

    _mutate_job(job_id, mutator)


def _finish_error(job_id: int, message: str) -> None:
    def mutator(state: dict) -> None:
        state["progress"].update({"phase": "error", "phase_label": "Symulacja przerwana błędem."})
        state.update(
            {
                "running": False,
                "stop_requested": False,
                "status": f"Błąd: {message}",
                "finished_at": _now_utc(),
                "result": None,
                "error": message,
            }
        )

    _mutate_job(job_id, mutator)


def _request_stop() -> bool:
    with _RUN_LOCK:
        if not _RUN_STATE["running"]:
            return False
        _RUN_STATE["stop_requested"] = True
        _RUN_STATE["status"] = "Zatrzymuję symulację..."
        return True


def _format_eta(seconds: int | None) -> str:
    if seconds is None:
        return "ETA --"
    if seconds <= 0:
        return "ETA 0s"
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return "ETA " + " ".join(parts)


def _snapshot_run_state(after_trade: int = 0, after_window: int = 0) -> dict[str, object]:
    with _RUN_LOCK:
        snap = deepcopy(_RUN_STATE)

    progress = snap["progress"]
    processed_units = int(progress.get("processed_units") or 0)
    total_units = int(progress.get("total_units") or 0)
    progress["percent"] = round((processed_units / total_units) * 100.0, 2) if total_units else 0.0

    eta_seconds = None
    started_monotonic = snap.get("started_monotonic")
    if snap["running"] and started_monotonic and processed_units > 0 and total_units > processed_units:
        elapsed = max(0.0, monotonic() - float(started_monotonic))
        eta_seconds = int(elapsed * (total_units - processed_units) / processed_units)
    elif total_units and processed_units >= total_units:
        eta_seconds = 0

    progress["eta_seconds"] = eta_seconds
    progress["eta_label"] = _format_eta(eta_seconds)
    total_windows = int(progress.get("total_windows") or 0)
    current_window = int(progress.get("current_window") or 0)
    progress["window_label"] = f"{current_window}/{total_windows}" if total_windows else ""
    total_bars = int(progress.get("total_bars") or 0)
    current_bar = int(progress.get("current_bar") or 0)
    progress["bar_label"] = f"{current_bar}/{total_bars}" if total_bars else ""

    trades = [trade for trade in snap["live_trades"] if int(trade.get("seq", 0)) > after_trade]
    windows = [window for window in snap["live_windows"] if int(window.get("seq", 0)) > after_window]

    return {
        "job_id": snap["job_id"],
        "running": snap["running"],
        "stop_requested": snap["stop_requested"],
        "status": snap["status"],
        "mode": snap["mode"],
        "dataset": snap["dataset"],
        "started_at": snap["started_at"],
        "finished_at": snap["finished_at"],
        "error": snap["error"],
        "progress": progress,
        "result": snap["result"],
        "new_trades": trades,
        "new_windows": windows,
        "trade_cursor": snap["trade_seq"],
        "window_cursor": snap["window_seq"],
        "live_trade_count": len(snap["live_trades"]),
        "live_window_count": len(snap["live_windows"]),
    }


def _update_backtest_progress(job_id: int, current_bar: int, total_bars: int) -> None:
    if _job_should_stop(job_id):
        return
    current_bar = min(max(int(current_bar), 0), max(int(total_bars), 0))
    _set_progress(
        job_id,
        processed_units=current_bar,
        total_units=total_bars,
        current_bar=current_bar,
        total_bars=total_bars,
        current_window=0,
        total_windows=0,
        current_candidate=None,
        total_candidates=0,
        phase="backtest",
        phase_label=f"Backtest: świeca {current_bar}/{total_bars}",
    )
    _set_status(job_id, f"Backtest w toku | świeca {current_bar}/{total_bars}")


def _phase_label_for_wfo(event: dict[str, object]) -> str:
    current_window = int(event.get("current_window") or 0)
    total_windows = int(event.get("total_windows") or 0)
    phase = str(event.get("phase") or "")
    current_candidate = event.get("current_candidate")
    total_candidates = int(event.get("total_candidates") or 0)

    if phase == "train":
        return f"WFO: okno {current_window}/{total_windows} | optymalizacja {current_candidate}/{total_candidates}"
    if phase == "test":
        return f"WFO: okno {current_window}/{total_windows} | test live"
    if phase == "window_complete":
        return f"WFO: ukończono okno {current_window}/{total_windows}"
    return f"WFO: przygotowanie {current_window}/{total_windows}" if total_windows else "WFO: przygotowanie"


def _update_wfo_progress(job_id: int, event: dict[str, object]) -> None:
    if _job_should_stop(job_id):
        return
    label = _phase_label_for_wfo(event)
    _set_progress(
        job_id,
        processed_units=int(event.get("processed_units") or 0),
        total_units=int(event.get("total_units") or 0),
        current_window=int(event.get("current_window") or 0),
        total_windows=int(event.get("total_windows") or 0),
        current_candidate=event.get("current_candidate"),
        total_candidates=int(event.get("total_candidates") or 0),
        phase=str(event.get("phase") or "wfo"),
        phase_label=label,
    )
    _set_status(job_id, label)


def _run_backtest_job(job_id: int, dataset_name: str, params_payload: dict, date_range: dict | None) -> None:
    try:
        _set_status(job_id, f"Wczytuję dataset {dataset_name}...")
        df = load_ohlcv_csv(dataset_name)
        df = _filter_df_by_range(df, date_range)
        params = strategy_params_from_payload(params_payload)

        _update_backtest_progress(job_id, 0, len(df))
        result = run_backtest(
            df,
            params,
            trade_callback=lambda trade: _append_trade(job_id, trade),
            progress_callback=lambda current, total: _update_backtest_progress(job_id, current, total),
            stop_check=lambda: _job_should_stop(job_id),
        )
        payload = _backtest_payload(dataset_name, df, result)
        _finish_success(job_id, f"Backtest gotowy dla {dataset_name}. Transakcje: {len(result.trades)}.", payload)
    except RunCancelled:
        _finish_cancelled(job_id, f"Backtest zatrzymany dla {dataset_name}.")
    except Exception as exc:  # noqa: BLE001
        _finish_error(job_id, str(exc))


def _run_wfo_job(job_id: int, dataset_name: str, params_payload: dict, wfo_payload: dict, date_range: dict | None) -> None:
    try:
        _set_status(job_id, f"Wczytuję dataset {dataset_name}...")
        df = load_ohlcv_csv(dataset_name)
        df = _filter_df_by_range(df, date_range)
        params = strategy_params_from_payload(params_payload)
        config = wfo_config_from_payload(wfo_payload)

        total_windows = wfo_window_count(len(df), config)
        if total_windows <= 0:
            raise ValueError("Too few candles for the selected WFO train/test settings")

        total_candidates = wfo_candidate_count(config)
        total_units = wfo_total_work_units(len(df), config)
        _set_progress(
            job_id,
            processed_units=0,
            total_units=total_units,
            current_window=0,
            total_windows=total_windows,
            current_candidate=None,
            total_candidates=total_candidates,
            phase="starting",
            phase_label=f"WFO: start 0/{total_windows}",
        )
        _set_status(job_id, f"WFO w toku | okno 0/{total_windows}")

        result = run_wfo(
            df,
            params,
            config,
            progress_callback=lambda event: _update_wfo_progress(job_id, event),
            trade_callback=lambda trade: _append_trade(job_id, trade),
            window_callback=lambda window: _append_window(job_id, window),
            stop_check=lambda: _job_should_stop(job_id),
        )

        include_chart = True
        chart_result = None
        if result.best_params and not _job_should_stop(job_id):
            _set_status(job_id, "WFO policzone. Buduję końcowy wykres...")
            _set_progress(job_id, phase="rendering", phase_label="Buduję końcowy wykres dla najlepszego zestawu parametrów.")
            try:
                chart_result = run_backtest(
                    df,
                    strategy_params_from_payload(result.best_params),
                    stop_check=lambda: _job_should_stop(job_id),
                )
            except RunCancelled:
                include_chart = False
                _set_status(job_id, "WFO ukończone, pomijam końcowy wykres po STOP.")

        payload = _wfo_payload(dataset_name, df, result, chart_result=chart_result, include_chart=include_chart)
        if include_chart:
            message = f"WFO gotowe dla {dataset_name}. Okna: {len(result.windows)} | transakcje: {len(result.trades)}."
        else:
            message = f"WFO gotowe bez końcowego wykresu dla {dataset_name}. Okna: {len(result.windows)} | transakcje: {len(result.trades)}."
        _finish_success(job_id, message, payload)
    except RunCancelled:
        _finish_cancelled(job_id, f"WFO zatrzymane dla {dataset_name}.")
    except Exception as exc:  # noqa: BLE001
        _finish_error(job_id, str(exc))


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


@app.get("/api/run-status")
async def run_status(
    after_trade: int = Query(default=0, ge=0),
    after_window: int = Query(default=0, ge=0),
) -> JSONResponse:
    return JSONResponse(_snapshot_run_state(after_trade=after_trade, after_window=after_window))


@app.post("/api/run-stop")
async def run_stop() -> JSONResponse:
    _request_stop()
    return JSONResponse(_snapshot_run_state())


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


@app.post("/api/run-backtest")
async def start_backtest(payload: dict) -> JSONResponse:
    dataset_name = str(payload.get("dataset", "")).strip()
    if not dataset_name:
        raise HTTPException(status_code=400, detail="Missing dataset")

    params_payload = dict(payload.get("params") or {})
    params_payload["force_close_on_end"] = bool(payload.get("force_close_on_end", False))
    strategy_params_from_payload(params_payload)

    try:
        job_id = _start_run("backtest", dataset_name)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    worker = Thread(
        target=_run_backtest_job,
        args=(job_id, dataset_name, params_payload, payload.get("range")),
        daemon=True,
    )
    worker.start()
    return JSONResponse(_snapshot_run_state())


@app.post("/api/run-wfo")
async def start_wfo(payload: dict) -> JSONResponse:
    dataset_name = str(payload.get("dataset", "")).strip()
    if not dataset_name:
        raise HTTPException(status_code=400, detail="Missing dataset")

    params_payload = dict(payload.get("params") or {})
    wfo_payload = dict(payload.get("wfo") or {})
    strategy_params_from_payload(params_payload)
    wfo_config_from_payload(wfo_payload)

    try:
        job_id = _start_run("wfo", dataset_name)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    worker = Thread(
        target=_run_wfo_job,
        args=(job_id, dataset_name, params_payload, wfo_payload, payload.get("range")),
        daemon=True,
    )
    worker.start()
    return JSONResponse(_snapshot_run_state())


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
