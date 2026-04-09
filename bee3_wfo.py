from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from bee3_engine import RunCancelled, run_backtest
from bee3_params import StrategyParams, WfoConfig, iter_wfo_param_grid
from bee3_stats import compute_summary, score_params


@dataclass(slots=True)
class WfoResult:
    windows: pd.DataFrame
    trades: pd.DataFrame
    equity: pd.DataFrame
    summary: dict[str, object]
    best_params: dict[str, object]


WfoProgressCallback = Callable[[dict[str, object]], None]
WfoTradeCallback = Callable[[dict[str, object]], None]
WfoWindowCallback = Callable[[dict[str, object]], None]
StopCheck = Callable[[], bool]


def wfo_window_count(total_bars: int, config: WfoConfig) -> int:
    step = config.step_bars or config.test_bars
    count = 0
    start = 0
    while start + config.train_bars + config.test_bars <= total_bars:
        count += 1
        start += step
    return count


def wfo_candidate_count(config: WfoConfig) -> int:
    return (
        len(config.half_length_grid)
        * len(config.atr_period_grid)
        * len(config.atr_multiplier_grid)
        * len(config.stop_loss_grid)
        * len(config.stop_loss_add_grid)
        * len(config.leverage_profit_grid)
    )


def wfo_total_work_units(total_bars: int, config: WfoConfig) -> int:
    windows = wfo_window_count(total_bars, config)
    return windows * (wfo_candidate_count(config) * config.train_bars + config.test_bars)


def _should_stop(stop_check: StopCheck | None) -> bool:
    return bool(stop_check and stop_check())


def _raise_if_stopped(stop_check: StopCheck | None) -> None:
    if _should_stop(stop_check):
        raise RunCancelled("Run stopped by user")


def _serialize_timestamp(value) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def run_wfo(
    df: pd.DataFrame,
    params: StrategyParams,
    config: WfoConfig,
    *,
    progress_callback: WfoProgressCallback | None = None,
    trade_callback: WfoTradeCallback | None = None,
    window_callback: WfoWindowCallback | None = None,
    stop_check: StopCheck | None = None,
) -> WfoResult:
    if config.train_bars <= 0 or config.test_bars <= 0:
        raise ValueError("train_bars and test_bars must be greater than zero")

    step = config.step_bars or config.test_bars
    current_capital = float(params.initial_capital)
    windows: list[dict[str, object]] = []
    stitched_trades: list[pd.DataFrame] = []
    stitched_equity: list[pd.DataFrame] = []
    last_best_params = params.as_dict()
    total_windows = wfo_window_count(len(df), config)
    total_candidates = wfo_candidate_count(config)
    total_work = max(1, wfo_total_work_units(len(df), config))
    processed_work = 0

    def report_progress(
        *,
        work_done: int,
        window_index: int,
        phase: str,
        candidate_index: int | None = None,
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(
            {
                "processed_units": min(max(work_done, 0), total_work),
                "total_units": total_work,
                "current_window": min(window_index, total_windows),
                "total_windows": total_windows,
                "phase": phase,
                "current_candidate": candidate_index,
                "total_candidates": total_candidates,
            }
        )

    start = 0
    window_id = 1
    report_progress(work_done=0, window_index=0, phase="starting")
    while start + config.train_bars + config.test_bars <= len(df):
        _raise_if_stopped(stop_check)
        train_df = df.iloc[start : start + config.train_bars].reset_index(drop=True)
        test_df = df.iloc[start + config.train_bars : start + config.train_bars + config.test_bars].reset_index(
            drop=True
        )

        best_score = None
        best_candidate = None
        best_train_result = None

        for candidate_index, candidate in enumerate(iter_wfo_param_grid(params, config), start=1):
            _raise_if_stopped(stop_check)
            candidate.initial_capital = current_capital
            candidate.daily_capital = current_capital
            candidate.force_close_on_end = True
            train_work_start = processed_work
            train_result = run_backtest(
                train_df,
                candidate,
                progress_callback=lambda current, total, train_work_start=train_work_start, candidate_index=candidate_index: report_progress(
                    work_done=train_work_start + current,
                    window_index=window_id,
                    phase="train",
                    candidate_index=candidate_index,
                ),
                stop_check=stop_check,
            )
            processed_work += len(train_df)
            score = score_params(
                train_result.trades,
                final_capital=float(train_result.summary["final_equity"]),
                initial_capital=current_capital,
                mode=config.scoring_mode,
            )
            if best_score is None or score > best_score:
                best_score = score
                best_candidate = candidate
                best_train_result = train_result

        if best_candidate is None or best_train_result is None:
            break

        best_candidate.initial_capital = current_capital
        best_candidate.daily_capital = current_capital
        best_candidate.force_close_on_end = True
        test_work_start = processed_work
        test_result = run_backtest(
            test_df,
            best_candidate,
            trade_callback=trade_callback,
            progress_callback=lambda current, total, test_work_start=test_work_start: report_progress(
                work_done=test_work_start + current,
                window_index=window_id,
                phase="test",
                candidate_index=total_candidates + 1,
            ),
            stop_check=stop_check,
        )
        processed_work += len(test_df)

        current_capital = float(test_result.summary["final_equity"])
        last_best_params = best_candidate.as_dict()
        last_best_params["initial_capital"] = params.initial_capital
        last_best_params["daily_capital"] = params.daily_capital
        last_best_params["force_close_on_end"] = params.force_close_on_end

        trade_frame = test_result.trades.copy()
        if not trade_frame.empty:
            trade_frame["window_id"] = window_id
            stitched_trades.append(trade_frame)

        equity_frame = test_result.equity.copy()
        if not equity_frame.empty:
            equity_frame["window_id"] = window_id
            stitched_equity.append(equity_frame)

        window_row = {
            "window_id": window_id,
            "train_start": train_df["time"].iloc[0],
            "train_end": train_df["time"].iloc[-1],
            "test_start": test_df["time"].iloc[0],
            "test_end": test_df["time"].iloc[-1],
            "best_score": round(float(best_score), 4),
            "best_half_length": best_candidate.half_length,
            "best_atr_period": best_candidate.atr_period,
            "best_atr_multiplier": best_candidate.atr_multiplier,
            "best_stop_loss": best_candidate.stop_loss,
            "best_stop_loss_add": best_candidate.stop_loss_add,
            "best_leverage_profit": best_candidate.leverage_profit,
            "train_return_pct": best_train_result.summary["return_pct"],
            "live_return_pct": test_result.summary["return_pct"],
            "live_net_pnl": test_result.summary["net_pnl"],
            "live_trade_count": test_result.summary["trade_count"],
            "live_max_drawdown_pct": test_result.summary["max_drawdown_pct"],
        }
        windows.append(window_row)
        if window_callback is not None:
            window_callback(
                {
                    **window_row,
                    "train_start": _serialize_timestamp(window_row["train_start"]),
                    "train_end": _serialize_timestamp(window_row["train_end"]),
                    "test_start": _serialize_timestamp(window_row["test_start"]),
                    "test_end": _serialize_timestamp(window_row["test_end"]),
                }
            )
        report_progress(work_done=processed_work, window_index=window_id, phase="window_complete")

        start += step
        window_id += 1

    windows_df = pd.DataFrame(windows)
    trades_df = pd.concat(stitched_trades, ignore_index=True) if stitched_trades else pd.DataFrame()
    equity_df = pd.concat(stitched_equity, ignore_index=True) if stitched_equity else pd.DataFrame()

    summary = compute_summary(
        trades_df,
        equity_df,
        initial_capital=params.initial_capital,
        final_equity=current_capital,
        kill_switch_hit=False,
    )

    return WfoResult(
        windows=windows_df,
        trades=trades_df,
        equity=equity_df,
        summary=summary,
        best_params=last_best_params,
    )
