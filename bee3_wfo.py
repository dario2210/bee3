from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bee3_engine import run_backtest
from bee3_params import StrategyParams, WfoConfig, iter_wfo_param_grid
from bee3_stats import compute_summary, score_params


@dataclass(slots=True)
class WfoResult:
    windows: pd.DataFrame
    trades: pd.DataFrame
    equity: pd.DataFrame
    summary: dict[str, object]
    best_params: dict[str, object]


def run_wfo(df: pd.DataFrame, params: StrategyParams, config: WfoConfig) -> WfoResult:
    if config.train_bars <= 0 or config.test_bars <= 0:
        raise ValueError("train_bars and test_bars must be greater than zero")

    step = config.step_bars or config.test_bars
    current_capital = float(params.initial_capital)
    windows: list[dict[str, object]] = []
    stitched_trades: list[pd.DataFrame] = []
    stitched_equity: list[pd.DataFrame] = []
    last_best_params = params.as_dict()

    start = 0
    window_id = 1
    while start + config.train_bars + config.test_bars <= len(df):
        train_df = df.iloc[start : start + config.train_bars].reset_index(drop=True)
        test_df = df.iloc[start + config.train_bars : start + config.train_bars + config.test_bars].reset_index(
            drop=True
        )

        best_score = None
        best_candidate = None
        best_train_result = None

        for candidate in iter_wfo_param_grid(params, config):
            candidate.initial_capital = current_capital
            candidate.daily_capital = current_capital
            candidate.force_close_on_end = True
            train_result = run_backtest(train_df, candidate)
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
        test_result = run_backtest(test_df, best_candidate)

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

        windows.append(
            {
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
        )

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
