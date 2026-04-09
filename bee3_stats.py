from __future__ import annotations

import numpy as np
import pandas as pd


def compute_summary(
    trades: pd.DataFrame,
    equity: pd.DataFrame,
    initial_capital: float,
    final_equity: float,
    kill_switch_hit: bool,
) -> dict[str, float | int | bool]:
    if equity is None or equity.empty:
        max_drawdown_pct = 0.0
    else:
        curve = equity["equity"].to_numpy(dtype=float)
        running_max = np.maximum.accumulate(curve)
        drawdown = np.divide(
            curve - running_max,
            running_max,
            out=np.zeros_like(curve, dtype=float),
            where=running_max != 0,
        )
        max_drawdown_pct = float(drawdown.min() * 100.0)

    closed = trades if trades is not None else pd.DataFrame()
    trade_count = int(len(closed))
    wins = closed[closed["pnl"] > 0] if not closed.empty else closed
    losses = closed[closed["pnl"] <= 0] if not closed.empty else closed

    gross_profit = float(wins["pnl"].sum()) if not wins.empty else 0.0
    gross_loss = float(losses["pnl"].sum()) if not losses.empty else 0.0
    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else 0.0
    win_rate = float(len(wins) / trade_count * 100.0) if trade_count else 0.0

    return {
        "initial_capital": round(float(initial_capital), 2),
        "final_equity": round(float(final_equity), 2),
        "net_pnl": round(float(final_equity - initial_capital), 2),
        "return_pct": round(((float(final_equity) / float(initial_capital)) - 1.0) * 100.0, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "trade_count": trade_count,
        "win_rate_pct": round(win_rate, 2),
        "profit_factor": round(float(profit_factor), 2),
        "avg_trade_pnl": round(float(closed["pnl"].mean()), 2) if trade_count else 0.0,
        "avg_bars_in_position": round(float(closed["bars_in_position"].mean()), 2)
        if trade_count and "bars_in_position" in closed.columns
        else 0.0,
        "kill_switch_hit": bool(kill_switch_hit),
    }


def score_params(
    trades: pd.DataFrame,
    final_capital: float,
    initial_capital: float,
    mode: str = "balanced",
) -> float:
    if trades is None or trades.empty:
        return -9999.0

    n = len(trades)
    ret_pct = (final_capital / initial_capital - 1.0) * 100.0

    wins_df = trades[trades["pnl"] > 0]
    losses_df = trades[trades["pnl"] <= 0]

    gross_profit = float(wins_df["pnl"].sum()) if not wins_df.empty else 0.0
    gross_loss = float(losses_df["pnl"].sum()) if not losses_df.empty else 0.0
    pf = gross_profit / abs(gross_loss) if gross_loss < 0 else 0.0

    equity = np.array([initial_capital] + list(initial_capital + trades["pnl"].cumsum().values))
    running_max = np.maximum.accumulate(equity)
    dd_arr = np.divide(
        equity - running_max,
        running_max,
        out=np.zeros_like(equity, dtype=float),
        where=running_max != 0,
    )
    max_dd_pct = float(dd_arr.min() * 100.0)

    activity_penalty = 20.0 if n < 3 else 8.0 if n < 5 else 0.0
    concentration_penalty = 0.0
    total_pnl = float(trades["pnl"].sum())
    if total_pnl > 1e-9:
        top_share = float(trades["pnl"].nlargest(min(2, n)).sum()) / total_pnl
        concentration_penalty = max(0.0, top_share - 0.6) * 20.0

    if mode == "return_only":
        return ret_pct - (10.0 if pf < 1.0 else 0.0)

    if mode == "defensive":
        pf_score = min(pf, 3.0) * 2.0
        dd_penalty = max(0.0, -max_dd_pct - 3.0) * 1.5
        pf_penalty = 15.0 if pf < 1.0 else 0.0
        return ret_pct * 0.5 + pf_score - dd_penalty - pf_penalty - activity_penalty - concentration_penalty

    pf_score = min(pf, 3.0) * 3.0
    dd_penalty = max(0.0, -max_dd_pct - 5.0) * 0.5
    pf_penalty = 10.0 if pf < 1.0 else 0.0
    return ret_pct + pf_score - dd_penalty - pf_penalty - activity_penalty - concentration_penalty
