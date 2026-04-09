from __future__ import annotations

import numpy as np
import pandas as pd


def weighted_prices(df: pd.DataFrame) -> np.ndarray:
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    return (high + low + close + close) / 4.0


def tr_components(df: pd.DataFrame) -> np.ndarray:
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)

    tr = np.zeros(len(df), dtype=float)
    if len(df) <= 1:
        return tr

    prev_close = close[:-1]
    tr[1:] = np.maximum(high[1:], prev_close) - np.minimum(low[1:], prev_close)
    return tr


def prefix_sums(values: np.ndarray) -> np.ndarray:
    prefix = np.zeros(len(values) + 1, dtype=float)
    prefix[1:] = np.cumsum(values)
    return prefix


def atr_for_index(prefix_tr: np.ndarray, target_index: int, atr_period: int) -> float:
    end_component_index = target_index - 10
    if end_component_index < 1:
        return 0.0

    start_component_index = max(1, end_component_index - atr_period + 1)
    total = prefix_tr[end_component_index + 1] - prefix_tr[start_component_index]
    return total / float(atr_period)


def centered_band_for_index(
    weighted: np.ndarray,
    prefix_tr: np.ndarray,
    target_index: int,
    half_length: int,
    atr_period: int,
    atr_multiplier: float,
    future_weighted: float | None,
) -> tuple[float, float, float]:
    if target_index < 0 or target_index >= len(weighted):
        return float("nan"), float("nan"), float("nan")

    center_weight = half_length + 1
    total = center_weight * weighted[target_index]
    total_weight = float(center_weight)

    for step in range(1, half_length + 1):
        weight = half_length - step + 1
        left_index = target_index - step
        if left_index >= 0:
            total += weight * weighted[left_index]
            total_weight += weight
        if step == 1 and future_weighted is not None:
            total += weight * future_weighted
            total_weight += weight

    tma = total / total_weight if total_weight else float("nan")
    atr = atr_for_index(prefix_tr, target_index, atr_period)
    upper = tma + atr_multiplier * atr
    lower = tma - atr_multiplier * atr
    return tma, upper, lower


def visible_centered_tma(
    df: pd.DataFrame,
    half_length: int,
    atr_period: int,
    atr_multiplier: float,
) -> pd.DataFrame:
    weighted = weighted_prices(df)
    prefix_tr = prefix_sums(tr_components(df))

    rows = []
    for index, timestamp in enumerate(df["time"]):
        future_weighted = weighted[index + 1] if index + 1 < len(weighted) else None
        tma, upper, lower = centered_band_for_index(
            weighted,
            prefix_tr,
            index,
            half_length,
            atr_period,
            atr_multiplier,
            future_weighted,
        )
        rows.append({"time": timestamp, "tma": tma, "upper": upper, "lower": lower})

    return pd.DataFrame.from_records(rows)
