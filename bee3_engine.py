from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from bee3_params import StrategyParams
from bee3_stats import compute_summary
from bee3_tma import centered_band_for_index, prefix_sums, tr_components, visible_centered_tma, weighted_prices


def mt5_round(value: float, digits: int) -> float:
    quant = Decimal("1").scaleb(-digits)
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


@dataclass(slots=True)
class OpenPosition:
    side: str
    volume: float
    entry_price: float
    stop_price: float
    entry_time: pd.Timestamp
    entry_bar_index: int
    entry_reason: str


@dataclass(slots=True)
class SimulationResult:
    params: dict[str, object]
    trades: pd.DataFrame
    equity: pd.DataFrame
    summary: dict[str, object]
    bands: pd.DataFrame
    open_positions: list[dict[str, object]]


class IcarusMmsSimulator:
    def __init__(self, df: pd.DataFrame, params: StrategyParams) -> None:
        self.df = df.reset_index(drop=True).copy()
        self.params = params

        self.balance = float(params.initial_capital)
        self.daily_capital = float(params.daily_capital or params.initial_capital)
        self.maxsize_buffer = float(params.initial_capital)
        self.leverage = float(params.leverage_initial)
        self.leverage_profit = float(params.leverage_profit)
        self.leverage_loss = float(params.leverage_loss)

        self.tma_automatic = bool(params.tma_automatic)
        self.tma_automatic_buy = False
        self.tma_automatic_sell = False
        self.add = bool(params.add_enabled)

        self.positions: list[OpenPosition] = []
        self.trades: list[dict[str, object]] = []
        self.equity_rows: list[dict[str, object]] = []

        self.kill_switch_hit = False
        self.weighted = weighted_prices(self.df)
        self.prefix_tr = prefix_sums(tr_components(self.df))

    @property
    def type_position(self) -> str:
        if not self.positions:
            return "NO POSITION"
        return "LONG" if self.positions[0].side == "long" else "SHORT"

    def _split_price(self, mid_price: float) -> tuple[float, float]:
        spread_ratio = self.params.spread_bps / 10000.0
        ask = mid_price * (1.0 + spread_ratio / 2.0)
        bid = mid_price * (1.0 - spread_ratio / 2.0)
        return mt5_round(ask, 6), mt5_round(bid, 6)

    def _mark_to_market_equity(self, bid: float, ask: float) -> float:
        equity = self.balance
        for position in self.positions:
            if position.side == "long":
                equity += (bid - position.entry_price) * position.volume
            else:
                equity += (position.entry_price - ask) * position.volume
        return equity

    def _compute_maxsize(self, ask: float) -> float:
        if ask <= 0:
            return 0.0
        size = self.leverage * (self.balance * (0.0000095 * mt5_round(100000.0 / ask, 2)))
        return mt5_round(size, 2)

    def _refresh_flat_state(self) -> None:
        if self.positions:
            return
        if self.maxsize_buffer > self.balance:
            self.leverage = self.leverage_loss
            self.add = False
        elif self.balance > self.maxsize_buffer:
            self.leverage = self.leverage_profit
        self.maxsize_buffer = self.balance

    def _close_position(
        self,
        position: OpenPosition,
        exit_price: float,
        exit_time: pd.Timestamp,
        exit_reason: str,
        exit_bar_index: int,
    ) -> None:
        pnl = (
            (exit_price - position.entry_price) * position.volume
            if position.side == "long"
            else (position.entry_price - exit_price) * position.volume
        )
        pnl = mt5_round(pnl, 2)
        self.balance = mt5_round(self.balance + pnl, 2)

        self.trades.append(
            {
                "side": position.side,
                "volume": position.volume,
                "entry_price": position.entry_price,
                "exit_price": mt5_round(exit_price, 6),
                "entry_time": position.entry_time,
                "exit_time": exit_time,
                "entry_reason": position.entry_reason,
                "exit_reason": exit_reason,
                "bars_in_position": max(0, exit_bar_index - position.entry_bar_index + 1),
                "pnl": pnl,
            }
        )
        self.positions.remove(position)

    def _close_all(self, bid: float, ask: float, exit_time: pd.Timestamp, exit_reason: str, exit_bar_index: int):
        for position in list(self.positions):
            exit_price = bid if position.side == "long" else ask
            self._close_position(position, exit_price, exit_time, exit_reason, exit_bar_index)

    def _check_stops(self, bid: float, ask: float, time_value: pd.Timestamp, bar_index: int) -> None:
        for position in list(self.positions):
            if position.side == "long" and bid <= position.stop_price:
                self._close_position(position, position.stop_price, time_value, "stop_loss", bar_index)
            elif position.side == "short" and ask >= position.stop_price:
                self._close_position(position, position.stop_price, time_value, "stop_loss", bar_index)

    def _band_snapshot(
        self,
        previous_index: int,
        provisional_high: float,
        provisional_low: float,
        provisional_close: float,
    ) -> tuple[float, float, float]:
        future_weighted = (provisional_high + provisional_low + provisional_close + provisional_close) / 4.0
        return centered_band_for_index(
            self.weighted,
            self.prefix_tr,
            previous_index,
            self.params.half_length,
            self.params.atr_period,
            self.params.atr_multiplier,
            future_weighted,
        )

    def _open_long(
        self,
        volume: float,
        ask: float,
        time_value: pd.Timestamp,
        bar_index: int,
        reason: str,
        stop_loss: float,
    ) -> None:
        if volume <= 0:
            return
        self.positions.append(
            OpenPosition(
                side="long",
                volume=volume,
                entry_price=mt5_round(ask, 6),
                stop_price=mt5_round(ask * (1.0 - stop_loss), 6),
                entry_time=time_value,
                entry_bar_index=bar_index,
                entry_reason=reason,
            )
        )

    def _open_short(
        self,
        volume: float,
        bid: float,
        time_value: pd.Timestamp,
        bar_index: int,
        reason: str,
        stop_loss: float,
    ) -> None:
        if volume <= 0:
            return
        self.positions.append(
            OpenPosition(
                side="short",
                volume=volume,
                entry_price=mt5_round(bid, 6),
                stop_price=mt5_round(bid * (1.0 + stop_loss), 6),
                entry_time=time_value,
                entry_bar_index=bar_index,
                entry_reason=reason,
            )
        )

    def _synthetic_ticks(self, bar: pd.Series) -> list[float]:
        raw_path = (
            [bar["open"], bar["low"], bar["high"], bar["close"]]
            if bar["close"] >= bar["open"]
            else [bar["open"], bar["high"], bar["low"], bar["close"]]
        )
        compact: list[float] = []
        for value in raw_path:
            if not compact or value != compact[-1]:
                compact.append(float(value))
        return compact

    def run(self) -> SimulationResult:
        for bar_index in range(len(self.df)):
            bar = self.df.iloc[bar_index]
            tick_path = self._synthetic_ticks(bar)

            provisional_high = float(bar["open"])
            provisional_low = float(bar["open"])

            for tick_offset, tick_price in enumerate(tick_path):
                provisional_high = max(provisional_high, tick_price)
                provisional_low = min(provisional_low, tick_price)
                time_value = pd.Timestamp(bar["time"]) + pd.Timedelta(seconds=tick_offset)
                ask, bid = self._split_price(tick_price)

                self._check_stops(bid, ask, time_value, bar_index)

                equity = self._mark_to_market_equity(bid, ask)
                saldo = equity - self.daily_capital

                if saldo < -4500.0:
                    self.tma_automatic = False
                    self.tma_automatic_buy = False
                    self.tma_automatic_sell = False
                    self.add = False
                    self.leverage = 0.5
                    self.kill_switch_hit = True
                    self._close_all(bid, ask, time_value, "daily_guard", bar_index)
                    continue

                if not self.positions:
                    self._refresh_flat_state()

                previous_index = bar_index - 1
                if previous_index < 0:
                    continue

                previous_bar = self.df.iloc[previous_index]
                _, upper_band, lower_band = self._band_snapshot(
                    previous_index,
                    provisional_high,
                    provisional_low,
                    tick_price,
                )

                type_position = self.type_position

                if ask < lower_band and type_position != "LONG" and self.tma_automatic:
                    self.tma_automatic_buy = True

                if bid > upper_band and type_position != "SHORT" and self.tma_automatic:
                    self.tma_automatic_sell = True

                maxsize = self._compute_maxsize(ask)

                if self.tma_automatic_buy and ask > float(previous_bar["close"]):
                    if type_position == "SHORT":
                        self._close_all(bid, ask, time_value, "flip_to_long", bar_index)
                        self._refresh_flat_state()
                        maxsize = self._compute_maxsize(ask)
                    self._open_long(maxsize, ask, time_value, bar_index, "tma_auto_buy", self.params.stop_loss)
                    self.tma_automatic_buy = False
                    self.tma_automatic_sell = False

                if self.tma_automatic_sell and bid < float(previous_bar["close"]):
                    if type_position == "LONG":
                        self._close_all(bid, ask, time_value, "flip_to_short", bar_index)
                        self._refresh_flat_state()
                        maxsize = self._compute_maxsize(ask)
                    self._open_short(maxsize, bid, time_value, bar_index, "tma_auto_sell", self.params.stop_loss)
                    self.tma_automatic_buy = False
                    self.tma_automatic_sell = False

                if self.add:
                    if len(self.positions) >= 2:
                        self.add = False
                    elif self.type_position == "NO POSITION":
                        self.add = False
                    elif (
                        self.type_position == "LONG"
                        and float(previous_bar["close"]) > float(previous_bar["open"])
                        and bid > float(previous_bar["close"])
                    ):
                        self._open_long(maxsize, ask, time_value, bar_index, "add_long", self.params.stop_loss_add)
                        self.add = False
                    elif (
                        self.type_position == "SHORT"
                        and float(previous_bar["close"]) < float(previous_bar["open"])
                        and ask < float(previous_bar["close"])
                    ):
                        self._open_short(maxsize, bid, time_value, bar_index, "add_short", self.params.stop_loss_add)
                        self.add = False

            final_ask, final_bid = self._split_price(float(bar["close"]))
            final_equity = self._mark_to_market_equity(final_bid, final_ask)
            self.equity_rows.append(
                {
                    "time": pd.Timestamp(bar["time"]),
                    "equity": mt5_round(final_equity, 2),
                    "balance": mt5_round(self.balance, 2),
                    "open_positions": len(self.positions),
                }
            )

        if self.params.force_close_on_end and self.positions:
            last_bar = self.df.iloc[-1]
            exit_time = pd.Timestamp(last_bar["time"]) + pd.Timedelta(seconds=4)
            ask, bid = self._split_price(float(last_bar["close"]))
            self._close_all(bid, ask, exit_time, "force_close_on_end", len(self.df) - 1)
            self.equity_rows[-1]["equity"] = mt5_round(self.balance, 2)
            self.equity_rows[-1]["balance"] = mt5_round(self.balance, 2)
            self.equity_rows[-1]["open_positions"] = 0

        ask, bid = self._split_price(float(self.df.iloc[-1]["close"]))
        final_equity = self._mark_to_market_equity(bid, ask)

        trades_df = pd.DataFrame(self.trades)
        if not trades_df.empty:
            trades_df = trades_df.sort_values(["entry_time", "exit_time"]).reset_index(drop=True)

        equity_df = pd.DataFrame(self.equity_rows)
        bands_df = visible_centered_tma(
            self.df,
            half_length=self.params.half_length,
            atr_period=self.params.atr_period,
            atr_multiplier=self.params.atr_multiplier,
        )
        summary = compute_summary(
            trades_df,
            equity_df,
            initial_capital=self.params.initial_capital,
            final_equity=final_equity,
            kill_switch_hit=self.kill_switch_hit,
        )

        return SimulationResult(
            params=self.params.as_dict(),
            trades=trades_df,
            equity=equity_df,
            summary=summary,
            bands=bands_df,
            open_positions=[
                {
                    "side": position.side,
                    "volume": position.volume,
                    "entry_price": position.entry_price,
                    "stop_price": position.stop_price,
                    "entry_time": position.entry_time.isoformat(),
                    "entry_reason": position.entry_reason,
                }
                for position in self.positions
            ],
        )


def run_backtest(df: pd.DataFrame, params: StrategyParams) -> SimulationResult:
    simulator = IcarusMmsSimulator(df, params)
    return simulator.run()
