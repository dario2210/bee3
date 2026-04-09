from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import product


@dataclass(slots=True)
class StrategyParams:
    initial_capital: float = 100000.0
    daily_capital: float = 100000.0
    half_length: int = 12
    atr_period: int = 200
    atr_multiplier: float = 2.8
    stop_loss: float = 0.01
    stop_loss_add: float = 0.01
    leverage_initial: float = 0.5
    leverage_profit: float = 0.5
    leverage_loss: float = 0.01
    tma_automatic: bool = True
    add_enabled: bool = False
    spread_bps: float = 0.0
    force_close_on_end: bool = False

    def as_dict(self) -> dict[str, float | int | bool]:
        return asdict(self)


DEFAULT_PARAMS = StrategyParams()


@dataclass(slots=True)
class WfoConfig:
    train_bars: int = 350
    test_bars: int = 120
    step_bars: int = 120
    scoring_mode: str = "balanced"
    half_length_grid: tuple[int, ...] = (10, 12, 14)
    atr_period_grid: tuple[int, ...] = (150, 200, 250)
    atr_multiplier_grid: tuple[float, ...] = (2.4, 2.8, 3.2)
    stop_loss_grid: tuple[float, ...] = (0.008, 0.01, 0.012)
    stop_loss_add_grid: tuple[float, ...] = (0.008, 0.01, 0.012)
    leverage_profit_grid: tuple[float, ...] = (0.25, 0.5, 1.0)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


DEFAULT_WFO = WfoConfig()


def parse_grid(value: str | list | tuple | None, cast):
    if value is None:
        return tuple()
    if isinstance(value, (list, tuple)):
        return tuple(cast(item) for item in value)
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    return tuple(cast(item) for item in items)


def strategy_params_from_payload(payload: dict | None) -> StrategyParams:
    payload = payload or {}
    base = DEFAULT_PARAMS.as_dict()
    base.update(payload)
    if not base.get("daily_capital"):
        base["daily_capital"] = float(base["initial_capital"])
    return StrategyParams(
        initial_capital=float(base["initial_capital"]),
        daily_capital=float(base["daily_capital"]),
        half_length=int(base["half_length"]),
        atr_period=int(base["atr_period"]),
        atr_multiplier=float(base["atr_multiplier"]),
        stop_loss=float(base["stop_loss"]),
        stop_loss_add=float(base["stop_loss_add"]),
        leverage_initial=float(base["leverage_initial"]),
        leverage_profit=float(base["leverage_profit"]),
        leverage_loss=float(base["leverage_loss"]),
        tma_automatic=bool(base["tma_automatic"]),
        add_enabled=bool(base["add_enabled"]),
        spread_bps=float(base["spread_bps"]),
        force_close_on_end=bool(base.get("force_close_on_end", False)),
    )


def wfo_config_from_payload(payload: dict | None) -> WfoConfig:
    payload = payload or {}
    return WfoConfig(
        train_bars=int(payload.get("train_bars", DEFAULT_WFO.train_bars)),
        test_bars=int(payload.get("test_bars", DEFAULT_WFO.test_bars)),
        step_bars=int(payload.get("step_bars", DEFAULT_WFO.step_bars)),
        scoring_mode=str(payload.get("scoring_mode", DEFAULT_WFO.scoring_mode)),
        half_length_grid=parse_grid(payload.get("half_length_grid", DEFAULT_WFO.half_length_grid), int)
        or DEFAULT_WFO.half_length_grid,
        atr_period_grid=parse_grid(payload.get("atr_period_grid", DEFAULT_WFO.atr_period_grid), int)
        or DEFAULT_WFO.atr_period_grid,
        atr_multiplier_grid=parse_grid(
            payload.get("atr_multiplier_grid", DEFAULT_WFO.atr_multiplier_grid), float
        )
        or DEFAULT_WFO.atr_multiplier_grid,
        stop_loss_grid=parse_grid(payload.get("stop_loss_grid", DEFAULT_WFO.stop_loss_grid), float)
        or DEFAULT_WFO.stop_loss_grid,
        stop_loss_add_grid=parse_grid(
            payload.get("stop_loss_add_grid", DEFAULT_WFO.stop_loss_add_grid), float
        )
        or DEFAULT_WFO.stop_loss_add_grid,
        leverage_profit_grid=parse_grid(
            payload.get("leverage_profit_grid", DEFAULT_WFO.leverage_profit_grid), float
        )
        or DEFAULT_WFO.leverage_profit_grid,
    )


def iter_wfo_param_grid(base: StrategyParams, config: WfoConfig):
    for half_length, atr_period, atr_multiplier, stop_loss, stop_loss_add, leverage_profit in product(
        config.half_length_grid,
        config.atr_period_grid,
        config.atr_multiplier_grid,
        config.stop_loss_grid,
        config.stop_loss_add_grid,
        config.leverage_profit_grid,
    ):
        yield StrategyParams(
            initial_capital=base.initial_capital,
            daily_capital=base.daily_capital,
            half_length=half_length,
            atr_period=atr_period,
            atr_multiplier=atr_multiplier,
            stop_loss=stop_loss,
            stop_loss_add=stop_loss_add,
            leverage_initial=base.leverage_initial,
            leverage_profit=leverage_profit,
            leverage_loss=base.leverage_loss,
            tma_automatic=base.tma_automatic,
            add_enabled=base.add_enabled,
            spread_bps=base.spread_bps,
            force_close_on_end=True,
        )
