# Bee3 Icarus MMS Lab

`bee3` to osobny dashboard do backtestu i WFO strategii odtworzonej z:

- `FTMO_ICARUSMMS_MAIN.mq5`
- `tma_indikator.mq5`

Na tym etapie aplikacja:

- odtwarza logikę TMA oraz mechanikę pozycji i kapitału z EA,
- pozwala wrzucać własne CSV OHLCV,
- uruchamia pojedynczy backtest albo walk-forward optimization,
- pokazuje transakcje na wykresie opartym o TradingView Lightweight Charts,
- zapisuje ostatnie wyniki do katalogu `results/`.

## Start lokalny

```bash
pip install -r requirements.txt
uvicorn bee3_dashboard:app --host 0.0.0.0 --port 8061
```

Potem otwórz:

```text
http://127.0.0.1:8061
```

## CSV

CSV powinien zawierać co najmniej:

- `time`
- `open`
- `high`
- `low`
- `close`

Opcjonalnie:

- `volume`

`time` może być w ISO-8601 albo jako unix timestamp w sekundach lub milisekundach.

## Ważna uwaga o 1:1

Silnik odwzorowuje logikę wejść, wyjść, TMA i money management z MQL.
Jeżeli dane wejściowe są świecowe, wykonanie intrabar jest symulowane po ścieżce:

- świeca wzrostowa: `open -> low -> high -> close`
- świeca spadkowa: `open -> high -> low -> close`

To pozwala testować strategię na OHLC bez ticków. Gdy później dołożymy feed tickowy,
warstwa wykonania może zostać jeszcze doszczelniona bez zmiany reguł strategii.
