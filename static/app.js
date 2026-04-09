const state = {
  chart: null,
  candleSeries: null,
  upperSeries: null,
  lowerSeries: null,
  markersApi: null,
  lastPayload: null,
};

function el(id) {
  return document.getElementById(id);
}

function status(message) {
  el("status-box").textContent = message;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const contentType = response.headers.get("content-type") || "";
  const data = contentType.includes("application/json") ? await response.json() : { detail: await response.text() };
  if (!response.ok) {
    throw new Error(data.detail || "Request failed");
  }
  return data;
}

function metricCard(label, value, sub = "") {
  const card = document.createElement("div");
  card.className = "metric";
  card.innerHTML = `
    <div class="label">${label}</div>
    <div class="value">${value}</div>
    <div class="sub">${sub}</div>
  `;
  return card;
}

function renderMetrics(summary = {}) {
  const grid = el("metrics-grid");
  grid.innerHTML = "";
  const cards = [
    ["Net PnL", `${(summary.net_pnl ?? 0).toFixed(2)} USD`, `Return ${(summary.return_pct ?? 0).toFixed(2)}%`],
    ["Final Equity", `${(summary.final_equity ?? 0).toFixed(2)} USD`, `Start ${(summary.initial_capital ?? 0).toFixed(2)} USD`],
    ["Max DD", `${(summary.max_drawdown_pct ?? 0).toFixed(2)}%`, "drawdown"],
    ["Trades", `${summary.trade_count ?? 0}`, `Win rate ${(summary.win_rate_pct ?? 0).toFixed(2)}%`],
    ["Profit Factor", `${(summary.profit_factor ?? 0).toFixed(2)}`, `Avg ${(summary.avg_trade_pnl ?? 0).toFixed(2)} USD`],
    ["Kill Switch", summary.kill_switch_hit ? "ARMED" : "idle", "daily guard -4500"],
  ];
  cards.forEach(([label, value, sub]) => grid.appendChild(metricCard(label, value, sub)));
}

function renderTable(tableId, rows, columns) {
  const table = el(tableId);
  if (!rows || !rows.length) {
    table.innerHTML = "<tr><td>Brak danych.</td></tr>";
    return;
  }

  const thead = `<thead><tr>${columns.map((column) => `<th>${column.label}</th>`).join("")}</tr></thead>`;
  const tbody = rows
    .map((row) => `<tr>${columns.map((column) => `<td>${row[column.key] ?? ""}</td>`).join("")}</tr>`)
    .join("");

  table.innerHTML = `${thead}<tbody>${tbody}</tbody>`;
}

function buildChart() {
  const container = el("chart");
  container.innerHTML = "";
  state.chart = LightweightCharts.createChart(container, {
    layout: {
      background: { color: "transparent" },
      textColor: "#dbeafe",
    },
    grid: {
      vertLines: { color: "rgba(255,255,255,0.06)" },
      horzLines: { color: "rgba(255,255,255,0.06)" },
    },
    rightPriceScale: {
      borderColor: "rgba(255,255,255,0.12)",
    },
    timeScale: {
      borderColor: "rgba(255,255,255,0.12)",
      timeVisible: true,
      secondsVisible: false,
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
  });

  state.candleSeries = state.chart.addSeries(LightweightCharts.CandlestickSeries, {
    upColor: "#22d3aa",
    downColor: "#ff7a59",
    wickUpColor: "#22d3aa",
    wickDownColor: "#ff7a59",
    borderVisible: false,
  });

  state.upperSeries = state.chart.addSeries(LightweightCharts.LineSeries, {
    color: "#69b7ff",
    lineWidth: 2,
  });

  state.lowerSeries = state.chart.addSeries(LightweightCharts.LineSeries, {
    color: "#ff5d73",
    lineWidth: 2,
  });

  new ResizeObserver(() => {
    state.chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
  }).observe(container);

  state.chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
}

function renderChart(chartData) {
  if (!state.chart) {
    buildChart();
  }

  state.candleSeries.setData(chartData.candles || []);
  state.upperSeries.setData(chartData.upperBand || []);
  state.lowerSeries.setData(chartData.lowerBand || []);

  if (state.markersApi) {
    state.markersApi.setMarkers([]);
  }
  state.markersApi = LightweightCharts.createSeriesMarkers(state.candleSeries, chartData.markers || []);
  state.chart.timeScale().fitContent();
}

function renderPayload(payload) {
  state.lastPayload = payload;
  renderMetrics(payload.summary || {});
  renderChart(payload.chart || { candles: [], upperBand: [], lowerBand: [], markers: [] });

  renderTable("trades-table", payload.trades || [], [
    { key: "entry_time", label: "Entry" },
    { key: "exit_time", label: "Exit" },
    { key: "side", label: "Side" },
    { key: "volume", label: "Vol" },
    { key: "entry_reason", label: "Reason In" },
    { key: "exit_reason", label: "Reason Out" },
    { key: "pnl", label: "PnL" },
  ]);

  renderTable("windows-table", payload.windows || [], [
    { key: "window_id", label: "Window" },
    { key: "best_half_length", label: "Half" },
    { key: "best_atr_period", label: "ATR" },
    { key: "best_atr_multiplier", label: "ATR mult" },
    { key: "best_stop_loss", label: "SL" },
    { key: "live_return_pct", label: "Live %" },
    { key: "live_trade_count", label: "Trades" },
  ]);

  const diagnostic = {
    dataset: payload.dataset,
    mode: payload.mode,
    best_params: payload.best_params || payload.params || {},
    open_positions: payload.open_positions || [],
    trade_count_total: payload.trade_count_total || 0,
    ui_trade_rows: (payload.trades || []).length,
  };
  el("result-json").textContent = JSON.stringify(diagnostic, null, 2);
}

function gatherStrategyParams() {
  return {
    initial_capital: Number(el("initial-capital").value),
    daily_capital: Number(el("daily-capital").value),
    half_length: Number(el("half-length").value),
    atr_period: Number(el("atr-period").value),
    atr_multiplier: Number(el("atr-multiplier").value),
    stop_loss: Number(el("stop-loss").value),
    stop_loss_add: Number(el("stop-loss-add").value),
    leverage_initial: Number(el("leverage-initial").value),
    leverage_profit: Number(el("leverage-profit").value),
    leverage_loss: Number(el("leverage-loss").value),
    spread_bps: Number(el("spread-bps").value),
  };
}

function gatherWfoConfig() {
  return {
    train_bars: Number(el("wfo-train-bars").value),
    test_bars: Number(el("wfo-test-bars").value),
    step_bars: Number(el("wfo-step-bars").value),
    scoring_mode: el("wfo-scoring").value,
    half_length_grid: el("grid-half-length").value,
    atr_period_grid: el("grid-atr-period").value,
    atr_multiplier_grid: el("grid-atr-multiplier").value,
    stop_loss_grid: el("grid-stop-loss").value,
    stop_loss_add_grid: el("grid-stop-loss-add").value,
    leverage_profit_grid: el("grid-leverage-profit").value,
  };
}

function gatherRunRange() {
  return {
    start_date: el("run-start-date").value || null,
    end_date: el("run-end-date").value || null,
  };
}

function gatherFetchConfig() {
  return {
    symbol: el("fetch-symbol").value.trim().toUpperCase(),
    interval: el("fetch-interval").value,
    market: el("fetch-market").value,
    start_date: el("fetch-start-date").value,
    end_date: el("fetch-end-date").value || null,
  };
}

function populateSelect(select, values, preferred) {
  const previous = preferred || select.value;
  select.innerHTML = "";
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
  if (previous && values.includes(previous)) {
    select.value = previous;
  }
}

async function loadMarketConfig() {
  const config = await fetchJson("/api/market-config");
  populateSelect(el("fetch-interval"), config.intervals, el("fetch-interval").value || "1h");
  populateSelect(el("fetch-market"), config.markets, el("fetch-market").value || "spot");
}

async function loadDatasets(selectName = null) {
  const data = await fetchJson("/api/datasets");
  const select = el("dataset-select");
  select.innerHTML = "";

  data.datasets.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.name;
    option.textContent = `${dataset.name} (${dataset.rows} rows)`;
    select.appendChild(option);
  });

  if (selectName) {
    select.value = selectName;
  }
}

async function fetchBinanceDataset() {
  const config = gatherFetchConfig();
  if (!config.symbol || !config.start_date) {
    status("Podaj symbol i date startu.");
    return;
  }
  status(`Pobieram swiece ${config.symbol} ${config.interval} z Binance...`);
  const payload = await fetchJson("/api/fetch-binance", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  await loadDatasets(payload.download.dataset);
  el("run-start-date").value = config.start_date || "";
  el("run-end-date").value = config.end_date || "";
  status(
    `Pobrano ${payload.download.rows} swiec do ${payload.download.dataset} (${payload.download.start_time} -> ${payload.download.end_time}).`
  );
}

async function runBacktest() {
  const dataset = el("dataset-select").value;
  if (!dataset) {
    status("Najpierw wybierz dataset albo pobierz dane.");
    return;
  }
  status("Uruchamiam backtest...");
  const payload = await fetchJson("/api/backtest", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      dataset,
      params: gatherStrategyParams(),
      range: gatherRunRange(),
      force_close_on_end: el("force-close").checked,
    }),
  });
  renderPayload(payload);
  status(`Backtest gotowy dla ${dataset}. Tabela pokazuje do 600 ostatnich transakcji.`);
}

async function runWfo() {
  const dataset = el("dataset-select").value;
  if (!dataset) {
    status("Najpierw wybierz dataset albo pobierz dane.");
    return;
  }
  status("Uruchamiam WFO. Przy wiekszym zakresie moze to potrwac kilkadziesiat sekund...");
  const payload = await fetchJson("/api/wfo", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      dataset,
      params: gatherStrategyParams(),
      range: gatherRunRange(),
      wfo: gatherWfoConfig(),
    }),
  });
  renderPayload(payload);
  status(`WFO gotowe dla ${dataset}. Tabela pokazuje do 600 ostatnich transakcji.`);
}

async function uploadDataset() {
  const input = el("dataset-upload");
  const file = input.files?.[0];
  if (!file) {
    return;
  }
  status(`Wgrywam ${file.name}...`);
  const formData = new FormData();
  formData.append("file", file);
  const payload = await fetchJson("/api/upload", { method: "POST", body: formData });
  await loadDatasets(payload.dataset);
  status(`Dataset ${payload.dataset} zapisany.`);
  input.value = "";
}

async function bootstrap() {
  buildChart();
  await loadMarketConfig();
  await loadDatasets();

  el("fetch-binance").addEventListener("click", () => fetchBinanceDataset().catch((error) => status(error.message)));
  el("run-backtest").addEventListener("click", () => runBacktest().catch((error) => status(error.message)));
  el("run-wfo").addEventListener("click", () => runWfo().catch((error) => status(error.message)));
  el("refresh-datasets").addEventListener("click", () => loadDatasets().catch((error) => status(error.message)));
  el("dataset-upload").addEventListener("change", () => uploadDataset().catch((error) => status(error.message)));
}

bootstrap().catch((error) => status(error.message));
