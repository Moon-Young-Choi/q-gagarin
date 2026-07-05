(function () {
  const COLORS = {
    profit: "#d83f7b",
    neutral: "#2d6f9f",
    unavailable: "#aeb8c5",
    orange: "#e08a17",
  };
  const GROUP_LABELS = {
    KRW_START: "KRW Start",
    BTC_START: "BTC Start",
    USDT_START: "USDT Start",
    ALL: "All",
  };

  const chart = document.getElementById("chart");
  const summaryCards = document.getElementById("summaryCards");
  const performanceCards = document.getElementById("performanceCards");
  const exchangeCards = document.getElementById("exchangeCards");
  const marketConfigCards = document.getElementById("marketConfigCards");
  const executionCards = document.getElementById("executionCards");
  const realRunTables = document.getElementById("realRunTables");
  const settingsConfig = document.getElementById("settingsConfig");
  const strategyCards = document.getElementById("strategyCards");
  const strategyList = document.getElementById("strategyList");
  const logTable = document.getElementById("logTable");
  const dryRunSummaryCards = document.getElementById("dryRunSummaryCards");
  const logModeFilter = document.getElementById("logModeFilter");
  const logTypeFilter = document.getElementById("logTypeFilter");
  const logStartAssetFilter = document.getElementById("logStartAssetFilter");
  const logStrategyFilter = document.getElementById("logStrategyFilter");
  const logCycleFilter = document.getElementById("logCycleFilter");
  const refreshLogsButton = document.getElementById("refreshLogsButton");
  const exportDryRunJsonButton = document.getElementById("exportDryRunJsonButton");
  const exportDryRunCsvButton = document.getElementById("exportDryRunCsvButton");
  const connectionLine = document.getElementById("connectionLine");
  const detailBody = document.getElementById("detailBody");
  const observeButton = document.getElementById("observeButton");
  const dryRunButton = document.getElementById("dryRunButton");
  const realGuardedButton = document.getElementById("realGuardedButton");
  const pauseButton = document.getElementById("pauseButton");
  const stopButton = document.getElementById("stopButton");
  const emergencyStopButton = document.getElementById("emergencyStopButton");
  const unpinButton = document.getElementById("unpinButton");
  const feeInput = document.getElementById("feeInput");
  const staleInput = document.getElementById("staleInput");
  const autoScaleInput = document.getElementById("autoScaleInput");
  const showUnavailableInput = document.getElementById("showUnavailableInput");
  const groupFilters = document.getElementById("groupFilters");
  const toast = document.getElementById("toast");
  const tabButtons = [...document.querySelectorAll("[data-tab-target]")];
  const tabPanels = [...document.querySelectorAll("[data-tab-panel]")];

  let latestState = null;
  let chartRows = [];
  let cycleIdToStateIndex = new Map();
  let cycleIdToPointIndex = new Map();
  let chartInitialized = false;
  let chartRenderPending = false;
  let eventsBound = false;
  let clampRelayout = false;
  let toastTimer = null;
  let liveSocket = null;
  let reconnectTimer = null;
  let reconnectAttempt = 0;
  let sseFallbackStarted = false;
  let pendingFrame = false;
  let pendingChangedCycleIds = new Set();
  let activeDetailCycleId = null;
  let renderedFrames = 0;
  let renderFramesPerSec = 0;
  let lastRenderDurationMs = 0;
  let renderDurationSamples = [];
  let cachedLogRows = [];
  let lastFrameReportAt = performance.now();
  let lastChartRenderAt = 0;
  let deferredRestyleTimer = null;
  let pendingCommandId = null;
  let pendingCommandTimer = null;
  const enabledGroups = new Set(["KRW_START", "BTC_START", "USDT_START"]);

  function parseNumber(value, fallback) {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function feeMetrics(feeRate) {
    const safeFeeRate = Math.max(0, Math.min(feeRate, 0.999999));
    const feeFactor = (1 - safeFeeRate) ** 3;

    return {
      feeRate: safeFeeRate,
      feeFactor,
      executableBreakEvenGross: 1 / feeFactor,
    };
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function formatNumber(value, digits = 8) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return "n/a";
    return Number(value).toLocaleString(undefined, {
      maximumFractionDigits: Math.max(digits, 8),
      minimumFractionDigits: digits,
    });
  }

  function formatCompact(value, digits = 2) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return "n/a";
    return Number(value).toLocaleString(undefined, {
      maximumFractionDigits: digits,
      minimumFractionDigits: digits,
    });
  }

  function formatPercent(value) {
    return value === null || value === undefined ? "n/a" : `${(Number(value) * 100).toFixed(4)}%`;
  }

  function formatMs(value) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return "n/a";
    return `${Number(value).toFixed(Math.abs(value) < 10 ? 3 : 1)} ms`;
  }

  function formatBytes(value) {
    if (!(value > 0)) return "n/a";
    const mib = value / 1024 / 1024;
    return `${mib.toFixed(1)} MiB`;
  }

  function formatTime(value) {
    if (!value) return "n/a";
    const date = typeof value === "number" ? new Date(value) : new Date(value);
    return Number.isNaN(date.getTime()) ? "n/a" : date.toLocaleString();
  }

  function formatLocalTimestamp(date = new Date()) {
    const pad = (value) => String(value).padStart(2, "0");
    return (
      `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}-` +
      `${pad(date.getHours())}${pad(date.getMinutes())}${pad(date.getSeconds())}`
    );
  }

  function showToast(message) {
    clearTimeout(toastTimer);
    toast.textContent = message;
    toast.classList.add("show");
    toastTimer = setTimeout(() => {
      toast.classList.remove("show");
    }, 3600);
  }

  function metric(label, value) {
    return `<div class="metric-card"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(value)}</span></div>`;
  }

  function statusRow(label, value, statusClass = "") {
    const valueHtml = statusClass
      ? `<span class="status-pill ${escapeHtml(statusClass)}">${escapeHtml(value)}</span>`
      : `<span>${escapeHtml(value)}</span>`;

    return `<div class="status-row"><strong>${escapeHtml(label)}</strong>${valueHtml}</div>`;
  }

  function runtimeConfig() {
    return (latestState && latestState.runtimeConfig) || {};
  }

  function engineState() {
    return latestState && (latestState.engineState || (latestState.engine && latestState.engine.state)) || "UNKNOWN";
  }

  function realRunReady() {
    const config = runtimeConfig();
    return Boolean(
      config.liveTradingEnabled &&
      latestState &&
      latestState.readiness &&
      latestState.readiness.passed &&
      latestState.privateWsStatus &&
      latestState.privateWsStatus.status === "open" &&
      latestState.privateCacheStatus &&
      latestState.privateCacheStatus.orderChanceFresh &&
      latestState.privateCacheStatus.accountBalanceFresh,
    );
  }

  function updateCommandButtons() {
    const state = engineState();
    const running = state === "RUNNING";
    const paused = state === "PAUSED";
    const stopped = state === "STOPPED";
    const pending = Boolean(pendingCommandId);

    observeButton.disabled = pending || !stopped;
    dryRunButton.disabled = pending || !stopped;
    realGuardedButton.disabled = pending || !stopped || !realRunReady();
    realGuardedButton.title = realGuardedButton.disabled && !realRunReady()
      ? "Real Guarded requires live trading, readiness PASS, private WS, and fresh private caches."
      : "";
    pauseButton.disabled = pending || !running;
    stopButton.disabled = pending || !(running || paused);
    emergencyStopButton.disabled = pending || !(running || paused || state === "ERROR");
  }

  function renderConfigTable(config) {
    const rows = Object.entries(config || {})
      .map(([key, value]) => (
        `<tr><th>${escapeHtml(key)}</th><td>${escapeHtml(Array.isArray(value) ? value.join(", ") : value)}</td></tr>`
      ))
      .join("");

    const readiness = latestState && latestState.readiness;
    const readinessRows = readiness && Array.isArray(readiness.items)
      ? readiness.items.map((entry) => statusRow(entry.label, entry.passed ? "PASS" : "FAIL", entry.passed ? "ok" : "off")).join("")
      : statusRow("Readiness", "n/a");

    settingsConfig.innerHTML = (
      (rows
        ? `<div class="detail-section"><div class="detail-title">Runtime config</div><div class="table-wrap"><table><tbody>${rows}</tbody></table></div></div>`
        : "<div class=\"status-list wide\"><div class=\"status-row\"><strong>Runtime config</strong><span>n/a</span></div></div>") +
      `<div class="detail-section"><div class="detail-title">Real-run readiness</div><div class="status-list wide">${readinessRows}</div></div>`
    );
  }

  function updateExchangeCards() {
    exchangeCards.innerHTML = [
      statusRow("Upbit", "Enabled", "ok"),
      statusRow("Binance", "Not implemented", "off"),
      statusRow("Bithumb", "Not implemented", "off"),
      statusRow("Bybit", "Not implemented", "off"),
    ].join("");
  }

  function updateRuntimePanels(metrics) {
    const config = runtimeConfig();
    const summary = latestState.summary || {};
    const stores = latestState.orderbookStores || {};
    const privateCache = latestState.privateCacheStatus || {};
    const observation = stores.observation || {};
    const validation = stores.validation || {};
    const feedStatus = latestState.feedStatus || {};
    const observationFeed = feedStatus.observation || latestState.wsStatus || {};
    const validationFeed = feedStatus.validation || {};

    marketConfigCards.innerHTML = [
      statusRow("Exchange", config.exchange || "upbit"),
      statusRow("Start assets", (config.enabledStartAssets || ["KRW", "BTC", "USDT"]).join(", ")),
      statusRow("Observation depth", config.observationOrderbookUnit || "n/a"),
      statusRow("Validation depth", config.validationOrderbookUnit || "n/a"),
      statusRow("Observation feed", `${observationFeed.openConnectionCount || 0}/${observationFeed.connectionCount || 0} open`),
      statusRow("Validation feed", `${validationFeed.openConnectionCount || 0}/${validationFeed.connectionCount || 0} open`),
      statusRow("Observation markets", observation.marketCount || "n/a"),
      statusRow("Validation markets", validation.marketCount || "n/a"),
      statusRow("Observation stale", observation.staleCount || 0),
      statusRow("Validation stale", validation.staleCount || 0),
      statusRow("Observation latency", formatMs(observation.averageLatencyMs)),
      statusRow("Validation latency", formatMs(validation.averageLatencyMs)),
      statusRow("Fee rate", String(metrics.feeRate)),
      statusRow("Break-even", formatNumber(metrics.executableBreakEvenGross)),
      statusRow("Stale threshold", `${summary.staleOrderbookMs || staleInput.value} ms`),
      statusRow("Required markets", summary.requiredMarketCount || "n/a"),
    ].join("");

    executionCards.innerHTML = [
      statusRow("Engine state", latestState.engineState || (latestState.engine && latestState.engine.state) || "n/a"),
      statusRow("Execution context", latestState.execution && latestState.execution.mode ? latestState.execution.mode : config.runMode || "OBSERVE"),
      statusRow("Run mode", config.runMode || "OBSERVE"),
      statusRow("Execution mode", config.executionMode || "LIMIT_IOC_AT_OBSERVED_BEST"),
      statusRow("Live trading", config.liveTradingEnabled ? "Enabled" : "Disabled", config.liveTradingEnabled ? "" : "off"),
      statusRow("Guard healthy", latestState.guardStatus && latestState.guardStatus.healthy ? "Yes" : "No", latestState.guardStatus && latestState.guardStatus.healthy ? "ok" : "off"),
      statusRow("Consecutive failures", latestState.guardStatus ? latestState.guardStatus.consecutiveFailures : "n/a"),
      statusRow("Open orders guard", latestState.guardStatus ? `${latestState.guardStatus.openOrderCount}/${latestState.guardStatus.maxOpenOrders}` : "n/a"),
      statusRow("Active real executions", latestState.guardStatus ? latestState.guardStatus.activeRealExecutionCount : "n/a"),
      statusRow("Readiness", latestState.readiness ? (latestState.readiness.passed ? "PASS" : "FAIL") : "n/a", latestState.readiness && latestState.readiness.passed ? "ok" : "off"),
      statusRow(
        "Dry-run balances",
        latestState.execution && latestState.execution.dryRunBalances
          ? Object.entries(latestState.execution.dryRunBalances).map(([asset, value]) => `${asset} ${value}`).join(" / ")
          : "n/a",
      ),
      statusRow("Private WS", latestState.privateWsStatus ? latestState.privateWsStatus.status : "not configured"),
      statusRow("Order chance cache", privateCache.orderChanceFresh ? "Fresh" : "Stale", privateCache.orderChanceFresh ? "ok" : "off"),
      statusRow("Account balance cache", privateCache.accountBalanceFresh ? "Fresh" : "Stale", privateCache.accountBalanceFresh ? "ok" : "off"),
      statusRow("Tracked orders", latestState.execution && latestState.execution.orders ? latestState.execution.orders.length : 0),
      statusRow("Recent fills", latestState.execution && latestState.execution.fills ? latestState.execution.fills.length : 0),
      statusRow("Order submission", config.liveTradingEnabled ? "Guarded real executor" : "Disabled by config", config.liveTradingEnabled ? "ok" : "off"),
      statusRow("Fill tracking", latestState.privateWsStatus && latestState.privateWsStatus.status === "open" ? "Private MyOrder WS" : "REST fallback / dry-run logs"),
    ].join("");

    renderConfigTable(config);
    updateExecutionTables();
  }

  function compactJson(value) {
    if (value === null || value === undefined) return "n/a";
    if (typeof value !== "object") return value;
    return JSON.stringify(value);
  }

  function updateExecutionTables() {
    const execution = latestState.execution || {};
    const orders = (execution.orders || []).filter((row) => row.mode !== "DRY_RUN").slice(-30).reverse();
    const fills = (execution.fills || []).filter((row) => row.mode !== "DRY_RUN").slice(-30).reverse();

    const orderRows = orders.map((row) => (
      `<tr><td>${escapeHtml(row.uuid || row.identifier || "n/a")}</td>` +
      `<td>${escapeHtml(row.market || "n/a")}</td>` +
      `<td>${escapeHtml(row.side || "n/a")}</td>` +
      `<td>${escapeHtml(row.state || "n/a")}</td>` +
      `<td>${escapeHtml(formatNumber(row.price, 8))}</td>` +
      `<td>${escapeHtml(formatNumber(row.remainingVolume, 8))}</td></tr>`
    )).join("");
    const fillRows = fills.map((row) => (
      `<tr><td>${escapeHtml(row.uuid || row.identifier || "n/a")}</td>` +
      `<td>${escapeHtml(row.market || "n/a")}</td>` +
      `<td>${escapeHtml(formatNumber(row.executedVolume, 8))}</td>` +
      `<td>${escapeHtml(formatNumber(row.paidFee, 8))}</td>` +
      `<td>${escapeHtml(formatNumber(row.tradeFee, 8))}</td>` +
      `<td>${escapeHtml(formatTime(row.tradeTimestamp || row.eventTimestamp))}</td></tr>`
    )).join("");

    realRunTables.innerHTML = (
      `<div class="detail-section"><div class="detail-title">Latest real-run orders</div>` +
      `<div class="table-wrap"><table><thead><tr>` +
      `<th>UUID/Identifier</th><th>Market</th><th>Side</th><th>State</th><th>Price</th><th>Remaining</th>` +
      `</tr></thead><tbody>${orderRows || "<tr><td colspan=\"6\">n/a</td></tr>"}</tbody></table></div></div>` +
      `<div class="detail-section"><div class="detail-title">Latest real-run fills</div>` +
      `<div class="table-wrap"><table><thead><tr>` +
      `<th>UUID/Identifier</th><th>Market</th><th>Executed</th><th>Paid fee</th><th>Trade fee</th><th>Time</th>` +
      `</tr></thead><tbody>${fillRows || "<tr><td colspan=\"6\">n/a</td></tr>"}</tbody></table></div></div>`
    );
  }

  function updateStrategyPanels() {
    const strategy = latestState.strategy || {};
    const active = strategy.activeStrategy || {};

    strategyCards.innerHTML = [
      statusRow("Active strategy", active.name || strategy.activeStrategyId || "n/a"),
      statusRow("Strategy id", active.id || "n/a"),
      statusRow("Version", active.version || "n/a"),
      statusRow("Hash", active.hash || "n/a"),
      statusRow("Source", "Source files"),
      statusRow("Dashboard editing", "Disabled", "off"),
      statusRow("Engine state", latestState.engineState || "n/a"),
    ].join("");

    strategyList.innerHTML = (strategy.availableStrategies || [])
      .map((item) => (
        `<article class="strategy-card">` +
        `<h3>${escapeHtml(item.name)} ${item.id === active.id ? "<span class=\"status-pill ok\">Active</span>" : ""}</h3>` +
        `<p>${escapeHtml(item.description)}</p>` +
        `<p>v${escapeHtml(item.version)} | ${escapeHtml(item.hash || "n/a")}</p>` +
        `</article>`
      ))
      .join("");
  }

  function updateLogPanel() {
    const events = cachedLogRows.length > 0 ? cachedLogRows : latestState.eventLog || [];

    if (events.length === 0) {
      logTable.innerHTML = "<div class=\"status-list wide\"><div class=\"status-row\"><strong>Events</strong><span>n/a</span></div></div>";
      return;
    }

    const rows = events.slice(-80).reverse().map((event) => (
      `<tr>` +
      `<td>${escapeHtml(formatTime(event.timestamp))}</td>` +
      `<td>${escapeHtml(event.mode || "n/a")}</td>` +
      `<td>${escapeHtml(event.normalizedType || event.type)}</td>` +
      `<td>${escapeHtml(event.startAsset || "n/a")}</td>` +
      `<td>${escapeHtml(event.cycleId || event.strategyId || event.planId || "n/a")}</td>` +
      `<td>${escapeHtml(event.reason || event.validationReason || event.message || "n/a")}</td>` +
      `</tr>`
    )).join("");

    logTable.innerHTML = (
      `<div class="table-wrap"><table><thead><tr>` +
      `<th>Time</th><th>Mode</th><th>Type</th><th>Start</th><th>Subject</th><th>Reason</th>` +
      `</tr></thead><tbody>${rows}</tbody></table></div>`
    );
  }

  function updateDryRunSummary(summary) {
    if (!summary) {
      dryRunSummaryCards.innerHTML = "";
      return;
    }

    dryRunSummaryCards.innerHTML = [
      metric("Opportunities", summary.totalOpportunities),
      metric("Accepted", summary.accepted),
      metric("Rejected", summary.rejected),
      metric("Sim done", summary.simulatedCompleteCycles),
      metric("Sim failed", summary.simulatedFailedCycles),
      metric("Expected net", formatNumber(summary.expectedNetProfit, 8)),
      metric("Sim net", formatNumber(summary.simulatedNetProfit, 8)),
      metric("Latency p95", formatMs(summary.latencyDistribution && summary.latencyDistribution.p95Ms)),
    ].join("");
  }

  function logQueryParams() {
    const params = new URLSearchParams();
    if (logModeFilter.value) params.set("mode", logModeFilter.value);
    if (logTypeFilter.value) params.set("type", logTypeFilter.value);
    if (logStartAssetFilter.value) params.set("startAsset", logStartAssetFilter.value);
    if (logStrategyFilter.value) params.set("strategyId", logStrategyFilter.value);
    if (logCycleFilter.value) params.set("cycleId", logCycleFilter.value);
    params.set("limit", "500");
    return params;
  }

  async function refreshLogs() {
    const response = await fetch(`/api/logs?${logQueryParams().toString()}`);
    const payload = await response.json();

    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "Failed to load logs");
    }

    cachedLogRows = payload.logs || [];
    updateLogPanel();

    const report = await fetch("/api/dry-run-report?limit=5000").then((item) => item.json());
    updateDryRunSummary(report.summary);
  }

  function downloadBlob(blob, filename) {
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  async function exportDryRun(format) {
    const response = await fetch(`/api/dry-run-report?format=${format}`);
    const blob = await response.blob();
    downloadBlob(blob, `dry-run-report.${format}`);
  }

  function averageRenderMs() {
    if (renderDurationSamples.length === 0) return null;
    return renderDurationSamples.reduce((sum, value) => sum + value, 0) / renderDurationSamples.length;
  }

  function recordRenderDuration(durationMs) {
    lastRenderDurationMs = durationMs;
    renderDurationSamples.push(durationMs);

    if (renderDurationSamples.length > 120) {
      renderDurationSamples = renderDurationSamples.slice(-120);
    }
  }

  function chartIsVisible() {
    return chart.offsetParent !== null;
  }

  function rebuildStateIndex() {
    cycleIdToStateIndex = new Map();
    (latestState.cycles || []).forEach((row, index) => {
      cycleIdToStateIndex.set(row.cycleId, index);
    });
  }

  function classify(row, metrics, staleThresholdMs) {
    if (row.status === "unavailable" || row.status === "stale" || row.grossMultiplier === null) {
      return "unavailable";
    }

    if (row.maxLegAgeMs !== null && row.maxLegAgeMs !== undefined && row.maxLegAgeMs > staleThresholdMs) {
      return "unavailable";
    }

    if (row.grossMultiplier > metrics.executableBreakEvenGross) {
      return row.direction === "reverse" ? "reverse-profit" : "canonical-profit";
    }

    return "neutral";
  }

  function decorateRow(row) {
    const metrics = feeMetrics(parseNumber(feeInput.value, 0));
    const staleThresholdMs = parseNumber(staleInput.value, 3000);
    const gross = row.grossMultiplier;
    const net = gross === null ? null : gross * metrics.feeFactor;
    const statusClass = classify({ ...row, grossMultiplier: gross }, metrics, staleThresholdMs);

    return {
      ...row,
      y: gross,
      netMultiplier: net,
      grossProfitRate: gross === null ? null : gross - 1,
      netProfitRate: net === null ? null : net - 1,
      feeRate: metrics.feeRate,
      executableBreakEvenGross: metrics.executableBreakEvenGross,
      isActuallyProfitable: statusClass === "canonical-profit" || statusClass === "reverse-profit",
      statusClass,
      markerColor: rowColor({ ...row, statusClass }),
      markerSymbol: row.direction === "reverse" ? "diamond" : "circle",
    };
  }

  function rowColor(row) {
    if (row.statusClass === "canonical-profit" || row.statusClass === "reverse-profit") return COLORS.profit;
    if (row.statusClass === "unavailable") return COLORS.unavailable;
    return COLORS.neutral;
  }

  function buildCustomData(row) {
    return {
      triangleId: row.triangleId,
      cycleId: row.cycleId,
      routeVariantId: row.routeVariantId,
      legacyCycleId: row.legacyCycleId,
      startAsset: row.startAsset,
      endAsset: row.endAsset,
      direction: row.direction,
      directionLabel: row.directionLabel,
      route: row.route,
      routeLabel: row.routeLabel,
      group: row.group,
      groupLabel: row.groupLabel,
      grossMultiplier: row.grossMultiplier,
      netMultiplier: row.netMultiplier,
      grossProfitRate: row.grossProfitRate,
      netProfitRate: row.netProfitRate,
      feeRate: row.feeRate,
      executableBreakEvenGross: row.executableBreakEvenGross,
      status: row.statusClass,
      validationStatus: row.validationStatus,
      validationReason: row.validationReason,
      executableStartAmount: row.executableStartAmount,
      maxExecutableStartAmount: row.maxExecutableStartAmount,
      limitingLeg: row.limitingLeg,
      limitingMarket: row.limitingMarket,
      expectedSlippageBps: row.expectedSlippageBps,
      bestLevelTouchRatio: row.bestLevelTouchRatio,
      residualAfterOrder: row.residualAfterOrder,
      strategyId: row.strategyId,
      strategyAccepted: row.strategyAccepted,
      strategyReason: row.strategyReason,
      assets: row.assets,
      markets: row.markets,
      legs: row.legs,
      latency: row.latency,
      timingTrace: row.timingTrace,
      timingBreakdown: row.timingBreakdown,
      history: row.history,
      calculatedAtEpochMs: row.calculatedAtEpochMs,
    };
  }

  function updateSummary(rows) {
    if (!latestState) return;
    const available = rows.filter((row) => row.statusClass !== "unavailable").length;
    const unavailable = rows.length - available;
    const metrics = feeMetrics(parseNumber(feeInput.value, 0));
    const ws = latestState.wsStatus || {};
    const wsLabel = `${ws.openConnectionCount || 0}/${ws.connectionCount || 0} open`;
    updateExchangeCards();
    updateRuntimePanels(metrics);
    updateStrategyPanels();
    updateLogPanel();

    summaryCards.innerHTML = [
      metric("Markets", latestState.summary.marketsLoaded),
      metric("Triangles", latestState.summary.uniqueTriangleCount || latestState.summary.uniqueTriangles),
      metric("Plotted points", latestState.summary.plottedCycleCount || rows.length),
      metric("Visible", rows.length),
      metric("Available", available),
      metric("Unavailable", unavailable),
      metric("Fee rate", String(metrics.feeRate)),
      metric("Break-even", formatNumber(metrics.executableBreakEvenGross)),
      metric("WebSocket", wsLabel),
    ].join("");

    connectionLine.textContent =
      `Last update ${formatTime(latestState.summary.lastUpdateTime)} | ` +
      `Calculated ${formatTime(latestState.lastCalculatedAt)}`;
    updateCommandButtons();
  }

  function updatePerformanceCards() {
    const metrics = latestState && latestState.metrics;
    if (!metrics) {
      performanceCards.innerHTML = "";
      return;
    }

    const cpu = metrics.cpu || {};
    const memory = metrics.memory || {};
    const eventLoop = metrics.eventLoop || {};
    const rates = metrics.rates || {};
    const latency = metrics.latency || {};
    const browser = metrics.browser || {};
    const mhzSuffix = cpu.currentMHzFallback ? " fallback" : "";
    const localAverageRenderMs = averageRenderMs();

    performanceCards.innerHTML = [
      metric("CPU", cpu.model || "n/a"),
      metric("Cores", cpu.logicalCores || "n/a"),
      metric("CPU MHz", `${formatCompact(cpu.currentMHz, 0)}${mhzSuffix}`),
      metric("Process CPU", `${formatCompact(cpu.processCpuPercent, 2)}%`),
      metric("Load avg", Array.isArray(cpu.loadAverage) ? cpu.loadAverage.map((v) => v.toFixed(2)).join(" / ") : "n/a"),
      metric("Memory", `${formatBytes(memory.rss)} RSS`),
      metric("Heap", formatBytes(memory.heapUsed)),
      metric("ELU", formatCompact(eventLoop.utilization, 4)),
      metric("Loop p95", formatMs(eventLoop.delay && eventLoop.delay.p95Ms)),
      metric("Upbit msg/s", formatCompact(rates.upbitOrderbookMessagesPerSec, 1)),
      metric("Observation msg/s", formatCompact(rates.observationOrderbookMessagesPerSec, 1)),
      metric("Validation msg/s", formatCompact(rates.validationOrderbookMessagesPerSec, 1)),
      metric("Parsed msg/s", formatCompact(rates.parsedMessagesPerSec, 1)),
      metric("Recalc/s", formatCompact(rates.recalculatedCyclesPerSec, 1)),
      metric("Pushed/s", formatCompact(rates.pushedPointUpdatesPerSec, 1)),
      metric("Render FPS", formatCompact(renderFramesPerSec, 1)),
      metric("Render ms", formatMs(lastRenderDurationMs)),
      metric("Avg render", formatMs(browser.averageRenderMs || localAverageRenderMs)),
      metric("Latency p95", formatMs(latency.p95)),
    ].join("");
  }

  function renderGroupFilters() {
    const groups = latestState.groups || [];
    groupFilters.innerHTML = groups
      .map((group) => {
        const checked = group.group === "ALL"
          ? (enabledGroups.size >= 3 ? "checked" : "")
          : (enabledGroups.has(group.group) ? "checked" : "");
        const label = GROUP_LABELS[group.group] || group.groupLabel;
        return (
          `<label><input type="checkbox" data-group="${group.group}" ${checked}> ` +
          `${escapeHtml(label)} (${group.count || 0} / ${group.pointCount || 0})</label>`
        );
      })
      .join("");
  }

  function xBounds() {
    if (latestState && latestState.xRange) {
      return [latestState.xRange.min, latestState.xRange.max];
    }

    const xs = (latestState && latestState.cycles || []).map((row) => row.baseX || row.x).filter(Number.isFinite);
    if (xs.length === 0) return [0.25, 1.75];
    return [Math.min(...xs) - 0.75, Math.max(...xs) + 0.75];
  }

  function buildShapes(metrics) {
    const shapes = [
      {
        type: "line",
        xref: "paper",
        x0: 0,
        x1: 1,
        yref: "y",
        y0: 1,
        y1: 1,
        line: { color: "rgba(80,90,105,0.35)", width: 1, dash: "dot" },
      },
      {
        type: "line",
        xref: "paper",
        x0: 0,
        x1: 1,
        yref: "y",
        y0: metrics.executableBreakEvenGross,
        y1: metrics.executableBreakEvenGross,
        line: { color: COLORS.orange, width: 2, dash: "dash" },
      },
    ];

    (latestState.groups || []).forEach((group, index, groups) => {
      if (group.separatorX && index < groups.length - 1) {
        shapes.push({
          type: "line",
          xref: "x",
          x0: group.separatorX,
          x1: group.separatorX,
          yref: "paper",
          y0: 0,
          y1: 1,
          line: { color: "rgba(80,90,105,0.22)", width: 1 },
        });
      }
    });

    return shapes;
  }

  function buildAnnotations(metrics) {
    const annotations = [
      {
        xref: "paper",
        x: 1,
        yref: "y",
        y: 1,
        text: "y = 1",
        showarrow: false,
        xanchor: "right",
        yanchor: "bottom",
        font: { size: 11, color: "#6b7280" },
      },
      {
        xref: "paper",
        x: 0.99,
        yref: "y",
        y: metrics.executableBreakEvenGross,
        text: "실제 방향별 수익분기선: gross > 1 / (1 - fee)^3",
        showarrow: false,
        xanchor: "right",
        yanchor: "bottom",
        font: { size: 12, color: COLORS.orange },
      },
    ];

    (latestState.groups || []).forEach((group) => {
      if (group.midX !== null) {
        annotations.push({
          xref: "x",
          x: group.midX,
          yref: "paper",
          y: 1.06,
          text: `${group.groupLabel}: ${group.count} triangles / ${group.pointCount} points`,
          showarrow: false,
          font: { size: 12, color: "#394150" },
        });
      }
    });

    return annotations;
  }

  function visibleDecoratedRows() {
    return (latestState.cycles || [])
      .map(decorateRow)
      .filter((row) => {
        if (!enabledGroups.has(row.group)) return false;
        if (!showUnavailableInput.checked && row.statusClass === "unavailable") return false;
        return true;
      });
  }

  function rebuildPointIndex() {
    cycleIdToPointIndex = new Map();
    chartRows.forEach((row, index) => {
      cycleIdToPointIndex.set(row.cycleId, index);
    });
  }

  function renderChart() {
    if (!latestState) return;

    const metrics = feeMetrics(parseNumber(feeInput.value, 0));
    chartRows = visibleDecoratedRows();
    rebuildPointIndex();
    updateSummary(chartRows);
    updatePerformanceCards();

    if (!chartIsVisible()) {
      chartRenderPending = true;
      return;
    }

    chartRenderPending = false;

    const yValues = chartRows
      .map((row) => row.y)
      .filter((value) => value !== null && Number.isFinite(value));
    const yExtents = [metrics.executableBreakEvenGross, 1, ...yValues];
    const yMin = Math.min(...yExtents);
    const yMax = Math.max(...yExtents);
    const yPad = Math.max((yMax - yMin) * 0.08, 0.000001);
    const [xMin, xMax] = xBounds();

    const trace = {
      type: "scattergl",
      mode: "markers",
      x: chartRows.map((row) => row.x),
      y: chartRows.map((row) => row.y),
      ids: chartRows.map((row) => row.cycleId),
      text: chartRows.map((row) => `${row.directionLabel} ${row.routeLabel}`),
      customdata: chartRows.map(buildCustomData),
      marker: {
        size: 8,
        symbol: chartRows.map((row) => row.markerSymbol),
        color: chartRows.map((row) => row.markerColor),
        opacity: 0.86,
        line: {
          width: chartRows.map((row) => (row.direction === "reverse" ? 1.4 : 0.4)),
          color: chartRows.map((row) => (row.direction === "reverse" ? "#7a2346" : "rgba(21,25,34,0.28)")),
        },
      },
      hoverinfo: "none",
      hovertemplate: null,
    };

    const layout = {
      margin: { l: 68, r: 28, t: 66, b: 54 },
      paper_bgcolor: "#ffffff",
      plot_bgcolor: "#ffffff",
      dragmode: "zoom",
      uirevision: "q-gagarin-live",
      xaxis: {
        title: "Triangle cycle index",
        type: "linear",
        zeroline: false,
        showgrid: false,
        range: [xMin, xMax],
        minallowed: xMin,
        maxallowed: xMax,
      },
      yaxis: {
        title: "grossMultiplier",
        zeroline: false,
        showgrid: true,
        gridcolor: "#e6eaf0",
        autorange: autoScaleInput.checked,
      },
      shapes: buildShapes(metrics),
      annotations: buildAnnotations(metrics),
      showlegend: false,
      hovermode: false,
    };

    if (autoScaleInput.checked) {
      layout.yaxis.range = [yMin - yPad, yMax + yPad];
    }

    const started = performance.now();
    Plotly.react(chart, [trace], layout, {
      responsive: true,
      scrollZoom: true,
      displaylogo: false,
      modeBarButtonsToAdd: ["pan2d", "zoom2d", "resetScale2d", "autoScale2d"],
    }).then(() => {
      chartInitialized = true;
      recordRenderDuration(performance.now() - started);
      renderedFrames += 1;
      lastChartRenderAt = performance.now();
      clampCurrentXRange();
    });
  }

  function clampRange(range) {
    const [xMin, xMax] = xBounds();
    if (!Array.isArray(range) || range.length !== 2) return [xMin, xMax];
    let [left, right] = range.map(Number);
    const width = right - left;
    const allowed = xMax - xMin;

    if (!Number.isFinite(left) || !Number.isFinite(right) || width <= 0 || width >= allowed) {
      return [xMin, xMax];
    }

    if (left < xMin) {
      right += xMin - left;
      left = xMin;
    }

    if (right > xMax) {
      left -= right - xMax;
      right = xMax;
    }

    return [Math.max(xMin, left), Math.min(xMax, right)];
  }

  function clampCurrentXRange() {
    if (!chart.layout || !chart.layout.xaxis || clampRelayout) return;
    const current = chart.layout.xaxis.range;
    const clamped = clampRange(current);

    if (!current || Math.abs(current[0] - clamped[0]) > 1e-9 || Math.abs(current[1] - clamped[1]) > 1e-9) {
      clampRelayout = true;
      Plotly.relayout(chart, { "xaxis.range": clamped }).finally(() => {
        clampRelayout = false;
      });
    }
  }

  function priceCell(leg, side) {
    const value = side === "ask" ? leg.askPrice : leg.bidPrice;
    const className = leg.usedSide === side ? " class=\"used-price\"" : "";
    return `<td${className}>${escapeHtml(formatNumber(value, 8))}</td>`;
  }

  function legTable(legs = []) {
    if (!Array.isArray(legs) || legs.length === 0) {
      return "<div class=\"detail-row\"><strong>Legs</strong><span>n/a</span></div>";
    }

    const rows = legs.map((leg) => (
      `<tr>` +
      `<td>${escapeHtml(leg.legIndex)}</td>` +
      `<td>${escapeHtml(leg.fromAsset)} -> ${escapeHtml(leg.toAsset)}</td>` +
      `<td>${escapeHtml(leg.marketCode || leg.market)}</td>` +
      `<td>${escapeHtml(leg.action)}</td>` +
      `<td>${escapeHtml(leg.usedSide)}</td>` +
      priceCell(leg, "ask") +
      priceCell(leg, "bid") +
      `<td>${escapeHtml(formatNumber(leg.askSize, 8))}</td>` +
      `<td>${escapeHtml(formatNumber(leg.bidSize, 8))}</td>` +
      `<td>${escapeHtml(formatNumber(leg.inputAmount, 10))}</td>` +
      `<td>${escapeHtml(formatNumber(leg.outputAmount, 10))}</td>` +
      `<td>${escapeHtml(formatTime(leg.orderbookTimestampMs))}</td>` +
      `<td>${escapeHtml(formatMs(leg.orderbookAgeMs))}</td>` +
      `</tr>`
    )).join("");

    return (
      `<div class="table-wrap"><table>` +
      `<thead><tr>` +
      `<th>#</th><th>From -> To</th><th>Market</th><th>Action</th><th>Used</th>` +
      `<th>Ask price</th><th>Bid price</th><th>Ask size</th><th>Bid size</th>` +
      `<th>Input</th><th>Output</th><th>Orderbook timestamp</th><th>Age</th>` +
      `</tr></thead><tbody>${rows}</tbody></table></div>`
    );
  }

  function historyTable(history = []) {
    if (!Array.isArray(history) || history.length === 0) return "";

    const rows = history.slice(-20).reverse().map((item) => (
      `<tr>` +
      `<td>${escapeHtml(formatTime(item.timestamp))}</td>` +
      `<td>${escapeHtml(formatNumber(item.grossMultiplier, 8))}</td>` +
      `<td>${escapeHtml(formatNumber(item.netMultiplier, 8))}</td>` +
      `<td>${escapeHtml(formatMs(item.latency && item.latency.upbitToServerMs))}</td>` +
      `</tr>`
    )).join("");

    return (
      `<div class="detail-section"><div class="detail-title">Recent multiplier history</div>` +
      `<div class="table-wrap"><table><thead><tr>` +
      `<th>Timestamp</th><th>Gross</th><th>Net</th><th>Upbit -> server</th>` +
      `</tr></thead><tbody>${rows}</tbody></table></div></div>`
    );
  }

  function latencyRows(row) {
    const latency = row.latency || {};
    const timing = row.timingBreakdown || {};
    const items = [
      ["Upbit -> server", `${formatMs(latency.upbitToServerMs)} clock-skew sensitive`],
      ["Server parse", formatMs(latency.serverParseMs)],
      ["Socket parse", formatMs(timing.socketParseMs)],
      ["Normalize", formatMs(timing.normalizeMs)],
      ["Cache write", formatMs(timing.cacheWriteMs)],
      ["Affected lookup", formatMs(timing.affectedCycleLookupMs)],
      ["Server calc", formatMs(latency.serverCalcMs)],
      ["Calc trace", formatMs(timing.calcMs)],
      ["Strategy", formatMs(timing.strategyMs)],
      ["Risk", formatMs(timing.riskMs)],
      ["Server queue", formatMs(latency.serverQueueMs)],
      ["Server -> client", formatMs(latency.serverToClientMs)],
      ["Client render", formatMs(latency.clientRenderMs)],
      ["Browser apply -> render", formatMs(timing.browserApplyToRenderMs)],
      ["End -> display", `${formatMs(latency.estimatedEndToDisplayMs)} clock-skew sensitive`],
    ];

    return items
      .map(([label, value]) => `<div class="detail-row"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(value)}</span></div>`)
      .join("");
  }

  function detailRows(row) {
    const route = Array.isArray(row.route) ? row.route.join(" -> ") : row.routeLabel;
    const items = [
      ["Triangle", (row.assets || []).join(" / ")],
      ["Direction", `${row.directionLabel} ${row.direction}`],
      ["Route", route],
      ["Group", row.groupLabel],
      ["Gross", formatNumber(row.grossMultiplier, 8)],
      ["Net", formatNumber(row.netMultiplier, 8)],
      ["Gross profit", formatPercent(row.grossProfitRate)],
      ["Net profit", formatPercent(row.netProfitRate)],
      ["Fee rate", String(row.feeRate)],
      ["Break-even", formatNumber(row.executableBreakEvenGross, 8)],
      ["Status", row.statusClass || row.status],
      ["Reason", row.unavailableReason || row.staleReason || "n/a"],
      ["Start asset", row.startAsset || "n/a"],
      ["End asset", row.endAsset || "n/a"],
      ["Route variant", row.routeVariantId || row.cycleId],
      ["Validation", row.validationStatus || "n/a"],
      ["Validation reason", row.validationReason || "n/a"],
      ["Execution feasibility", row.executionFeasibility || "n/a"],
      ["Executable start", formatNumber(row.executableStartAmount, 8)],
      ["Max executable", formatNumber(row.maxExecutableStartAmount, 8)],
      ["Limiting leg", row.limitingLeg || "n/a"],
      ["Limiting market", row.limitingMarket || "n/a"],
      ["Expected slippage", row.expectedSlippageBps === null || row.expectedSlippageBps === undefined
        ? "n/a"
        : `${Number(row.expectedSlippageBps).toFixed(3)} bps`],
      ["Best touch ratio", row.bestLevelTouchRatio === null || row.bestLevelTouchRatio === undefined
        ? "n/a"
        : formatPercent(row.bestLevelTouchRatio)],
      ["Residual after order", formatNumber(row.residualAfterOrder, 8)],
      ["Strategy", row.strategyId || "n/a"],
      ["Strategy reason", row.strategyReason || "n/a"],
      ["Freshness", `newest ${formatMs(row.newestLegAgeMs)} / oldest ${formatMs(row.oldestLegAgeMs)}`],
      ["Calculated", formatTime(row.calculatedAtEpochMs || row.calculatedAtIso)],
    ];

    return (
      `<div class="detail-section">` +
      `<div class="detail-title">${escapeHtml(row.directionLabel)} ${escapeHtml(row.direction)} | ${escapeHtml(route)}</div>` +
      items.map(([label, value]) => `<div class="detail-row"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(value)}</span></div>`).join("") +
      `</div>` +
      `<div class="detail-section"><div class="detail-title">Leg orderbooks</div>${legTable(row.legs)}</div>` +
      `<div class="detail-section"><div class="detail-title">Latency</div>${latencyRows(row)}</div>` +
      historyTable(row.history)
    );
  }

  function setDetail(row) {
    if (!row) return;
    activeDetailCycleId = row.cycleId;
    detailBody.innerHTML = detailRows(row);
  }

  function bindPlotlyEvents() {
    if (eventsBound) return;
    eventsBound = true;

    chart.on("plotly_click", (event) => {
      if (!event.points || !event.points[0]) return;
      setDetail(chartRows[event.points[0].pointIndex]);
    });

    chart.on("plotly_relayout", (event) => {
      if (clampRelayout || !event) return;
      if (
        event["xaxis.autorange"] ||
        event["xaxis.range[0]"] !== undefined ||
        event["xaxis.range[1]"] !== undefined
      ) {
        clampCurrentXRange();
      }
    });
  }

  function applyState(state) {
    const dashboardReceiveEpochMs = Date.now();
    latestState = state;
    (latestState.cycles || []).forEach((row) => {
      row.timingTrace = {
        ...(row.timingTrace || {}),
        dashboardReceiveEpochMs,
      };
    });
    rebuildStateIndex();
    if (!feeInput.dataset.initialized) {
      feeInput.value = String(state.summary.feeRate || 0);
      staleInput.value = String(state.summary.staleOrderbookMs || 3000);
      feeInput.dataset.initialized = "true";
    }
    renderGroupFilters();
    renderChart();
    bindPlotlyEvents();
  }

  function mergeChangedCycles(changedCycles, deltaMeta = {}) {
    for (const changed of changedCycles || []) {
      const index = cycleIdToStateIndex.get(changed.cycleId);
      if (index === undefined) continue;
      const latency = {
        ...(latestState.cycles[index].latency || {}),
        ...(changed.latency || {}),
      };
      const timingTrace = {
        ...(latestState.cycles[index].timingTrace || {}),
        ...(changed.timingTrace || {}),
        dashboardReceiveEpochMs: deltaMeta.clientReceivedEpochMs,
      };

      if (Number.isFinite(deltaMeta.clientReceivedEpochMs) && Number.isFinite(deltaMeta.sentAtEpochMs)) {
        latency.serverToClientMs = deltaMeta.clientReceivedEpochMs - deltaMeta.sentAtEpochMs;
      }

      latestState.cycles[index] = {
        ...latestState.cycles[index],
        ...changed,
        latency,
        timingTrace,
      };
      pendingChangedCycleIds.add(changed.cycleId);
    }
  }

  function restyleChart() {
    pendingFrame = false;
    const applyingCycleIds = new Set(pendingChangedCycleIds);
    pendingChangedCycleIds.clear();
    if (!latestState || !chartInitialized) {
      renderChart();
      return;
    }

    const started = performance.now();
    const applyStartEpochMs = Date.now();
    const browserApplyStartPerfMs = performance.now();
    chartRows = visibleDecoratedRows();
    rebuildPointIndex();
    updateSummary(chartRows);
    updatePerformanceCards();

    if (!chartIsVisible()) {
      chartRenderPending = true;
      return;
    }

    const update = {
      x: [chartRows.map((row) => row.x)],
      y: [chartRows.map((row) => row.y)],
      ids: [chartRows.map((row) => row.cycleId)],
      text: [chartRows.map((row) => `${row.directionLabel} ${row.routeLabel}`)],
      customdata: [chartRows.map(buildCustomData)],
      "marker.color": [chartRows.map((row) => row.markerColor)],
      "marker.symbol": [chartRows.map((row) => row.markerSymbol)],
      "marker.line.width": [chartRows.map((row) => (row.direction === "reverse" ? 1.4 : 0.4))],
      "marker.line.color": [chartRows.map((row) => (row.direction === "reverse" ? "#7a2346" : "rgba(21,25,34,0.28)"))],
    };

    Plotly.restyle(chart, update, [0]).then(() => {
      const plotUpdatedEpochMs = Date.now();
      const browserRenderDonePerfMs = performance.now();
      recordRenderDuration(performance.now() - started);
      for (const cycleId of applyingCycleIds) {
        const index = cycleIdToStateIndex.get(cycleId);
        if (index === undefined) continue;
        const row = latestState.cycles[index];
        const latency = {
          ...(row.latency || {}),
          clientRenderMs: lastRenderDurationMs,
          estimatedEndToDisplayMs: Number.isFinite(row.lastUpbitTimestampMs)
            ? plotUpdatedEpochMs - row.lastUpbitTimestampMs
            : null,
          clientApplyStartEpochMs: applyStartEpochMs,
          clientPlotUpdatedEpochMs: plotUpdatedEpochMs,
        };
        const timingTrace = {
          ...(row.timingTrace || {}),
          browserApplyStartPerfMs,
          browserRenderDonePerfMs,
        };
        const timingBreakdown = {
          ...(row.timingBreakdown || {}),
          browserApplyToRenderMs: browserRenderDonePerfMs - browserApplyStartPerfMs,
        };
        latestState.cycles[index] = {
          ...row,
          latency,
          timingTrace,
          timingBreakdown,
        };
      }

      chartRows = visibleDecoratedRows();
      rebuildPointIndex();
      renderedFrames += 1;
      lastChartRenderAt = performance.now();
      updateSummary(chartRows);
      updatePerformanceCards();
      clampCurrentXRange();

      if (activeDetailCycleId) {
        const row = chartRows.find((item) => item.cycleId === activeDetailCycleId);
        if (row) setDetail(row);
      }
    });
  }

  function scheduleRestyle() {
    if (pendingFrame) return;
    const now = performance.now();
    const minRenderGapMs = 100;

    if (now - lastChartRenderAt < minRenderGapMs) {
      clearTimeout(deferredRestyleTimer);
      deferredRestyleTimer = setTimeout(scheduleRestyle, minRenderGapMs - (now - lastChartRenderAt));
      return;
    }

    pendingFrame = true;
    requestAnimationFrame(restyleChart);
  }

  function applyDelta(delta) {
    if (!latestState) return;
    const clientReceivedEpochMs = Date.now();
    if (delta.stateDelta) {
      latestState = {
        ...latestState,
        ...delta.stateDelta,
      };
    }
    if (delta.lastCalculatedAt) {
      latestState.lastCalculatedAt = delta.lastCalculatedAt;
    }
    if (delta.summaryDelta) {
      latestState.summary = {
        ...latestState.summary,
        ...delta.summaryDelta,
      };
    }
    if (delta.metrics) {
      latestState.metrics = delta.metrics;
    }
    mergeChangedCycles(delta.changedCycles, {
      clientReceivedEpochMs,
      sentAtEpochMs: delta.sentAtEpochMs,
    });

    if (pendingChangedCycleIds.size > 0) {
      scheduleRestyle();
    } else if (delta.metrics || delta.stateDelta || delta.summaryDelta) {
      chartRows = visibleDecoratedRows();
      rebuildPointIndex();
      updateSummary(chartRows);
      updatePerformanceCards();
      updateCommandButtons();
    }
  }

  function connectSseFallback() {
    if (sseFallbackStarted) return;
    sseFallbackStarted = true;
    if (!window.EventSource) return;
    const events = new EventSource("/api/events");

    events.addEventListener("state", (event) => {
      applyState(JSON.parse(event.data));
    });

    events.addEventListener("delta", (event) => {
      applyDelta(JSON.parse(event.data));
    });

    events.addEventListener("status", (event) => {
      const status = JSON.parse(event.data);
      if (latestState) {
        latestState.wsStatus = status;
      }
    });

    events.addEventListener("error", (event) => {
      showToast(event.data || "Live stream error");
    });

    events.onerror = () => {
      connectionLine.textContent = "SSE disconnected. Browser will retry.";
    };
  }

  function connectLiveSocket() {
    clearTimeout(reconnectTimer);
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    liveSocket = new WebSocket(`${protocol}//${window.location.host}/ws/live`);

    liveSocket.addEventListener("open", () => {
      reconnectAttempt = 0;
      connectionLine.textContent = "WebSocket connected.";
    });

    liveSocket.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);

      if (message.type === "hello") {
        return;
      }

      if (message.type === "full-state") {
        applyState(message);
        return;
      }

      if (message.type === "delta" || message.type === "metrics") {
        applyDelta(message);
        return;
      }

      if (message.type === "status") {
        if (latestState) {
          if (message.feedName === "validation") {
            latestState.feedStatus = {
              ...(latestState.feedStatus || {}),
              validation: message.status,
            };
          } else {
            latestState.wsStatus = message.status;
            latestState.feedStatus = {
              ...(latestState.feedStatus || {}),
              observation: message.status,
            };
          }
        }
        return;
      }

      if (message.type === "error") {
        showToast(message.message || (message.error && message.error.message) || "Live stream error");
      }
    });

    liveSocket.addEventListener("close", () => {
      const delay = Math.min(1000 * 2 ** reconnectAttempt, 15000);
      reconnectAttempt += 1;
      connectionLine.textContent = `WebSocket disconnected. Reconnecting in ${Math.round(delay / 1000)}s.`;
      if (reconnectAttempt >= 2) {
        connectSseFallback();
      }
      reconnectTimer = setTimeout(connectLiveSocket, delay);
    });

    liveSocket.addEventListener("error", () => {
      showToast("WebSocket error; falling back if needed.");
    });
  }

  async function fetchFullState() {
    const response = await fetch("/api/state");
    applyState(await response.json());
  }

  function clearPendingCommand() {
    pendingCommandId = null;
    clearTimeout(pendingCommandTimer);
    pendingCommandTimer = null;
    updateCommandButtons();
  }

  async function pollCommandStatus(commandId, attempt = 0) {
    const response = await fetch(`/api/commands/${encodeURIComponent(commandId)}`);
    const payload = response.ok ? await response.json() : null;
    const status = payload && payload.status;

    if (status && status.status === "accepted") {
      showToast(`${status.command} accepted (${status.runMode || "current"})`);
      clearPendingCommand();
      return;
    }

    if (status && status.status === "rejected") {
      showToast(`${status.command} rejected: ${status.message || "unknown reason"}`);
      clearPendingCommand();
      return;
    }

    if (attempt >= 30) {
      showToast("Command is still queued. Check engine process.");
      clearPendingCommand();
      return;
    }

    pendingCommandTimer = setTimeout(() => {
      pollCommandStatus(commandId, attempt + 1).catch((error) => {
        showToast(error.message);
        clearPendingCommand();
      });
    }, 500);
  }

  async function sendEngineCommand(command, options = {}) {
    const response = await fetch("/api/command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command, ...options }),
    });
    const result = await response.json();

    if (!response.ok || !result.ok) {
      throw new Error(result.error || `Command failed: ${command}`);
    }

    pendingCommandId = result.commandId;
    updateCommandButtons();
    showToast(`${result.command} queued${result.runMode ? ` (${result.runMode})` : ""}`);
    pollCommandStatus(result.commandId).catch((error) => {
      showToast(error.message);
      clearPendingCommand();
    });
  }

  function activateTab(tabName) {
    tabButtons.forEach((button) => {
      const active = button.dataset.tabTarget === tabName;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });

    tabPanels.forEach((panel) => {
      const active = panel.dataset.tabPanel === tabName;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });

    if (tabName === "arbitrage") {
      if (chartRenderPending) {
        renderChart();
      } else if (chartInitialized) {
        setTimeout(() => {
          Plotly.Plots.resize(chart);
          clampCurrentXRange();
        }, 0);
      }
    } else if (tabName === "logs") {
      refreshLogs().catch((error) => showToast(error.message));
    }
  }

  async function capture() {
    if (!latestState) return;
    const now = new Date();
    const stamp = formatLocalTimestamp(now);
    const snapshot = {
      ...latestState,
      clientFeeRate: parseNumber(feeInput.value, 0),
      clientStaleThresholdMs: parseNumber(staleInput.value, 3000),
      renderedCycles: chartRows,
      chartRange: {
        x: chart.layout && chart.layout.xaxis ? chart.layout.xaxis.range : null,
        y: chart.layout && chart.layout.yaxis ? chart.layout.yaxis.range : null,
      },
      visibleFilters: {
        groups: [...enabledGroups],
        showUnavailable: showUnavailableInput.checked,
        autoScaleY: autoScaleInput.checked,
      },
      renderMetrics: {
        renderFramesPerSec,
        lastRenderDurationMs,
      },
    };

    try {
      const imageDataUrl = await Plotly.toImage(chart, {
        format: "png",
        width: chart.clientWidth || 1400,
        height: chart.clientHeight || 720,
        scale: 2,
      });
      const response = await fetch("/api/capture", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          imageDataUrl,
          snapshot,
          timestamp: now.toISOString(),
        }),
      });
      const result = await response.json();

      if (!response.ok || !result.ok) {
        throw new Error(result.error || "Server-side capture failed");
      }

      showToast(`Saved ${result.pngPath} and ${result.jsonPath}`);
    } catch (error) {
      showToast(`${error.message}. Downloading locally instead.`);
      const imageDataUrl = await Plotly.toImage(chart, { format: "png", scale: 2 });
      const pngLink = document.createElement("a");
      pngLink.href = imageDataUrl;
      pngLink.download = `upbit-triangle-live-${stamp}.png`;
      pngLink.click();

      const jsonLink = document.createElement("a");
      jsonLink.href = URL.createObjectURL(
        new Blob([JSON.stringify(snapshot, null, 2)], { type: "application/json" }),
      );
      jsonLink.download = `upbit-triangle-live-${stamp}.json`;
      jsonLink.click();
    }
  }

  function bindControls() {
    tabButtons.forEach((button) => {
      button.addEventListener("click", () => {
        activateTab(button.dataset.tabTarget);
      });
    });
    feeInput.addEventListener("input", () => {
      renderChart();
    });
    staleInput.addEventListener("input", () => {
      renderChart();
    });
    autoScaleInput.addEventListener("change", renderChart);
    showUnavailableInput.addEventListener("change", renderChart);
    observeButton.addEventListener("click", () => sendEngineCommand("Start", { runMode: "OBSERVE" }).catch((error) => showToast(error.message)));
    dryRunButton.addEventListener("click", () => sendEngineCommand("Start", { runMode: "DRY_RUN" }).catch((error) => showToast(error.message)));
    realGuardedButton.addEventListener("click", () => sendEngineCommand("Start", { runMode: "REAL_GUARDED" }).catch((error) => showToast(error.message)));
    pauseButton.addEventListener("click", () => sendEngineCommand("Pause").catch((error) => showToast(error.message)));
    stopButton.addEventListener("click", () => sendEngineCommand("Stop").catch((error) => showToast(error.message)));
    emergencyStopButton.addEventListener("click", () => sendEngineCommand("Stop", { emergency: true }).catch((error) => showToast(error.message)));
    refreshLogsButton.addEventListener("click", () => refreshLogs().catch((error) => showToast(error.message)));
    exportDryRunJsonButton.addEventListener("click", () => exportDryRun("json").catch((error) => showToast(error.message)));
    exportDryRunCsvButton.addEventListener("click", () => exportDryRun("csv").catch((error) => showToast(error.message)));
    [logModeFilter, logTypeFilter, logStartAssetFilter].forEach((input) => {
      input.addEventListener("change", () => refreshLogs().catch((error) => showToast(error.message)));
    });
    [logStrategyFilter, logCycleFilter].forEach((input) => {
      input.addEventListener("input", () => {
        clearTimeout(input.dataset.timerId);
        const timerId = setTimeout(() => refreshLogs().catch((error) => showToast(error.message)), 250);
        input.dataset.timerId = String(timerId);
      });
    });
    unpinButton.addEventListener("click", () => {
      activeDetailCycleId = null;
      detailBody.textContent = "Click a point.";
    });
    groupFilters.addEventListener("change", (event) => {
      const group = event.target.dataset.group;
      if (!group) return;
      if (group === "ALL") {
        ["KRW_START", "BTC_START", "USDT_START"].forEach((item) => {
          if (event.target.checked) {
            enabledGroups.add(item);
          } else {
            enabledGroups.delete(item);
          }
        });
        renderGroupFilters();
        renderChart();
        return;
      }
      if (event.target.checked) {
        enabledGroups.add(group);
      } else {
        enabledGroups.delete(group);
      }
      renderChart();
    });
  }

  function updateFrameRate() {
    const now = performance.now();
    const elapsedSec = (now - lastFrameReportAt) / 1000;
    if (elapsedSec >= 1) {
      renderFramesPerSec = renderedFrames / elapsedSec;
      renderedFrames = 0;
      lastFrameReportAt = now;
      updatePerformanceCards();

      if (liveSocket && liveSocket.readyState === WebSocket.OPEN) {
        liveSocket.send(JSON.stringify({
          type: "client-metrics",
          renderedFrames: Math.round(renderFramesPerSec),
          renderMs: lastRenderDurationMs,
          averageRenderMs: averageRenderMs(),
        }));
      }
    }
    requestAnimationFrame(updateFrameRate);
  }

  async function init() {
    bindControls();
    await fetchFullState();
    if (window.WebSocket) {
      connectLiveSocket();
    } else {
      connectSseFallback();
    }
    requestAnimationFrame(updateFrameRate);
  }

  init().catch((error) => {
    showToast(error.message);
    connectionLine.textContent = error.message;
  });
})();
