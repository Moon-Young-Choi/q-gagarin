const path = require("node:path");
const fs = require("node:fs/promises");
const { CommandStatusStore } = require("../core/commandStatusStore");
const { CommandInbox } = require("../core/commandInbox");
const { DEFAULT_RUNTIME_CONFIG, loadRuntimeConfig } = require("../core/runtimeConfig");
const { replayDryRunReport } = require("../replay/replayEngine");
const { createStrategyRegistry } = require("../strategies/registry");
const { createTelemetryReadModel } = require("../ops/telemetryReadModel");
const { createCommandQueue } = require("../ops/commandQueue");
const {
  DEFAULT_DRAFT_CONFIG_PATH,
  baseDraftConfig,
  diffConfigs,
  parseConfigValue,
  readActiveConfig,
  readDraftConfig,
  setConfigPath,
  validateConfig,
  writeJsonAtomic,
} = require("./configDraft");
const { renderKeyValues, renderTable } = require("./renderers/table");

const COMMANDS = [
  ["/help", "List slash commands or show command help"],
  ["/status", "Show engine state, run mode, and market summary"],
  ["/summary", "Alias for /status"],
  ["/start <observe|dry|real-guarded>", "Queue an engine Start command"],
  ["/pause", "Queue Pause; existing order management continues"],
  ["/stop", "Queue Stop"],
  ["/emergency-stop", "Queue Stop with emergency=true"],
  ["/readiness", "Show real-run readiness checklist"],
  ["/mode", "Show current run mode"],
  ["/market", "Show market feed/cache status; subcommands: feeds, fees, stale, latency, exchanges"],
  ["/strategy list|active|explain|select", "Inspect or select configured strategy"],
  ["/system", "Show system status; subcommands: perf, latency, guards, files"],
  ["/latency", "Show latency/performance budget"],
  ["/execution", "Show execution balances, orders, fills, and guards"],
  ["/balances", "Show dry-run and real balance snapshots"],
  ["/logs [--kind events|decisions|orders|fills|errors] [--follow]", "Read append-only logs"],
  ["/dryrun report [--format json|csv]", "Show dry-run review report"],
  ["/replay dryrun", "Replay dry-run from TAPE_JSON/CYCLES_JSON without exchange access"],
  ["/config show|validate|draft", "Inspect, validate, or draft runtime config changes"],
  ["/settings", "Alias for /config show"],
  ["/opportunity show|legs|latency|plan <rank|cycleId>", "Show selected desk opportunity detail"],
  ["/export desk --format txt|json|csv", "Export current desk ranking"],
  ["/watch", "Watch /status"],
  ["/quit", "Exit interactive shell"],
];

function createCliContext(options = {}) {
  const runtimeDir = options.runtimeDir || path.resolve(process.cwd(), "out", "runtime");
  const logDir = options.logDir || path.resolve(process.cwd(), "out", "logs");
  const configPath = options.configPath || path.resolve(process.cwd(), "config", "runtime.json");
  const draftConfigPath = options.draftConfigPath || DEFAULT_DRAFT_CONFIG_PATH;
  const telemetry = options.telemetry || createTelemetryReadModel({
    runtimeDir,
    logDir,
    snapshotPath: options.snapshotPath,
    deltaPath: options.deltaPath,
  });
  const commandStatusStore = options.commandStatusStore || new CommandStatusStore({ runtimeDir });
  const commandQueue = options.commandQueue || createCommandQueue({
    runtimeDir,
    commandInbox: options.commandInbox || new CommandInbox({ runtimeDir }),
    commandStatusStore,
    readSnapshot: () => telemetry.snapshot(),
    source: "cli",
  });

  return {
    telemetry,
    commandQueue,
    commandStatusStore,
    strategyRegistry: options.strategyRegistry || createStrategyRegistry(),
    configPath,
    draftConfigPath,
    output: options.output || process.stdout,
    errorOutput: options.errorOutput || process.stderr,
    pollIntervalMs: options.pollIntervalMs || 250,
    statusPollTimeoutMs: options.statusPollTimeoutMs || 2500,
    watchIntervalMs: options.watchIntervalMs || 1000,
    lastDeskRows: [],
    followSeenLogKeys: new Set(),
  };
}

function write(context, text = "") {
  context.output.write(`${text}\n`);
}

function formatPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return `${(numeric * 100).toFixed(3)}%`;
}

function formatNumber(value, digits = 8) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  if (Math.abs(numeric) >= 1000) return numeric.toFixed(2);
  return numeric.toFixed(digits).replace(/\.?0+$/, "");
}

function engineState(snapshot = {}) {
  return snapshot.engineState || (snapshot.engine && snapshot.engine.state) || "UNKNOWN";
}

function snapshotTimestampMs(snapshot = {}) {
  const candidates = [
    snapshot.lastCalculatedAt,
    snapshot.summary && snapshot.summary.lastUpdateTime,
    snapshot.serverStartedAt,
  ];

  for (const candidate of candidates) {
    const parsed = Date.parse(candidate || "");
    if (Number.isFinite(parsed)) return parsed;
  }

  return null;
}

function isProcessAlive(pid) {
  const numericPid = Number(pid);
  if (!Number.isInteger(numericPid) || numericPid <= 0) return null;

  try {
    process.kill(numericPid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    if (error.code === "EPERM") return true;
    return null;
  }
}

function runtimeLiveness(snapshot = {}, options = {}) {
  const state = engineState(snapshot);
  const pid = snapshot.engineProcess && snapshot.engineProcess.pid;
  const pidAlive = isProcessAlive(pid);
  const timestampMs = snapshotTimestampMs(snapshot);
  const nowMs = options.nowMs || Date.now();
  const ageMs = timestampMs === null ? null : Math.max(0, nowMs - timestampMs);
  const staleAfterMs = Number.parseInt(
    options.staleAfterMs || process.env.Q_GAGARIN_STALE_SNAPSHOT_MS || "15000",
    10,
  );
  const stateLooksActive = ["RUNNING", "PAUSED", "ERROR"].includes(state);
  const staleByPid = stateLooksActive && pidAlive === false;
  const staleByAge = stateLooksActive &&
    ageMs !== null &&
    Number.isFinite(staleAfterMs) &&
    staleAfterMs > 0 &&
    ageMs > staleAfterMs;
  const effectiveState = staleByPid || staleByAge
    ? `STALE (${state})`
    : state;

  return {
    state,
    effectiveState,
    pid,
    pidAlive,
    ageMs,
    staleByPid,
    staleByAge,
    staleAfterMs,
  };
}

function runtimeConfig(snapshot = {}) {
  return snapshot.runtimeConfig || {};
}

function summary(snapshot = {}) {
  return snapshot.summary || {};
}

function renderStatus(snapshot = {}) {
  const config = runtimeConfig(snapshot);
  const stateSummary = summary(snapshot);
  const liveness = runtimeLiveness(snapshot);
  const scheduler = stateSummary.cycleScheduler || snapshot.cycleScheduler || {};

  return [
    "Status",
    renderKeyValues([
      ["Engine", liveness.effectiveState],
      ["Mode", config.runMode || "OBSERVE"],
      ["Exchange", config.exchange || "upbit"],
      ["Live trading", config.liveTradingEnabled === true],
      ["Markets loaded", stateSummary.marketsLoaded || 0],
      ["Triangles", stateSummary.uniqueTriangleCount || stateSummary.uniqueTriangles || 0],
      ["Plotted cycles", stateSummary.plottedCycleCount || 0],
      ["Available multipliers", stateSummary.availableLiveMultipliers || 0],
      ["Warm-up", scheduler.totalCycleCount ? `${scheduler.completedCycleCount || 0}/${scheduler.totalCycleCount}` : "-"],
      ["Pending cycles", scheduler.pendingCycleCount ?? stateSummary.pendingCycleCount ?? 0],
      ["Snapshot age ms", liveness.ageMs ?? "-"],
      ["Engine pid alive", liveness.pidAlive === null ? "-" : liveness.pidAlive],
      ["Last update", stateSummary.lastUpdateTime || snapshot.lastCalculatedAt || "-"],
    ]),
  ].join("\n");
}

function renderReadiness(snapshot = {}) {
  const readiness = snapshot.readiness || {};
  const score = readiness.score || {};
  const checks = readiness.items || readiness.checks || [];
  const rows = checks.map((item) => [
    item.id || item.name || "-",
    item.ok === true || item.passed === true ? "pass" : "fail",
    item.message || item.label || item.reason || "",
  ]);

  return [
    "Readiness",
    renderKeyValues([
      ["Passed", readiness.passed === true || score.passed === true],
      ["Score", score.total ? `${score.passed || 0}/${score.total}` : "-"],
      ["Failed", score.failed || rows.filter((row) => row[1] === "fail").length],
    ]),
    rows.length > 0 ? renderTable(["Check", "Status", "Detail"], rows) : "No readiness checks in snapshot.",
  ].join("\n");
}

function renderMarket(snapshot = {}, section = "") {
  const stores = snapshot.orderbookStores || {};
  const privateCache = snapshot.privateCacheStatus || {};
  const privateWs = snapshot.privateWsStatus || {};
  const feed = snapshot.feedStatus || {};
  const stateSummary = summary(snapshot);

  if (section === "feeds") {
    const wsStatus = snapshot.wsStatus || {};
    const validationWsStatus = snapshot.validationWsStatus || {};
    return [
      "Market Feeds",
      renderTable(["Feed", "Status", "Open", "Total", "Unit"], [
        ["observation", wsStatus.status || feed.observationStatus || "-", wsStatus.openConnectionCount ?? "-", wsStatus.connections && wsStatus.connections.length || "-", runtimeConfig(snapshot).observationOrderbookUnit || 5],
        ["validation", validationWsStatus.status || feed.validationStatus || "-", validationWsStatus.openConnectionCount ?? "-", validationWsStatus.connections && validationWsStatus.connections.length || "-", runtimeConfig(snapshot).validationOrderbookUnit || 30],
        ["private", privateWs.status || "not_configured", privateWs.openConnectionCount ?? "-", "-", "-"],
      ]),
    ].join("\n");
  }

  if (section === "fees") {
    const rows = [
      ["fee policies", privateCache.feePolicyCount ?? privateCache.loadedFeePolicyCount ?? 0],
      ["market policies", privateCache.marketPolicyCount ?? privateCache.loadedMarketPolicyCount ?? 0],
      ["required markets", stateSummary.requiredMarketCount || 0],
      ["last refresh", privateCache.lastRefreshAt || privateCache.updatedAt || "-"],
      ["last error", privateCache.lastError && privateCache.lastError.message || privateCache.error || "-"],
    ];
    return ["Market Fees", renderKeyValues(rows)].join("\n");
  }

  if (section === "stale") {
    return [
      "Market Staleness",
      renderTable(["Store", "Markets", "Stale", "Oldest age"], [
        ["observation", stores.observation && stores.observation.marketCount || 0, stores.observation && stores.observation.staleCount || 0, stores.observation && stores.observation.oldestAgeMs || "-"],
        ["validation", stores.validation && stores.validation.marketCount || 0, stores.validation && stores.validation.staleCount || 0, stores.validation && stores.validation.oldestAgeMs || "-"],
      ]),
    ].join("\n");
  }

  if (section === "latency") {
    return renderLatency(snapshot);
  }

  if (section === "exchanges") {
    const exchange = runtimeConfig(snapshot).exchange || "upbit";
    return [
      "Market Exchanges",
      renderTable(["Exchange", "Status", "Role"], [
        [exchange, "enabled", "configured runtime exchange"],
        ["binance", "not_implemented", "-"],
        ["bithumb", "not_implemented", "-"],
        ["bybit", "not_implemented", "-"],
      ]),
    ].join("\n");
  }

  return [
    "Market",
    renderKeyValues([
      ["Exchange", runtimeConfig(snapshot).exchange || "upbit"],
      ["Observation unit", runtimeConfig(snapshot).observationOrderbookUnit || 5],
      ["Validation unit", runtimeConfig(snapshot).validationOrderbookUnit || 30],
      ["Observation stale", stores.observation && stores.observation.staleCount || 0],
      ["Validation stale", stores.validation && stores.validation.staleCount || 0],
      ["Private WS", privateWs.status || "not_configured"],
      ["OrderChance loaded", privateCache.feePolicyCount ?? privateCache.loadedFeePolicyCount ?? 0],
      ["Market policies", privateCache.marketPolicyCount ?? privateCache.loadedMarketPolicyCount ?? 0],
      ["Required markets", stateSummary.requiredMarketCount || 0],
      ["Feed", feed.status || "-"],
      ["Last error", stateSummary.marketDiscoveryError && stateSummary.marketDiscoveryError.message || "-"],
    ]),
  ].join("\n");
}

function renderSystem(snapshot = {}, section = "") {
  const budget = snapshot.performanceBudget || {};
  const processInfo = snapshot.engineProcess || {};
  const metrics = sanitizeLegacyBrowserMetrics(snapshot.metrics || {});
  const guard = snapshot.guardStatus || {};

  if (section === "perf") {
    return [
      "System Performance",
      renderKeyValues([
        ["CPU", metrics.cpu && metrics.cpu.processCpuPercent],
        ["Memory RSS", metrics.memory && metrics.memory.rss],
        ["Heap used", metrics.memory && metrics.memory.heapUsed],
        ["Event loop utilization", metrics.eventLoop && metrics.eventLoop.utilization],
        ["Event loop p95 ms", metrics.eventLoop && metrics.eventLoop.delay && metrics.eventLoop.delay.p95Ms],
      ]),
      metrics.rates ? renderTable(
        ["Rate", "Per second"],
        Object.entries(metrics.rates).map(([name, value]) => [name, value]),
      ) : "",
    ].filter(Boolean).join("\n");
  }

  if (section === "latency") {
    return renderLatency(snapshot);
  }

  if (section === "guards") {
    return [
      "System Guards",
      renderKeyValues([
        ["Healthy", guard.healthy === true],
        ["Emergency stop", snapshot.emergencyStop && snapshot.emergencyStop.active === true],
        ["Consecutive failures", guard.consecutiveFailures ?? "-"],
        ["Max consecutive failures", guard.maxConsecutiveFailures ?? "-"],
        ["Open orders", guard.openOrderCount ?? "-"],
        ["Max open orders", guard.maxOpenOrders ?? "-"],
        ["Active real executions", guard.activeRealExecutionCount ?? "-"],
        ["Readiness passed", snapshot.readiness && snapshot.readiness.passed === true],
      ]),
    ].join("\n");
  }

  if (section === "files") {
    const runtimeDir = processInfo.runtimeDir || path.resolve(process.cwd(), "out", "runtime");
    return [
      "System Files",
      renderKeyValues([
        ["Runtime dir", runtimeDir],
        ["Snapshot", processInfo.snapshotPath || path.join(runtimeDir, "latest-snapshot.json")],
        ["Delta", processInfo.deltaPath || path.join(runtimeDir, "latest-delta.json")],
        ["Command inbox", path.join(runtimeDir, "commands", "inbox")],
        ["Command processed", path.join(runtimeDir, "commands", "processed")],
        ["Command status", path.join(runtimeDir, "command-status")],
        ["Logs", path.resolve(process.cwd(), "out", "logs")],
      ]),
    ].join("\n");
  }

  return [
    "System",
    renderKeyValues([
      ["Engine pid", processInfo.pid || process.pid],
      ["Engine state", engineState(snapshot)],
      ["Snapshot type", snapshot.type || "-"],
      ["Server started", snapshot.serverStartedAt || "-"],
      ["Market-data max age", budget.marketData && budget.marketData.maxOldestLegAgeMs],
      ["Decision max age", budget.decision && budget.decision.maxDecisionAgeMs],
      ["Execution max ack", budget.execution && budget.execution.maxOrderAckMs],
      ["Execution max reconciliation", budget.execution && budget.execution.maxReconciliationMs],
      ["Display latency affects trading", budget.displayLatencyAffectsTrading === true],
    ]),
  ].join("\n");
}

function renderExecution(snapshot = {}, section = "") {
  const execution = snapshot.execution || {};
  const balances = execution.dryRunBalances || execution.dryRunCapital || {};
  const realBalances = execution.realBalances || {};
  const guard = snapshot.guardStatus || {};
  const residuals = realBalances.residualBalances || balances.residualBalances || {};
  const balanceRows = Object.entries(balances.availableBalances || balances.available || balances)
    .filter(([, value]) => typeof value !== "object")
    .map(([asset, value]) => [asset, formatNumber(value)]);

  if (section === "orders") {
    const orders = execution.latestOrders || execution.orders || execution.realOrders || [];
    const rows = (Array.isArray(orders) ? orders : []).slice(-20).map((order) => [
      order.timestamp || order.createdAt || order.orderTimestamp || "-",
      order.market || "-",
      order.side || "-",
      order.state || order.status || order.type || "-",
      order.uuid || order.identifier || order.commandId || "-",
    ]);
    return ["Execution Orders", rows.length > 0 ? renderTable(["Time", "Market", "Side", "Status", "ID"], rows) : "No order snapshot."].join("\n");
  }

  if (section === "fills") {
    const fills = execution.latestFills || execution.fills || execution.realFills || [];
    const rows = (Array.isArray(fills) ? fills : []).slice(-20).map((fill) => [
      fill.timestamp || fill.tradeTimestamp || fill.createdAt || "-",
      fill.market || "-",
      fill.side || "-",
      formatNumber(fill.volume || fill.filledVolume || fill.amount),
      formatNumber(fill.paidFee || fill.tradeFee || fill.fee),
    ]);
    return ["Execution Fills", rows.length > 0 ? renderTable(["Time", "Market", "Side", "Volume", "Fee"], rows) : "No fill snapshot."].join("\n");
  }

  if (section === "pnl") {
    const realRunLimits = snapshot.realRunLimits || {};
    const byStart = realRunLimits.summaryByStartAsset || {};
    const rows = Object.entries(byStart).map(([asset, row]) => [
      asset,
      row.cycles || row.cycleCount || 0,
      formatNumber(row.pnl || row.realizedPnl || 0),
      formatNumber(row.dailyLoss || 0),
      formatNumber(row.paidFee || 0),
    ]);
    return ["Execution PnL", rows.length > 0 ? renderTable(["Start", "Cycles", "PnL", "Daily loss", "Paid fee"], rows) : "No PnL snapshot."].join("\n");
  }

  if (section === "residuals") {
    const residualEvents = realBalances.residualEvents || execution.residualEvents || [];
    const rows = Object.entries(residuals).map(([asset, value]) => [asset, formatNumber(value)]);
    const eventRows = (Array.isArray(residualEvents) ? residualEvents : []).slice(-10).map((event) => [
      event.timestamp || event.createdAt || "-",
      event.asset || "-",
      formatNumber(event.amount),
      event.reason || "-",
      event.cycleId || event.planId || "-",
    ]);
    return [
      "Execution Residuals",
      rows.length > 0 ? renderTable(["Asset", "Amount"], rows) : "No residual balances.",
      eventRows.length > 0 ? renderTable(["Time", "Asset", "Amount", "Reason", "Cycle/Plan"], eventRows) : "",
    ].filter(Boolean).join("\n");
  }

  if (section === "guards") {
    return [
      "Execution Guards",
      jsonForDisplay({
        guardStatus: guard,
        privateWsStatus: snapshot.privateWsStatus || {},
        privateCacheStatus: snapshot.privateCacheStatus || {},
        emergencyStop: snapshot.emergencyStop || {},
      }),
    ].join("\n");
  }

  return [
    "Execution",
    renderKeyValues([
      ["Mode", runtimeConfig(snapshot).runMode || "OBSERVE"],
      ["Engine", engineState(snapshot)],
      ["Live trading", runtimeConfig(snapshot).liveTradingEnabled === true],
      ["Open orders", execution.openOrderCount ?? guard.openOrderCount ?? 0],
      ["Active real executions", execution.activeRealExecutionCount ?? 0],
      ["Emergency stop", snapshot.emergencyStop && snapshot.emergencyStop.active === true],
    ]),
    balanceRows.length > 0 ? renderTable(["Asset", "Available"], balanceRows) : "No dry-run balance snapshot.",
    Object.keys(residuals).length > 0
      ? renderTable(["Residual asset", "Amount"], Object.entries(residuals).map(([asset, value]) => [asset, formatNumber(value)]))
      : "No residual balances.",
  ].join("\n");
}

function renderBalances(snapshot = {}) {
  const execution = snapshot.execution || {};
  const dry = execution.dryRunBalances || execution.dryRunCapital || {};
  const real = execution.realBalances || {};
  const rows = [];

  for (const [asset, value] of Object.entries(dry.availableBalances || dry.available || {})) {
    rows.push(["dry-run", asset, "available", formatNumber(value)]);
  }
  for (const [asset, value] of Object.entries(dry.reservedBalances || dry.reserved || {})) {
    rows.push(["dry-run", asset, "reserved", formatNumber(value)]);
  }
  for (const [asset, value] of Object.entries(real.availableBalances || real.available || {})) {
    rows.push(["real", asset, "available", formatNumber(value)]);
  }
  for (const [asset, value] of Object.entries(real.lockedBalances || real.locked || {})) {
    rows.push(["real", asset, "locked", formatNumber(value)]);
  }
  for (const [asset, value] of Object.entries(real.residualBalances || {})) {
    rows.push(["real", asset, "residual", formatNumber(value)]);
  }

  return [
    "Balances",
    rows.length > 0 ? renderTable(["Scope", "Asset", "Kind", "Amount"], rows) : "No balance snapshot.",
  ].join("\n");
}

function renderLatency(snapshot = {}) {
  const budget = snapshot.performanceBudget || {};
  const metrics = sanitizeLegacyBrowserMetrics(snapshot.metrics || {});
  const rows = [
    ["market-data", "maxOldestLegAgeMs", budget.marketData && budget.marketData.maxOldestLegAgeMs],
    ["market-data", "maxLegTimestampSkewMs", budget.marketData && budget.marketData.maxLegTimestampSkewMs],
    ["market-data", "maxExchangeToServerLatencyMs", budget.marketData && budget.marketData.maxExchangeToServerLatencyMs],
    ["decision", "maxDecisionAgeMs", budget.decision && budget.decision.maxDecisionAgeMs],
    ["execution", "maxOrderAckMs", budget.execution && budget.execution.maxOrderAckMs],
    ["execution", "maxReconciliationMs", budget.execution && budget.execution.maxReconciliationMs],
    ["display", "affectsTrading", budget.displayLatencyAffectsTrading === true],
  ];

  return [
    "Latency",
    renderTable(["Domain", "Metric", "Value"], rows),
    Object.keys(metrics).length > 0 ? JSON.stringify(metrics, null, 2) : "",
  ].filter(Boolean).join("\n");
}

function sanitizeLegacyBrowserMetrics(metrics = {}) {
  const sanitized = { ...metrics };
  delete sanitized.browser;

  if (sanitized.rates) {
    sanitized.rates = { ...sanitized.rates };
    delete sanitized.rates.browserRenderedFramesPerSec;
  }

  if (sanitized.counters) {
    sanitized.counters = { ...sanitized.counters };
    delete sanitized.counters.browserRenderedFrames;
  }

  return sanitized;
}

function jsonForDisplay(value) {
  return JSON.stringify(value, null, 2);
}

function csvEscape(value) {
  const text = value === null || value === undefined ? "" : String(value);
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function csvRows(headers, rows) {
  return [
    headers.map(csvEscape).join(","),
    ...rows.map((row) => row.map(csvEscape).join(",")),
  ].join("\n");
}

function cycleProfitRate(row = {}) {
  return Number(row.netProfitRate ?? row.expectedNetProfitRate ?? row.profitRate ?? row.netRate ?? 0);
}

function cycleStartAsset(row = {}) {
  return row.startAsset || row.start || row.route && row.route[0] || "-";
}

function cycleRoute(row = {}) {
  if (Array.isArray(row.route)) return row.route.join("->");
  if (Array.isArray(row.assets)) return row.assets.join("->");
  if (Array.isArray(row.routeAssets)) return row.routeAssets.join("->");
  if (row.routeLabel) return row.routeLabel;
  return row.routeVariantId || row.cycleId || "-";
}

function rankedDeskRows(snapshot = {}, options = {}) {
  const cycles = Array.isArray(snapshot.cycles) ? snapshot.cycles : [];
  const positionalStart = options.args && options.args[0] && ["KRW", "BTC", "USDT"].includes(String(options.args[0]).toUpperCase())
    ? String(options.args[0]).toUpperCase()
    : "";
  const startFilter = String(options.start || positionalStart || "").toUpperCase();
  const profitableOnly = options.profitable === true;
  const top = Number.parseInt(options.top || "20", 10);

  return cycles
    .filter((row) => !startFilter || cycleStartAsset(row).toUpperCase() === startFilter)
    .filter((row) => !profitableOnly || cycleProfitRate(row) > 0)
    .sort((left, right) => cycleProfitRate(right) - cycleProfitRate(left))
    .slice(0, Number.isInteger(top) && top > 0 ? top : 20);
}

function deskTableRows(rows) {
  return rows.map((row, index) => [
    index + 1,
    cycleStartAsset(row),
    cycleRoute(row),
    formatPercent(cycleProfitRate(row)),
    formatNumber(row.maxExecutableStartAmount ?? row.startAmount ?? row.executableStartAmount),
    row.limitingLeg || row.limitingLegIndex || "-",
    row.status || row.validationStatus || (cycleProfitRate(row) > 0 ? "PROFIT" : "-"),
  ]);
}

function renderDesk(snapshot = {}, options = {}, context = {}) {
  const startFilter = String(options.start || options.args && options.args[0] || "").toUpperCase();
  const filtered = rankedDeskRows(snapshot, options);
  const stateSummary = summary(snapshot);
  const scheduler = stateSummary.cycleScheduler || snapshot.cycleScheduler || {};
  const warmupLine = scheduler.totalCycleCount && (
    scheduler.pendingCycleCount > 0 || scheduler.warmupComplete === false
  )
    ? `Warm-up ${scheduler.completedCycleCount || 0}/${scheduler.totalCycleCount}; pending ${scheduler.pendingCycleCount || 0}`
    : "";

  context.lastDeskRows = filtered;

  return [
    `Arbitrage Desk${startFilter ? ` - ${startFilter}` : ""}`,
    warmupLine,
    filtered.length > 0
      ? renderTable(["#", "Start", "Route", "Net", "ExecAmt", "LimitLeg", "Status"], deskTableRows(filtered))
      : "No opportunities in latest snapshot.",
  ].filter(Boolean).join("\n");
}

function exportDesk(snapshot = {}, options = {}, context = {}) {
  const rows = rankedDeskRows(snapshot, options);
  context.lastDeskRows = rows;

  if (String(options.format || "").toLowerCase() === "json" || options.json) {
    return jsonForDisplay(rows);
  }

  if (String(options.format || "").toLowerCase() === "csv" || options.csv) {
    return csvRows(
      ["rank", "startAsset", "cycleId", "route", "netProfitRate", "executableStartAmount", "limitingLeg", "status"],
      rows.map((row, index) => [
        index + 1,
        cycleStartAsset(row),
        row.cycleId || row.routeVariantId || "",
        cycleRoute(row),
        cycleProfitRate(row),
        row.maxExecutableStartAmount ?? row.startAmount ?? row.executableStartAmount ?? "",
        row.limitingLeg || row.limitingLegIndex || "",
        row.status || row.validationStatus || "",
      ]),
    );
  }

  return renderDesk(snapshot, options, context);
}

function resolveOpportunity(snapshot = {}, selector, context = {}) {
  const text = String(selector || "").trim();
  const rankMatch = text.match(/^#?(\d+)$/);
  const candidates = context.lastDeskRows && context.lastDeskRows.length > 0
    ? context.lastDeskRows
    : rankedDeskRows(snapshot, { top: 200 });

  if (rankMatch) {
    const index = Number(rankMatch[1]) - 1;
    return candidates[index] || null;
  }

  return candidates.find((row) => (
    row.cycleId === text ||
    row.routeVariantId === text ||
    row.triangleId === text
  )) || null;
}

function renderOpportunity(row = {}, view = "show") {
  if (!row) return "Opportunity not found.";

  if (view === "legs") {
    const legs = row.legs || row.route || row.steps || row.executionPlan && row.executionPlan.legs || [];
    const rows = legs.map((leg, index) => {
      if (typeof leg === "string") return [index + 1, leg, "-", "-", "-"];
      return [
        index + 1,
        leg.market || "-",
        `${leg.fromAsset || leg.from || "?"}->${leg.toAsset || leg.to || "?"}`,
        leg.side || leg.action || "-",
        leg.price || leg.observedBestPrice || leg.bestPrice || "-",
      ];
    });
    return rows.length > 0 ? renderTable(["#", "Market", "Route", "Side", "Price"], rows) : "No leg detail.";
  }

  if (view === "latency") {
    const latency = row.latency || row.timingTrace || row.performanceBudget || {};
    return [
      "Opportunity Latency",
      Object.keys(latency).length > 0 ? jsonForDisplay(latency) : "No latency detail.",
    ].join("\n");
  }

  if (view === "plan") {
    const plan = row.executionPlan || row.plan || row.strategyPlan || null;
    return plan ? jsonForDisplay(plan) : "No execution plan detail.";
  }

  const validation = row.depthValidation || row.validation || {};
  return [
    "Opportunity",
    renderKeyValues([
      ["Cycle", row.cycleId || row.routeVariantId || "-"],
      ["Start", cycleStartAsset(row)],
      ["Route", cycleRoute(row)],
      ["Net", formatPercent(cycleProfitRate(row))],
      ["Executable amount", formatNumber(row.maxExecutableStartAmount ?? row.startAmount ?? row.executableStartAmount)],
      ["Limiting leg", row.limitingLeg || row.limitingLegIndex || "-"],
      ["Status", row.status || row.validationStatus || validation.validationStatus || "-"],
      ["Reason", row.reason || row.validationReason || validation.validationReason || "-"],
      ["Strategy", row.strategyId || row.strategy && row.strategy.strategyId || "-"],
    ]),
  ].join("\n");
}

function renderLogs(rows = []) {
  const tableRows = rows.map((row) => [
    row.timestamp || row.ts || "-",
    row.kind || row.type || "-",
    row.mode || "-",
    row.startAsset || "-",
    row.command || row.cycleId || row.reason || row.message || row.strategyId || "-",
  ]);

  return tableRows.length > 0
    ? renderTable(["Time", "Type", "Mode", "Start", "Detail"], tableRows)
    : "No logs matched.";
}

function logRecordKey(row = {}) {
  return row.eventId ||
    row.traceId && `${row.logKind || "log"}:${row.traceId}:${row.timestamp || row.ts || ""}` ||
    `${row.logKind || "log"}:${row.timestamp || row.ts || ""}:${row.type || ""}:${row.commandId || row.cycleId || row.identifier || ""}`;
}

function filterNewLogs(rows = [], context) {
  const fresh = [];

  for (const row of rows) {
    const key = logRecordKey(row);
    if (context.followSeenLogKeys.has(key)) continue;
    context.followSeenLogKeys.add(key);
    fresh.push(row);
  }

  return fresh;
}

function renderStrategyList(context, activeStrategyId) {
  const rows = context.strategyRegistry.list().map((strategy) => [
    strategy.id === activeStrategyId ? "*" : "",
    strategy.id,
    strategy.version,
    strategy.hash || "-",
    strategy.description,
  ]);

  return [
    "Strategies",
    renderTable(["Active", "ID", "Version", "Hash", "Description"], rows),
  ].join("\n");
}

async function handleStrategyCommand(parsed, context) {
  const action = parsed.args[0] || "active";
  const snapshot = await context.telemetry.snapshot();
  const activeStrategyId = runtimeConfig(snapshot).activeStrategyId || (await readActiveConfig(context.configPath)).activeStrategyId;

  if (action === "list") {
    return renderStrategyList(context, activeStrategyId);
  }

  if (action === "active") {
    return [
      "Active Strategy",
      renderKeyValues([
        ["Strategy", activeStrategyId || "-"],
        ["Engine state", engineState(snapshot)],
      ]),
    ].join("\n");
  }

  if (action === "explain") {
    const strategyId = parsed.args[1] || activeStrategyId;
    const strategy = context.strategyRegistry.get(strategyId);
    return [
      "Strategy",
      renderKeyValues([
        ["ID", strategy.id],
        ["Name", strategy.name],
        ["Version", strategy.version],
        ["Hash", strategy.hash || "-"],
        ["Description", strategy.description],
        ["Default config", jsonForDisplay(strategy.defaultConfig)],
      ]),
    ].join("\n");
  }

  if (action === "select") {
    const strategyId = parsed.args[1];
    if (!strategyId) throw new Error("/strategy select requires a strategy id");
    const strategy = context.strategyRegistry.get(strategyId);
    const active = await readActiveConfig(context.configPath);
    const nextConfig = {
      ...active,
      activeStrategyId: strategy.id,
    };
    validateConfig(nextConfig, {
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });

    if (engineState(snapshot) === "STOPPED") {
      await writeJsonAtomic(context.configPath, nextConfig);
      return `Strategy selected for next start: ${strategy.id}`;
    }

    const draft = await baseDraftConfig({
      configPath: context.configPath,
      draftPath: context.draftConfigPath,
    });
    draft.activeStrategyId = strategy.id;
    validateConfig(draft, {
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    await writeJsonAtomic(context.draftConfigPath, draft);
    return `Engine is ${engineState(snapshot)}. Strategy saved to draft only: ${strategy.id}`;
  }

  throw new Error(`Unknown strategy action: ${action}`);
}

async function handleConfigCommand(parsed, context) {
  const action = parsed.args[0] || "show";
  const active = await readActiveConfig(context.configPath);
  const draft = await readDraftConfig(context.draftConfigPath);

  if (action === "show") {
    const target = parsed.options.draft ? (draft || active) : active;
    return jsonForDisplay(target);
  }

  if (action === "validate") {
    const target = parsed.options.draft ? (draft || active) : active;
    validateConfig(target, {
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    return `Config valid: ${parsed.options.draft ? "draft" : "active"}`;
  }

  if (action !== "draft") {
    throw new Error(`Unknown config action: ${action}`);
  }

  const draftAction = parsed.args[1] || "show";

  if (draftAction === "show") {
    return jsonForDisplay(draft || active);
  }

  if (draftAction === "set") {
    const configPath = parsed.args[2];
    const rawValue = parsed.args[3];
    if (!configPath || rawValue === undefined) {
      throw new Error("/config draft set requires <path> <value>");
    }
    const next = await baseDraftConfig({
      configPath: context.configPath,
      draftPath: context.draftConfigPath,
    });
    setConfigPath(next, configPath, parseConfigValue(rawValue));
    validateConfig(next, {
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    await writeJsonAtomic(context.draftConfigPath, next);
    return `Draft updated: ${configPath}`;
  }

  if (draftAction === "diff") {
    const rows = diffConfigs(active, draft || active).map((entry) => [
      entry.path,
      JSON.stringify(entry.active),
      JSON.stringify(entry.draft),
    ]);
    return rows.length > 0
      ? renderTable(["Path", "Active", "Draft"], rows)
      : "No draft changes.";
  }

  if (draftAction === "save") {
    const snapshot = await context.telemetry.snapshot();
    if (!draft) return "No draft config to save.";
    validateConfig(draft, {
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    if (engineState(snapshot) !== "STOPPED") {
      return `Engine is ${engineState(snapshot)}. Draft was validated but active config was not changed.`;
    }
    await writeJsonAtomic(context.configPath, draft);
    await fs.rm(context.draftConfigPath, { force: true });
    return "Draft saved to active config.";
  }

  throw new Error(`Unknown config draft action: ${draftAction}`);
}

async function readJsonFile(filePath, fallback) {
  if (!filePath) return fallback;
  return JSON.parse(await fs.readFile(filePath, "utf8"));
}

async function handleReplayCommand(parsed) {
  const action = parsed.args[0] || "dryrun";
  if (action !== "dryrun") throw new Error(`Unknown replay action: ${action}`);

  const tapePath = parsed.options.tape || process.env.TAPE_JSON;
  const cyclesPath = parsed.options.cycles || process.env.CYCLES_JSON || path.resolve(process.cwd(), "out", "upbit-canonical-cycles.json");
  if (!tapePath) {
    throw new Error("Replay requires --tape <file> or TAPE_JSON=<file>");
  }

  const runtimeConfig = parsed.options.config || process.env.RUNTIME_CONFIG
    ? loadRuntimeConfig({ configPath: parsed.options.config || process.env.RUNTIME_CONFIG })
    : DEFAULT_RUNTIME_CONFIG;
  const tape = await readJsonFile(tapePath, []);
  const cyclesPayload = await readJsonFile(cyclesPath, []);
  const cycles = Array.isArray(cyclesPayload) ? cyclesPayload : cyclesPayload.cycles || [];
  const result = await replayDryRunReport({
    cycles,
    tape,
    runtimeConfig: {
      ...runtimeConfig,
      runMode: "DRY_RUN",
    },
    feeRate: Number(process.env.UPBIT_TAKER_FEE_RATE || 0),
    staleOrderbookMs: Number(process.env.STALE_ORDERBOOK_MS || 3000),
    nowMs: parsed.options.nowMs || process.env.REPLAY_NOW_MS ? Number(parsed.options.nowMs || process.env.REPLAY_NOW_MS) : Date.now(),
  });

  if (String(parsed.options.format || "").toLowerCase() === "summary") {
    return [
      "Replay Dry-run",
      renderKeyValues([
        ["Generated", result.generatedAt],
        ["Candidates", result.candidateCount],
        ["Accepted", result.acceptedCount],
        ["Rejected", result.rejectedCount],
        ["Execution plans", result.executionPlans.length],
        ["Manifest", result.replayManifest && result.replayManifest.fingerprints && result.replayManifest.fingerprints.overall],
      ]),
    ].join("\n");
  }

  return jsonForDisplay({
    generatedAt: result.generatedAt,
    candidateCount: result.candidateCount,
    acceptedCount: result.acceptedCount,
    rejectedCount: result.rejectedCount,
    executionPlanCount: result.executionPlans.length,
    replayManifest: result.replayManifest,
    dryRunReport: result.dryRunReport,
    dryRunExecutions: result.dryRunExecutions,
    plans: result.executionPlans.map((plan) => ({
      planId: plan.planId,
      cycleId: plan.cycleId,
      startAsset: plan.startAsset,
      startAmount: plan.startAmount,
      expectedNetProfit: plan.expectedNetProfit,
      strategyId: plan.strategyId,
    })),
  });
}

function helpText(commandName = "") {
  if (!commandName) {
    return [
      "q-gagarin CLI",
      "Slash commands only. Natural-language input is rejected.",
      "",
      renderTable(["Command", "Description"], COMMANDS),
    ].join("\n");
  }

  const match = COMMANDS.find(([command]) => command.split(" ")[0] === `/${commandName}`);
  if (!match) return `No help for /${commandName}. Try /help.`;
  return `${match[0]}\n${match[1]}`;
}

function modeFromStartArg(arg) {
  const normalized = String(arg || "observe").trim().toLowerCase();
  if (normalized === "observe" || normalized === "obs") return "OBSERVE";
  if (normalized === "dry" || normalized === "dry-run" || normalized === "dry_run") return "DRY_RUN";
  if (normalized === "real-guarded" || normalized === "real_guarded") return "REAL_GUARDED";
  throw new Error(`Unsupported start mode: ${arg}. Use observe, dry, or real-guarded.`);
}

async function pollCommandStatus(context, commandId) {
  const deadline = Date.now() + context.statusPollTimeoutMs;
  let latest = null;

  while (Date.now() <= deadline) {
    latest = await context.commandQueue.readStatus(commandId);
    if (latest && latest.status && latest.status !== "queued") return latest;
    await new Promise((resolve) => setTimeout(resolve, context.pollIntervalMs));
  }

  return latest;
}

async function queueEngineCommand(context, payload) {
  const queued = await context.commandQueue.queue(payload);
  const status = await pollCommandStatus(context, queued.commandId);
  const effectiveStatus = status || queued;
  const failedItems = Array.isArray(effectiveStatus.failedItems)
    ? effectiveStatus.failedItems
    : effectiveStatus.readiness && Array.isArray(effectiveStatus.readiness.items)
      ? effectiveStatus.readiness.items.filter((item) => !item.passed).map((item) => item.id)
      : [];

  return [
    `Command queued: ${queued.command} (${queued.commandId})`,
    renderKeyValues([
      ["Status", effectiveStatus.status || "queued"],
      ["Run mode", effectiveStatus.runMode || queued.runMode || "-"],
      ["Source", effectiveStatus.source || queued.source || "cli"],
      ["Message", effectiveStatus.message || "-"],
    ]),
    failedItems.length > 0 ? renderTable(["Readiness failure"], failedItems.map((item) => [item])) : "",
  ].join("\n");
}

async function readLogsForCommand(parsed, context) {
  return context.telemetry.logs({
    kind: parsed.options.kind || "all",
    limit: Number.parseInt(parsed.options.limit || "50", 10),
    mode: parsed.options.mode || "",
    startAsset: parsed.options.start || parsed.options.startAsset || "",
    strategyId: parsed.options.strategy || parsed.options.strategyId || "",
    cycleId: parsed.options.cycle || parsed.options.cycleId || "",
    type: parsed.options.type || "",
  });
}

async function runOnce(parsed, context) {
  const snapshotCommands = new Set([
    "status",
    "summary",
    "mode",
    "readiness",
    "market",
    "system",
    "latency",
    "execution",
    "balances",
    "desk",
  ]);

  if (parsed.name === "empty") return { exit: false, output: "" };
  if (parsed.name === "quit" || parsed.name === "exit") return { exit: true, output: "" };
  if (parsed.name === "clear") return { exit: false, output: "\x1Bc" };
  if (parsed.name === "help") return { exit: false, output: helpText(parsed.args[0]) };
  if (parsed.name === "settings") {
    return { exit: false, output: await handleConfigCommand({ ...parsed, name: "config", args: ["show"] }, context) };
  }

  if (parsed.name === "start") {
    return {
      exit: false,
      output: await queueEngineCommand(context, {
        command: "Start",
        runMode: modeFromStartArg(parsed.args[0]),
      }),
    };
  }

  if (parsed.name === "pause") {
    return { exit: false, output: await queueEngineCommand(context, { command: "Pause" }) };
  }

  if (parsed.name === "stop") {
    return { exit: false, output: await queueEngineCommand(context, { command: "Stop" }) };
  }

  if (parsed.name === "emergency-stop") {
    return { exit: false, output: await queueEngineCommand(context, { command: "Stop", emergency: true }) };
  }

  if (parsed.name === "strategy") {
    return { exit: false, output: await handleStrategyCommand(parsed, context) };
  }

  if (parsed.name === "config") {
    return { exit: false, output: await handleConfigCommand(parsed, context) };
  }

  if (parsed.name === "replay") {
    return { exit: false, output: await handleReplayCommand(parsed, context) };
  }

  if (parsed.name === "export") {
    const target = parsed.args[0];
    if (target !== "desk") throw new Error(`Unknown export target: ${target}`);
    const snapshot = await context.telemetry.snapshot();
    return {
      exit: false,
      output: exportDesk(snapshot, {
        ...parsed.options,
        args: parsed.args.slice(1),
      }, context),
    };
  }

  if (parsed.name === "opportunity") {
    const action = parsed.args[0] || "show";
    const selector = parsed.args[1];
    if (!selector) throw new Error(`/opportunity ${action} requires <rank|cycleId>`);
    const snapshot = await context.telemetry.snapshot();
    const row = resolveOpportunity(snapshot, selector, context);
    if (parsed.options.json) return { exit: false, output: jsonForDisplay(row) };
    return { exit: false, output: renderOpportunity(row, action) };
  }

  if (snapshotCommands.has(parsed.name)) {
    const snapshot = await context.telemetry.snapshot();
    if (parsed.name === "desk" && (parsed.options.csv || parsed.options.format || parsed.options.json)) {
      return {
        exit: false,
        output: exportDesk(snapshot, {
          ...parsed.options,
          args: parsed.args,
        }, context),
      };
    }
    if (parsed.options.json) return { exit: false, output: JSON.stringify(snapshot, null, 2) };
    if (parsed.name === "status" || parsed.name === "summary") return { exit: false, output: renderStatus(snapshot) };
    if (parsed.name === "mode") {
      return { exit: false, output: runtimeConfig(snapshot).runMode || "OBSERVE" };
    }
    if (parsed.name === "readiness") return { exit: false, output: renderReadiness(snapshot) };
    if (parsed.name === "market") return { exit: false, output: renderMarket(snapshot, parsed.args[0] || "") };
    if (parsed.name === "system") return { exit: false, output: renderSystem(snapshot, parsed.args[0] || "") };
    if (parsed.name === "latency") return { exit: false, output: renderLatency(snapshot) };
    if (parsed.name === "execution") return { exit: false, output: renderExecution(snapshot, parsed.args[0] || "") };
    if (parsed.name === "balances") return { exit: false, output: renderBalances(snapshot) };
    if (parsed.name === "desk") {
      return {
        exit: false,
        output: renderDesk(snapshot, {
          ...parsed.options,
          args: parsed.args,
        }, context),
      };
    }
  }

  if (parsed.name === "logs") {
    const result = await readLogsForCommand(parsed, context);

    if (parsed.options.json) return { exit: false, output: JSON.stringify(result.logs, null, 2) };
    return { exit: false, output: renderLogs(result.logs) };
  }

  if (parsed.name === "dryrun" && parsed.args[0] === "report") {
    const result = await context.telemetry.dryRunReport({
      limit: Number.parseInt(parsed.options.limit || "5000", 10),
      sinceMs: parsed.options.sinceMs || "",
      from: parsed.options.from || "",
      to: parsed.options.to || "",
    });
    const format = String(parsed.options.format || "").toLowerCase();

    if (format === "json" || parsed.options.json) return { exit: false, output: JSON.stringify(result.summary, null, 2) };
    if (format === "csv" || parsed.options.csv) return { exit: false, output: result.csv.trimEnd() };
    return {
      exit: false,
      output: [
        "Dry-run Report",
        renderKeyValues([
          ["Opportunities", result.summary.totalOpportunities],
          ["Accepted", result.summary.accepted],
          ["Rejected", result.summary.rejected],
          ["Complete cycles", result.summary.simulatedCompleteCycles],
          ["Failed cycles", result.summary.simulatedFailedCycles],
          ["Complete rate", formatPercent(result.summary.simulatedCompleteRate)],
          ["Expected/sim gap", formatNumber(result.summary.expectedSimulatedGapRate)],
        ]),
      ].join("\n"),
    };
  }

  throw new Error(`Unknown command: /${parsed.name}. Try /help.`);
}

async function runCliCommand(parsed, context) {
  if (parsed.name === "watch") {
    return runWatch({
      ...parsed,
      name: "status",
      options: {
        ...parsed.options,
        watch: true,
      },
    }, context);
  }

  if (parsed.options.watch || parsed.options.follow) {
    return runWatch(parsed, context);
  }

  const result = await runOnce(parsed, context);
  if (result.output) write(context, result.output);
  return result;
}

async function runWatch(parsed, context) {
  const follow = parsed.options.follow === true;

  while (true) {
    if (follow && parsed.name === "logs") {
      const result = await readLogsForCommand(parsed, context);
      const rows = filterNewLogs(result.logs, context);
      if (rows.length > 0) {
        write(context, parsed.options.json ? JSON.stringify(rows, null, 2) : renderLogs(rows));
      }
      await new Promise((resolve) => setTimeout(resolve, Math.max(250, context.pollIntervalMs)));
      continue;
    }

    const result = await runOnce({
      ...parsed,
      options: {
        ...parsed.options,
        watch: false,
        follow: false,
      },
    }, context);

    write(context, result.output);
    if (follow) {
      await new Promise((resolve) => setTimeout(resolve, Math.max(250, context.pollIntervalMs)));
    } else {
      await new Promise((resolve) => setTimeout(resolve, Math.max(250, context.watchIntervalMs)));
      context.output.write("\x1Bc");
    }
  }
}

module.exports = {
  COMMANDS,
  createCliContext,
  filterNewLogs,
  helpText,
  logRecordKey,
  modeFromStartArg,
  runtimeLiveness,
  renderDesk,
  renderExecution,
  renderMarket,
  renderReadiness,
  renderStatus,
  renderSystem,
  runCliCommand,
  runOnce,
};
