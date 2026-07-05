const { buildGraph, mapToSortedObject } = require("../lib/marketGraph");
const {
  findUniqueTriangles,
  buildDirectionalCycles,
  getHubBreakdownCounts,
} = require("../lib/triangles");
const { calculateCycleMultiplier } = require("../lib/multiplier");
const { fetchUpbitMarkets, fetchOrderbooks } = require("../lib/upbitApi");
const { DEFAULT_RUNTIME_CONFIG, freezeRuntimeConfig } = require("../core/runtimeConfig");
const { TimingTrace, perfNowNs } = require("../core/timingTrace");
const { buildExecutionPlan, executionLogMode } = require("../execution/executionPlan");
const { createStrategyRegistry } = require("../strategies/registry");
const { validateDepthAwareCandidate } = require("./candidateValidator");
const {
  ObservationOrderbookStore,
  ValidationOrderbookStore,
} = require("./orderbookStore");
const { RuntimeMetrics } = require("./metrics");
const {
  computeFeeMetrics,
  classifyOpportunity,
  buildStableCycleLayout,
  getCycleFreshness,
  calculateLatencyBreakdown,
} = require("./liveUtils");
const { performance } = require("node:perf_hooks");

function parseFeeRate(value, defaultValue = 0) {
  if (value === undefined || value === null || value === "") {
    return defaultValue;
  }

  const feeRate = Number.parseFloat(value);

  if (!Number.isFinite(feeRate) || feeRate < 0 || feeRate >= 1) {
    throw new Error(`Invalid fee rate: ${value}`);
  }

  return feeRate;
}

function toCalculationOrderbook(topOfBook) {
  if (!topOfBook) {
    return null;
  }

  const units = Array.isArray(topOfBook.orderbook_units) && topOfBook.orderbook_units.length > 0
    ? topOfBook.orderbook_units
    : [
        {
          ask_price: topOfBook.askPrice,
          bid_price: topOfBook.bidPrice,
          ask_size: topOfBook.askSize,
          bid_size: topOfBook.bidSize,
        },
      ];

  return {
    market: topOfBook.market,
    timestamp: topOfBook.timestamp,
    receivedAt: topOfBook.receivedAt,
    streamType: topOfBook.streamType,
    orderbookUnit: topOfBook.orderbookUnit || units.length,
    orderbook_units: units,
  };
}

function normalizeRestOrderbook(orderbook, receivedAt = Date.now(), options = {}) {
  const unit = orderbook && Array.isArray(orderbook.orderbook_units) && orderbook.orderbook_units[0];

  if (!orderbook || !unit) {
    return null;
  }

  const orderbookUnit = options.orderbookUnit || orderbook.orderbook_units.length;
  const orderbookUnits = orderbook.orderbook_units.slice(0, orderbookUnit).map((item) => ({
    ask_price: Number(item.ask_price),
    bid_price: Number(item.bid_price),
    ask_size: Number(item.ask_size),
    bid_size: Number(item.bid_size),
  }));

  return {
    market: orderbook.market,
    askPrice: Number(unit.ask_price),
    bidPrice: Number(unit.bid_price),
    askSize: Number(unit.ask_size),
    bidSize: Number(unit.bid_size),
    timestamp: Number(orderbook.timestamp),
    streamType: "REST",
    receivedAt,
    orderbookUnit,
    orderbook_units: orderbookUnits,
  };
}

function markerColorFor(direction, status, isActuallyProfitable) {
  if (status === "stale" || status === "unavailable") {
    return "#aeb8c5";
  }

  if (isActuallyProfitable) {
    return "#d83f7b";
  }

  return "#2d6f9f";
}

function compactCycleForDelta(row) {
  return {
    triangleId: row.triangleId,
    cycleId: row.cycleId,
    routeVariantId: row.routeVariantId,
    legacyCycleId: row.legacyCycleId,
    startAsset: row.startAsset,
    endAsset: row.endAsset,
    direction: row.direction,
    directionLabel: row.directionLabel,
    y: row.y,
    grossMultiplier: row.grossMultiplier,
    netMultiplier: row.netMultiplier,
    grossProfitRate: row.grossProfitRate,
    netProfitRate: row.netProfitRate,
    markerColor: row.markerColor,
    markerSymbol: row.markerSymbol,
    status: row.status,
    isActuallyProfitable: row.isActuallyProfitable,
    unavailableReason: row.unavailableReason,
    staleReason: row.staleReason,
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
    executionFeasibility: row.executionFeasibility,
    timingBreakdown: row.timingBreakdown,
    latency: row.latency,
    lastChangedMarket: row.lastChangedMarket,
    lastUpbitTimestampMs: row.lastUpbitTimestampMs,
    calculatedAtEpochMs: row.calculatedAtEpochMs,
    calculatedAtIso: row.calculatedAtIso,
  };
}

class LiveTriangleState {
  constructor(options = {}) {
    this.feeRate = parseFeeRate(options.feeRate, 0);
    this.staleOrderbookMs = options.staleOrderbookMs || 3000;
    this.runtimeConfig = freezeRuntimeConfig(options.runtimeConfig || DEFAULT_RUNTIME_CONFIG);
    this.serverStartedAt = new Date().toISOString();
    this.engineState = options.engineState || "STOPPED";
    this.marketRows = [];
    this.quoteCounts = new Map();
    this.triangles = [];
    this.cycles = [];
    this.cycleIndex = new Map();
    this.cycleRows = new Map();
    this.cycleHistory = new Map();
    this.marketToCycleIds = new Map();
    this.dirtyCycleIds = new Set();
    this.groups = [];
    this.groupCounts = {};
    this.xRange = { min: 0.25, max: 1.75 };
    this.metrics = options.metrics || new RuntimeMetrics();
    this.observationStore = options.observationStore || new ObservationOrderbookStore({
      orderbookUnit: this.runtimeConfig.observationOrderbookUnit,
      staleOrderbookMs: this.staleOrderbookMs,
      metrics: this.metrics,
    });
    this.validationStore = options.validationStore || new ValidationOrderbookStore({
      orderbookUnit: this.runtimeConfig.validationOrderbookUnit,
      staleOrderbookMs: this.staleOrderbookMs,
      metrics: this.metrics,
    });
    this.orderbooks = this.observationStore.orderbooks;
    this.wsStatus = {
      stopped: true,
      connections: [],
      openConnectionCount: 0,
    };
    this.validationWsStatus = {
      stopped: true,
      connections: [],
      openConnectionCount: 0,
    };
    this.strategyRegistry = options.strategyRegistry || createStrategyRegistry();
    this.activeStrategy = this.strategyRegistry.get(this.runtimeConfig.activeStrategyId);
    this.eventLog = [];
    this.logStore = options.logStore || null;
    this.executionHandler = options.executionHandler || null;
    this.maxEventLogEntries = options.maxEventLogEntries || 1000;
    this.lastCalculatedAt = null;
    this.lastOrderbookReceivedAt = null;
    this.lastFallbackPollAt = null;
    this.lastFallbackPollError = null;
  }

  setRuntimeConfig(runtimeConfig) {
    this.runtimeConfig = freezeRuntimeConfig(runtimeConfig);
    this.activeStrategy = this.strategyRegistry.get(this.runtimeConfig.activeStrategyId);
  }

  setExecutionHandler(handler) {
    this.executionHandler = typeof handler === "function" ? handler : null;
  }

  async initialize() {
    const upbitMarkets = await fetchUpbitMarkets();
    const { normalizedMarkets, graph, pairMap, quoteCounts } = buildGraph(upbitMarkets);
    const triangles = findUniqueTriangles(graph, pairMap);
    const directionalCycles = buildDirectionalCycles(triangles, pairMap, {
      enabledStartAssets: this.runtimeConfig.enabledStartAssets,
    });
    const layout = buildStableCycleLayout(directionalCycles);

    this.marketRows = normalizedMarkets;
    this.quoteCounts = quoteCounts;
    this.triangles = triangles;
    this.cycles = layout.cycles;
    this.cycleIndex = new Map(this.cycles.map((cycle) => [cycle.cycleId, cycle]));
    this.groups = layout.groups;
    this.groupCounts = layout.groupCounts;
    this.xRange = layout.xRange;
    this.hubBreakdown = getHubBreakdownCounts(triangles);
    this.requiredMarkets = [...new Set(this.cycles.flatMap((cycle) => cycle.markets))].sort();
    this.marketToCycleIds = this.buildMarketToCycleIds();
    this.recalculateAll({ markDirty: false });
  }

  async loadInitialOrderbooks(options = {}) {
    const result = await fetchOrderbooks(this.requiredMarkets, options);
    const receivedAt = Date.now();

    for (const orderbook of result.orderbookMap.values()) {
      const observation = normalizeRestOrderbook(orderbook, receivedAt, {
        orderbookUnit: this.runtimeConfig.observationOrderbookUnit,
      });
      const validation = normalizeRestOrderbook(orderbook, receivedAt, {
        orderbookUnit: this.runtimeConfig.validationOrderbookUnit,
      });

      if (observation) {
        this.observationStore.update(observation, receivedAt);
        this.lastOrderbookReceivedAt = new Date(receivedAt).toISOString();
      }

      if (validation) {
        this.validationStore.update(validation, receivedAt);
      }
    }

    this.lastFallbackPollAt = new Date(receivedAt).toISOString();
    this.lastFallbackPollError = result.errors.length > 0 ? result.errors : null;
    this.recalculateAll({ markDirty: options.markDirty !== false, lastChangedMarket: null });
    return result;
  }

  async fallbackPoll(options = {}) {
    try {
      const result = await this.loadInitialOrderbooks(options);
      this.lastFallbackPollError = result.errors.length > 0 ? result.errors : null;
      return result;
    } catch (error) {
      this.lastFallbackPollError = error.message;
      throw error;
    }
  }

  updateOrderbook(orderbook) {
    return this.updateObservationOrderbook(orderbook);
  }

  updateObservationOrderbook(orderbook) {
    if (!orderbook || !orderbook.market) {
      return [];
    }

    const cacheUpdatedPerfMs = performance.now();
    const cacheWriteStartPerfNs = perfNowNs();
    const normalized = this.observationStore.update(orderbook);

    if (!normalized) {
      return [];
    }
    const cacheWriteDonePerfNs = perfNowNs();

    this.lastOrderbookReceivedAt = new Date(normalized.receivedAt || Date.now()).toISOString();

    if (normalized.streamType !== "REST") {
      this.metrics.increment("upbitOrderbookMessages");
      this.metrics.increment("parsedMessages");
    }

    const affectedCycleLookupStartPerfNs = perfNowNs();
    const affectedCycleIds = [...(this.marketToCycleIds.get(normalized.market) || [])];
    const affectedCycleLookupDonePerfNs = perfNowNs();
    const timings = {
      ...(normalized.timings || {}),
      upbitTimestampMs: Number(normalized.timestamp),
      orderbookCacheUpdatedPerfMs: cacheUpdatedPerfMs,
      cacheWriteStartPerfNs,
      cacheWriteDonePerfNs,
      affectedCycleLookupStartPerfNs,
      affectedCycleLookupDonePerfNs,
    };

    this.recalculateCycles(affectedCycleIds, {
      markDirty: true,
      lastChangedMarket: normalized.market,
      lastUpbitTimestampMs: Number(normalized.timestamp),
      timings,
    });

    return affectedCycleIds;
  }

  updateValidationOrderbook(orderbook) {
    if (!orderbook || !orderbook.market) {
      return [];
    }

    const normalized = this.validationStore.update(orderbook);

    if (!normalized) {
      return [];
    }

    const affectedCycleIds = [...(this.marketToCycleIds.get(normalized.market) || [])];
    this.recalculateCycles(affectedCycleIds, {
      markDirty: true,
      lastChangedMarket: normalized.market,
      lastUpbitTimestampMs: Number(normalized.timestamp),
    });

    return affectedCycleIds;
  }

  setWsStatus(status, feedName = "observation") {
    if (feedName === "validation") {
      this.validationWsStatus = status;
      return;
    }

    this.wsStatus = status;
  }

  getCalculationOrderbooks() {
    return new Map(
      [...this.observationStore.entries()].map(([market, topOfBook]) => [
        market,
        toCalculationOrderbook(topOfBook),
      ]),
    );
  }

  getValidationOrderbooks() {
    return new Map(
      [...this.validationStore.entries()].map(([market, orderbook]) => [
        market,
        toCalculationOrderbook(orderbook),
      ]),
    );
  }

  shouldUseFallback(nowMs = Date.now()) {
    if (!this.wsStatus || this.wsStatus.openConnectionCount === 0) {
      return true;
    }

    if (!this.lastOrderbookReceivedAt) {
      return true;
    }

    return nowMs - Date.parse(this.lastOrderbookReceivedAt) > this.staleOrderbookMs;
  }

  buildMarketToCycleIds() {
    const index = new Map();

    for (const cycle of this.cycles) {
      for (const market of cycle.markets) {
        if (!index.has(market)) {
          index.set(market, new Set());
        }

        index.get(market).add(cycle.cycleId);
      }
    }

    return index;
  }

  setFeeRate(value) {
    this.feeRate = parseFeeRate(value, this.feeRate);
    this.recalculateAll({ markDirty: true });
  }

  selectStrategy(strategyId) {
    if (this.engineState !== "STOPPED") {
      throw new Error("Strategy can only be selected while STOPPED");
    }

    this.activeStrategy = this.strategyRegistry.get(strategyId);
    this.runtimeConfig = freezeRuntimeConfig({
      ...this.runtimeConfig,
      activeStrategyId: strategyId,
    });
    this.logEvent("strategy-selection", {
      strategyId,
      reason: "selected while STOPPED",
    });
    this.recalculateAll({ markDirty: true });
  }

  logEvent(type, payload = {}) {
    const event = {
      sequence: this.eventLog.length + 1,
      timestamp: new Date().toISOString(),
      type,
      ...payload,
    };

    this.eventLog.push(event);

    if (this.eventLog.length > this.maxEventLogEntries) {
      this.eventLog = this.eventLog.slice(-this.maxEventLogEntries);
    }

    if (this.logStore) {
      this.logStore.append("events", event).catch(() => {});

      if (type === "error") {
        this.logStore.append("errors", event).catch(() => {});
      }
    }

    return event;
  }

  logDecision(row) {
    const mode = executionLogMode(this.runtimeConfig.runMode);
    const expectedNetProfit = row.executableStartAmount === null || row.executableStartAmount === undefined ||
      row.netProfitRate === null || row.netProfitRate === undefined
      ? null
      : Number(row.executableStartAmount) * Number(row.netProfitRate);
    const latencyMs = row.latency && row.latency.estimatedEndToDisplayMs;

    this.logEvent("strategy-decision", {
      mode,
      cycleId: row.cycleId,
      routeVariantId: row.routeVariantId,
      startAsset: row.startAsset,
      direction: row.direction,
      strategyId: row.strategyId,
      accepted: row.strategyAccepted,
      reason: row.strategyReason,
      validationReason: row.validationReason,
      expectedNetProfit,
      latencyMs,
    });

    if (this.logStore) {
      this.logStore.append("decisions", {
        type: "strategy-decision",
        mode,
        cycleId: row.cycleId,
        routeVariantId: row.routeVariantId,
        startAsset: row.startAsset,
        direction: row.direction,
        strategyId: row.strategyId,
        accepted: row.strategyAccepted,
        reason: row.strategyReason,
        validationReason: row.validationReason,
        grossMultiplier: row.grossMultiplier,
        netMultiplier: row.netMultiplier,
        expectedNetProfit,
        latencyMs,
        timingBreakdown: row.timingBreakdown,
      }).catch(() => {});
    }
  }

  enqueueExecutionCandidate(row, cycle, validationOrderbooks) {
    if (!this.executionHandler || !row.strategyAccepted) {
      return;
    }

    if (!["DRY_RUN", "REAL_GUARDED", "REAL_AUTO"].includes(this.runtimeConfig.runMode)) {
      return;
    }

    if (this.engineState !== "RUNNING") {
      return;
    }

    const plan = buildExecutionPlan({
      cycle,
      row,
      validationOrderbooks,
      runtimeConfig: this.runtimeConfig,
      feeRate: this.feeRate,
      staleOrderbookMs: this.staleOrderbookMs,
      engineState: this.engineState,
    });

    Promise.resolve()
      .then(() => this.executionHandler(plan, { row }))
      .catch((error) => {
        this.logEvent("error", {
          source: "execution-handler",
          cycleId: row.cycleId,
          message: error.message,
        });
      });
  }

  appendHistory(row) {
    if (row.grossMultiplier === null) {
      row.history = this.cycleHistory.get(row.cycleId) || [];
      return;
    }

    const history = this.cycleHistory.get(row.cycleId) || [];
    history.push({
      timestamp: row.calculatedAtIso,
      grossMultiplier: row.grossMultiplier,
      netMultiplier: row.netMultiplier,
      latency: row.latency,
    });

    const trimmed = history.slice(-20);
    this.cycleHistory.set(row.cycleId, trimmed);
    row.history = trimmed;
  }

  recalculateCycle(cycleId, options = {}) {
    const cycle = this.cycleIndex.get(cycleId);

    if (!cycle) {
      return null;
    }

    const calculatedAtEpochMs = options.nowMs || Date.now();
    const calculatedAtIso = new Date(calculatedAtEpochMs).toISOString();
    const calculationOrderbooks = options.calculationOrderbooks || this.getCalculationOrderbooks();
    const validationOrderbooks = options.validationOrderbooks || this.getValidationOrderbooks();
    const feeMetrics = computeFeeMetrics(this.feeRate);
    const calculationStartPerfMs = performance.now();
    const calcStartPerfNs = perfNowNs();
    const freshness = getCycleFreshness(cycle, calculationOrderbooks, this.staleOrderbookMs, calculatedAtEpochMs);
    const grossResult = calculateCycleMultiplier(cycle, null, calculationOrderbooks, 0, {
      nowMs: calculatedAtEpochMs,
    });
    const netResult = calculateCycleMultiplier(cycle, null, calculationOrderbooks, this.feeRate, {
      nowMs: calculatedAtEpochMs,
    });
    const calculationEndPerfMs = performance.now();
    const calcDonePerfNs = perfNowNs();
    const grossMultiplier = grossResult.available ? grossResult.multiplier : null;
    const netMultiplier = netResult.available ? netResult.multiplier : null;
    const status = freshness.status === "available" && (!grossResult.available || !netResult.available)
      ? "unavailable"
      : freshness.status;
    const unavailableReason = freshness.unavailableReason ||
      grossResult.unavailableReason ||
      netResult.unavailableReason ||
      null;
    const grossProfitRate = grossMultiplier === null ? null : grossMultiplier - 1;
    const netProfitRate = netMultiplier === null ? null : netMultiplier - 1;
    const isActuallyProfitable = status === "available" &&
      grossMultiplier !== null &&
      grossMultiplier > feeMetrics.executableBreakEvenGross;
    const latency = calculateLatencyBreakdown({
      ...(options.timings || {}),
      calculationStartPerfMs,
      calculationEndPerfMs,
    });
    const legTimestamps = freshness.legTimestamps || [];
    const lastUpbitTimestampMs = options.lastUpbitTimestampMs ||
      (legTimestamps.length > 0 ? Math.max(...legTimestamps) : null);
    const riskStartPerfNs = perfNowNs();
    const depthValidation = validateDepthAwareCandidate(cycle, validationOrderbooks, {
      feeRate: this.feeRate,
      nowMs: calculatedAtEpochMs,
      staleOrderbookMs: this.staleOrderbookMs,
      config: this.runtimeConfig.candidateValidation,
    });
    const riskDonePerfNs = perfNowNs();
    const row = {
      triangleId: cycle.triangleId,
      cycleId: cycle.cycleId,
      legacyCycleId: cycle.legacyCycleId,
      routeVariantId: cycle.routeVariantId,
      startAsset: cycle.startAsset,
      endAsset: cycle.endAsset,
      direction: cycle.direction,
      directionLabel: cycle.directionLabel,
      group: cycle.group,
      groupLabel: cycle.groupLabel,
      groupIndex: cycle.groupIndex,
      allHub: cycle.allHub,
      baseX: cycle.baseX,
      x: cycle.x,
      xOffset: cycle.xOffset,
      y: grossMultiplier,
      markerSymbol: cycle.markerSymbol,
      markerColor: markerColorFor(cycle.direction, status, isActuallyProfitable),
      assets: cycle.triangleAssets,
      route: cycle.route,
      routeLabel: cycle.routeLabel,
      markets: cycle.markets,
      legs: netResult.available ? netResult.conversions : grossResult.conversions,
      grossMultiplier,
      netMultiplier,
      grossProfitRate,
      netProfitRate,
      feeRate: this.feeRate,
      executableBreakEvenGross: feeMetrics.executableBreakEvenGross,
      isActuallyProfitable,
      status,
      opportunityClass: classifyOpportunity(grossMultiplier, this.feeRate, status, cycle.direction),
      staleReason: status === "stale" ? unavailableReason : null,
      unavailableReason: status === "unavailable" ? unavailableReason : null,
      validationStatus: depthValidation.validationStatus,
      validationReason: depthValidation.validationReason,
      executableStartAmount: depthValidation.executableStartAmount,
      maxExecutableStartAmount: depthValidation.maxExecutableStartAmount,
      limitingLeg: depthValidation.limitingLeg,
      limitingMarket: depthValidation.limitingMarket,
      expectedSlippageBps: depthValidation.expectedSlippageBps,
      bestLevelTouchRatio: depthValidation.bestLevelTouchRatio,
      residualAfterOrder: depthValidation.residualAfterOrder,
      depthLegs: depthValidation.depthLegs,
      legTimestamps,
      newestLegAgeMs: freshness.newestLegAgeMs,
      oldestLegAgeMs: freshness.oldestLegAgeMs,
      maxLegAgeMs: freshness.maxLegAgeMs,
      lastChangedMarket: options.lastChangedMarket || null,
      lastUpbitTimestampMs,
      lastOrderbookTimestamp: freshness.lastOrderbookTimestamp,
      oldestOrderbookReceivedAt: freshness.oldestOrderbookReceivedAt,
      calculatedAtEpochMs,
      calculatedAtIso,
      latency,
    };
    const strategyStartPerfNs = perfNowNs();
    const strategyDecision = this.activeStrategy.evaluate({
      cycle,
      row,
      depthValidation,
      config: this.activeStrategy.defaultConfig,
    });
    const strategyDonePerfNs = perfNowNs();
    const timingTrace = new TimingTrace({
      ...(options.timings || {}),
      exchangeTimestampEpochMs: options.timings && options.timings.exchangeTimestampEpochMs ||
        options.lastUpbitTimestampMs ||
        lastUpbitTimestampMs,
      calcStartPerfNs,
      calcDonePerfNs,
      strategyStartPerfNs,
      strategyDonePerfNs,
      riskStartPerfNs,
      riskDonePerfNs,
      telemetryPublishPerfNs: perfNowNs(),
      telemetryPublishEpochMs: Date.now(),
      clockSkewSensitive: ["exchangeTimestampEpochMs", "socketReceiveEpochMs", "dashboardReceiveEpochMs"],
    });

    row.strategyId = strategyDecision.strategyId;
    row.strategyAccepted = strategyDecision.accepted;
    row.strategyReason = strategyDecision.reason;
    row.executionFeasibility = strategyDecision.accepted && depthValidation.validationStatus === "accepted"
      ? "EXECUTABLE_CANDIDATE"
      : strategyDecision.reason || depthValidation.validationReason || "NOT_EXECUTABLE";
    row.timingTrace = timingTrace.serialize();
    row.timingBreakdown = timingTrace.breakdown();

    this.appendHistory(row);
    this.cycleRows.set(cycle.cycleId, row);
    this.metrics.increment("recalculatedCycles");

    if (latency.upbitToServerMs !== null) {
      this.metrics.addLatencySample(latency.upbitToServerMs, calculatedAtEpochMs);
    }

    if (options.markDirty !== false) {
      this.dirtyCycleIds.add(cycle.cycleId);
    }

    if (options.logDecision !== false) {
      this.logDecision(row);
      this.enqueueExecutionCandidate(row, cycle, validationOrderbooks);
    }

    return row;
  }

  recalculateCycles(cycleIds, options = {}) {
    const calculationOrderbooks = this.getCalculationOrderbooks();
    const validationOrderbooks = this.getValidationOrderbooks();

    return cycleIds
      .map((cycleId) => this.recalculateCycle(cycleId, {
        ...options,
        calculationOrderbooks,
        validationOrderbooks,
      }))
      .filter(Boolean);
  }

  recalculateAll(options = {}) {
    return this.recalculateCycles(this.cycles.map((cycle) => cycle.cycleId), options);
  }

  buildCycleRows(now = new Date()) {
    this.recalculateAll({
      nowMs: now.getTime(),
      markDirty: false,
      logDecision: false,
    });

    return this.cycles.map((cycle) => this.cycleRows.get(cycle.cycleId)).filter(Boolean);
  }

  refreshAgingCycles(now = new Date()) {
    const nowMs = now.getTime();
    const previousRows = new Map(this.cycleRows);
    const rows = this.recalculateAll({
      nowMs,
      markDirty: false,
      logDecision: false,
    });

    for (const row of rows) {
      const previous = previousRows.get(row.cycleId);

      if (
        !previous ||
        previous.status !== row.status ||
        previous.staleReason !== row.staleReason ||
        previous.unavailableReason !== row.unavailableReason ||
        previous.validationStatus !== row.validationStatus ||
        previous.validationReason !== row.validationReason ||
        previous.strategyAccepted !== row.strategyAccepted ||
        previous.strategyReason !== row.strategyReason ||
        previous.executionFeasibility !== row.executionFeasibility
      ) {
        this.dirtyCycleIds.add(row.cycleId);
      }
    }

    return this.dirtyCycleIds.size;
  }

  consumeDelta(now = new Date()) {
    this.lastCalculatedAt = now.toISOString();
    const dirtyIds = [...this.dirtyCycleIds];
    this.dirtyCycleIds.clear();
    const changedCycles = dirtyIds
      .map((cycleId) => this.cycleRows.get(cycleId))
      .filter(Boolean)
      .map(compactCycleForDelta);

    if (changedCycles.length > 0) {
      this.metrics.increment("pushedPointUpdates", changedCycles.length);
    }

    const metrics = this.metrics.snapshot(this.wsStatus, now.getTime());

    return {
      type: "delta",
      sentAtEpochMs: now.getTime(),
      changedCycles,
      summaryDelta: this.getSummary(this.cycles.map((cycle) => this.cycleRows.get(cycle.cycleId)).filter(Boolean)),
      metrics,
    };
  }

  getSummary(cycles) {
    const availableCount = cycles.filter((cycle) => cycle.status === "available").length;
    const unavailableCount = cycles.length - availableCount;
    const feeMetrics = computeFeeMetrics(this.feeRate);
    const startAssetCounts = cycles.reduce((counts, cycle) => {
      counts[cycle.startAsset] = (counts[cycle.startAsset] || 0) + 1;
      return counts;
    }, {});

    return {
      marketsLoaded: this.marketRows.length,
      quoteCounts: mapToSortedObject(this.quoteCounts),
      uniqueTriangles: this.triangles.length,
      uniqueTriangleCount: this.triangles.length,
      plottedCycleCount: this.cycles.length,
      canonicalCycleCount: this.cycles.filter((cycle) => cycle.direction === "canonical").length,
      reverseCycleCount: this.cycles.filter((cycle) => cycle.direction === "reverse").length,
      availableLiveMultipliers: availableCount,
      unavailableOrStaleCycles: unavailableCount,
      feeRate: this.feeRate,
      executableBreakEvenGross: feeMetrics.executableBreakEvenGross,
      lastUpdateTime: this.lastOrderbookReceivedAt,
      staleOrderbookMs: this.staleOrderbookMs,
      requiredMarketCount: this.requiredMarkets ? this.requiredMarkets.length : 0,
      fallbackLastPolledAt: this.lastFallbackPollAt,
      fallbackLastError: this.lastFallbackPollError,
      startAssetCounts,
      strategyAcceptedCount: cycles.filter((cycle) => cycle.strategyAccepted).length,
      strategyRejectedCount: cycles.filter((cycle) => cycle.strategyAccepted === false).length,
    };
  }

  getOrderbookStoreStatus(nowMs = Date.now()) {
    return {
      observation: this.observationStore.getStatus(nowMs),
      validation: this.validationStore.getStatus(nowMs),
    };
  }

  getStrategySnapshot() {
    return {
      activeStrategyId: this.activeStrategy.id,
      activeStrategy: {
        id: this.activeStrategy.id,
        name: this.activeStrategy.name,
        version: this.activeStrategy.version,
        hash: this.activeStrategy.hash || null,
        description: this.activeStrategy.description,
        defaultConfig: this.activeStrategy.defaultConfig,
      },
      availableStrategies: this.strategyRegistry.list(),
    };
  }

  getSnapshot(now = new Date()) {
    const cycles = this.buildCycleRows(now);

    this.lastCalculatedAt = now.toISOString();

    return {
      type: "full-state",
      summary: this.getSummary(cycles),
      groups: this.groups,
      groupCounts: this.groupCounts,
      xRange: this.xRange,
      cycles,
      serverStartedAt: this.serverStartedAt,
      lastCalculatedAt: this.lastCalculatedAt,
      wsStatus: this.wsStatus,
      metrics: this.metrics.snapshot(this.wsStatus, now.getTime()),
      runtimeConfig: this.runtimeConfig,
      orderbookStores: this.getOrderbookStoreStatus(now.getTime()),
      feedStatus: {
        observation: this.wsStatus,
        validation: this.validationWsStatus,
      },
      strategy: this.getStrategySnapshot(),
      engineState: this.engineState,
      eventLog: this.eventLog.slice(-200),
    };
  }

  getHealth() {
    return {
      ok: true,
      serverStartedAt: this.serverStartedAt,
      wsStatus: this.wsStatus,
      marketsLoaded: this.marketRows.length,
      cycles: this.cycles.length,
      lastOrderbookReceivedAt: this.lastOrderbookReceivedAt,
      metrics: this.metrics.snapshot(this.wsStatus),
      runtimeConfig: this.runtimeConfig,
      orderbookStores: this.getOrderbookStoreStatus(),
      feedStatus: {
        observation: this.wsStatus,
        validation: this.validationWsStatus,
      },
      strategy: this.getStrategySnapshot(),
      engineState: this.engineState,
    };
  }
}

module.exports = {
  LiveTriangleState,
  parseFeeRate,
  normalizeRestOrderbook,
  toCalculationOrderbook,
};
