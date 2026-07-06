const test = require("node:test");
const assert = require("node:assert/strict");
const { LiveTriangleState } = require("../src/live/liveState");
const { DEFAULT_RUNTIME_CONFIG } = require("../src/core/runtimeConfig");

function memoryLogStore() {
  const records = {
    events: [],
    decisions: [],
    market: [],
    orders: [],
    fills: [],
    errors: [],
    commands: [],
  };

  return {
    records,
    append(kind, payload) {
      records[kind].push(payload);
      return Promise.resolve(payload);
    },
  };
}

function orderbook(market, units, metadata = {}) {
  return {
    market,
    timestamp: metadata.timestamp ?? 1000,
    exchangeTimestampMs: metadata.exchangeTimestampMs ?? metadata.timestamp ?? 1000,
    receivedAt: metadata.receivedAt ?? 1000,
    serverReceivedAtMs: metadata.serverReceivedAtMs ?? metadata.receivedAt ?? 1000,
    streamType: metadata.streamType || "TEST",
    orderbookUnit: metadata.orderbookUnit ?? 30,
    unit: metadata.unit ?? metadata.orderbookUnit ?? 30,
    orderbookLevel: metadata.orderbookLevel ?? 0,
    orderbook_units: units,
  };
}

function profitableCycle() {
  return {
    triangleId: "KRW|BTC|ETH",
    cycleId: "KRW|BTC|ETH:canonical:KRW",
    legacyCycleId: "KRW-BTC-ETH",
    routeVariantId: "KRW|BTC|ETH:canonical:KRW",
    startAsset: "KRW",
    endAsset: "KRW",
    direction: "canonical",
    directionLabel: "Canonical",
    group: "KRW_START",
    groupLabel: "KRW Start",
    groupIndex: 0,
    allHub: false,
    baseX: 1,
    x: 1,
    xOffset: 0,
    markerSymbol: "circle",
    triangleAssets: ["KRW", "BTC", "ETH"],
    route: ["KRW", "BTC", "ETH", "KRW"],
    routeLabel: "KRW -> BTC -> ETH -> KRW",
    markets: ["KRW-BTC", "BTC-ETH", "KRW-ETH"],
    steps: [
      { index: 0, fromAsset: "KRW", toAsset: "BTC", market: "KRW-BTC" },
      { index: 1, fromAsset: "BTC", toAsset: "ETH", market: "BTC-ETH" },
      { index: 2, fromAsset: "ETH", toAsset: "KRW", market: "KRW-ETH" },
    ],
  };
}

function orderbooks({ thinFirstLeg = false, metadata = {} } = {}) {
  return new Map([
    ["KRW-BTC", orderbook("KRW-BTC", [
      { ask_price: 1000000, bid_price: 999000, ask_size: thinFirstLeg ? 0.000001 : 1, bid_size: 1 },
    ], metadata["KRW-BTC"] || metadata)],
    ["BTC-ETH", orderbook("BTC-ETH", [
      { ask_price: 0.1, bid_price: 0.09, ask_size: 10, bid_size: 10 },
    ], metadata["BTC-ETH"] || metadata)],
    ["KRW-ETH", orderbook("KRW-ETH", [
      { ask_price: 90000, bid_price: 120000, ask_size: 10, bid_size: 10 },
    ], metadata["KRW-ETH"] || metadata)],
  ]);
}

function runtimeConfig(overrides = {}) {
  return {
    ...DEFAULT_RUNTIME_CONFIG,
    runMode: "DRY_RUN",
    activeStrategyId: "depthAwareBestIoc",
    ...overrides,
    candidateValidation: {
      ...DEFAULT_RUNTIME_CONFIG.candidateValidation,
      startAmountByAsset: { KRW: 10000 },
      minOrderAmountByAsset: { KRW: 5000 },
      maxTouchRatioPerBestLevel: 0.3,
      minResidualRatioPerBestLevel: 0,
      minResidualAbsoluteByAsset: { KRW: 0, BTC: 0, USDT: 0 },
      minNetProfitRate: 0,
      ...(overrides.candidateValidation || {}),
    },
  };
}

function stateWithCycle(options = {}) {
  const cycle = profitableCycle();
  const logStore = memoryLogStore();
  const state = new LiveTriangleState({
    engineState: options.engineState || "RUNNING",
    logStore,
    runtimeConfig: runtimeConfig(options.runtimeConfig || {}),
    feeRate: 0,
    staleOrderbookMs: 5000,
    executionHandler: options.executionHandler,
  });
  state.cycles = [cycle];
  state.cycleIndex = new Map([[cycle.cycleId, cycle]]);
  state.marketToCycleIds = state.buildMarketToCycleIds();

  return { state, logStore, cycle };
}

function installOrderbooks(state, books = orderbooks()) {
  for (const book of books.values()) {
    state.observationStore.update(book, book.receivedAt || Date.now());
    state.validationStore.update(book, book.receivedAt || Date.now());
  }
}

test("live state degrades instead of throwing when Upbit market discovery fails", async () => {
  const logStore = memoryLogStore();
  const state = new LiveTriangleState({
    logStore,
    fetchMarkets: async () => {
      throw new Error("timeout of 15000ms exceeded");
    },
    fetchOrderbooks: async () => {
      throw new Error("orderbooks should not be fetched without markets");
    },
  });

  const initialized = await state.initialize();
  const orderbooks = await state.loadInitialOrderbooks();
  const health = state.getHealth();
  const snapshot = state.getSnapshot();

  assert.equal(initialized.ok, false);
  assert.match(initialized.error.message, /timeout/);
  assert.equal(orderbooks.requestedMarketCount, 0);
  assert.equal(health.ok, true);
  assert.equal(health.degraded, true);
  assert.equal(health.marketDataStatus.initialized, false);
  assert.match(health.marketDataStatus.initializationError.message, /timeout/);
  assert.equal(snapshot.summary.marketsLoaded, 0);
  assert.equal(snapshot.summary.requiredMarketCount, 0);
  assert.match(snapshot.summary.marketDiscoveryError.message, /timeout/);
  assert.equal(snapshot.cycles.length, 0);
  assert.equal(state.eventLog.some((event) => event.type === "market.initialize_failed"), true);
});

test("live state retries market discovery during fallback polling", async () => {
  const logStore = memoryLogStore();
  const requestedMarketBatches = [];
  let marketDiscoveryAttempts = 0;
  const state = new LiveTriangleState({
    logStore,
    runtimeConfig: runtimeConfig(),
    fetchMarkets: async () => {
      marketDiscoveryAttempts += 1;

      if (marketDiscoveryAttempts === 1) {
        throw new Error("temporary market discovery outage");
      }

      return ["KRW-BTC", "BTC-ETH", "KRW-ETH"];
    },
    fetchOrderbooks: async (markets) => {
      requestedMarketBatches.push(markets.slice());
      return {
        orderbookMap: orderbooks(),
        errors: [],
        requestedMarketCount: markets.length,
        fetchedMarketCount: markets.length,
      };
    },
  });

  const initial = await state.initialize();
  const recovered = await state.fallbackPoll({ markDirty: false });

  assert.equal(initial.ok, false);
  assert.equal(recovered.marketDiscoveryRecovered, true);
  assert.equal(marketDiscoveryAttempts, 2);
  assert.equal(state.initializationError, null);
  assert.deepEqual(state.requiredMarkets, ["BTC-ETH", "KRW-BTC", "KRW-ETH"]);
  assert.deepEqual(requestedMarketBatches, [["BTC-ETH", "KRW-BTC", "KRW-ETH"]]);
  assert.equal(state.cycles.length > 0, true);
  assert.equal(state.observationStore.get("KRW-BTC").streamType, "REST");
  assert.equal(state.validationStore.get("KRW-BTC").orderbookUnit, 30);
  assert.equal(state.eventLog.some((event) => event.type === "market.initialize_recovered"), true);
});

test("live scanner logs candidate strategy and execution plan audit events", async () => {
  const handledPlans = [];
  const { state, logStore, cycle } = stateWithCycle({
    executionHandler(plan) {
      handledPlans.push(plan);
    },
  });

  const row = state.recalculateCycle(cycle.cycleId, {
    nowMs: 1000,
    calculationOrderbooks: orderbooks(),
    validationOrderbooks: orderbooks(),
  });
  await Promise.resolve();
  const eventTypes = logStore.records.events.map((event) => event.type);

  assert.equal(row.strategyAccepted, true);
  assert.equal(handledPlans.length, 1);
  assert.equal(eventTypes.includes("candidate.detected"), true);
  assert.equal(eventTypes.includes("candidate.validated"), true);
  assert.equal(eventTypes.includes("strategy.accepted"), true);
  assert.equal(eventTypes.includes("execution.plan_created"), true);
  assert.equal(logStore.records.decisions.length, 1);
  assert.equal(logStore.records.decisions[0].type, "strategy-decision");

  const planEvent = logStore.records.events.find((event) => event.type === "execution.plan_created");
  assert.equal(planEvent.mode, "DRY_RUN");
  assert.equal(planEvent.startAsset, "KRW");
  assert.equal(planEvent.strategyId, "depthAwareLimitIoc");
  assert.equal(planEvent.validationStatus, "accepted");
  assert.equal(planEvent.excludeFromDryRunSummary, true);
});

test("live scheduler coalesces duplicate dirty cycles and respects batch limits", async () => {
  const { state, cycle } = stateWithCycle({
    runtimeConfig: {
      activeStrategyId: "topOfBookBaseline",
    },
  });
  const secondCycle = {
    ...cycle,
    cycleId: "KRW|BTC|ETH:reverse:KRW",
    routeVariantId: "KRW|BTC|ETH:reverse:KRW",
    direction: "reverse",
  };
  state.cycles = [cycle, secondCycle];
  state.cycleIndex = new Map(state.cycles.map((item) => [item.cycleId, item]));
  state.cycleRows = new Map(state.cycles.map((item) => [item.cycleId, {
    cycleId: item.cycleId,
    status: "warming",
    validationStatus: "pending",
  }]));
  installOrderbooks(state);

  state.queueCycleRecalculation([cycle.cycleId, cycle.cycleId, secondCycle.cycleId], {
    reason: "test",
  });
  state.stopScheduler();
  assert.equal(state.pendingCycleIds.size, 2);

  const first = await state.processPendingCycles({ maxCycles: 1, maxTickMs: 1000 });
  assert.equal(first.processed, 1);
  assert.equal(first.remaining, 1);
  assert.equal(state.pendingCycleIds.size, 1);

  const second = await state.processPendingCycles({ maxCycles: 10, maxTickMs: 1000 });
  assert.equal(second.processed, 1);
  assert.equal(second.remaining, 0);
  assert.equal(state.getSchedulerStatus().completedCycleCount, 2);
});

test("bounded warm-up suppresses detailed rejected decision logs and emits summary", async () => {
  const { state, logStore, cycle } = stateWithCycle();
  installOrderbooks(state, orderbooks({ thinFirstLeg: true }));

  state.queueCycleRecalculation([cycle.cycleId], {
    reason: "warm-up",
    warmup: true,
  });
  state.stopScheduler();
  await state.processPendingCycles({ maxCycles: 10, maxTickMs: 1000 });

  const eventTypes = logStore.records.events.map((event) => event.type);
  assert.equal(eventTypes.includes("candidate.detected"), false);
  assert.equal(eventTypes.includes("candidate.validated"), false);
  assert.equal(eventTypes.includes("strategy.rejected"), false);
  assert.equal(logStore.records.decisions.length, 0);
  assert.equal(eventTypes.includes("strategy-decision-summary"), true);
  assert.equal(logStore.records.events.find((event) => event.type === "strategy-decision-summary").rejectedCount, 1);
});

test("live snapshot exposes compact cycle rows and scheduler progress", async () => {
  const { state, cycle } = stateWithCycle();
  installOrderbooks(state);
  await state.processPendingCycles({ maxCycles: 10, maxTickMs: 1000 });
  state.recalculateCycle(cycle.cycleId, {
    nowMs: 1000,
    calculationOrderbooks: orderbooks(),
    validationOrderbooks: orderbooks(),
    logDecision: false,
  });

  const snapshot = state.getSnapshot(new Date("2026-07-06T00:00:00.000Z"));

  assert.equal(snapshot.cycles.length, 1);
  assert.equal(snapshot.summary.cycleScheduler.totalCycleCount, 1);
  assert.equal(snapshot.summary.cycleScheduler.completedCycleCount, 1);
  assert.equal(snapshot.cycles[0].cycleId, cycle.cycleId);
  assert.equal(Object.hasOwn(snapshot.cycles[0], "validationOrderbookMetadata"), false);
  assert.equal(Object.hasOwn(snapshot.cycles[0], "history"), false);
});

test("live scanner applies market and side fee policies to depth validation and plans", async () => {
  const handledPlans = [];
  const { state, cycle } = stateWithCycle({
    executionHandler(plan) {
      handledPlans.push(plan);
    },
  });
  state.setFeePolicyByMarket(new Map([
    ["KRW-BTC", { bidFee: 0.01, askFee: 0.04, makerBidFee: 0, makerAskFee: 0 }],
    ["BTC-ETH", { bidFee: 0.02, askFee: 0.04, makerBidFee: 0, makerAskFee: 0 }],
    ["KRW-ETH", { bidFee: 0.04, askFee: 0.03, makerBidFee: 0, makerAskFee: 0 }],
  ]));

  const row = state.recalculateCycle(cycle.cycleId, {
    nowMs: 1000,
    calculationOrderbooks: orderbooks(),
    validationOrderbooks: orderbooks(),
  });
  await Promise.resolve();

  assert.equal(row.strategyAccepted, true);
  assert.deepEqual(row.depthLegs.map((leg) => leg.feeSide), ["bid", "bid", "ask"]);
  assert.deepEqual(row.depthLegs.map((leg) => leg.feeRate), [0.01, 0.02, 0.03]);
  assert.equal(handledPlans.length, 1);
  assert.equal(handledPlans[0].feePolicyByMarket instanceof Map, true);
  assert.equal(handledPlans[0].feePolicyByMarket.get("KRW-BTC").bidFee, 0.01);
});

test("live scanner logs risk and strategy rejection audit events without execution plan", async () => {
  const handledPlans = [];
  const { state, logStore, cycle } = stateWithCycle({
    executionHandler(plan) {
      handledPlans.push(plan);
    },
  });

  const row = state.recalculateCycle(cycle.cycleId, {
    nowMs: 1000,
    calculationOrderbooks: orderbooks(),
    validationOrderbooks: orderbooks({ thinFirstLeg: true }),
  });
  const eventTypes = logStore.records.events.map((event) => event.type);

  assert.equal(row.strategyAccepted, false);
  assert.equal(row.validationReason, "DEPTH_INSUFFICIENT");
  assert.equal(handledPlans.length, 0);
  assert.equal(eventTypes.includes("candidate.detected"), true);
  assert.equal(eventTypes.includes("candidate.validated"), true);
  assert.equal(eventTypes.includes("risk.rejected"), true);
  assert.equal(eventTypes.includes("strategy.rejected"), true);
  assert.equal(eventTypes.includes("execution.plan_created"), false);

  const riskEvent = logStore.records.events.find((event) => event.type === "risk.rejected");
  assert.equal(riskEvent.reason, "DEPTH_INSUFFICIENT");
  assert.equal(riskEvent.cycleId, cycle.cycleId);
  assert.equal(riskEvent.excludeFromDryRunSummary, true);
});

test("live scanner lets strategy contract block plans when baseline accepts but depth rejects", async () => {
  const handledPlans = [];
  const { state, logStore, cycle } = stateWithCycle({
    runtimeConfig: {
      activeStrategyId: "topOfBookBaseline",
    },
    executionHandler(plan) {
      handledPlans.push(plan);
    },
  });

  const row = state.recalculateCycle(cycle.cycleId, {
    nowMs: 1000,
    calculationOrderbooks: orderbooks(),
    validationOrderbooks: orderbooks({ thinFirstLeg: true }),
  });
  const eventTypes = logStore.records.events.map((event) => event.type);

  assert.equal(row.strategyAccepted, true);
  assert.equal(row.strategyId, "topOfBookBaseline");
  assert.equal(row.validationReason, "DEPTH_INSUFFICIENT");
  assert.equal(row.executionFeasibility, "DEPTH_INSUFFICIENT");
  assert.equal(handledPlans.length, 0);
  assert.equal(eventTypes.includes("strategy.accepted"), true);
  assert.equal(eventTypes.includes("execution.plan_created"), false);
});

test("live scanner rejects execution plans when observation and validation snapshots diverge", async () => {
  const handledPlans = [];
  const { state, logStore, cycle } = stateWithCycle({
    executionHandler(plan) {
      handledPlans.push(plan);
    },
  });

  const row = state.recalculateCycle(cycle.cycleId, {
    nowMs: 1600,
    calculationOrderbooks: orderbooks({ metadata: { timestamp: 1000, receivedAt: 1000 } }),
    validationOrderbooks: orderbooks({ metadata: { timestamp: 1000, receivedAt: 1600 } }),
  });

  assert.equal(row.strategyAccepted, false);
  assert.equal(row.validationReason, "OBSERVATION_VALIDATION_SNAPSHOT_GAP");
  assert.equal(row.observationValidationGapMs, 600);
  assert.equal(row.observationValidationGapMarket, "KRW-BTC");
  assert.equal(handledPlans.length, 0);

  const riskEvent = logStore.records.events.find((event) => event.type === "risk.rejected");
  assert.equal(riskEvent.reason, "OBSERVATION_VALIDATION_SNAPSHOT_GAP");
  assert.equal(riskEvent.observationValidationGapMs, 600);
});

test("live state records replayable market orderbook update audit events", () => {
  const { state, logStore } = stateWithCycle();

  state.updateObservationOrderbook(orderbook("KRW-BTC", [
    { ask_price: 100, bid_price: 99, ask_size: 1, bid_size: 2 },
    { ask_price: 101, bid_price: 98, ask_size: 3, bid_size: 4 },
  ]));
  state.updateValidationOrderbook(orderbook("KRW-BTC", [
    { ask_price: 100, bid_price: 99, ask_size: 1, bid_size: 2 },
    { ask_price: 101, bid_price: 98, ask_size: 3, bid_size: 4 },
  ]));

  assert.equal(logStore.records.market.length, 2);

  const observation = logStore.records.market.find((event) => event.feedName === "observation");
  const validation = logStore.records.market.find((event) => event.feedName === "validation");

  assert.equal(observation.type, "market.orderbook_update");
  assert.equal(observation.mode, "DRY_RUN");
  assert.equal(observation.engineState, "RUNNING");
  assert.equal(observation.market, "KRW-BTC");
  assert.equal(observation.unit, 5);
  assert.equal(observation.traceId, "observation:KRW-BTC:1000:1");
  assert.equal(observation.exchangeTimestampMs, 1000);
  assert.equal(observation.serverReceivedAtMs, 1000);
  assert.equal(observation.orderbook_units.length, 1);
  assert.equal(observation.payload.orderbookUnitCount, 2);
  assert.equal(observation.payload.loggedOrderbookUnitCount, 1);
  assert.equal(observation.payload.marketLogMode, "compact");
  assert.equal(observation.excludeFromDryRunSummary, true);
  assert.equal(validation.unit, 30);
  assert.equal(validation.traceId, "validation:KRW-BTC:1000:1");
});

test("live state samples repeated market orderbook audit logs per feed and market", () => {
  const { state, logStore } = stateWithCycle();

  state.updateObservationOrderbook(orderbook("KRW-BTC", [
    { ask_price: 100, bid_price: 99, ask_size: 1, bid_size: 2 },
  ], { receivedAt: 1000, timestamp: 1000 }));
  state.updateObservationOrderbook(orderbook("KRW-BTC", [
    { ask_price: 101, bid_price: 98, ask_size: 1, bid_size: 2 },
  ], { receivedAt: 1001, timestamp: 1001 }));
  state.updateObservationOrderbook(orderbook("KRW-BTC", [
    { ask_price: 102, bid_price: 97, ask_size: 1, bid_size: 2 },
  ], { receivedAt: 7000, timestamp: 7000 }));

  assert.equal(logStore.records.market.length, 2);
  assert.equal(state.pendingDirtyMarkets.size, 1);
  assert.equal(state.pendingCycleIds.has("KRW|BTC|ETH:canonical:KRW"), true);
});
