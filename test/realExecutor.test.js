const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { RealExecutor } = require("../src/execution/realExecutor");
const { AppendOnlyLogStore } = require("../src/core/appendOnlyLog");

const cycle = {
  cycleId: "BTC|ETH|KRW:canonical:KRW",
  startAsset: "KRW",
  steps: [
    { index: 0, fromAsset: "KRW", toAsset: "BTC", market: "KRW-BTC" },
    { index: 1, fromAsset: "BTC", toAsset: "KRW", market: "KRW-BTC" },
  ],
  markets: ["KRW-BTC"],
};

function orderbook() {
  return {
    market: "KRW-BTC",
    timestamp: 1000,
    orderbook_units: [{ ask_price: 100, bid_price: 90, ask_size: 10, bid_size: 10 }],
  };
}

function orderbookWithPrices(askPrice, bidPrice) {
  return {
    market: "KRW-BTC",
    timestamp: Date.now(),
    orderbook_units: [{ ask_price: askPrice, bid_price: bidPrice, ask_size: 10, bid_size: 10 }],
  };
}

function memoryLogStore(events) {
  return {
    append(kind, payload) {
      events.push({ kind, ...payload });
      return Promise.resolve(payload);
    },
  };
}

function zeroMinMarketPolicy() {
  return {
    market: {
      id: "KRW-BTC",
      bid: { min_total: "0" },
      ask: { min_total: "0" },
      min_total: "0",
    },
  };
}

async function readUntil(logStore, kind, predicate, attempts = 20) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const rows = await logStore.readAll(kind, { limit: 100 });
    if (predicate(rows)) return rows;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  return logStore.readAll(kind, { limit: 100 });
}

test("real executor refuses to run unless live trading is enabled", async () => {
  const executor = new RealExecutor({
    liveTradingEnabled: false,
    restClient: {},
  });

  await assert.rejects(() => executor.execute({}), /liveTradingEnabled=false/);
});

test("real executor submits limit IOC at observed best price", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order, executed_volume: order.volume || "1", avg_price: order.price || "90" };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier) || submitted.at(-1);
        return { ...order, uuid: params.uuid, executed_volume: order.volume || "1", avg_price: order.price || "90" };
      },
    },
  });
  const result = await executor.execute({
    planId: "real-1",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(submitted[0].ord_type, "limit");
  assert.equal(submitted[0].time_in_force, "ioc");
  assert.equal(submitted[0].price, "100");
  assert.equal(submitted[1].side, "ask");
  assert.equal(submitted[1].price, "90");
  assert.equal(events.some((event) => event.type === "cycle.done" && event.legacyType === "cycle.real_done"), true);
  assert.equal(events.some((event) => event.type === "cycle.real_done" && event.canonicalType === "cycle.done"), true);
});

test("real executor writes schema-complete audit events through append-only log", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-real-audit-"));
  const logStore = new AppendOnlyLogStore({ logDir: dir });
  const submitted = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore,
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return {
          uuid: `uuid-audit-${submitted.length}`,
          ...order,
          executed_volume: order.volume || "1",
          avg_price: order.price || "90",
          state: "done",
        };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier) || submitted.at(-1);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "90",
          paid_fee: "0",
          trade_fee: "0",
          state: "done",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-audit-1",
    cycle,
    startAmount: 100,
    strategyId: "depthAwareBestIoc",
    engineState: "RUNNING",
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });
  const orders = await readUntil(logStore, "orders", (rows) => rows.some((event) => event.type === "cycle.done"));
  const fills = await readUntil(logStore, "fills", (rows) => rows.some((event) => event.type === "order.fill"));
  const cycleDone = orders.find((event) => event.type === "cycle.done");
  const orderAck = orders.find((event) => event.type === "order.ack");
  const orderSubmitted = orders.find((event) => event.type === "order.submitted");
  const fill = fills.find((event) => event.type === "order.fill");

  assert.equal(result.ok, true);
  assert.equal(cycleDone.auditSchema.ok, true);
  assert.equal(cycleDone.startAmount, 100);
  assert.equal(cycleDone.legResults.length, 2);
  assert.equal(orderSubmitted.auditSchema.ok, true);
  assert.equal(orderAck.auditSchema.ok, true);
  assert.equal(fill.auditSchema.ok, true);
  assert.equal(fill.engineState, "RUNNING");
  assert.equal(fill.strategyId, "depthAwareBestIoc");
});

test("real executor follows dry-run guards when private WS is disconnected", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-private-ws-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier) || submitted.at(-1);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "90",
          paid_fee: "0",
          trade_fee: "0",
          state: "done",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-private-ws-required",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: false,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(submitted.length, 2);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "PRIVATE_WS_DISCONNECTED"), false);
});

test("real executor does not apply real-only decision age before submitting the first order", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {
          maxDecisionAgeMs: 100,
        },
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-stale-first-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier) || submitted.at(-1);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "90",
          paid_fee: "0",
          trade_fee: "0",
          state: "done",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-stale-before-first-order",
    cycle,
    startAmount: 100,
    decisionAgeMs: 0,
    nowMs: Date.now() - 1000,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(submitted.length, 2);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "DECISION_STALE"), false);
});

test("real executor does not apply real-only decision age before subsequent legs", async () => {
  const submitted = [];
  const events = [];
  const baseNowMs = Date.now();
  let guardCalls = 0;
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {
          maxDecisionAgeMs: 100,
        },
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return {
          uuid: `uuid-stale-leg-${submitted.length}`,
          ...order,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "90",
          paid_fee: "0",
          trade_fee: "0",
          state: "done",
        };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier) || submitted.at(-1);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "90",
          paid_fee: "0",
          trade_fee: "0",
          state: "done",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-stale-before-second-order",
    cycle,
    startAmount: 100,
    decisionAgeMs: 0,
    nowMs: baseNowMs,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    getGuardContext() {
      guardCalls += 1;
      return {
        privateWsConnected: true,
        orderChanceFresh: true,
        accountBalanceFresh: true,
        validationDepthFresh: true,
        nowMs: guardCalls >= 3 ? baseNowMs + 200 : baseNowMs,
      };
    },
  });

  assert.equal(result.ok, true);
  assert.equal(submitted.length, 2);
  assert.equal(events.some((event) => event.type === "order.rejected" && event.legIndex === 2 && event.rejectionReason === "DECISION_STALE"), false);
  assert.equal(events.some((event) => event.type === "execution.state_changed" && event.executionState === "CYCLE_RESIDUAL"), false);
});

test("real executor records order submit failures as cycle aborts", async () => {
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {
          orderRateLimitPerSecond: 8,
        },
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder() {
        const error = new Error("insufficient funds");
        error.response = {
          status: 400,
          data: {
            error: {
              name: "insufficient_funds_bid",
              message: "Insufficient bid balance",
            },
          },
        };
        throw error;
      },
    },
  });

  const result = await executor.execute({
    planId: "real-submit-failure",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "ORDER_SUBMIT_FAILED");
  assert.equal(result.residualAsset, "KRW");
  assert.equal(events.some((event) => event.type === "order.submit_failed" && event.error.code === "insufficient_funds_bid"), true);
  assert.equal(events.some((event) => event.type === "order.rejected" && event.rejectionReason === "ORDER_SUBMIT_FAILED"), true);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "ORDER_SUBMIT_FAILED"), true);
  assert.equal(events.some((event) => event.type === "execution.state_changed" && event.executionState === "CYCLE_ABORTED"), true);
});

test("real executor rejects plans that exceed available start-asset balance", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: "unexpected", ...order };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-balance-guard",
    cycle,
    startAsset: "KRW",
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
    availableBalances: { KRW: 50 },
    lockedBalances: { KRW: 10 },
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "BALANCE_INSUFFICIENT");
  assert.equal(submitted.length, 0);
  assert.equal(events.some((event) => event.type === "order.rejected" && event.rejectionReason === "BALANCE_INSUFFICIENT"), true);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "BALANCE_INSUFFICIENT"), true);
});

test("real executor can build BEST_IOC orders only when configured by caller", () => {
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    runtimeConfig: {
      executionMode: "BEST_IOC",
      executionPolicy: {},
    },
    restClient: {},
  });
  const buy = executor.buildOrderForLeg(cycle.steps[0], 100, orderbook(), "id-1");
  const sell = executor.buildOrderForLeg(cycle.steps[1], 1, orderbook(), "id-2");

  assert.equal(buy.ord_type, "best");
  assert.equal(buy.side, "bid");
  assert.equal(buy.price, "100");
  assert.equal(sell.ord_type, "best");
  assert.equal(sell.side, "ask");
  assert.equal(sell.volume, "1");
});

test("real executor normalizes limit prices to Upbit price units", () => {
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      executionPolicy: {},
    },
    restClient: {},
  });
  const buy = executor.buildOrderForLeg(cycle.steps[0], 1000000, orderbookWithPrices(123456, 123456), "id-1");
  const sell = executor.buildOrderForLeg(cycle.steps[1], 1, orderbookWithPrices(123456, 123456), "id-2");

  assert.equal(buy.price, "123400");
  assert.equal(buy.priceUnit, 100);
  assert.equal(buy.priceWasRounded, true);
  assert.equal(sell.price, "123500");
  assert.equal(sell.priceUnit, 100);
  assert.equal(sell.priceWasRounded, true);
});

test("real executor caps orders to configured best-level touch ratio", () => {
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: {
        maxTouchRatioPerBestLevel: 0.25,
        minResidualRatioPerBestLevel: 0.2,
      },
      executionPolicy: {},
    },
    restClient: {},
  });
  const buy = executor.buildOrderForLeg(cycle.steps[0], 1000, orderbook(), "id-1");
  const sell = executor.buildOrderForLeg(cycle.steps[1], 10, orderbook(), "id-2");

  assert.equal(buy.volume, "2.5");
  assert.equal(buy.bestLevelTouchRatio, 0.25);
  assert.equal(buy.maxBestLevelTouchRatio, 0.25);
  assert.equal(buy.minBestLevelResidualRatio, 0.2);
  assert.equal(buy.liquidityCapped, true);
  assert.equal(buy.unsubmittedInputAmount, 750);
  assert.equal(sell.volume, "2.5");
  assert.equal(sell.bestLevelTouchRatio, 0.25);
  assert.equal(sell.unsubmittedInputAmount, 7.5);
});

test("real executor keeps bid order notional inside all-in fee budget", () => {
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      executionPolicy: {},
    },
    restClient: {},
  });
  const buy = executor.buildOrderForLeg(cycle.steps[0], 100, orderbook(), "id-fee", null, null, {
    feeRate: 0.0005,
  });

  assert.equal(buy.side, "bid");
  assert.equal(buy.volume, "0.999500249875");
  assert.equal(Number(buy.submittedInputAmount.toFixed(12)), 99.950024987506);
  assert.equal(Number(buy.expectedFeeAmount.toFixed(12)), 0.049975012494);
  assert.equal(Number(buy.submittedAllInInputAmount.toFixed(12)), 100);
  assert.equal(buy.expectedFeeAsset, "KRW");
});

test("real executor rejects orders below market minimum total before submission", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async createOrder(order) {
        submitted.push(order);
        return { uuid: "unexpected", ...order };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-min-total",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "MIN_ORDER_TOTAL");
  assert.equal(result.minTotal, 5000);
  assert.equal(submitted.length, 0);
  assert.equal(events.some((event) => event.type === "order.rejected" && event.rejectionReason === "MIN_ORDER_TOTAL"), true);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "MIN_ORDER_TOTAL"), true);
});

test("real executor rejects orders above market maximum total before submission", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async createOrder(order) {
        submitted.push(order);
        return { uuid: "unexpected", ...order };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-max-total",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
    getMarketPolicy() {
      return {
        market: {
          id: "KRW-BTC",
          bid: { min_total: "0", max_total: "50" },
          ask: { min_total: "0", max_total: "50" },
        },
      };
    },
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "MAX_ORDER_TOTAL");
  assert.equal(result.orderTotal, 100);
  assert.equal(result.maxTotal, 50);
  assert.equal(submitted.length, 0);
  assert.equal(events.some((event) => event.type === "order.rejected" && event.rejectionReason === "MAX_ORDER_TOTAL"), true);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "MAX_ORDER_TOTAL"), true);
});

test("real executor prices each leg from latest validation orderbooks", async () => {
  const submitted = [];
  const events = [];
  const validationSnapshots = [
    new Map([["KRW-BTC", orderbookWithPrices(100, 90)]]),
    new Map([["KRW-BTC", orderbookWithPrices(105, 80)]]),
  ];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        return { ...order, uuid: params.uuid, executed_volume: order.volume || "1", avg_price: order.price || "80" };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-latest-leg-price",
    cycle,
    startAmount: 100,
    validationOrderbooks: validationSnapshots[0],
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
    getValidationOrderbooks() {
      return validationSnapshots.shift() || validationSnapshots[0];
    },
  });

  assert.equal(result.ok, true);
  assert.equal(submitted[0].price, "100");
  assert.equal(submitted[1].price, "80");
  assert.equal(events.some((event) => event.type === "cycle.aborted"), false);
});

test("real executor continues through deteriorated leg pricing", async () => {
  const submitted = [];
  const events = [];
  const deepOrderbook = (askPrice, bidPrice) => ({
    market: "KRW-BTC",
    timestamp: Date.now(),
    orderbook_units: [{ ask_price: askPrice, bid_price: bidPrice, ask_size: 1000, bid_size: 1000 }],
  });
  const validationSnapshots = [
    new Map([["KRW-BTC", deepOrderbook(100, 100)]]),
    new Map([["KRW-BTC", deepOrderbook(100, 80)]]),
  ];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        return { ...order, uuid: params.uuid, executed_volume: order.volume || "1", avg_price: order.price || "80" };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-no-recover",
    cycle,
    startAmount: 10000,
    expectedOutputAmount: 11000,
    validationOrderbooks: validationSnapshots[0],
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
    getValidationOrderbooks() {
      return validationSnapshots.shift() || validationSnapshots[0];
    },
  });

  assert.equal(result.ok, true);
  assert.equal(submitted.length, 2);
  assert.equal(submitted[1].side, "ask");
  assert.equal(submitted[1].price, "80");
  assert.equal(events.some((event) => event.type === "cycle.aborted"), false);
});

test("real executor records residual input when liquidity policy caps the submitted order", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: {
        minOrderAmountByAsset: { BTC: 0, KRW: 0 },
        maxTouchRatioPerBestLevel: 0.25,
        minResidualRatioPerBestLevel: 0,
      },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume,
          remaining_volume: "0",
          avg_price: order.price,
          paid_fee: "0",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-liquidity-cap",
    cycle,
    startAmount: 1000,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(submitted[0].volume, "2.5");
  assert.equal(submitted[0].liquidityCapped, true);
  assert.equal(submitted[0].unsubmittedInputAmount, 750);
  assert.equal(submitted[1].volume, "2.5");
  assert.equal(result.legResults[0].isLiquidityCapped, true);
  assert.equal(result.legResults[0].hasResidual, true);
  assert.equal(result.legResults[0].residualAsset, "KRW");
  assert.equal(result.legResults[0].residualAmount, 750);
  assert.equal(result.legResults[0].unsubmittedInputAmount, 750);
  assert.equal(result.legResults[0].bestLevelTouchRatio, 0.25);
  assert.equal(events.some((event) => event.type === "execution.liquidity_capped" && event.unsubmittedInputAmount === 750), true);
  assert.equal(events.some((event) => event.kind === "fills" && event.type === "order.fill" && event.residualAmount === 750), true);
});

test("real executor records IOC partial fills and continues with actual filled amount", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        if (params.uuid === "uuid-1") {
          return {
            ...order,
            uuid: params.uuid,
            executed_volume: "0.5",
            remaining_volume: "0.5",
            avg_price: "100",
            paid_fee: "0",
          };
        }

        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume,
          remaining_volume: "0",
          avg_price: order.price,
          paid_fee: "0",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-partial",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(submitted[0].volume, "1");
  assert.equal(submitted[1].volume, "0.5");
  assert.equal(events.some((event) => event.type === "order.partial" && event.legIndex === 1), true);
  assert.equal(events.some((event) => event.executionState === "LEG_1_PARTIAL"), true);
});

test("real executor returns leg fill summaries and fee totals", async () => {
  const submitted = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        if (params.uuid === "uuid-1") {
          return {
            ...order,
            uuid: params.uuid,
            executed_volume: "1",
            remaining_volume: "0",
            avg_price: "100",
            paid_fee: "0.01",
            trade_fee: "0.011",
          };
        }

        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume,
          remaining_volume: "0",
          avg_price: order.price,
          paid_fee: "1",
          trade_fee: "1.1",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-fee-summary",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(result.legResults.length, 2);
  assert.equal(result.legResults[0].paidFee, 0.01);
  assert.equal(result.legResults[0].tradeFee, 0.011);
  assert.equal(result.legResults[0].outputAmount, 1);
  assert.equal(result.legResults[0].feeAsset, "KRW");
  assert.equal(result.feeSummary.legs, 2);
  assert.equal(result.feeSummary.totalPaidFee, 1.01);
  assert.equal(result.feeSummary.totalTradeFee.toFixed(3), "1.111");
  assert.equal(result.feeSummary.totalByAsset.KRW, 1.01);
});

test("real executor records latency but does not stop before next leg when latency exceeds budget", async () => {
  const submissions = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {
          maxOrderAckMs: 1,
          maxReconciliationMs: 3000,
        },
      },
    },
    restClient: {},
    marketPolicyProvider: zeroMinMarketPolicy,
    orderManager: {
      createIdentifier({ legIndex }) {
        return `id-${legIndex}`;
      },
      async submitOrder(order) {
        submissions.push(order);
        return {
          identifier: order.identifier,
          order,
          ack: { uuid: `uuid-${submissions.length}`, identifier: order.identifier },
          orderSubmitStartPerfNs: "1000000",
          orderAckPerfNs: "4000000",
        };
      },
      async reconcileSubmittedOrder({ orderAck, identifier }) {
        return {
          order: {
            uuid: orderAck.uuid,
            identifier,
            executed_volume: "1",
            remaining_volume: "0",
            avg_price: "100",
            paid_fee: "0",
          },
          source: "rest-query",
          reconciliationStartedPerfNs: "5000000",
          reconciliationDonePerfNs: "6000000",
          orderQueryDonePerfNs: "6000000",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-slow-ack",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, true);
  assert.equal(submissions.length, 2);
  assert.equal(events.some((event) => event.type === "risk.rejected" && event.reason === "ORDER_ACK_LATENCY"), false);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "ORDER_ACK_LATENCY"), false);
  assert.equal(result.legResults[0].latencySummary.execution.orderAckMs.valueMs, 3);
});

test("real executor can abort partial fills by policy", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        partialFillPolicy: "ABORT_ON_PARTIAL",
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: "0.5",
          remaining_volume: "0.5",
          avg_price: "100",
          paid_fee: "0",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-partial-abort",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "PARTIAL_FILL_ABORTED_BY_POLICY");
  assert.equal(submitted.length, 1);
  assert.equal(events.some((event) => event.executionState === "CYCLE_RESIDUAL"), true);
  assert.equal(events.some((event) => event.type === "cycle.aborted" && event.reason === "PARTIAL_FILL_ABORTED_BY_POLICY"), true);
  assert.equal(events.some((event) => event.type === "cycle.real_fail" && event.canonicalType === "cycle.aborted"), true);
});

test("real executor requests balance refresh after fills and cycle completion", async () => {
  const submitted = [];
  const refreshes = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        partialFillPolicy: "CONTINUE_IF_ABOVE_MIN",
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "100",
          paid_fee: "0",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-balance-refresh",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
    refreshAccountBalances(metadata) {
      refreshes.push(metadata);
      return Promise.resolve({ ok: true });
    },
  });

  assert.equal(result.ok, true);
  assert.equal(submitted.length, 2);
  assert.equal(refreshes.filter((entry) => entry.reason === "ORDER_FILL").length, 2);
  assert.equal(refreshes.some((entry) => entry.reason === "CYCLE_DONE" && entry.force === true), true);
});

test("real executor ignores balance refresh failures", async () => {
  const submitted = [];
  const events = [];
  const executor = new RealExecutor({
    liveTradingEnabled: true,
    logStore: memoryLogStore(events),
    runtimeConfig: {
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      candidateValidation: { minOrderAmountByAsset: { BTC: 0, KRW: 0 } },
      executionPolicy: {
        partialFillPolicy: "CONTINUE_IF_ABOVE_MIN",
        realRunLimits: {},
        marketDataGuards: {},
        executionGuards: {},
      },
    },
    restClient: {
      async getOrderChance() {
        return zeroMinMarketPolicy();
      },
      async createOrder(order) {
        submitted.push(order);
        return { uuid: `uuid-${submitted.length}`, ...order };
      },
      async getOrder(params) {
        const order = submitted.find((item) => item.identifier === params.identifier);
        return {
          ...order,
          uuid: params.uuid,
          executed_volume: order.volume || "1",
          remaining_volume: "0",
          avg_price: order.price || "100",
          paid_fee: "0",
        };
      },
    },
  });

  const result = await executor.execute({
    planId: "real-balance-refresh-fails",
    cycle,
    startAmount: 100,
    validationOrderbooks: new Map([["KRW-BTC", orderbook()]]),
  }, {
    privateWsConnected: true,
    orderChanceFresh: true,
    accountBalanceFresh: true,
    validationDepthFresh: true,
    refreshAccountBalances() {
      throw new Error("accounts unavailable");
    },
  });

  assert.equal(result.ok, true);
  assert.equal(submitted.length, 2);
  assert.equal(events.some((event) => event.type === "balance.refresh_failed" && event.message === "accounts unavailable"), true);
});
