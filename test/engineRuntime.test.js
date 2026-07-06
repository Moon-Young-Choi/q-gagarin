const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { buildPerformanceBudgetSnapshot, EngineRuntime } = require("../src/engine/engineRuntime");
const { DEFAULT_RUNTIME_CONFIG } = require("../src/core/runtimeConfig");
const { AppendOnlyLogStore } = require("../src/core/appendOnlyLog");
const { CommandInbox } = require("../src/core/commandInbox");
const { CommandStatusStore } = require("../src/core/commandStatusStore");
const { FillTracker } = require("../src/execution/fillTracker");
const { BalanceTracker } = require("../src/execution/balanceTracker");

test("engine runtime performance budget separates trading and display latency domains", () => {
  const snapshot = buildPerformanceBudgetSnapshot(DEFAULT_RUNTIME_CONFIG);

  assert.deepEqual(snapshot.tradingLatencyDomains, ["marketData", "decision", "execution"]);
  assert.deepEqual(snapshot.ignoredLatencyDomains, ["display"]);
  assert.equal(snapshot.displayLatencyAffectsTrading, false);
  assert.equal(
    snapshot.decision.maxDecisionAgeMs,
    DEFAULT_RUNTIME_CONFIG.executionPolicy.marketDataGuards.maxDecisionAgeMs,
  );
  assert.equal(
    snapshot.execution.maxOrderAckMs,
    DEFAULT_RUNTIME_CONFIG.executionPolicy.executionGuards.maxOrderAckMs,
  );
});

function fakeState() {
  return {
    engineState: "STOPPED",
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    wsStatus: { stopped: true, openConnectionCount: 0, connections: [] },
    validationWsStatus: { stopped: true, openConnectionCount: 0, connections: [] },
    eventLog: [],
    setExecutionHandler() {},
    setRuntimeConfig(config) {
      this.runtimeConfig = config;
    },
    logEvent(type, payload) {
      this.eventLog.push({ type, ...payload });
    },
    getSnapshot() {
      return {
        type: "full-state",
        summary: { marketsLoaded: 0 },
        cycles: [],
        groups: [],
        engineState: this.engineState,
        runtimeConfig: this.runtimeConfig,
        wsStatus: this.wsStatus,
      };
    },
    refreshAgingCycles() {
      return 0;
    },
    consumeDelta(now = new Date()) {
      return {
        type: "delta",
        sentAtEpochMs: now.getTime(),
        changedCycles: [],
        summaryDelta: { marketsLoaded: 0 },
        metrics: {},
      };
    },
    getOrderbookStoreStatus() {
      return {
        observation: { marketCount: 0, staleCount: 0 },
        validation: { marketCount: 0, staleCount: 0 },
      };
    },
  };
}

function fakeWsClient() {
  return {
    starts: 0,
    stops: 0,
    start() {
      this.starts += 1;
    },
    stop() {
      this.stops += 1;
    },
    on() {},
  };
}

function wait(ms = 0) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test("engine runtime rebuilds public feeds after fallback market discovery recovery", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-feed-rebuild-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const commandStatusStore = new CommandStatusStore({ runtimeDir: dir });
  const state = fakeState();
  const createdClients = [];
  await logStore.ensureFiles();

  state.requiredMarkets = [];
  state.shouldUseFallback = () => true;
  state.fallbackPoll = async () => {
    state.requiredMarkets = ["BTC-ETH", "KRW-BTC"];
    return {
      errors: [],
      marketDiscoveryRecovered: true,
    };
  };

  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore,
    state,
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    orderbookClientFactory(markets, clientOptions) {
      const client = fakeWsClient();
      client.markets = markets.slice();
      client.clientOptions = clientOptions;
      createdClients.push(client);
      return client;
    },
    validationFeedStartDelayMs: 0,
    startedAtEpochMs: Date.now() - 1000,
  });

  runtime.createFeedClients();
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";
  runtime.startFeeds();
  await wait(5);

  const initialObservationClient = createdClients[0];
  const initialValidationClient = createdClients[1];
  const result = await runtime.fallbackPoll();
  await wait(5);
  const events = await logStore.readAll("events");

  assert.equal(result.marketDiscoveryRecovered, true);
  assert.equal(createdClients.length, 4);
  assert.deepEqual(initialObservationClient.markets, []);
  assert.deepEqual(initialValidationClient.markets, []);
  assert.equal(initialObservationClient.stops, 1);
  assert.equal(initialValidationClient.stops, 1);
  assert.deepEqual(createdClients[2].markets, ["BTC-ETH", "KRW-BTC"]);
  assert.deepEqual(createdClients[3].markets, ["BTC-ETH", "KRW-BTC"]);
  assert.equal(createdClients[2].starts, 1);
  assert.equal(createdClients[3].starts, 1);
  assert.equal(
    events.some((event) => (
      event.type === "market_data.feeds_rebuilt" &&
      event.previousMarketCount === 0 &&
      event.nextMarketCount === 2 &&
      event.marketDiscoveryRecovered === true
    )),
    true,
  );

  await runtime.stop();
});

test("engine runtime serializes fallback polls and skips feed rebuild after pause", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-fallback-serial-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const state = fakeState();
  const createdClients = [];
  let fallbackCalls = 0;
  let resolveFallback;
  await logStore.ensureFiles();

  state.requiredMarkets = [];
  state.shouldUseFallback = () => true;
  state.fallbackPoll = async () => {
    fallbackCalls += 1;
    return new Promise((resolve) => {
      resolveFallback = () => {
        state.requiredMarkets = ["KRW-BTC"];
        resolve({
          errors: [],
          marketDiscoveryRecovered: true,
        });
      };
    });
  };

  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state,
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    orderbookClientFactory(markets) {
      const client = fakeWsClient();
      client.markets = markets.slice();
      createdClients.push(client);
      return client;
    },
    validationFeedStartDelayMs: 0,
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  const firstPoll = runtime.fallbackPoll();
  const secondPoll = runtime.fallbackPoll();
  assert.equal(fallbackCalls, 1);
  assert.equal(await secondPoll, undefined);

  runtime.machine.state = "PAUSED";
  runtime.state.engineState = "PAUSED";
  resolveFallback();
  const result = await firstPoll;
  const events = await logStore.readAll("events");

  assert.equal(result.marketDiscoveryRecovered, true);
  assert.equal(runtime.fallbackPollInFlight, false);
  assert.equal(createdClients.length, 0);
  assert.equal(events.some((event) => event.type === "market_data.feeds_rebuilt"), false);
});

test("engine runtime skips commands created before process start", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-old-command-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  await logStore.ensureFiles();
  const oldCommand = await logStore.append("commands", {
    type: "cli.command",
    mode: "DRY_RUN",
    engineState: "STOPPED",
    command: "Start",
    commandId: "11111111-1111-4111-8111-111111111111",
    source: "cli",
  });
  const observationClient = fakeWsClient();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    observationClient,
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.parse(oldCommand.timestamp) + 1,
  });

  await runtime.seedProcessedCommands();
  await runtime.processCommands();

  assert.equal(runtime.machine.state, "STOPPED");
  assert.equal(observationClient.starts, 0);
});

test("engine runtime accepts fresh mode-aware commands and writes delta/status", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-command-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const commandStatusStore = new CommandStatusStore({ runtimeDir: dir });
  await logStore.ensureFiles();
  await logStore.append("commands", {
    type: "cli.command",
    mode: "DRY_RUN",
    engineState: "STOPPED",
    command: "Start",
    commandId: "22222222-2222-4222-8222-222222222222",
    runMode: "DRY_RUN",
    source: "cli",
  });
  const observationClient = fakeWsClient();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore,
    state: fakeState(),
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    observationClient,
    validationClient: fakeWsClient(),
    validationFeedStartDelayMs: 0,
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.processCommands();
  const status = await commandStatusStore.read("22222222-2222-4222-8222-222222222222");
  const delta = JSON.parse(await fs.readFile(path.join(dir, "latest-delta.json"), "utf8"));
  const events = await logStore.readAll("events");

  assert.equal(runtime.machine.state, "RUNNING");
  assert.equal(runtime.runtimeConfig.runMode, "DRY_RUN");
  assert.equal(observationClient.starts, 1);
  assert.equal(status.status, "accepted");
  const stateChanged = events.find((event) => event.type === "engine.state_changed" && event.nextState === "RUNNING");
  assert.equal(Boolean(stateChanged), true);
  assert.equal(stateChanged.mode, "DRY_RUN");
  assert.equal(stateChanged.engineState, "RUNNING");
  assert.equal(stateChanged.auditSchema.ok, true);
  assert.equal(delta.type, "delta");
  assert.equal(delta.stateDelta.engineState, "RUNNING");
});

test("engine runtime processes atomic command inbox files once", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-command-inbox-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const commandStatusStore = new CommandStatusStore({ runtimeDir: dir });
  const commandInbox = new CommandInbox({
    runtimeDir: dir,
    randomUUID: () => "77777777-7777-4777-8777-777777777777",
    now: () => new Date("2026-07-06T12:00:00.000Z"),
  });
  await logStore.ensureFiles();
  await commandInbox.enqueue({
    command: "Start",
    runMode: "DRY_RUN",
    source: "cli",
  });
  const observationClient = fakeWsClient();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore,
    commandInbox,
    state: fakeState(),
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    observationClient,
    validationClient: fakeWsClient(),
    validationFeedStartDelayMs: 0,
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.processCommands();
  await runtime.processCommands();
  const status = await commandStatusStore.read("77777777-7777-4777-8777-777777777777");
  const commandAudit = await logStore.readAll("commands");
  const pending = await commandInbox.listPending();

  assert.equal(runtime.machine.state, "RUNNING");
  assert.equal(status.status, "accepted");
  assert.equal(commandAudit.length, 1);
  assert.equal(commandAudit[0].type, "cli.command");
  assert.equal(commandAudit[0].auditSchema.ok, true);
  assert.equal(pending.length, 0);
});

test("engine runtime records readiness failures for rejected real-guarded CLI starts", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-command-readiness-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const commandStatusStore = new CommandStatusStore({ runtimeDir: dir });
  const commandInbox = new CommandInbox({
    runtimeDir: dir,
    randomUUID: () => "88888888-8888-4888-8888-888888888888",
    now: () => new Date("2026-07-06T12:00:00.000Z"),
  });
  await logStore.ensureFiles();
  await commandInbox.enqueue({
    command: "Start",
    runMode: "REAL_GUARDED",
    source: "cli",
  });
  const observationClient = fakeWsClient();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore,
    commandInbox,
    state: fakeState(),
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    observationClient,
    validationClient: fakeWsClient(),
    validationFeedStartDelayMs: 0,
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.processCommands();
  const status = await commandStatusStore.read("88888888-8888-4888-8888-888888888888");
  const events = await logStore.readAll("events");

  assert.equal(runtime.machine.state, "STOPPED");
  assert.equal(observationClient.starts, 0);
  assert.equal(status.status, "rejected");
  assert.match(status.message, /REAL_GUARDED readiness checklist failed/);
  assert.equal(status.readiness.passed, false);
  assert.equal(status.failedItems.length > 0, true);
  assert.equal(events.some((event) => event.type === "readiness.blocked" && event.readiness.passed === false), true);
});

test("engine runtime rejects unsafe queued operator command metadata", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-command-policy-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const commandStatusStore = new CommandStatusStore({ runtimeDir: dir });
  await logStore.ensureFiles();
  await logStore.append("commands", {
    type: "cli.command",
    mode: "DRY_RUN",
    engineState: "STOPPED",
    command: "Pause",
    commandId: "33333333-3333-4333-8333-333333333333",
    runMode: "DRY_RUN",
    source: "cli",
  });
  const observationClient = fakeWsClient();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore,
    state: fakeState(),
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    observationClient,
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.processCommands();
  const status = await commandStatusStore.read("33333333-3333-4333-8333-333333333333");
  const errors = await logStore.readAll("errors");

  assert.equal(runtime.machine.state, "STOPPED");
  assert.equal(observationClient.starts, 0);
  assert.equal(status.status, "rejected");
  assert.match(status.message, /runMode is allowed only with Start/);
  assert.equal(errors.some((entry) => entry.type === "engine.command.rejected"), true);
});

test("engine runtime wires default Upbit REST client to append-only audit log", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-rest-audit-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const runtimeConfig = {
    ...DEFAULT_RUNTIME_CONFIG,
    liveTradingEnabled: true,
    runMode: "REAL_GUARDED",
  };
  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig,
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.now() - 1000,
  });

  assert.equal(runtime.restClient.logStore, logStore);
  assert.equal(runtime.restClient.mode, "REAL");
  assert.equal(runtime.realExecutor.restClient, runtime.restClient);
});

test("engine runtime gates REAL_AUTO start with readiness checks", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-real-auto-gate-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_AUTO",
      liveTradingEnabled: true,
    },
    restClient: {
      async checkPermissions() {
        return {
          viewAccounts: false,
          viewOrdersChance: false,
          errors: [{ permission: "test", message: "not ready" }],
        };
      },
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.now() - 1000,
  });

  await assert.rejects(
    () => runtime.applyCommand("Start", { source: "operator" }),
    /REAL_AUTO readiness checklist failed/,
  );
  const events = await logStore.readAll("events");

  assert.equal(runtime.machine.state, "STOPPED");
  assert.equal(events.some((event) => event.type === "readiness.blocked" && event.readiness.passed === false), true);
});

test("engine runtime applies configured dry-run readiness guard thresholds", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-readiness-guards-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const previousAccessKey = process.env.UPBIT_ACCESS_KEY;
  const previousSecretKey = process.env.UPBIT_SECRET_KEY;
  const state = fakeState();

  await logStore.ensureFiles();
  await logStore.append("decisions", {
    type: "strategy-decision",
    mode: "DRY_RUN",
    accepted: true,
    startAsset: "KRW",
    strategyId: "depthAwareLimitIoc",
    cycleId: "cycle-krw",
    expectedNetProfit: 10,
  });
  await logStore.append("decisions", {
    type: "strategy-decision",
    mode: "DRY_RUN",
    accepted: true,
    startAsset: "BTC",
    strategyId: "depthAwareLimitIoc",
    cycleId: "cycle-btc",
    expectedNetProfit: 10,
  });
  await logStore.append("events", {
    type: "cycle.done",
    mode: "DRY_RUN",
    engineState: "RUNNING",
    startAsset: "KRW",
    strategyId: "depthAwareLimitIoc",
    cycleId: "cycle-krw",
    expectedNetProfit: 10,
    pnl: 9,
  });
  await logStore.append("events", {
    type: "cycle.done",
    mode: "DRY_RUN",
    engineState: "RUNNING",
    startAsset: "BTC",
    strategyId: "depthAwareLimitIoc",
    cycleId: "cycle-btc",
    expectedNetProfit: 10,
    pnl: 9,
  });

  state.getSnapshot = () => ({
    type: "full-state",
    summary: { marketsLoaded: 2 },
    cycles: [],
    groups: [],
    engineState: state.engineState,
    runtimeConfig: state.runtimeConfig,
    feedStatus: {
      observation: { openConnectionCount: 1 },
      validation: { openConnectionCount: 1 },
    },
    orderbookStores: {
      validation: { staleCount: 0 },
    },
  });

  process.env.UPBIT_ACCESS_KEY = "test-access";
  process.env.UPBIT_SECRET_KEY = "test-secret";

  try {
    const runtime = new EngineRuntime({
      runtimeDir: dir,
      logStore,
      commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
      state,
      runtimeConfig: {
        ...DEFAULT_RUNTIME_CONFIG,
        runMode: "REAL_GUARDED",
        liveTradingEnabled: true,
        enabledStartAssets: ["KRW", "BTC"],
        executionPolicy: {
          ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
          readinessGuards: {
            ...DEFAULT_RUNTIME_CONFIG.executionPolicy.readinessGuards,
            minimumDryRunSamples: 2,
            minimumDryRunSamplesPerStartAsset: 2,
            minimumDryRunCompleteRate: 0.75,
            maxDryRunExpectedSimulatedGapRate: 0.25,
          },
        },
      },
      restClient: {
        async checkPermissions() {
          return {
            viewAccounts: true,
            viewOrdersChance: true,
            accounts: [
              { currency: "KRW", balance: "100000", locked: "0" },
              { currency: "BTC", balance: "0.01", locked: "0" },
            ],
            errors: [],
          };
        },
      },
      observationClient: fakeWsClient(),
      validationClient: fakeWsClient(),
      startedAtEpochMs: Date.now() - 1000,
    });
    runtime.privateWsStatus = { status: "open", stopped: false };

    const readiness = await runtime.checkReadiness();
    const events = await logStore.readAll("events");
    const checked = events.find((event) => event.type === "readiness.checked");

    assert.equal(readiness.passed, false);
    assert.equal(readiness.items.some((item) => item.id === "dry-run-sample-count" && item.passed === true), true);
    assert.equal(checked.score.failed, readiness.score.failed);
    assert.equal(checked.failedCount, readiness.score.failed);
    assert.equal(
      readiness.items.some((item) => (
        item.id === "dry-run-start-asset-sample-count-KRW" &&
        item.passed === false &&
        item.required === 2
      )),
      true,
    );
    assert.equal(
      readiness.items.some((item) => item.id === "dry-run-start-asset-sample-count-BTC" && item.passed === false),
      true,
    );
  } finally {
    if (previousAccessKey === undefined) {
      delete process.env.UPBIT_ACCESS_KEY;
    } else {
      process.env.UPBIT_ACCESS_KEY = previousAccessKey;
    }

    if (previousSecretKey === undefined) {
      delete process.env.UPBIT_SECRET_KEY;
    } else {
      process.env.UPBIT_SECRET_KEY = previousSecretKey;
    }
  }
});

test("engine runtime refreshes fee policies for required markets and injects live state", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-fee-policies-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const state = {
    ...fakeState(),
    requiredMarkets: ["KRW-BTC", "BTC-ETH"],
    injectedFeePolicies: null,
    setFeePolicyByMarket(feePolicyByMarket) {
      this.injectedFeePolicies = feePolicyByMarket;
    },
  };
  const chanceCalls = [];

  await logStore.ensureFiles();

  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state,
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    restClient: {
      async checkPermissions() {
        return {
          viewAccounts: false,
          viewOrdersChance: true,
          errors: [],
        };
      },
      async getOrderChance(market) {
        chanceCalls.push(market);
        return {
          market: {
            id: market,
            bid: { minTotal: market === "KRW-BTC" ? "5000" : "0.00005" },
            ask: { minTotal: market === "KRW-BTC" ? "6000" : "0.00006" },
          },
          bidFee: market === "KRW-BTC" ? 0.0005 : 0.0007,
          askFee: market === "KRW-BTC" ? 0.0004 : 0.0008,
          makerBidFee: 0.0002,
          makerAskFee: 0.0001,
        };
      },
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.refreshPrivateCaches();
  const snapshot = runtime.snapshot();

  assert.deepEqual(chanceCalls, ["BTC-ETH", "KRW-BTC"]);
  assert.equal(runtime.isOrderChanceFresh(), true);
  assert.equal(runtime.feePolicyByMarket.size, 2);
  assert.equal(runtime.feePolicyByMarket.get("KRW-BTC").bidFee, 0.0005);
  assert.equal(runtime.marketPolicyByMarket.size, 2);
  assert.equal(runtime.marketPolicyByMarket.get("KRW-BTC").bid.minTotal, 5000);
  assert.equal(runtime.marketPolicyByMarket.get("KRW-BTC").ask.minTotal, 6000);
  assert.equal(state.injectedFeePolicies instanceof Map, true);
  assert.equal(state.injectedFeePolicies.get("BTC-ETH").askFee, 0.0008);
  assert.equal(snapshot.privateCacheStatus.feePolicyMarketCount, 2);
  assert.equal(snapshot.privateCacheStatus.feePolicyRequiredMarketCount, 2);
  assert.equal(snapshot.privateCacheStatus.feePolicyByMarket["KRW-BTC"].source, "orders/chance");
  assert.equal(snapshot.privateCacheStatus.marketPolicyMarketCount, 2);
  assert.equal(snapshot.privateCacheStatus.marketPolicyByMarket["KRW-BTC"].bidMinTotal, 5000);
  assert.deepEqual(snapshot.privateCacheStatus.feePolicyLoadErrors, []);
});

test("engine runtime marks order chance stale when any required market policy is missing", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-fee-policy-missing-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const state = {
    ...fakeState(),
    requiredMarkets: ["KRW-BTC"],
  };

  await logStore.ensureFiles();

  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state,
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    restClient: {
      async checkPermissions() {
        return {
          viewAccounts: false,
          viewOrdersChance: true,
          errors: [],
        };
      },
      async getOrderChance(market) {
        if (market === "BTC-ETH") {
          throw new Error("orders/chance unavailable");
        }

        return {
          market: {
            id: market,
            bid: { minTotal: "5000" },
            ask: { minTotal: "5000" },
          },
          bidFee: 0.0005,
          askFee: 0.0004,
          makerBidFee: 0.0002,
          makerAskFee: 0.0001,
        };
      },
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.refreshPrivateCaches();
  assert.equal(runtime.isOrderChanceFresh(), true);

  state.requiredMarkets = ["KRW-BTC", "BTC-ETH"];
  await runtime.refreshPrivateCaches();
  const snapshot = runtime.snapshot();

  assert.equal(runtime.isOrderChanceFresh(), false);
  assert.equal(snapshot.privateCacheStatus.orderChanceFresh, false);
  assert.equal(snapshot.privateCacheStatus.orderChanceTimestampFresh, true);
  assert.deepEqual(snapshot.privateCacheStatus.missingFeePolicyMarkets, ["BTC-ETH"]);
  assert.deepEqual(snapshot.privateCacheStatus.missingMarketPolicyMarkets, ["BTC-ETH"]);
  assert.deepEqual(snapshot.privateCacheStatus.feePolicyLoadErrorMarkets, ["BTC-ETH"]);
  assert.deepEqual(snapshot.privateCacheStatus.orderChanceFreshness.loadErrorMarkets, ["BTC-ETH"]);
});

test("engine runtime treats incomplete order chance fee policies as stale", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-fee-policy-incomplete-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });

  await logStore.ensureFiles();

  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: {
      ...fakeState(),
      requiredMarkets: ["KRW-BTC"],
    },
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    feePolicyByMarket: new Map([
      ["KRW-BTC", {
        market: "KRW-BTC",
        bidFee: null,
        askFee: 0.0004,
        makerBidFee: 0.0002,
        makerAskFee: 0.0001,
        source: "orders/chance",
        loadedAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      }],
    ]),
    marketPolicyByMarket: new Map([
      ["KRW-BTC", {
        market: "KRW-BTC",
        quoteAsset: "KRW",
        baseAsset: "BTC",
        bid: { minTotal: 5000 },
        ask: { minTotal: 5000 },
        source: "orders/chance",
      }],
    ]),
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.orderChanceCacheUpdatedAt = Date.now();

  const snapshot = runtime.snapshot();

  assert.equal(runtime.isOrderChanceFresh(), false);
  assert.equal(snapshot.privateCacheStatus.orderChanceTimestampFresh, true);
  assert.deepEqual(snapshot.privateCacheStatus.incompleteFeePolicyMarkets, ["KRW-BTC"]);
  assert.deepEqual(snapshot.privateCacheStatus.missingFeePolicyMarkets, []);
});

test("engine runtime passes cached market policies to real executor", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-market-policy-context-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  let suppliedPolicy = null;
  let suppliedAvailableBalances = null;
  let suppliedLockedBalances = null;

  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_GUARDED",
      liveTradingEnabled: true,
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    balanceTracker: new BalanceTracker({
      accounts: [
        { currency: "KRW", balance: "123", locked: "7" },
      ],
    }),
    marketPolicyByMarket: new Map([
      ["KRW-BTC", {
        market: "KRW-BTC",
        quoteAsset: "KRW",
        baseAsset: "BTC",
        bid: { minTotal: 12345 },
        ask: { minTotal: 23456 },
        source: "orders/chance",
      }],
    ]),
    realExecutor: {
      async execute(plan, context) {
        suppliedPolicy = context.getMarketPolicy("KRW-BTC");
        suppliedAvailableBalances = context.availableBalances;
        suppliedLockedBalances = context.lockedBalances;
        return {
          ok: false,
          mode: "REAL",
          reason: "TEST_STOP",
          planId: plan.planId,
          cycleId: plan.cycleId,
          startAsset: plan.startAsset,
        };
      },
    },
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  const result = await runtime.handleExecutionCandidate({
    planId: "plan-market-policy-context",
    cycleId: "cycle-market-policy-context",
    startAsset: "KRW",
    cycle: { cycleId: "cycle-market-policy-context", startAsset: "KRW" },
  });

  assert.equal(result.reason, "TEST_STOP");
  assert.equal(suppliedPolicy.market, "KRW-BTC");
  assert.equal(suppliedPolicy.bid.minTotal, 12345);
  assert.equal(suppliedPolicy.source, "orders/chance");
  assert.equal(suppliedAvailableBalances.KRW, 123);
  assert.equal(suppliedLockedBalances.KRW, 7);
});

test("engine runtime pause blocks new executions without stopping order management", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-pause-orders-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const observationClient = fakeWsClient();
  const validationClient = fakeWsClient();
  const privateWsClient = fakeWsClient();
  const cancelCalls = [];
  const fillTracker = new FillTracker({
    logStore,
    mode: "REAL",
  });
  await logStore.ensureFiles();
  fillTracker.upsertOrder({
    uuid: "uuid-paused-open",
    identifier: "id-paused-open",
    market: "KRW-BTC",
    state: "wait",
    cycleId: "cycle-paused",
    startAsset: "KRW",
  });
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    observationClient,
    validationClient,
    privateWsClient,
    fillTracker,
    realExecutor: {
      orderManager: {
        async cancelOpenOrders(openOrders, metadata) {
          cancelCalls.push({ openOrders, metadata });
          return [];
        },
      },
    },
    validationFeedStartDelayMs: 0,
    startedAtEpochMs: Date.now() - 1000,
  });

  await runtime.applyCommand("Start", { source: "operator", runMode: "DRY_RUN" });
  await runtime.applyCommand("Pause", { source: "operator" });
  const result = await runtime.handleExecutionCandidate({
    planId: "plan-paused",
    cycleId: "cycle-paused",
    startAsset: "KRW",
    cycle: { cycleId: "cycle-paused", startAsset: "KRW" },
  });
  const snapshot = runtime.snapshot();

  assert.equal(runtime.machine.state, "PAUSED");
  assert.equal(result, null);
  assert.equal(cancelCalls.length, 0);
  assert.equal(observationClient.starts, 1);
  assert.equal(observationClient.stops, 0);
  assert.equal(validationClient.stops, 0);
  assert.equal(privateWsClient.starts, 1);
  assert.equal(privateWsClient.stops, 0);
  assert.equal(snapshot.engine.canAcceptNewOpportunity, false);
  assert.equal(snapshot.engine.canSubmitFirstLegOrder, false);
  assert.equal(snapshot.engine.shouldContinueOrderManagement, true);
  assert.equal(fillTracker.openOrders().length, 1);

  await runtime.stop();
});

test("engine runtime applies stop policy by cancelling tracked open orders", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-stop-cancel-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const cancelCalls = [];
  const fillTracker = new FillTracker({
    logStore,
    mode: "REAL",
  });
  await logStore.ensureFiles();
  fillTracker.upsertOrder({
    uuid: "uuid-open",
    identifier: "id-open",
    market: "KRW-BTC",
    state: "wait",
    cycleId: "cycle-open",
    startAsset: "KRW",
  });
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      executionPolicy: {
        ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
        stopPolicy: "CANCEL_OPEN_ORDERS",
      },
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    fillTracker,
    realExecutor: {
      orderManager: {
        async cancelOpenOrders(openOrders, metadata) {
          cancelCalls.push({ openOrders, metadata });
          return openOrders.map((order) => ({ ok: true, order }));
        },
      },
    },
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  await runtime.applyCommand("Stop", { source: "operator" });

  assert.equal(runtime.machine.state, "STOPPED");
  assert.equal(cancelCalls.length, 1);
  assert.equal(cancelCalls[0].openOrders.length, 1);
  assert.equal(cancelCalls[0].openOrders[0].uuid, "uuid-open");
  assert.equal(cancelCalls[0].metadata.stopPolicy, "CANCEL_OPEN_ORDERS");
  assert.equal(cancelCalls[0].metadata.source, "operator");
});

test("engine runtime applies emergency stop policy by cancelling tracked open orders", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-emergency-cancel-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const cancelCalls = [];
  const fillTracker = new FillTracker({
    logStore,
    mode: "REAL",
  });
  await logStore.ensureFiles();
  fillTracker.upsertOrder({
    uuid: "uuid-emergency-open",
    identifier: "id-emergency-open",
    market: "KRW-BTC",
    state: "wait",
    cycleId: "cycle-emergency",
    startAsset: "KRW",
  });
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_GUARDED",
      liveTradingEnabled: true,
      executionPolicy: {
        ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
        stopPolicy: "CANCEL_OPEN_ORDERS",
      },
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    fillTracker,
    realExecutor: {
      orderManager: {
        async cancelOpenOrders(openOrders, metadata) {
          cancelCalls.push({ openOrders, metadata });
          return openOrders.map((order) => ({ ok: true, order }));
        },
      },
    },
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  const stop = await runtime.activateEmergencyStop("MAX_DAILY_LOSS", {
    source: "real-run-limits",
    mode: "REAL",
  });
  const events = await logStore.readAll("events");
  const orders = await logStore.readAll("orders");
  const triggered = events.find((event) => event.type === "emergency_stop.triggered");

  assert.equal(stop.active, true);
  assert.equal(runtime.machine.state, "ERROR");
  assert.equal(cancelCalls.length, 1);
  assert.equal(cancelCalls[0].openOrders.length, 1);
  assert.equal(cancelCalls[0].openOrders[0].uuid, "uuid-emergency-open");
  assert.equal(cancelCalls[0].metadata.command, "EmergencyStop");
  assert.equal(cancelCalls[0].metadata.emergency, true);
  assert.equal(cancelCalls[0].metadata.emergencyStopReason, "MAX_DAILY_LOSS");
  assert.equal(orders.some((event) => event.type === "order.cancel_intent"), true);
  assert.equal(triggered.stopOrderPolicy.intentCount, 1);
  assert.equal(triggered.stopOrderPolicy.cancelAttemptCount, 1);
  assert.equal(triggered.stopOrderPolicy.cancelOkCount, 1);
  assert.equal(triggered.stopOrderPolicy.cancelFailedCount, 0);
});

test("engine runtime triggers emergency stop when private WS disconnects during active execution", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-private-ws-stop-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const privateWsClient = {
    handlers: {},
    on(event, handler) {
      this.handlers[event] = handler;
    },
    start() {},
    stop() {},
  };

  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_GUARDED",
      liveTradingEnabled: true,
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    privateWsClient,
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";
  runtime.activeRealExecutionCount = 1;
  runtime.createFeedClients();

  privateWsClient.handlers.status({ status: "closed", stopped: false });
  await wait(20);
  const events = await logStore.readAll("events");
  const triggered = events.find((event) => event.type === "emergency_stop.triggered");

  assert.equal(runtime.machine.state, "ERROR");
  assert.equal(runtime.emergencyStop.active, true);
  assert.equal(runtime.emergencyStop.reason, "PRIVATE_WS_DISCONNECT_DURING_ACTIVE_EXECUTION");
  assert.equal(triggered.reason, "PRIVATE_WS_DISCONNECT_DURING_ACTIVE_EXECUTION");
  assert.equal(triggered.details.source, "private-ws");
  assert.equal(triggered.details.activeRealExecutionCount, 1);
});

test("engine runtime triggers emergency stop when realized daily loss reaches limit", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-loss-stop-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const runtimeConfig = {
    ...DEFAULT_RUNTIME_CONFIG,
    runMode: "REAL_GUARDED",
    liveTradingEnabled: true,
    executionPolicy: {
      ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
      realRunLimits: {
        ...DEFAULT_RUNTIME_CONFIG.executionPolicy.realRunLimits,
        maxDailyLossByAsset: {
          ...DEFAULT_RUNTIME_CONFIG.executionPolicy.realRunLimits.maxDailyLossByAsset,
          KRW: 5,
        },
      },
    },
  };
  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig,
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    realExecutor: {
      async execute() {
        return {
          ok: true,
          mode: "REAL",
          planId: "plan-loss",
          cycleId: "cycle-loss",
          startAsset: "KRW",
          startAmount: 10,
          outputAmount: 4,
          pnl: -6,
          feeSummary: { totalPaidFee: 2, totalTradeFee: 2.5, legs: 3 },
          legResults: [
            { legIndex: 1, market: "KRW-BTC", paidFee: 1 },
            { legIndex: 2, market: "BTC-ETH", paidFee: 0.5 },
            { legIndex: 3, market: "KRW-ETH", paidFee: 0.5 },
          ],
        };
      },
    },
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  const result = await runtime.handleExecutionCandidate({
    planId: "plan-loss",
    cycleId: "cycle-loss",
    startAsset: "KRW",
    startAmount: 10,
    cycle: { cycleId: "cycle-loss", startAsset: "KRW" },
  });
  const snapshot = runtime.snapshot();
  const events = await logStore.readAll("events");
  const realized = events.find((event) => event.type === "pnl.realized");

  assert.equal(result.ok, true);
  assert.equal(realized.startAsset, "KRW");
  assert.equal(realized.accountingAsset, "KRW");
  assert.equal(realized.startAmount, 10);
  assert.equal(realized.outputAmount, 4);
  assert.equal(realized.pnl, -6);
  assert.equal(realized.realizedLoss, 6);
  assert.equal(realized.feeSummary.totalPaidFee, 2);
  assert.equal(realized.legResults.length, 3);
  assert.equal(realized.mode, "REAL");
  assert.equal(realized.engineState, "RUNNING");
  assert.equal(realized.strategyId, runtimeConfig.activeStrategyId);
  assert.equal(realized.auditSchema.ok, true);
  assert.equal(runtime.machine.state, "ERROR");
  assert.equal(snapshot.emergencyStop.active, true);
  assert.equal(snapshot.emergencyStop.reason, "MAX_DAILY_LOSS");
  assert.equal(snapshot.realRunLimits.dailyLossByAsset.KRW, 6);
  assert.equal(snapshot.realRunLimits.recentResults[0].feeSummary.totalTradeFee, 2.5);

  await runtime.applyCommand("Stop", { source: "operator" });
  assert.equal(runtime.machine.state, "STOPPED");
  assert.equal(runtime.emergencyStop.active, false);
});

test("engine runtime records residual assets from failed real execution", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-residual-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  const balanceTracker = new BalanceTracker({
    accounts: [
      { currency: "KRW", balance: "100000", locked: "0" },
    ],
  });
  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_GUARDED",
      liveTradingEnabled: true,
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    balanceTracker,
    realExecutor: {
      async execute() {
        return {
          ok: false,
          mode: "REAL",
          reason: "PARTIAL_FILL_ABORTED_BY_POLICY",
          planId: "plan-residual",
          cycleId: "cycle-residual",
          startAsset: "KRW",
          residualAsset: "KRW",
          residualAmount: 2500,
          actualAmount: 0.001,
          legIndex: 1,
          legResults: [
            {
              legIndex: 1,
              market: "KRW-BTC",
              fromAsset: "KRW",
              toAsset: "BTC",
              residualAsset: "KRW",
              residualAmount: 2500,
              outputAmount: 0.001,
              isPartial: true,
            },
          ],
          feeSummary: { totalPaidFee: 0, totalTradeFee: 0, legs: 1 },
        };
      },
    },
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  const result = await runtime.handleExecutionCandidate({
    planId: "plan-residual",
    cycleId: "cycle-residual",
    startAsset: "KRW",
    strategyId: "depthAwareLimitIoc",
    cycle: { cycleId: "cycle-residual", startAsset: "KRW" },
  });
  const snapshot = runtime.snapshot();
  const events = await logStore.readAll("events");
  const residualEvents = events.filter((event) => event.type === "position.residual_recorded");

  assert.equal(result.ok, false);
  assert.equal(snapshot.execution.realBalances.residualBalances.KRW, 2500);
  assert.equal(snapshot.execution.realBalances.residualBalances.BTC, 0.001);
  assert.equal(snapshot.execution.realBalances.residualEvents.length, 2);
  assert.equal(residualEvents.length, 2);
  assert.equal(residualEvents.some((event) => event.asset === "KRW" && event.amount === 2500), true);
  assert.equal(residualEvents.some((event) => event.asset === "BTC" && event.amount === 0.001), true);
  assert.equal(residualEvents.every((event) => event.mode === "REAL"), true);
  assert.equal(residualEvents.every((event) => event.engineState === "RUNNING"), true);
  assert.equal(residualEvents.every((event) => event.strategyId === "depthAwareLimitIoc"), true);
  assert.equal(residualEvents.every((event) => event.auditSchema.ok), true);
});

test("engine runtime refuses real execution before injected executor when live trading is disabled", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-live-disabled-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  let executeCalls = 0;
  await logStore.ensureFiles();
  const runtime = new EngineRuntime({
    runtimeDir: dir,
    logStore,
    commandStatusStore: new CommandStatusStore({ runtimeDir: dir }),
    state: fakeState(),
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_GUARDED",
      liveTradingEnabled: false,
    },
    observationClient: fakeWsClient(),
    validationClient: fakeWsClient(),
    realExecutor: {
      async execute() {
        executeCalls += 1;
        return { ok: true };
      },
    },
    startedAtEpochMs: Date.now() - 1000,
  });
  runtime.machine.state = "RUNNING";
  runtime.state.engineState = "RUNNING";

  const result = await runtime.handleExecutionCandidate({
    planId: "plan-disabled",
    cycleId: "cycle-disabled",
    startAsset: "KRW",
    cycle: { cycleId: "cycle-disabled", startAsset: "KRW" },
  });
  const errors = await logStore.readAll("errors");

  assert.equal(result.ok, false);
  assert.equal(result.reason, "LIVE_TRADING_DISABLED");
  assert.equal(executeCalls, 0);
  assert.equal(errors.some((entry) => entry.type === "real_execution_refused" && entry.reason === "LIVE_TRADING_DISABLED"), true);
});
