const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { LiveTriangleState, parseFeeRate } = require("../live/liveState");
const { UpbitWsOrderbookClient } = require("../exchanges/upbit/publicWsOrderbookClient");
const { freezeRuntimeConfig, loadRuntimeConfig, RUN_MODES } = require("../core/runtimeConfig");
const { AppendOnlyLogStore } = require("../core/appendOnlyLog");
const { CommandInbox } = require("../core/commandInbox");
const { CommandStatusStore } = require("../core/commandStatusStore");
const { RunStateMachine, STATES, normalizeCommand } = require("../core/runStateMachine");
const { UpbitPrivateWsClient } = require("../exchanges/upbit/privateWsClient");
const { UpbitExchangeRestClient } = require("../exchanges/upbit/exchangeRestClient");
const { UpbitRateLimitScheduler } = require("../exchanges/upbit/rateLimitScheduler");
const {
  hasCompleteFeePolicy,
  isFeePolicyExpired,
  normalizeFeePolicy,
} = require("../exchanges/upbit/feeModel");
const {
  hasCompleteMarketPolicy,
  policyForMarket,
} = require("../exchanges/upbit/marketPolicy");
const { FillTracker } = require("../execution/fillTracker");
const { DryRunExecutor } = require("../execution/dryRunExecutor");
const { RealExecutor } = require("../execution/realExecutor");
const { RiskGuard } = require("../execution/riskGuards");
const { BalanceTracker } = require("../execution/balanceTracker");
const { RealRunLimits } = require("../execution/realRunLimits");
const { EmergencyStop } = require("../execution/emergencyStop");
const { executionLogMode } = require("../execution/executionPlan");
const { checkRealRunReadiness } = require("../execution/readinessCheck");
const { normalizeQueuedCommandRecord, validateCommandMetadata } = require("../core/commandPolicy");
const {
  DISPLAY_LATENCY_DOMAIN,
  TRADING_LATENCY_DOMAINS,
} = require("../core/performanceBudget");

async function writeJsonAtomic(filePath, payload) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tmpPath = `${filePath}.${process.pid}.${crypto.randomUUID()}.tmp`;
  await fs.writeFile(tmpPath, `${JSON.stringify(payload, null, 2)}\n`);
  await fs.rename(tmpPath, filePath);
}

function summarizeStopOrderPolicyResult(result = {}) {
  const intentEvents = result.intentEvents || [];
  const cancelResults = result.cancelResults || [];

  return {
    intentCount: intentEvents.length,
    cancelAttemptCount: cancelResults.length,
    cancelOkCount: cancelResults.filter((entry) => entry && entry.ok).length,
    cancelFailedCount: cancelResults.filter((entry) => entry && !entry.ok).length,
  };
}

function positiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function feePolicyMapToObject(feePolicyByMarket) {
  return Object.fromEntries(
    [...feePolicyByMarket.entries()].map(([market, policy]) => [market, {
      market: policy.market || market,
      bidFee: policy.bidFee,
      askFee: policy.askFee,
      makerBidFee: policy.makerBidFee,
      makerAskFee: policy.makerAskFee,
      source: policy.source,
      loadedAt: policy.loadedAt,
      expiresAt: policy.expiresAt,
    }]),
  );
}

function marketPolicyMapToObject(marketPolicyByMarket) {
  return Object.fromEntries(
    [...marketPolicyByMarket.entries()].map(([market, policy]) => [market, {
      market: policy.market || market,
      quoteAsset: policy.quoteAsset,
      baseAsset: policy.baseAsset,
      bidMinTotal: policy.bid && policy.bid.minTotal,
      bidMaxTotal: policy.bid && policy.bid.maxTotal,
      askMinTotal: policy.ask && policy.ask.minTotal,
      askMaxTotal: policy.ask && policy.ask.maxTotal,
      minTotal: policy.minTotal,
      maxTotal: policy.maxTotal,
      priceUnit: policy.priceUnit,
      source: policy.source,
      state: policy.state,
    }]),
  );
}

function buildPerformanceBudgetSnapshot(runtimeConfig = {}) {
  const executionPolicy = runtimeConfig.executionPolicy || {};
  const executionGuards = executionPolicy.executionGuards || {};
  const marketDataGuards = executionPolicy.marketDataGuards || {};

  return {
    marketData: marketDataGuards,
    decision: {
      maxDecisionAgeMs: marketDataGuards.maxDecisionAgeMs,
    },
    execution: {
      maxOrderAckMs: executionGuards.maxOrderAckMs,
      maxReconciliationMs: executionGuards.maxReconciliationMs,
    },
    tradingLatencyDomains: TRADING_LATENCY_DOMAINS.slice(),
    ignoredLatencyDomains: [DISPLAY_LATENCY_DOMAIN],
    displayLatencyAffectsTrading: false,
  };
}

class EngineRuntime {
  constructor(options = {}) {
    this.runtimeDir = options.runtimeDir || path.resolve(process.cwd(), "out", "runtime");
    this.snapshotPath = options.snapshotPath || path.join(this.runtimeDir, "latest-snapshot.json");
    this.deltaPath = options.deltaPath || path.join(this.runtimeDir, "latest-delta.json");
    this.commandPollIntervalMs = options.commandPollIntervalMs || 500;
    this.snapshotIntervalMs = options.snapshotIntervalMs ||
      Number.parseInt(process.env.FULL_SNAPSHOT_INTERVAL_MS || "10000", 10);
    this.deltaIntervalMs = options.deltaIntervalMs ||
      Number.parseInt(process.env.UI_DELTA_INTERVAL_MS || "250", 10);
    this.agingSweepIntervalMs = options.agingSweepIntervalMs ||
      Number.parseInt(process.env.AGING_SWEEP_INTERVAL_MS || "1000", 10);
    this.logStore = options.logStore || new AppendOnlyLogStore({
      logDir: options.logDir || path.resolve(process.cwd(), "out", "logs"),
    });
    this.commandStatusStore = options.commandStatusStore || new CommandStatusStore({
      runtimeDir: this.runtimeDir,
    });
    this.commandInbox = options.commandInbox || new CommandInbox({
      runtimeDir: this.runtimeDir,
    });
    this.runtimeConfig = options.runtimeConfig || loadRuntimeConfig({
      configPath: options.runtimeConfigPath,
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    this.rateLimitScheduler = options.rateLimitScheduler || new UpbitRateLimitScheduler({
      orderReservationTtlMs: Number.parseInt(process.env.UPBIT_ORDER_RESERVATION_TTL_MS || "3000", 10),
    });
    this.restClient = options.restClient || (
      this.runtimeConfig.liveTradingEnabled || (process.env.UPBIT_ACCESS_KEY && process.env.UPBIT_SECRET_KEY)
        ? new UpbitExchangeRestClient({
            liveTradingEnabled: this.runtimeConfig.liveTradingEnabled,
            chanceTtlMs: this.runtimeConfig.executionPolicy.executionGuards.orderChanceTtlMs,
            logStore: this.logStore,
            mode: String(this.runtimeConfig.runMode || "").startsWith("REAL") ? "REAL" : this.runtimeConfig.runMode,
            scheduler: this.rateLimitScheduler,
          })
        : null
    );
    this.state = options.state || new LiveTriangleState({
      feeRate: parseFeeRate(process.env.UPBIT_TAKER_FEE_RATE, 0),
      staleOrderbookMs: options.staleOrderbookMs || Number.parseInt(process.env.STALE_ORDERBOOK_MS || "3000", 10),
      runtimeConfig: this.runtimeConfig,
      logStore: this.logStore,
      rateLimitScheduler: this.rateLimitScheduler,
    });
    this.machine = options.runStateMachine || new RunStateMachine({
      log: (event) => {
        this.state.engineState = event.nextState;
        this.state.logEvent(event.type, event);
      },
    });
    this.state.engineState = this.machine.state;
    this.orderbookBatchSize = Number.parseInt(process.env.UPBIT_ORDERBOOK_BATCH_SIZE || "50", 10);
    this.orderbookDelayMs = Number.parseInt(process.env.UPBIT_ORDERBOOK_DELAY_MS || "200", 10);
    this.wsMarketsPerConnection = Number.parseInt(process.env.UPBIT_WS_MARKETS_PER_CONNECTION || "100", 10);
    this.wsConnectionDelayMs = options.wsConnectionDelayMs !== undefined
      ? options.wsConnectionDelayMs
      : Number.parseInt(process.env.UPBIT_WS_CONNECTION_DELAY_MS || "1000", 10);
    this.validationFeedStartDelayMs = options.validationFeedStartDelayMs !== undefined
      ? options.validationFeedStartDelayMs
      : Number.parseInt(process.env.UPBIT_VALIDATION_WS_START_DELAY_MS || "4000", 10);
    this.orderbookClientFactory = options.orderbookClientFactory ||
      ((markets, clientOptions) => new UpbitWsOrderbookClient(markets, clientOptions));
    this.observationClientInjected = Boolean(options.observationClient);
    this.validationClientInjected = Boolean(options.validationClient);
    this.observationClient = options.observationClient || null;
    this.validationClient = options.validationClient || null;
    this.privateWsClient = options.privateWsClient || null;
    this.privateWsStatus = {
      status: "not_configured",
      stopped: true,
    };
    this.fillTracker = options.fillTracker || new FillTracker({
      logStore: this.logStore,
      mode: "REAL",
    });
    this.balanceTracker = options.balanceTracker || new BalanceTracker({
      startAssets: this.runtimeConfig.enabledStartAssets,
    });
    this.dryRunExecutor = options.dryRunExecutor || new DryRunExecutor({
      logStore: this.logStore,
      simulatedBalances: this.runtimeConfig.executionPolicy.simulatedBalances,
      validationConfig: this.runtimeConfig.candidateValidation,
    });
    this.riskGuard = options.riskGuard || new RiskGuard({
      config: this.runtimeConfig.executionPolicy,
    });
    this.realRunLimits = options.realRunLimits || new RealRunLimits({
      limits: this.runtimeConfig.executionPolicy.realRunLimits,
    });
    this.emergencyStop = options.emergencyStop || new EmergencyStop({
      logStore: this.logStore,
    });
    this.realExecutor = options.realExecutor || (this.restClient ? new RealExecutor({
      restClient: this.restClient,
      fillTracker: this.fillTracker,
      logStore: this.logStore,
      runtimeConfig: this.runtimeConfig,
      riskGuard: this.riskGuard,
      liveTradingEnabled: this.runtimeConfig.liveTradingEnabled,
    }) : null);
    this.readiness = null;
    this.restPermissions = null;
    this.feePolicyByMarket = options.feePolicyByMarket instanceof Map
      ? new Map(options.feePolicyByMarket)
      : new Map(Object.entries(options.feePolicyByMarket || {}));
    this.marketPolicyByMarket = options.marketPolicyByMarket instanceof Map
      ? new Map(options.marketPolicyByMarket)
      : new Map(Object.entries(options.marketPolicyByMarket || {}));
    this.feePolicyLoadErrors = [];
    this.orderChanceCacheUpdatedAt = null;
    this.accountBalanceUpdatedAt = null;
    this.lastExecutionBalanceRefreshAt = 0;
    this.balanceRefreshMinIntervalMs = options.balanceRefreshMinIntervalMs ?? 300;
    this.activeRealExecutionCount = 0;
    this.executionCooldownMs = options.executionCooldownMs || 5000;
    this.lastExecutionByCycleId = new Map();
    this.commandTimer = null;
    this.snapshotTimer = null;
    this.deltaTimer = null;
    this.fallbackTimer = null;
    this.privateCacheTimer = null;
    this.fallbackPollInFlight = false;
    this.privateCacheRefreshInFlight = false;
    this.validationStartTimer = null;
    this.privateCacheRefreshIntervalMs = options.privateCacheRefreshIntervalMs ||
      Number.parseInt(process.env.Q_GAGARIN_PRIVATE_CACHE_REFRESH_INTERVAL_MS || "", 10) ||
      Math.max(1000, Math.min(
        15000,
        Math.floor(Number(this.runtimeConfig.executionPolicy.executionGuards.accountBalanceTtlMs || 30000) / 2),
      ));
    this.preparationPromise = null;
    this.preparationTimeoutMs = Number.parseInt(process.env.Q_GAGARIN_PREPARATION_TIMEOUT_MS || "120000", 10);
    this.preparationMinCoverageRatio = Number(process.env.Q_GAGARIN_PREPARATION_MIN_COVERAGE_RATIO || "0.95");
    this.preparationMaxStaleRatio = Number(process.env.Q_GAGARIN_PREPARATION_MAX_STALE_RATIO || "0.10");
    this.preparation = this.initialPreparationStatus();
    this.processedCommandKeys = new Set();
    this.startedAtEpochMs = options.startedAtEpochMs || Date.now();
    this.lastAgingSweepAt = 0;
    this.started = false;

    if (this.feePolicyByMarket.size > 0 && typeof this.state.setFeePolicyByMarket === "function") {
      this.state.setFeePolicyByMarket(this.feePolicyByMarket);
    }

    if (this.marketPolicyByMarket.size > 0 && typeof this.state.setMarketPolicyByMarket === "function") {
      this.state.setMarketPolicyByMarket(this.marketPolicyByMarket);
    }

    this.state.setExecutionHandler((plan, metadata) => this.handleExecutionCandidate(plan, metadata));
  }

  initialPreparationStatus() {
    return {
      phase: "idle",
      startedAt: null,
      completedAt: null,
      blockedAt: null,
      progress: {
        requiredMarketCount: 0,
        restOrderbookFetched: 0,
        restOrderbookRequested: 0,
        restOrderbookPercent: 0,
        observationCoverageRatio: 0,
        validationCoverageRatio: 0,
        observationStaleRatio: 1,
        validationStaleRatio: 1,
        observationRestOnlyCount: 0,
        validationRestOnlyCount: 0,
        observationWsConfirmedCount: 0,
        validationWsConfirmedCount: 0,
        observationQuietCount: 0,
        validationQuietCount: 0,
        wsOpenConnections: 0,
        wsConnectionCount: 0,
      },
      blockers: [],
      error: null,
    };
  }

  updatePreparation(patch = {}) {
    this.preparation = {
      ...this.preparation,
      ...patch,
      progress: {
        ...(this.preparation && this.preparation.progress || {}),
        ...(patch.progress || {}),
      },
      blockers: patch.blockers || this.preparation.blockers || [],
    };
    return this.preparation;
  }

  async initialize() {
    await this.logStore.ensureFiles();
    await this.commandInbox.ensureDirs();
    await this.seedProcessedCommands();

    await this.writeSnapshot();
  }

  createFeedClients() {
    this.createPublicFeedClients();

    if (this.runtimeConfig.liveTradingEnabled || (process.env.UPBIT_ACCESS_KEY && process.env.UPBIT_SECRET_KEY)) {
      this.privateWsClient = this.privateWsClient || new UpbitPrivateWsClient({
        scheduler: this.rateLimitScheduler,
      });
      this.privateWsClient.on("myOrder", (event) => {
        this.fillTracker.handleMyOrder(event);
      });
      this.privateWsClient.on("status", (status) => {
        this.privateWsStatus = status;
      });
      this.privateWsClient.on("error", (error) => {
        this.state.logEvent("error", { source: "private-ws", error });
        this.logStore.append("errors", { source: "private-ws", error }).catch(() => {});
      });
    }
  }

  createPublicFeedClients() {
    this.observationClient = this.observationClient || this.orderbookClientFactory(this.state.requiredMarkets || [], {
      chunkSize: this.wsMarketsPerConnection,
      connectionDelayMs: this.wsConnectionDelayMs,
      orderbookUnit: this.runtimeConfig.observationOrderbookUnit,
      scheduler: this.rateLimitScheduler,
      feedName: "observation",
    });
    this.validationClient = this.validationClient || this.orderbookClientFactory(this.state.requiredMarkets || [], {
      chunkSize: this.wsMarketsPerConnection,
      connectionDelayMs: this.wsConnectionDelayMs,
      orderbookUnit: this.runtimeConfig.validationOrderbookUnit,
      scheduler: this.rateLimitScheduler,
      feedName: "validation",
    });

    this.observationClient.on("orderbook", (orderbook) => {
      this.state.updateObservationOrderbook(orderbook);
    });
    this.observationClient.on("status", (status) => {
      this.state.setWsStatus(status, "observation");
    });
    this.observationClient.on("error", (error) => {
      this.state.logEvent("error", { source: "observation-ws", error });
      this.logStore.append("errors", { source: "observation-ws", error }).catch(() => {});
    });
    this.validationClient.on("orderbook", (orderbook) => {
      this.state.updateValidationOrderbook(orderbook);
    });
    this.validationClient.on("status", (status) => {
      this.state.setWsStatus(status, "validation");
    });
    this.validationClient.on("error", (error) => {
      this.state.logEvent("error", { source: "validation-ws", error });
      this.logStore.append("errors", { source: "validation-ws", error }).catch(() => {});
    });
  }

  async start(options = {}) {
    await this.initialize();

    if (options.autoStart !== false && this.machine.state === STATES.STOPPED) {
      await this.applyCommand("Start", { source: "engine-autostart" });
    }

    this.commandTimer = setInterval(() => {
      this.processCommands().catch((error) => {
        this.machine.fail(error);
        this.logStore.append("errors", { source: "command-poll", message: error.message }).catch(() => {});
      });
    }, this.commandPollIntervalMs);
    this.snapshotTimer = setInterval(() => {
      this.writeSnapshot().catch((error) => {
        this.logStore.append("errors", { source: "snapshot", message: error.message }).catch(() => {});
      });
    }, this.snapshotIntervalMs);
    this.deltaTimer = setInterval(() => {
      this.writeDelta().catch((error) => {
        this.logStore.append("errors", { source: "delta", message: error.message }).catch(() => {});
      });
    }, Math.max(50, this.deltaIntervalMs));
    this.fallbackTimer = setInterval(() => {
      this.fallbackPoll().catch((error) => {
        this.logStore.append("errors", { source: "fallback", message: error.message }).catch(() => {});
      });
    }, Math.max(this.state.staleOrderbookMs, 5000));
    this.privateCacheTimer = setInterval(() => {
      this.refreshRunningPrivateCaches().catch((error) => {
        this.logStore.append("errors", { source: "private-cache-refresh", message: error.message }).catch(() => {});
      });
    }, this.privateCacheRefreshIntervalMs);

    return this;
  }

  async stop() {
    if (this.commandTimer) clearInterval(this.commandTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    if (this.deltaTimer) clearInterval(this.deltaTimer);
    if (this.fallbackTimer) clearInterval(this.fallbackTimer);
    if (this.privateCacheTimer) clearInterval(this.privateCacheTimer);
    if (this.state && typeof this.state.stopScheduler === "function") {
      this.state.stopScheduler();
    }
    this.stopFeeds();
    if (this.rateLimitScheduler && typeof this.rateLimitScheduler.stop === "function") {
      this.rateLimitScheduler.stop();
    }
    await this.writeSnapshot();
  }

  async seedProcessedCommands() {
    const commands = await this.logStore.readAll("commands", { limit: 1000 });

    for (const command of commands) {
      const commandEpochMs = Date.parse(command.timestamp || "");

      if (!Number.isFinite(commandEpochMs) || commandEpochMs <= this.startedAtEpochMs) {
        this.processedCommandKeys.add(command.commandId || `${command.timestamp}:${command.command}`);
      }
    }
  }

  startFeeds() {
    this.startPublicFeeds();
    if (this.privateWsClient) {
      this.privateWsClient.start();
    }
  }

  startPublicFeeds() {
    if (this.observationClient) {
      this.observationClient.start();
    }
    clearTimeout(this.validationStartTimer);
    this.validationStartTimer = setTimeout(() => {
      this.validationStartTimer = null;
      if (this.validationClient) {
        this.validationClient.start();
      }
    }, Math.max(0, this.validationFeedStartDelayMs));
  }

  stopFeeds() {
    this.stopPublicFeeds();
    if (this.privateWsClient) this.privateWsClient.stop();
  }

  stopPublicFeeds() {
    clearTimeout(this.validationStartTimer);
    this.validationStartTimer = null;
    if (this.observationClient) this.observationClient.stop();
    if (this.validationClient) this.validationClient.stop();
  }

  preparationGateStatus(nowMs = Date.now()) {
    const requiredMarketCount = Array.isArray(this.state.requiredMarkets) ? this.state.requiredMarkets.length : 0;
    const stores = this.state.getOrderbookStoreStatus(nowMs);
    const observation = stores.observation || {};
    const validation = stores.validation || {};
    const observationStatus = this.state.wsStatus || {};
    const validationStatus = this.state.validationWsStatus || {};
    const observationCoverageRatio = requiredMarketCount > 0
      ? Math.min(1, Number(observation.marketCount || 0) / requiredMarketCount)
      : 0;
    const validationCoverageRatio = requiredMarketCount > 0
      ? Math.min(1, Number(validation.marketCount || 0) / requiredMarketCount)
      : 0;
    const observationStaleRatio = Number(observation.marketCount || 0) > 0
      ? Number(observation.staleCount || 0) / Number(observation.marketCount || 1)
      : 1;
    const validationStaleRatio = Number(validation.marketCount || 0) > 0
      ? Number(validation.staleCount || 0) / Number(validation.marketCount || 1)
      : 1;
    const observationRestOnlyCount = Number(observation.restOnlyCount || 0);
    const validationRestOnlyCount = Number(validation.restOnlyCount || 0);
    const observationWsConfirmedCount = Number(observation.wsConfirmedCount || 0);
    const validationWsConfirmedCount = Number(validation.wsConfirmedCount || 0);
    const observationQuietCount = Number(observation.quietCount || 0);
    const validationQuietCount = Number(validation.quietCount || 0);
    const wsConnectionCount = Number(observationStatus.connectionCount || 0) + Number(validationStatus.connectionCount || 0);
    const wsOpenConnections = Number(observationStatus.openConnectionCount || 0) + Number(validationStatus.openConnectionCount || 0);
    const observationWsConfirmedRatio = requiredMarketCount > 0
      ? Math.min(1, observationWsConfirmedCount / requiredMarketCount)
      : 0;
    const validationWsConfirmedRatio = requiredMarketCount > 0
      ? Math.min(1, validationWsConfirmedCount / requiredMarketCount)
      : 0;
    const observationWsReady = observationWsConfirmedRatio >= this.preparationMinCoverageRatio;
    const validationWsReady = validationWsConfirmedRatio >= this.preparationMinCoverageRatio;
    const missingMessageConnections = [
      ...((observationStatus.connections || []).map((connection) => ["observation", connection])),
      ...((validationStatus.connections || []).map((connection) => ["validation", connection])),
    ].filter(([, connection]) => connection.status === "open" && !connection.lastMessageAt);
    const blockers = [];

    if (requiredMarketCount === 0) blockers.push("NO_REQUIRED_MARKETS");
    if (
      observationCoverageRatio < this.preparationMinCoverageRatio ||
      validationCoverageRatio < this.preparationMinCoverageRatio
    ) {
      blockers.push("REST_WARMUP_INCOMPLETE");
    }
    if (wsConnectionCount === 0 || wsOpenConnections < wsConnectionCount) blockers.push("WS_CONNECTION_PENDING");
    if (!observationWsReady || !validationWsReady) blockers.push("WS_CONFIRMATION_INCOMPLETE");
    if (missingMessageConnections.length > 0 && (!observationWsReady || !validationWsReady)) {
      blockers.push("WS_CONNECTION_NO_MESSAGES");
    }

    return {
      ready: blockers.length === 0,
      blockers,
      progress: {
        requiredMarketCount,
        observationCoverageRatio,
        validationCoverageRatio,
        observationStaleRatio,
        validationStaleRatio,
        observationStaleCount: observation.staleCount || 0,
        validationStaleCount: validation.staleCount || 0,
        observationRestOnlyCount,
        validationRestOnlyCount,
        observationWsConfirmedCount,
        validationWsConfirmedCount,
        observationWsConfirmedRatio,
        validationWsConfirmedRatio,
        observationQuietCount,
        validationQuietCount,
        observationMarketCount: observation.marketCount || 0,
        validationMarketCount: validation.marketCount || 0,
        wsOpenConnections,
        wsConnectionCount,
        missingMessageConnectionCount: missingMessageConnections.length,
      },
    };
  }

  async waitForPreparationGate() {
    const startedAtMs = Date.now();
    let lastSnapshotAt = 0;

    while (this.machine.state === STATES.PREPARING) {
      const nowMs = Date.now();
      const gate = this.preparationGateStatus(nowMs);
      this.updatePreparation({
        phase: "freshness-gate",
        progress: gate.progress,
        blockers: gate.blockers,
      });

      if (gate.ready) return gate;
      if (nowMs - startedAtMs >= this.preparationTimeoutMs) {
        const error = new Error(`PREPARATION_TIMEOUT: ${gate.blockers.join(",") || "UNKNOWN"}`);
        error.blockers = gate.blockers;
        error.progress = gate.progress;
        throw error;
      }

      if (nowMs - lastSnapshotAt >= 1000) {
        lastSnapshotAt = nowMs;
        await this.writeSnapshot().catch(() => {});
      }
      await new Promise((resolve) => {
        setTimeout(resolve, 500);
      });
    }

    return {
      ready: false,
      blockers: ["PREPARATION_CANCELLED"],
      progress: this.preparation.progress,
    };
  }

  startPreparation(metadata = {}) {
    if (this.preparationPromise) return this.preparationPromise;
    this.preparationPromise = this.runPreparation(metadata)
      .finally(() => {
        this.preparationPromise = null;
      });
    return this.preparationPromise;
  }

  async runPreparation(metadata = {}) {
    const startedAt = new Date().toISOString();
    this.updatePreparation({
      phase: "market-discovery",
      startedAt,
      completedAt: null,
      blockedAt: null,
      error: null,
      blockers: [],
    });
    await this.writeSnapshot().catch(() => {});

    try {
      if (typeof this.state.initialize !== "function" || typeof this.state.loadInitialOrderbooks !== "function") {
        this.updatePreparation({
          phase: "injected-state-ready",
          progress: {
            requiredMarketCount: Array.isArray(this.state.requiredMarkets) ? this.state.requiredMarkets.length : 0,
          },
          blockers: [],
        });
        this.createFeedClients();
        this.started = true;
        this.startFeeds();

        if (String(this.runtimeConfig.runMode || "").startsWith("REAL_")) {
          const readiness = await this.checkReadiness();
          if (!readiness.passed) {
            const error = new Error(`${this.runtimeConfig.runMode} readiness checklist failed`);
            error.readiness = readiness;
            error.blockers = readiness.items.filter((entry) => !entry.passed).map((entry) => entry.id);
            throw error;
          }
        }

        const nextState = this.machine.markReady();
        this.state.engineState = nextState;
        this.updatePreparation({
          phase: "ready",
          completedAt: new Date().toISOString(),
          blockers: [],
        });
        await this.logStore.append("events", {
          type: "engine.state_changed",
          mode: executionLogMode(this.runtimeConfig.runMode),
          engineState: nextState,
          command: "Start",
          previousState: STATES.PREPARING,
          nextState,
          runMode: this.runtimeConfig.runMode,
          ...metadata,
        });
        await this.writeSnapshot();
        await this.writeDelta({ forceAgingSweep: true });
        return {
          ready: true,
          blockers: [],
          progress: this.preparation.progress,
        };
      }

      const previousRequiredMarkets = this.requiredMarketsKey();
      const discovery = await this.state.initialize();
      if (!discovery.ok) {
        const message = discovery.error && discovery.error.message || "Market discovery failed";
        throw new Error(message);
      }

      const requiredMarketCount = Array.isArray(this.state.requiredMarkets) ? this.state.requiredMarkets.length : 0;
      this.updatePreparation({
        phase: "rest-orderbook-warmup",
        progress: {
          requiredMarketCount,
          restOrderbookRequested: requiredMarketCount,
          restOrderbookFetched: 0,
          restOrderbookPercent: requiredMarketCount === 0 ? 100 : 0,
        },
      });
      await this.writeSnapshot().catch(() => {});

      const orderbookResult = await this.state.loadInitialOrderbooks({
        batchSize: this.orderbookBatchSize,
        delayMs: this.orderbookDelayMs,
        markDirty: false,
        priority: "warmup",
        onProgress: (progress) => {
          const requested = progress.requestedMarketCount || requiredMarketCount;
          const fetched = progress.fetchedMarketCount || 0;
          this.updatePreparation({
            phase: "rest-orderbook-warmup",
            progress: {
              requiredMarketCount,
              restOrderbookRequested: requested,
              restOrderbookFetched: fetched,
              restOrderbookPercent: requested > 0 ? (fetched / requested) * 100 : 100,
            },
          });
        },
      });
      this.updatePreparation({
        phase: "rest-orderbook-warmup",
        progress: {
          requiredMarketCount,
          restOrderbookRequested: orderbookResult.requestedMarketCount || requiredMarketCount,
          restOrderbookFetched: orderbookResult.fetchedMarketCount || 0,
          restOrderbookPercent: orderbookResult.requestedMarketCount > 0
            ? (orderbookResult.fetchedMarketCount / orderbookResult.requestedMarketCount) * 100
            : 100,
        },
        blockers: orderbookResult.errors && orderbookResult.errors.length > 0 ? ["REST_ORDERBOOK_ERRORS"] : [],
      });

      const nextRequiredMarkets = this.requiredMarketsKey();
      if (this.started && previousRequiredMarkets !== nextRequiredMarkets) {
        this.stopPublicFeeds();
        this.observationClient = null;
        this.validationClient = null;
      }

      this.updatePreparation({ phase: "websocket-connect" });
      this.createFeedClients();
      this.started = true;
      this.startFeeds();
      await this.writeSnapshot().catch(() => {});

      const gate = await this.waitForPreparationGate();
      if (!gate.ready || this.machine.state !== STATES.PREPARING) {
        return gate;
      }

      if (String(this.runtimeConfig.runMode || "").startsWith("REAL_")) {
        this.updatePreparation({ phase: "real-readiness" });
        await this.writeSnapshot().catch(() => {});
        const readiness = await this.checkReadiness();
        if (!readiness.passed) {
          const error = new Error(`${this.runtimeConfig.runMode} readiness checklist failed`);
          error.readiness = readiness;
          error.blockers = readiness.items.filter((entry) => !entry.passed).map((entry) => entry.id);
          throw error;
        }
      }

      const nextState = this.machine.markReady();
      this.state.engineState = nextState;
      this.updatePreparation({
        phase: "ready",
        completedAt: new Date().toISOString(),
        blockers: [],
        progress: gate.progress,
      });
      await this.logStore.append("events", {
        type: "engine.state_changed",
        mode: executionLogMode(this.runtimeConfig.runMode),
        engineState: nextState,
        command: "Start",
        previousState: STATES.PREPARING,
        nextState,
        runMode: this.runtimeConfig.runMode,
        ...metadata,
      });
      await this.logStore.append("events", {
        type: "engine.preparation_complete",
        mode: executionLogMode(this.runtimeConfig.runMode),
        engineState: nextState,
        runMode: this.runtimeConfig.runMode,
        ...metadata,
      });
      await this.writeSnapshot();
      await this.writeDelta({ forceAgingSweep: true });
      return gate;
    } catch (error) {
      if (this.machine.state === STATES.PREPARING) {
        this.machine.blockPreparation(error.message);
        this.state.engineState = this.machine.state;
      }
      this.stopFeeds();
      this.updatePreparation({
        phase: "blocked",
        blockedAt: new Date().toISOString(),
        error: error.message,
        blockers: error.blockers || ["PREPARATION_FAILED"],
        progress: error.progress || this.preparation.progress,
      });
      if (error.readiness) {
        await this.logStore.append("events", {
          type: "readiness.blocked",
          command: "Start",
          readiness: error.readiness,
          engineState: this.machine.state,
          runMode: this.runtimeConfig.runMode,
          blockers: this.preparation.blockers,
          ...metadata,
        });
      }
      await this.logStore.append("errors", {
        type: "engine.preparation_failed",
        mode: executionLogMode(this.runtimeConfig.runMode),
        engineState: this.machine.state,
        message: error.message,
        blockers: this.preparation.blockers,
        ...metadata,
      });
      if (metadata.commandId) {
        await this.commandStatusStore.write(metadata.commandId, {
          status: "rejected",
          command: "Start",
          runMode: this.runtimeConfig.runMode,
          source: metadata.source || "cli",
          message: error.message,
          ...(error.readiness ? { readiness: error.readiness } : {}),
          failedItems: this.preparation.blockers,
        }).catch(() => {});
      }
      await this.writeSnapshot().catch(() => {});
      await this.writeDelta({ forceAgingSweep: true }).catch(() => {});
      return {
        ready: false,
        blockers: this.preparation.blockers,
        error,
      };
    }
  }

  setRunMode(runMode) {
    const requestedRunMode = String(runMode || "").trim().toUpperCase();

    if (!RUN_MODES.has(requestedRunMode)) {
      throw new Error(`Invalid runMode: ${runMode}`);
    }

    const liveTradingEnabled = requestedRunMode.startsWith("REAL_") &&
      process.env.Q_GAGARIN_LIVE_TRADING_ENABLED === "true";
    const allowLiveTrading = process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true";

    const nextConfig = freezeRuntimeConfig({
      ...this.runtimeConfig,
      runMode: requestedRunMode,
      liveTradingEnabled,
    }, {
      allowLiveTrading,
    });

    this.state.setRuntimeConfig(nextConfig, { allowLiveTrading });
    this.runtimeConfig = nextConfig;
    this.syncExecutionClients();
    return this.runtimeConfig.runMode;
  }

  restoreRuntimeConfig(config) {
    this.state.setRuntimeConfig(config, { allowLiveTrading: true });
    this.runtimeConfig = config;
    this.syncExecutionClients();
  }

  syncExecutionClients() {
    const liveTradingEnabled = this.runtimeConfig.liveTradingEnabled === true;

    if (this.restClient) {
      this.restClient.liveTradingEnabled = liveTradingEnabled;
      this.restClient.mode = String(this.runtimeConfig.runMode || "").startsWith("REAL") ? "REAL" : this.runtimeConfig.runMode;
    }

    if (this.realExecutor) {
      this.realExecutor.runtimeConfig = this.runtimeConfig;
      this.realExecutor.liveTradingEnabled = liveTradingEnabled;
      if (this.realExecutor.orderManager) {
        this.realExecutor.orderManager.runtimeConfig = this.runtimeConfig;
      }
    }
  }

  async applyCommand(commandInput, metadata = {}) {
    const command = normalizeCommand(commandInput);
    const commandMetadata = validateCommandMetadata(command, metadata);
    const previousState = this.machine.state;
    const previousConfig = this.runtimeConfig;
    let configChanged = false;

    if (command === "Start" && commandMetadata.runMode) {
      this.setRunMode(commandMetadata.runMode);
      configChanged = true;
    }

    let nextState;
    try {
      nextState = this.machine.apply(command);
    } catch (error) {
      if (configChanged) {
        this.restoreRuntimeConfig(previousConfig);
      }
      throw error;
    }

    const shouldStartPreparation = command === "Start" &&
      nextState === STATES.PREPARING &&
      previousState === STATES.STOPPED;

    if (command === "Stop" && nextState === STATES.STOPPED) {
      await this.handleStopOrderPolicy(this.runtimeConfig.executionPolicy.stopPolicy, {
        command,
        source: metadata.source || "cli",
        emergency: commandMetadata.emergency,
      });
      this.stopFeeds();
      this.emergencyStop.clear({
        source: metadata.source || "cli",
        reason: commandMetadata.emergency ? "emergency-stop-command" : "stop-command",
      });
    }

    this.state.engineState = nextState;
    const event = await this.logStore.append("events", {
      type: "engine.state_changed",
      mode: executionLogMode(this.runtimeConfig.runMode),
      engineState: nextState,
      command,
      previousState,
      nextState,
      runMode: this.runtimeConfig.runMode,
      ...metadata,
    });
    if (metadata.commandId) {
      await this.commandStatusStore.write(metadata.commandId, {
        status: "accepted",
        command,
        previousState,
        nextState,
        runMode: this.runtimeConfig.runMode,
        eventTimestamp: event.timestamp,
        source: metadata.source || "cli",
      });
    }
    await this.writeSnapshot();
    await this.writeDelta({ forceAgingSweep: true });
    if (shouldStartPreparation) {
      this.startPreparation({
        command,
        source: metadata.source || "cli",
        commandId: metadata.commandId,
      }).catch(() => {});
    }
    return nextState;
  }

  async processCommands() {
    await this.processCommandInbox();
    await this.processLegacyCommandLog();
  }

  async appendCommandAudit(commandRecord, command = null) {
    const commandRunMode = command && command.runMode || commandRecord.runMode || this.runtimeConfig.runMode;

    return this.logStore.append("commands", {
      type: commandRecord.type || `${commandRecord.source || "cli"}.command`,
      mode: executionLogMode(commandRunMode),
      engineState: this.machine.state,
      command: command ? command.command : commandRecord.command,
      commandId: commandRecord.commandId,
      runMode: command && command.runMode || commandRecord.runMode,
      emergency: Boolean(command && command.emergency || commandRecord.emergency),
      source: commandRecord.source || "cli",
      queuedAt: commandRecord.createdAt || commandRecord.queuedAt || commandRecord.timestamp,
    });
  }

  async processCommandInbox() {
    const entries = await this.commandInbox.listPending();

    for (const entry of entries) {
      const commandRecord = entry.record || {};
      const key = commandRecord.commandId || entry.fileName;
      if (this.processedCommandKeys.has(key)) {
        await this.commandInbox.markProcessed(entry);
        continue;
      }
      this.processedCommandKeys.add(key);
      let command;

      try {
        if (entry.error) throw entry.error;
        command = normalizeQueuedCommandRecord(commandRecord);
        await this.appendCommandAudit(commandRecord, command);
        await this.applyCommand(command.command, {
          commandId: command.commandId,
          source: command.source || "cli",
          runMode: command.runMode,
          emergency: command.emergency,
        });
      } catch (error) {
        await this.logStore.append("errors", {
          type: "engine.command.rejected",
          command: commandRecord.command,
          commandId: commandRecord.commandId,
          source: commandRecord.source || "cli",
          message: error.message,
        });
        if (commandRecord.commandId) {
          const failedItems = error.readiness && Array.isArray(error.readiness.items)
            ? error.readiness.items.filter((item) => !item.passed).map((item) => item.id)
            : undefined;
          await this.commandStatusStore.write(commandRecord.commandId, {
            status: "rejected",
            command: commandRecord.command,
            runMode: commandRecord.runMode,
            source: commandRecord.source || "cli",
            message: error.message,
            ...(error.readiness ? { readiness: error.readiness } : {}),
            ...(failedItems ? { failedItems } : {}),
          });
        }
      } finally {
        await this.commandInbox.markProcessed(entry).catch((error) => {
          if (error.code !== "ENOENT") throw error;
        });
      }
    }
  }

  async processLegacyCommandLog() {
    const commands = await this.logStore.readAll("commands", { limit: 1000 });

    for (const commandRecord of commands) {
      const key = commandRecord.commandId || `${commandRecord.timestamp}:${commandRecord.command}`;
      if (this.processedCommandKeys.has(key)) continue;
      this.processedCommandKeys.add(key);
      let command;

      try {
        command = normalizeQueuedCommandRecord(commandRecord);
        await this.applyCommand(command.command, {
          commandId: command.commandId,
          source: command.source || "cli",
          runMode: command.runMode,
          emergency: command.emergency,
        });
      } catch (error) {
        await this.logStore.append("errors", {
          type: "engine.command.rejected",
          command: commandRecord.command,
          commandId: commandRecord.commandId,
          message: error.message,
        });
        if (commandRecord.commandId) {
          const failedItems = error.readiness && Array.isArray(error.readiness.items)
            ? error.readiness.items.filter((item) => !item.passed).map((item) => item.id)
            : undefined;
          await this.commandStatusStore.write(commandRecord.commandId, {
            status: "rejected",
            command: commandRecord.command,
            runMode: commandRecord.runMode,
            source: commandRecord.source || "cli",
            message: error.message,
            ...(error.readiness ? { readiness: error.readiness } : {}),
            ...(failedItems ? { failedItems } : {}),
          });
        }
      }
    }
  }

  async handleStopOrderPolicy(policy = "CANCEL_OPEN_ORDERS", metadata = {}) {
    const openOrders = typeof this.fillTracker.openOrders === "function" ? this.fillTracker.openOrders() : [];
    const intentEvents = this.fillTracker.handleStopPolicy(policy);

    if (policy !== "CANCEL_OPEN_ORDERS" || openOrders.length === 0) {
      return {
        intentEvents,
        cancelResults: [],
      };
    }

    const orderManager = this.realExecutor && this.realExecutor.orderManager;

    if (!orderManager || typeof orderManager.cancelOpenOrders !== "function") {
      await this.logStore.append("errors", {
        type: "order_cancel_manager_missing",
        mode: "REAL",
        openOrderCount: openOrders.length,
        stopPolicy: policy,
        message: "Stop policy requested open-order cancellation but no order manager is configured",
        ...metadata,
      });
      return {
        intentEvents,
        cancelResults: [],
      };
    }

    try {
      const cancelResults = await orderManager.cancelOpenOrders(openOrders, {
        stopPolicy: policy,
        ...metadata,
      });
      return {
        intentEvents,
        cancelResults,
      };
    } catch (error) {
      await this.logStore.append("errors", {
        type: "order_cancel_stop_policy_failed",
        mode: "REAL",
        openOrderCount: openOrders.length,
        stopPolicy: policy,
        message: error.message,
        ...metadata,
      });
      return {
        intentEvents,
        cancelResults: [],
      };
    }
  }

  async fallbackPoll() {
    if (this.machine.state !== STATES.RUNNING || !this.state.shouldUseFallback()) {
      return;
    }

    if (this.fallbackPollInFlight) {
      return;
    }

    this.fallbackPollInFlight = true;

    try {
      const previousRequiredMarkets = this.requiredMarketsKey();
      const result = await this.state.fallbackPoll({
        batchSize: this.orderbookBatchSize,
        delayMs: this.orderbookDelayMs,
      });
      const nextRequiredMarkets = this.requiredMarketsKey();

      if (this.machine.state !== STATES.RUNNING) {
        return result;
      }

      if (previousRequiredMarkets !== nextRequiredMarkets) {
        await this.rebuildPublicFeedClients({
          reason: "required-markets-changed",
          previousMarketCount: JSON.parse(previousRequiredMarkets).length,
          nextMarketCount: JSON.parse(nextRequiredMarkets).length,
          marketDiscoveryRecovered: Boolean(result && result.marketDiscoveryRecovered),
        });
        await this.writeSnapshot();
        await this.writeDelta({ forceAgingSweep: true });
      }

      return result;
    } finally {
      this.fallbackPollInFlight = false;
    }
  }

  requiredMarketsKey(markets = this.state.requiredMarkets || []) {
    return JSON.stringify([...new Set(markets)].sort());
  }

  async rebuildPublicFeedClients(metadata = {}) {
    if (this.observationClientInjected || this.validationClientInjected) {
      await this.logStore.append("events", {
        type: "market_data.feed_rebuild_skipped",
        reason: "injected-public-feed-client",
        ...metadata,
      });
      return false;
    }

    this.stopPublicFeeds();
    this.observationClient = null;
    this.validationClient = null;
    this.createPublicFeedClients();

    if (this.machine.state === STATES.RUNNING) {
      this.startPublicFeeds();
    }

    await this.logStore.append("events", {
      type: "market_data.feeds_rebuilt",
      ...metadata,
    });
    return true;
  }

  firstRequiredMarket() {
    return this.state.requiredMarkets && this.state.requiredMarkets[0] || "KRW-BTC";
  }

  requiredFeePolicyMarkets() {
    const markets = Array.isArray(this.state.requiredMarkets) && this.state.requiredMarkets.length > 0
      ? this.state.requiredMarkets
      : [this.firstRequiredMarket()];

    return [...new Set(markets.filter(Boolean))].sort();
  }

  async refreshFeePolicies(markets = this.requiredFeePolicyMarkets()) {
    this.feePolicyLoadErrors = [];

    if (!this.restClient || typeof this.restClient.getOrderChance !== "function") {
      this.feePolicyLoadErrors.push({
        market: null,
        message: "REST getOrderChance is not configured",
      });
      return {
        loadedCount: 0,
        requiredCount: markets.length,
        errors: this.feePolicyLoadErrors.slice(),
      };
    }

    const ttlMs = this.runtimeConfig.executionPolicy.executionGuards.orderChanceTtlMs;
    const loadedChances = [];

    for (const market of markets) {
      try {
        const chance = await this.restClient.getOrderChance(market);
        loadedChances.push({ market, chance });
      } catch (error) {
        this.feePolicyLoadErrors.push({
          market,
          message: error.message,
        });
      }
    }

    const loadedAtMs = Date.now();
    for (const { market, chance } of loadedChances) {
      const policy = normalizeFeePolicy({
        ...chance,
        loadedAt: new Date(loadedAtMs).toISOString(),
        expiresAt: new Date(loadedAtMs + ttlMs).toISOString(),
      });
      this.feePolicyByMarket.set(market, policy);
      this.marketPolicyByMarket.set(market, policyForMarket(market, {
        ...chance,
        source: chance.source || "orders/chance",
      }));
    }

    if (typeof this.state.setFeePolicyByMarket === "function") {
      this.state.setFeePolicyByMarket(this.feePolicyByMarket);
    }

    if (typeof this.state.setMarketPolicyByMarket === "function") {
      this.state.setMarketPolicyByMarket(this.marketPolicyByMarket);
    }

    if (markets.length > 0 && this.feePolicyLoadErrors.length === 0) {
      this.orderChanceCacheUpdatedAt = loadedAtMs;
    }

    return {
      loadedCount: markets.length - this.feePolicyLoadErrors.length,
      requiredCount: markets.length,
      errors: this.feePolicyLoadErrors.slice(),
    };
  }

  isFresh(timestamp, ttlMs, nowMs = Date.now()) {
    return timestamp !== null &&
      timestamp !== undefined &&
      nowMs - Number(timestamp) <= Number(ttlMs || 0);
  }

  orderChanceFreshnessStatus(markets = this.requiredFeePolicyMarkets(), nowMs = Date.now()) {
    let requestedMarkets = markets;
    let requestedNowMs = nowMs;

    if (typeof markets === "number") {
      requestedMarkets = this.requiredFeePolicyMarkets();
      requestedNowMs = markets;
    }

    const requiredMarkets = [...new Set((Array.isArray(requestedMarkets) && requestedMarkets.length > 0
      ? requestedMarkets
      : this.requiredFeePolicyMarkets()).filter(Boolean))].sort();
    const timestampFresh = this.isFresh(
      this.orderChanceCacheUpdatedAt,
      this.runtimeConfig.executionPolicy.executionGuards.orderChanceTtlMs,
      requestedNowMs,
    );
    const missingFeePolicyMarkets = requiredMarkets.filter((market) => !this.feePolicyByMarket.has(market));
    const missingMarketPolicyMarkets = requiredMarkets.filter((market) => !this.marketPolicyByMarket.has(market));
    const incompleteFeePolicyMarkets = requiredMarkets.filter((market) => {
      const policy = this.feePolicyByMarket.get(market);
      return policy && !hasCompleteFeePolicy(policy);
    });
    const incompleteMarketPolicyMarkets = requiredMarkets.filter((market) => {
      const policy = this.marketPolicyByMarket.get(market);
      return policy && !hasCompleteMarketPolicy(policy);
    });
    const expiredFeePolicyMarkets = requiredMarkets.filter((market) => {
      const policy = this.feePolicyByMarket.get(market);
      return policy && isFeePolicyExpired(policy, requestedNowMs);
    });
    const loadErrorMarkets = this.feePolicyLoadErrors.map((error) => error.market || null);

    return {
      fresh: timestampFresh &&
        this.feePolicyLoadErrors.length === 0 &&
        missingFeePolicyMarkets.length === 0 &&
        missingMarketPolicyMarkets.length === 0 &&
        incompleteFeePolicyMarkets.length === 0 &&
        incompleteMarketPolicyMarkets.length === 0 &&
        expiredFeePolicyMarkets.length === 0,
      timestampFresh,
      requiredMarkets,
      missingFeePolicyMarkets,
      missingMarketPolicyMarkets,
      incompleteFeePolicyMarkets,
      incompleteMarketPolicyMarkets,
      expiredFeePolicyMarkets,
      loadErrorMarkets,
    };
  }

  isOrderChanceFresh() {
    return this.orderChanceFreshnessStatus().fresh;
  }

  planMarkets(plan = {}) {
    const steps = plan.cycle && Array.isArray(plan.cycle.steps) ? plan.cycle.steps : [];
    return [...new Set(steps.map((step) => step && step.market).filter(Boolean))].sort();
  }

  isPlanOrderChanceFresh(plan = {}) {
    const markets = this.planMarkets(plan);
    return this.orderChanceFreshnessStatus(markets.length > 0 ? markets : undefined).fresh;
  }

  isAccountBalanceFresh() {
    return this.isFresh(
      this.accountBalanceUpdatedAt,
      this.runtimeConfig.executionPolicy.executionGuards.accountBalanceTtlMs,
    );
  }

  updateAccountBalances(accounts = [], updatedAtMs = Date.now()) {
    this.balanceTracker.updateFromAccounts(accounts || []);
    this.accountBalanceUpdatedAt = updatedAtMs;
  }

  async refreshPrivateCaches() {
    if (!this.restClient) {
      this.restPermissions = {
        viewAccounts: false,
        viewOrdersChance: false,
        errors: [{ permission: "REST client", message: "not configured" }],
      };
      return this.restPermissions;
    }

    this.restPermissions = await this.restClient.checkPermissions({
      market: this.firstRequiredMarket(),
    });

    const now = Date.now();
    if (this.restPermissions.viewAccounts) {
      this.updateAccountBalances(this.restPermissions.accounts || [], now);
    }

    if (this.restPermissions.viewOrdersChance) {
      await this.refreshFeePolicies();
    }

    if (this.restPermissions.viewAccounts) {
      let accounts = this.restPermissions.accounts || [];
      if (typeof this.restClient.getAccounts === "function") {
        accounts = await this.restClient.getAccounts();
        this.restPermissions.accounts = accounts;
      }
      this.updateAccountBalances(accounts, Date.now());
    }

    return this.restPermissions;
  }

  async refreshAccountBalancesAfterExecution(metadata = {}) {
    if (!this.restClient || typeof this.restClient.getAccounts !== "function") {
      return {
        ok: false,
        skipped: true,
        reason: "ACCOUNT_BALANCE_REFRESH_UNAVAILABLE",
      };
    }

    const nowMs = Date.now();
    const force = metadata.force === true;
    if (!force && nowMs - this.lastExecutionBalanceRefreshAt < this.balanceRefreshMinIntervalMs) {
      return {
        ok: false,
        skipped: true,
        reason: "ACCOUNT_BALANCE_REFRESH_THROTTLED",
      };
    }

    this.lastExecutionBalanceRefreshAt = nowMs;
    try {
      const accounts = await this.restClient.getAccounts();
      this.updateAccountBalances(accounts || [], Date.now());
      return {
        ok: true,
        accountCount: Array.isArray(accounts) ? accounts.length : 0,
        reason: metadata.reason || null,
      };
    } catch (error) {
      await this.logStore.append("events", {
        type: "balance.refresh_failed",
        mode: "REAL",
        engineState: this.machine.state,
        reason: metadata.reason || "EXECUTION_BALANCE_REFRESH",
        planId: metadata.planId || null,
        cycleId: metadata.cycleId || null,
        legIndex: metadata.legIndex || null,
        market: metadata.market || null,
        message: error.message,
      });
      return {
        ok: false,
        reason: "ACCOUNT_BALANCE_REFRESH_FAILED",
        message: error.message,
      };
    }
  }

  async refreshRunningPrivateCaches() {
    if (!String(this.runtimeConfig.runMode || "").startsWith("REAL_")) {
      return {
        ok: true,
        skipped: true,
        reason: "NOT_REAL_RUN",
      };
    }

    if (![STATES.PREPARING, STATES.RUNNING].includes(this.machine.state)) {
      return {
        ok: true,
        skipped: true,
        reason: "ENGINE_NOT_ACTIVE",
      };
    }

    if (this.privateCacheRefreshInFlight) {
      return {
        ok: false,
        skipped: true,
        reason: "PRIVATE_CACHE_REFRESH_IN_FLIGHT",
      };
    }

    this.privateCacheRefreshInFlight = true;
    try {
      const accountAgeMs = this.accountBalanceUpdatedAt ? Date.now() - this.accountBalanceUpdatedAt : Infinity;
      if (accountAgeMs >= this.privateCacheRefreshIntervalMs || !this.isAccountBalanceFresh()) {
        return this.refreshAccountBalancesAfterExecution({
          reason: "PERIODIC_ACCOUNT_REFRESH",
          force: true,
        });
      }

      return {
        ok: true,
        skipped: true,
        reason: "ACCOUNT_BALANCE_STILL_FRESH",
      };
    } finally {
      this.privateCacheRefreshInFlight = false;
    }
  }

  async ensureExecutionPrivateCaches(plan = {}) {
    const markets = this.planMarkets(plan);
    const refreshed = {
      accounts: false,
      orderChanceMarkets: [],
    };

    if (
      this.accountBalanceUpdatedAt !== null &&
      !this.isAccountBalanceFresh() &&
      this.restClient &&
      typeof this.restClient.getAccounts === "function"
    ) {
      const result = await this.refreshAccountBalancesAfterExecution({
        reason: "EXECUTION_PRECHECK_ACCOUNT_REFRESH",
        force: true,
        planId: plan.planId || null,
        cycleId: plan.cycleId || (plan.cycle && plan.cycle.cycleId) || null,
        startAsset: plan.startAsset || (plan.cycle && plan.cycle.startAsset) || null,
        strategyId: plan.strategyId || this.runtimeConfig.activeStrategyId,
      });
      if (!result || result.ok !== true) {
        return {
          ok: false,
          reason: result && result.reason || "ACCOUNT_BALANCE_REFRESH_FAILED",
          refreshed,
        };
      }
      refreshed.accounts = true;
    }

    const chanceStatus = markets.length > 0 ? this.orderChanceFreshnessStatus(markets) : { fresh: true };
    if (!chanceStatus.fresh) {
      if (!this.restClient || typeof this.restClient.getOrderChance !== "function") {
        return {
          ok: false,
          reason: "ORDER_CHANCE_REFRESH_UNAVAILABLE",
          refreshed,
        };
      }

      const result = await this.refreshFeePolicies(markets);
      refreshed.orderChanceMarkets = markets.slice();
      const refreshedStatus = this.orderChanceFreshnessStatus(markets);

      if (!refreshedStatus.fresh) {
        return {
          ok: false,
          reason: "ORDER_CHANCE_REFRESH_FAILED",
          refreshed,
          orderChance: {
            loadedCount: result.loadedCount,
            requiredCount: result.requiredCount,
            errors: result.errors,
            status: refreshedStatus,
          },
        };
      }
    }

    return {
      ok: true,
      refreshed,
      context: this.currentGuardContext(plan),
    };
  }

  validationDepthFresh() {
    const status = this.state.getOrderbookStoreStatus();
    const validation = status.validation || null;
    if (!validation) return false;
    if (Object.hasOwn(validation, "wsConfirmedCount")) {
      return Number(validation.wsConfirmedCount || 0) > 0;
    }
    return validation.staleCount === 0;
  }

  currentGuardContext(plan = null) {
    const balanceSnapshot = this.balanceTracker.snapshot();
    const planMarkets = plan ? this.planMarkets(plan) : [];

    return {
      emergencyStopActive: this.emergencyStop.active,
      emergencyStopReason: this.emergencyStop.reason,
      privateWsConnected: this.privateWsStatus.status === "open",
      orderChanceFresh: planMarkets.length > 0
        ? this.orderChanceFreshnessStatus(planMarkets).fresh
        : this.isOrderChanceFresh(),
      accountBalanceFresh: this.isAccountBalanceFresh(),
      availableBalances: balanceSnapshot.availableBalances,
      lockedBalances: balanceSnapshot.lockedBalances,
      validationDepthFresh: this.validationDepthFresh(),
      nowMs: Date.now(),
      dailyLossByAsset: this.realRunLimits.dailyLossByAsset(),
    };
  }

  buildPrivateCacheStatus(options = {}) {
    const orderChanceFreshness = this.orderChanceFreshnessStatus();
    const status = {
      orderChanceFresh: orderChanceFreshness.fresh,
      accountBalanceFresh: this.isAccountBalanceFresh(),
      orderChanceCacheUpdatedAt: this.orderChanceCacheUpdatedAt,
      accountBalanceUpdatedAt: this.accountBalanceUpdatedAt,
      feePolicyMarketCount: this.feePolicyByMarket.size,
      feePolicyRequiredMarketCount: orderChanceFreshness.requiredMarkets.length,
      requiredFeePolicyMarkets: orderChanceFreshness.requiredMarkets,
      feePolicyLoadErrors: this.feePolicyLoadErrors.slice(),
      marketPolicyMarketCount: this.marketPolicyByMarket.size,
      restPermissions: this.restPermissions,
      orderChanceFreshness,
      orderChanceTimestampFresh: orderChanceFreshness.timestampFresh,
      missingFeePolicyMarkets: orderChanceFreshness.missingFeePolicyMarkets,
      missingMarketPolicyMarkets: orderChanceFreshness.missingMarketPolicyMarkets,
      incompleteFeePolicyMarkets: orderChanceFreshness.incompleteFeePolicyMarkets,
      incompleteMarketPolicyMarkets: orderChanceFreshness.incompleteMarketPolicyMarkets,
      expiredFeePolicyMarkets: orderChanceFreshness.expiredFeePolicyMarkets,
      feePolicyLoadErrorMarkets: orderChanceFreshness.loadErrorMarkets,
    };

    if (options.includePolicyMaps) {
      status.feePolicyByMarket = feePolicyMapToObject(this.feePolicyByMarket);
      status.marketPolicyByMarket = marketPolicyMapToObject(this.marketPolicyByMarket);
    }

    return status;
  }

  shouldThrottleExecution(plan, nowMs = Date.now()) {
    const key = plan.cycleId || (plan.cycle && plan.cycle.cycleId);
    if (!key) return false;
    const previous = this.lastExecutionByCycleId.get(key);

    if (previous && nowMs - previous < this.executionCooldownMs) {
      return true;
    }

    this.lastExecutionByCycleId.set(key, nowMs);
    return false;
  }

  async handleExecutionCandidate(plan) {
    if (this.machine.state !== STATES.RUNNING) {
      return null;
    }

    if (!["DRY_RUN", "REAL_GUARDED", "REAL_AUTO"].includes(this.runtimeConfig.runMode)) {
      return null;
    }

    if (this.runtimeConfig.runMode !== "DRY_RUN") {
      const configuredMaxActiveTriangleExecutions =
        this.runtimeConfig.executionPolicy &&
        this.runtimeConfig.executionPolicy.realRunLimits &&
        this.runtimeConfig.executionPolicy.realRunLimits.maxActiveTriangleExecutions;
      const maxActiveTriangleExecutions = configuredMaxActiveTriangleExecutions === null ||
        configuredMaxActiveTriangleExecutions === undefined
        ? 1
        : Number(configuredMaxActiveTriangleExecutions);
      if (maxActiveTriangleExecutions > 0 && this.activeRealExecutionCount >= maxActiveTriangleExecutions) {
        await this.logStore.append("events", {
          type: "execution.skipped",
          mode: "REAL",
          engineState: this.machine.state,
          planId: plan.planId,
          cycleId: plan.cycleId || (plan.cycle && plan.cycle.cycleId),
          startAsset: plan.startAsset || (plan.cycle && plan.cycle.startAsset),
          strategyId: plan.strategyId || this.runtimeConfig.activeStrategyId,
          reason: "REAL_EXECUTION_BUSY",
          activeRealExecutionCount: this.activeRealExecutionCount,
          maxActiveTriangleExecutions,
        });
        return {
          ok: false,
          reason: "REAL_EXECUTION_BUSY",
          activeRealExecutionCount: this.activeRealExecutionCount,
          maxActiveTriangleExecutions,
        };
      }
    }

    if (this.shouldThrottleExecution(plan)) {
      return null;
    }

    if (this.runtimeConfig.runMode === "DRY_RUN") {
      return this.dryRunExecutor.execute({
        ...plan,
        engineState: this.machine.state,
      });
    }

    if (this.runtimeConfig.liveTradingEnabled !== true) {
      await this.logStore.append("errors", {
        type: "real_execution_refused",
        mode: "REAL",
        planId: plan.planId,
        cycleId: plan.cycleId || (plan.cycle && plan.cycle.cycleId),
        reason: "LIVE_TRADING_DISABLED",
        message: "Real execution refused because liveTradingEnabled=false",
      });
      return {
        ok: false,
        reason: "LIVE_TRADING_DISABLED",
      };
    }

    if (!this.realExecutor) {
      await this.logStore.append("errors", {
        type: "real_executor_missing",
        mode: "REAL",
        planId: plan.planId,
        cycleId: plan.cycleId,
        message: "Real executor is not configured",
      });
      return null;
    }

    this.activeRealExecutionCount += 1;
    try {
      const privateCaches = await this.ensureExecutionPrivateCaches(plan);
      if (!privateCaches.ok) {
        await this.logStore.append("events", {
          type: "execution.skipped",
          mode: "REAL",
          engineState: this.machine.state,
          planId: plan.planId,
          cycleId: plan.cycleId || (plan.cycle && plan.cycle.cycleId),
          startAsset: plan.startAsset || (plan.cycle && plan.cycle.startAsset),
          strategyId: plan.strategyId || this.runtimeConfig.activeStrategyId,
          reason: privateCaches.reason,
          refreshed: privateCaches.refreshed,
          orderChance: privateCaches.orderChance,
        });
        return {
          ok: false,
          reason: privateCaches.reason,
          refreshed: privateCaches.refreshed,
        };
      }

      const initialGuardContext = privateCaches.context || this.currentGuardContext(plan);
      const result = await this.realExecutor.execute(plan, {
        ...initialGuardContext,
        getGuardContext: () => this.currentGuardContext(plan),
        getValidationOrderbooks: () => this.state.getValidationOrderbooks(),
        getMarketPolicy: (market) => this.marketPolicyByMarket.get(market) || null,
        getFeePolicy: (market) => this.feePolicyByMarket.get(market) || null,
        refreshAccountBalances: (metadata) => this.refreshAccountBalancesAfterExecution(metadata),
      });

      if (result && result.ok) {
        const startAsset = result.startAsset || plan.startAsset || (plan.cycle && plan.cycle.startAsset);
        const planId = result.planId || plan.planId;
        const cycleId = result.cycleId || plan.cycleId || (plan.cycle && plan.cycle.cycleId);
        const realizedRecord = this.realRunLimits.recordCycleResult({
          ...result,
          startAsset,
          planId,
          cycleId,
        });
        const strategyId = result.strategyId || plan.strategyId || this.runtimeConfig.activeStrategyId;
        await this.logStore.append("events", {
          type: "pnl.realized",
          mode: "REAL",
          engineState: this.machine.state,
          planId,
          cycleId,
          startAsset,
          strategyId,
          accountingAsset: startAsset,
          startAmount: realizedRecord.startAmount,
          outputAmount: realizedRecord.outputAmount,
          pnl: realizedRecord.pnl,
          realizedLoss: realizedRecord.realizedLoss,
          feeSummary: realizedRecord.feeSummary,
          legResults: result.legResults || [],
          tradingDay: realizedRecord.tradingDay,
        });
      }

      if (result) {
        await this.recordExecutionResiduals(result, plan);
      }

      return result;
    } catch (error) {
      await this.logStore.append("errors", {
        type: "real_execution_error",
        mode: "REAL",
        planId: plan.planId,
        cycleId: plan.cycleId,
        message: error.message,
        emergencyStop: false,
      });

      return {
        ok: false,
        reason: error.message,
        emergencyStop: false,
      };
    } finally {
      this.activeRealExecutionCount = Math.max(0, this.activeRealExecutionCount - 1);
    }
  }

  async recordExecutionResiduals(result = {}, plan = {}) {
    if (!this.balanceTracker || typeof this.balanceTracker.recordResidual !== "function") {
      return [];
    }

    const planId = result.planId || plan.planId;
    const cycleId = result.cycleId || plan.cycleId || (plan.cycle && plan.cycle.cycleId);
    const startAsset = result.startAsset || plan.startAsset || (plan.cycle && plan.cycle.startAsset);
    const strategyId = result.strategyId || plan.strategyId || this.runtimeConfig.activeStrategyId;
    const reason = result.reason || null;
    const records = [];
    const seen = new Set();
    const addResidual = async ({ asset, amount, legIndex, source }) => {
      const residualAmount = positiveNumber(amount);
      if (!asset || residualAmount === null) return;

      const key = [asset, residualAmount.toPrecision(15), legIndex || ""].join(":");
      if (seen.has(key)) return;
      seen.add(key);

      const recorded = this.balanceTracker.recordResidual({
        asset,
        amount: residualAmount,
        planId,
        cycleId,
        startAsset,
        strategyId,
        legIndex,
        reason,
        source,
      });

      if (recorded.ok) {
        const event = await this.logStore.append("events", {
          ...recorded.event,
          mode: "REAL",
          exchange: "upbit",
          engineState: this.machine.state,
        });
        records.push({
          ...recorded,
          eventTimestamp: event.timestamp,
        });
      }
    };
    const legResults = Array.isArray(result.legResults) ? result.legResults : [];

    for (const leg of legResults) {
      await addResidual({
        asset: leg.residualAsset,
        amount: leg.residualAmount,
        legIndex: leg.legIndex,
        source: "real-execution-partial",
      });
    }

    await addResidual({
      asset: result.residualAsset,
      amount: result.residualAmount,
      legIndex: result.legIndex,
      source: "real-execution-residual",
    });

    if (result.ok === false) {
      const lastLeg = legResults[legResults.length - 1] || null;
      await addResidual({
        asset: result.actualAsset || result.currentAsset || (lastLeg && lastLeg.toAsset) || result.residualAsset,
        amount: result.actualAmount,
        legIndex: result.legIndex || (lastLeg && lastLeg.legIndex),
        source: "real-execution-current-position",
      });
    }

    return records;
  }

  async activateEmergencyStop(reason, details = {}) {
    const wasActive = this.emergencyStop.active === true;
    const snapshot = this.emergencyStop.trigger(reason, details);
    const error = new Error(reason || "EMERGENCY_STOP");

    if (this.machine.state !== STATES.ERROR) {
      this.machine.fail(error);
    }

    this.state.engineState = this.machine.state;
    const stopOrderPolicy = wasActive
      ? { skipped: "EMERGENCY_STOP_ALREADY_ACTIVE" }
      : summarizeStopOrderPolicyResult(await this.handleStopOrderPolicy(
          this.runtimeConfig.executionPolicy.stopPolicy,
          {
            command: "EmergencyStop",
            source: details.source || "emergency-stop",
            emergency: true,
            emergencyStopReason: reason || "EMERGENCY_STOP",
          },
        ));
    await this.logStore.append("events", {
      type: "emergency_stop.triggered",
      mode: details.mode || "REAL",
      reason,
      details,
      emergencyStop: snapshot,
      stopOrderPolicy,
    });
    await this.writeSnapshot();
    await this.writeDelta({ forceAgingSweep: true });
    return snapshot;
  }

  snapshot() {
    const snapshot = this.state.getSnapshot();

    return {
      ...snapshot,
      runtimeConfig: this.runtimeConfig,
      engineState: this.machine.state,
      engine: this.machine.snapshot(),
      engineProcess: {
        pid: process.pid,
        startedAt: new Date(this.startedAtEpochMs).toISOString(),
        startedAtEpochMs: this.startedAtEpochMs,
        runtimeDir: this.runtimeDir,
        snapshotPath: this.snapshotPath,
        deltaPath: this.deltaPath,
      },
      privateWsStatus: this.privateWsStatus,
      readiness: this.readiness,
      preparation: this.preparation,
      rateLimit: this.rateLimitScheduler.snapshot(),
      orderCapacity: this.rateLimitScheduler.orderCapacitySnapshot(),
      privateCacheStatus: this.buildPrivateCacheStatus({ includePolicyMaps: true }),
      guardStatus: {
        consecutiveFailures: this.riskGuard.consecutiveFailures,
        openOrderCount: this.riskGuard.openOrderCount,
        activeRealExecutionCount: this.activeRealExecutionCount,
        maxConsecutiveFailures: this.runtimeConfig.executionPolicy.realRunLimits.maxConsecutiveFailures,
        maxOpenOrders: this.runtimeConfig.executionPolicy.realRunLimits.maxOpenOrders,
        maxCyclesPerMinute: this.runtimeConfig.executionPolicy.realRunLimits.maxCyclesPerMinute,
        healthy:
          this.emergencyStop.active !== true &&
          this.riskGuard.consecutiveFailures < this.runtimeConfig.executionPolicy.realRunLimits.maxConsecutiveFailures &&
          this.riskGuard.openOrderCount < this.runtimeConfig.executionPolicy.realRunLimits.maxOpenOrders,
      },
      emergencyStop: this.emergencyStop.snapshot(),
      realRunLimits: this.realRunLimits.snapshot(),
      performanceBudget: buildPerformanceBudgetSnapshot(this.runtimeConfig),
      execution: {
        ...this.fillTracker.snapshot(),
        dryRunBalances: this.dryRunExecutor.balances,
        dryRunCapital: this.dryRunExecutor.capitalSnapshot(),
        realBalances: this.balanceTracker.snapshot(),
        mode: this.runtimeConfig.runMode,
        liveTradingEnabled: this.runtimeConfig.liveTradingEnabled,
      },
    };
  }

  stateDelta(now = new Date()) {
    const nowMs = now.getTime();

    return {
      engineState: this.state.engineState,
      engine: this.machine.snapshot(),
      lastCalculatedAt: this.state.lastCalculatedAt,
      wsStatus: this.state.wsStatus,
      feedStatus: {
        observation: this.state.wsStatus,
        validation: this.state.validationWsStatus,
      },
      runtimeConfig: this.runtimeConfig,
      orderbookStores: this.state.getOrderbookStoreStatus(nowMs),
      privateWsStatus: this.privateWsStatus,
      readiness: this.readiness,
      preparation: this.preparation,
      rateLimit: this.rateLimitScheduler.snapshot(nowMs),
      orderCapacity: this.rateLimitScheduler.orderCapacitySnapshot(nowMs),
      privateCacheStatus: this.buildPrivateCacheStatus(),
      guardStatus: {
        consecutiveFailures: this.riskGuard.consecutiveFailures,
        openOrderCount: this.riskGuard.openOrderCount,
        activeRealExecutionCount: this.activeRealExecutionCount,
        maxConsecutiveFailures: this.runtimeConfig.executionPolicy.realRunLimits.maxConsecutiveFailures,
        maxOpenOrders: this.runtimeConfig.executionPolicy.realRunLimits.maxOpenOrders,
        maxCyclesPerMinute: this.runtimeConfig.executionPolicy.realRunLimits.maxCyclesPerMinute,
        healthy:
          this.emergencyStop.active !== true &&
          this.riskGuard.consecutiveFailures < this.runtimeConfig.executionPolicy.realRunLimits.maxConsecutiveFailures &&
          this.riskGuard.openOrderCount < this.runtimeConfig.executionPolicy.realRunLimits.maxOpenOrders,
      },
      emergencyStop: this.emergencyStop.snapshot(),
      realRunLimits: this.realRunLimits.snapshot(nowMs),
      performanceBudget: buildPerformanceBudgetSnapshot(this.runtimeConfig),
      execution: {
        ...this.fillTracker.snapshot(),
        dryRunBalances: this.dryRunExecutor.balances,
        dryRunCapital: this.dryRunExecutor.capitalSnapshot(),
        realBalances: this.balanceTracker.snapshot(),
        mode: this.runtimeConfig.runMode,
        liveTradingEnabled: this.runtimeConfig.liveTradingEnabled,
      },
      eventLog: this.state.eventLog.slice(-200),
    };
  }

  delta(options = {}) {
    const now = options.now || new Date();
    const nowMs = now.getTime();

    if (options.forceAgingSweep || nowMs - this.lastAgingSweepAt >= this.agingSweepIntervalMs) {
      this.state.refreshAgingCycles(now);
      this.lastAgingSweepAt = nowMs;
    }

    const delta = this.state.consumeDelta(now);

    return {
      ...delta,
      stateDelta: this.stateDelta(now),
    };
  }

  async checkReadiness() {
    const restPermissions = await this.refreshPrivateCaches();
    const snapshot = this.state.getSnapshot();
    const readinessGuards = this.runtimeConfig.executionPolicy.readinessGuards || {};
    this.readiness = await checkRealRunReadiness({
      runtimeConfig: this.runtimeConfig,
      engineSnapshot: {
        ...snapshot,
        privateWsStatus: this.privateWsStatus,
        orderChanceFresh: this.isOrderChanceFresh(),
        accountBalanceFresh: this.isAccountBalanceFresh(),
      },
      restPermissions,
      logStore: this.logStore,
      ...readinessGuards,
    });
    await this.logStore.append("events", {
      type: "readiness.checked",
      passed: this.readiness.passed,
      score: this.readiness.score,
      passedCount: this.readiness.score && this.readiness.score.passed,
      failedCount: this.readiness.score && this.readiness.score.failed,
      failedItems: this.readiness.items.filter((entry) => !entry.passed).map((entry) => entry.id),
    });
    return this.readiness;
  }

  async writeSnapshot() {
    const snapshot = this.snapshot();
    await writeJsonAtomic(this.snapshotPath, snapshot);
    return snapshot;
  }

  async writeDelta(options = {}) {
    const delta = this.delta(options);
    await writeJsonAtomic(this.deltaPath, delta);
    return delta;
  }
}

async function startEngineRuntime(options = {}) {
  const runtime = new EngineRuntime(options);
  return runtime.start(options);
}

module.exports = {
  buildPerformanceBudgetSnapshot,
  EngineRuntime,
  startEngineRuntime,
  writeJsonAtomic,
};
