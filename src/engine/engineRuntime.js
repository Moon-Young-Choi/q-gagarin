const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { LiveTriangleState, parseFeeRate } = require("../live/liveState");
const { UpbitWsOrderbookClient } = require("../upbit/wsOrderbookClient");
const { loadRuntimeConfig } = require("../core/runtimeConfig");
const { AppendOnlyLogStore } = require("../core/appendOnlyLog");
const { RunStateMachine, STATES, normalizeCommand } = require("../core/runStateMachine");
const { UpbitPrivateWsClient } = require("../exchanges/upbit/privateWsClient");
const { UpbitExchangeRestClient } = require("../exchanges/upbit/exchangeRestClient");
const { FillTracker } = require("../execution/fillTracker");
const { DryRunExecutor } = require("../execution/dryRunExecutor");
const { RealExecutor } = require("../execution/realExecutor");
const { RiskGuard } = require("../execution/riskGuard");
const { checkRealRunReadiness } = require("../core/readinessChecker");

async function writeJsonAtomic(filePath, payload) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tmpPath = `${filePath}.${process.pid}.${crypto.randomUUID()}.tmp`;
  await fs.writeFile(tmpPath, `${JSON.stringify(payload, null, 2)}\n`);
  await fs.rename(tmpPath, filePath);
}

class EngineRuntime {
  constructor(options = {}) {
    this.runtimeDir = options.runtimeDir || path.resolve(process.cwd(), "out", "runtime");
    this.snapshotPath = options.snapshotPath || path.join(this.runtimeDir, "latest-snapshot.json");
    this.commandPollIntervalMs = options.commandPollIntervalMs || 500;
    this.snapshotIntervalMs = options.snapshotIntervalMs || 1000;
    this.fallbackIntervalMs = null;
    this.logStore = options.logStore || new AppendOnlyLogStore({
      logDir: options.logDir || path.resolve(process.cwd(), "out", "logs"),
    });
    this.runtimeConfig = options.runtimeConfig || loadRuntimeConfig({
      configPath: options.runtimeConfigPath,
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    this.restClient = options.restClient || (
      this.runtimeConfig.liveTradingEnabled || (process.env.UPBIT_ACCESS_KEY && process.env.UPBIT_SECRET_KEY)
        ? new UpbitExchangeRestClient({
            liveTradingEnabled: this.runtimeConfig.liveTradingEnabled,
            chanceTtlMs: this.runtimeConfig.executionPolicy.executionGuards.orderChanceTtlMs,
          })
        : null
    );
    this.state = options.state || new LiveTriangleState({
      feeRate: parseFeeRate(process.env.UPBIT_TAKER_FEE_RATE, 0),
      staleOrderbookMs: options.staleOrderbookMs || Number.parseInt(process.env.STALE_ORDERBOOK_MS || "3000", 10),
      runtimeConfig: this.runtimeConfig,
      logStore: this.logStore,
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
    this.dryRunExecutor = options.dryRunExecutor || new DryRunExecutor({
      logStore: this.logStore,
      simulatedBalances: this.runtimeConfig.executionPolicy.simulatedBalances,
      validationConfig: this.runtimeConfig.candidateValidation,
    });
    this.riskGuard = options.riskGuard || new RiskGuard({
      config: this.runtimeConfig.executionPolicy,
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
    this.orderChanceCacheUpdatedAt = null;
    this.accountBalanceUpdatedAt = null;
    this.activeRealExecutionCount = 0;
    this.executionCooldownMs = options.executionCooldownMs || 5000;
    this.lastExecutionByCycleId = new Map();
    this.commandTimer = null;
    this.snapshotTimer = null;
    this.fallbackTimer = null;
    this.processedCommandKeys = new Set();
    this.started = false;

    this.state.setExecutionHandler((plan, metadata) => this.handleExecutionCandidate(plan, metadata));
  }

  async initialize() {
    await this.logStore.ensureFiles();

    if (!this.started) {
      await this.state.initialize();
      await this.state.loadInitialOrderbooks({
        batchSize: this.orderbookBatchSize,
        delayMs: this.orderbookDelayMs,
        markDirty: false,
      });
      this.createFeedClients();
      this.started = true;
    }

    await this.writeSnapshot();
  }

  createFeedClients() {
    this.observationClient = this.observationClient || new UpbitWsOrderbookClient(this.state.requiredMarkets || [], {
      chunkSize: this.wsMarketsPerConnection,
      orderbookUnit: this.runtimeConfig.observationOrderbookUnit,
    });
    this.validationClient = this.validationClient || new UpbitWsOrderbookClient(this.state.requiredMarkets || [], {
      chunkSize: this.wsMarketsPerConnection,
      orderbookUnit: this.runtimeConfig.validationOrderbookUnit,
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

    if (this.runtimeConfig.liveTradingEnabled || (process.env.UPBIT_ACCESS_KEY && process.env.UPBIT_SECRET_KEY)) {
      this.privateWsClient = this.privateWsClient || new UpbitPrivateWsClient();
      this.privateWsClient.on("myOrder", (event) => {
        this.fillTracker.handleMyOrder(event);
      });
      this.privateWsClient.on("status", (status) => {
        this.privateWsStatus = status;
        const disconnected = status.status !== "open" && status.stopped !== true;
        if (disconnected) {
          const guard = this.riskGuard.evaluatePrivateWsDisconnect(this.activeRealExecutionCount);
          if (!guard.ok && guard.emergencyStop) {
            const error = new Error(guard.rejectionReason);
            this.machine.fail(error);
            this.state.engineState = this.machine.state;
            this.logStore.append("errors", {
              type: "emergency_stop",
              source: "private-ws",
              message: error.message,
            }).catch(() => {});
          }
        }
      });
      this.privateWsClient.on("error", (error) => {
        this.state.logEvent("error", { source: "private-ws", error });
        this.logStore.append("errors", { source: "private-ws", error }).catch(() => {});
      });
    }
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
    this.fallbackTimer = setInterval(() => {
      this.fallbackPoll().catch((error) => {
        this.logStore.append("errors", { source: "fallback", message: error.message }).catch(() => {});
      });
    }, Math.max(this.state.staleOrderbookMs, 5000));

    return this;
  }

  async stop() {
    if (this.commandTimer) clearInterval(this.commandTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    if (this.fallbackTimer) clearInterval(this.fallbackTimer);
    this.stopFeeds();
    await this.writeSnapshot();
  }

  startFeeds() {
    this.observationClient.start();
    this.validationClient.start();
    if (this.privateWsClient) {
      this.privateWsClient.start();
    }
  }

  stopFeeds() {
    if (this.observationClient) this.observationClient.stop();
    if (this.validationClient) this.validationClient.stop();
    if (this.privateWsClient) this.privateWsClient.stop();
  }

  async applyCommand(commandInput, metadata = {}) {
    const command = normalizeCommand(commandInput);
    const previousState = this.machine.state;

    if (command === "Start" && this.runtimeConfig.runMode === "REAL_GUARDED") {
      const readiness = await this.checkReadiness();
      if (!readiness.passed) {
        await this.logStore.append("events", {
          type: "readiness.blocked",
          command,
          readiness,
          ...metadata,
        });
        throw new Error("REAL_GUARDED readiness checklist failed");
      }
    }

    const nextState = this.machine.apply(command);

    if (command === "Start" && nextState === STATES.RUNNING && previousState === STATES.STOPPED) {
      this.startFeeds();
    }

    if (command === "Stop" && nextState === STATES.STOPPED) {
      this.fillTracker.handleStopPolicy(this.runtimeConfig.executionPolicy.stopPolicy);
      this.stopFeeds();
    }

    this.state.engineState = nextState;
    await this.logStore.append("events", {
      type: "engine.command",
      command,
      previousState,
      nextState,
      ...metadata,
    });
    await this.writeSnapshot();
    return nextState;
  }

  async processCommands() {
    const commands = await this.logStore.readAll("commands", { limit: 1000 });

    for (const command of commands) {
      const key = command.commandId || `${command.timestamp}:${command.command}`;
      if (this.processedCommandKeys.has(key)) continue;
      this.processedCommandKeys.add(key);
      try {
        await this.applyCommand(command.command, {
          commandId: command.commandId,
          source: command.source || "dashboard",
        });
      } catch (error) {
        await this.logStore.append("errors", {
          type: "engine.command.rejected",
          command: command.command,
          commandId: command.commandId,
          message: error.message,
        });
      }
    }
  }

  async fallbackPoll() {
    if (this.machine.state !== STATES.RUNNING || !this.state.shouldUseFallback()) {
      return;
    }

    await this.state.fallbackPoll({
      batchSize: this.orderbookBatchSize,
      delayMs: this.orderbookDelayMs,
    });
  }

  firstRequiredMarket() {
    return this.state.requiredMarkets && this.state.requiredMarkets[0] || "KRW-BTC";
  }

  isFresh(timestamp, ttlMs) {
    return timestamp !== null &&
      timestamp !== undefined &&
      Date.now() - Number(timestamp) <= Number(ttlMs || 0);
  }

  isOrderChanceFresh() {
    return this.isFresh(
      this.orderChanceCacheUpdatedAt,
      this.runtimeConfig.executionPolicy.executionGuards.orderChanceTtlMs,
    );
  }

  isAccountBalanceFresh() {
    return this.isFresh(
      this.accountBalanceUpdatedAt,
      this.runtimeConfig.executionPolicy.executionGuards.accountBalanceTtlMs,
    );
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
      this.accountBalanceUpdatedAt = now;
    }

    if (this.restPermissions.viewOrdersChance) {
      this.orderChanceCacheUpdatedAt = now;
    }

    return this.restPermissions;
  }

  validationDepthFresh() {
    const status = this.state.getOrderbookStoreStatus();
    return status.validation && status.validation.staleCount === 0;
  }

  currentGuardContext() {
    return {
      privateWsConnected: this.privateWsStatus.status === "open",
      orderChanceFresh: this.isOrderChanceFresh(),
      accountBalanceFresh: this.isAccountBalanceFresh(),
      validationDepthFresh: this.validationDepthFresh(),
      nowMs: Date.now(),
      dailyLoss: 0,
    };
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

    if (this.shouldThrottleExecution(plan)) {
      return null;
    }

    if (this.runtimeConfig.runMode === "DRY_RUN") {
      return this.dryRunExecutor.execute({
        ...plan,
        engineState: this.machine.state,
      });
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
      const result = await this.realExecutor.execute(plan, {
        ...this.currentGuardContext(),
        getGuardContext: () => this.currentGuardContext(),
      });

      if (result && result.emergencyStop) {
        const error = new Error(result.reason || "REAL_EXECUTION_EMERGENCY_STOP");
        this.machine.fail(error);
        this.state.engineState = this.machine.state;
        await this.logStore.append("errors", {
          type: "emergency_stop",
          mode: "REAL",
          planId: plan.planId,
          cycleId: plan.cycleId,
          message: error.message,
        });
      }

      return result;
    } catch (error) {
      const failed = this.riskGuard.recordFailure(error.message);
      await this.logStore.append("errors", {
        type: "real_execution_error",
        mode: "REAL",
        planId: plan.planId,
        cycleId: plan.cycleId,
        message: error.message,
        emergencyStop: failed.emergencyStop,
      });

      if (failed.emergencyStop) {
        this.machine.fail(error);
        this.state.engineState = this.machine.state;
      }

      return {
        ok: false,
        reason: error.message,
        emergencyStop: failed.emergencyStop,
      };
    } finally {
      this.activeRealExecutionCount = Math.max(0, this.activeRealExecutionCount - 1);
    }
  }

  snapshot() {
    const snapshot = this.state.getSnapshot();

    return {
      ...snapshot,
      engine: this.machine.snapshot(),
      engineProcess: {
        pid: process.pid,
        runtimeDir: this.runtimeDir,
        snapshotPath: this.snapshotPath,
      },
      privateWsStatus: this.privateWsStatus,
      readiness: this.readiness,
      privateCacheStatus: {
        orderChanceFresh: this.isOrderChanceFresh(),
        accountBalanceFresh: this.isAccountBalanceFresh(),
        orderChanceCacheUpdatedAt: this.orderChanceCacheUpdatedAt,
        accountBalanceUpdatedAt: this.accountBalanceUpdatedAt,
        restPermissions: this.restPermissions,
      },
      guardStatus: {
        consecutiveFailures: this.riskGuard.consecutiveFailures,
        openOrderCount: this.riskGuard.openOrderCount,
        activeRealExecutionCount: this.activeRealExecutionCount,
        maxConsecutiveFailures: this.runtimeConfig.executionPolicy.realRunLimits.maxConsecutiveFailures,
        maxOpenOrders: this.runtimeConfig.executionPolicy.realRunLimits.maxOpenOrders,
        maxCyclesPerMinute: this.runtimeConfig.executionPolicy.realRunLimits.maxCyclesPerMinute,
        healthy:
          this.riskGuard.consecutiveFailures < this.runtimeConfig.executionPolicy.realRunLimits.maxConsecutiveFailures &&
          this.riskGuard.openOrderCount < this.runtimeConfig.executionPolicy.realRunLimits.maxOpenOrders,
      },
      execution: {
        mode: this.runtimeConfig.runMode,
        liveTradingEnabled: this.runtimeConfig.liveTradingEnabled,
        dryRunBalances: this.dryRunExecutor.balances,
        ...this.fillTracker.snapshot(),
      },
    };
  }

  async checkReadiness() {
    const restPermissions = await this.refreshPrivateCaches();
    const snapshot = this.state.getSnapshot();
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
    });
    await this.logStore.append("events", {
      type: "readiness.checked",
      passed: this.readiness.passed,
      failedItems: this.readiness.items.filter((entry) => !entry.passed).map((entry) => entry.id),
    });
    return this.readiness;
  }

  async writeSnapshot() {
    const snapshot = this.snapshot();
    await writeJsonAtomic(this.snapshotPath, snapshot);
    return snapshot;
  }
}

async function startEngineRuntime(options = {}) {
  const runtime = new EngineRuntime(options);
  return runtime.start(options);
}

module.exports = {
  EngineRuntime,
  startEngineRuntime,
  writeJsonAtomic,
};
