const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { LiveTriangleState, parseFeeRate } = require("../live/liveState");
const { UpbitWsOrderbookClient } = require("../upbit/wsOrderbookClient");
const { freezeRuntimeConfig, loadRuntimeConfig, RUN_MODES } = require("../core/runtimeConfig");
const { AppendOnlyLogStore } = require("../core/appendOnlyLog");
const { CommandStatusStore } = require("../core/commandStatusStore");
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
    this.wsConnectionDelayMs = options.wsConnectionDelayMs !== undefined
      ? options.wsConnectionDelayMs
      : Number.parseInt(process.env.UPBIT_WS_CONNECTION_DELAY_MS || "1000", 10);
    this.validationFeedStartDelayMs = options.validationFeedStartDelayMs !== undefined
      ? options.validationFeedStartDelayMs
      : Number.parseInt(process.env.UPBIT_VALIDATION_WS_START_DELAY_MS || "4000", 10);
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
    this.deltaTimer = null;
    this.fallbackTimer = null;
    this.validationStartTimer = null;
    this.processedCommandKeys = new Set();
    this.startedAtEpochMs = options.startedAtEpochMs || Date.now();
    this.lastAgingSweepAt = 0;
    this.started = false;

    this.state.setExecutionHandler((plan, metadata) => this.handleExecutionCandidate(plan, metadata));
  }

  async initialize() {
    await this.logStore.ensureFiles();
    await this.seedProcessedCommands();

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
      connectionDelayMs: this.wsConnectionDelayMs,
      orderbookUnit: this.runtimeConfig.observationOrderbookUnit,
    });
    this.validationClient = this.validationClient || new UpbitWsOrderbookClient(this.state.requiredMarkets || [], {
      chunkSize: this.wsMarketsPerConnection,
      connectionDelayMs: this.wsConnectionDelayMs,
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

    return this;
  }

  async stop() {
    if (this.commandTimer) clearInterval(this.commandTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    if (this.deltaTimer) clearInterval(this.deltaTimer);
    if (this.fallbackTimer) clearInterval(this.fallbackTimer);
    this.stopFeeds();
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
    this.observationClient.start();
    clearTimeout(this.validationStartTimer);
    this.validationStartTimer = setTimeout(() => {
      this.validationClient.start();
    }, Math.max(0, this.validationFeedStartDelayMs));
    if (this.privateWsClient) {
      this.privateWsClient.start();
    }
  }

  stopFeeds() {
    clearTimeout(this.validationStartTimer);
    if (this.observationClient) this.observationClient.stop();
    if (this.validationClient) this.validationClient.stop();
    if (this.privateWsClient) this.privateWsClient.stop();
  }

  setRunMode(runMode) {
    const requestedRunMode = String(runMode || "").trim().toUpperCase();

    if (!RUN_MODES.has(requestedRunMode)) {
      throw new Error(`Invalid runMode: ${runMode}`);
    }

    this.runtimeConfig = freezeRuntimeConfig({
      ...this.runtimeConfig,
      runMode: requestedRunMode,
    }, {
      allowLiveTrading: process.env.Q_GAGARIN_ALLOW_LIVE_TRADING === "true",
    });
    this.state.setRuntimeConfig(this.runtimeConfig);
    return this.runtimeConfig.runMode;
  }

  async applyCommand(commandInput, metadata = {}) {
    const command = normalizeCommand(commandInput);
    const previousState = this.machine.state;
    const previousConfig = this.runtimeConfig;
    let configChanged = false;

    if (command === "Start" && metadata.runMode) {
      this.setRunMode(metadata.runMode);
      configChanged = true;
    }

    if (command === "Start" && this.runtimeConfig.runMode === "REAL_GUARDED") {
      const readiness = await this.checkReadiness();
      if (!readiness.passed) {
        if (configChanged) {
          this.runtimeConfig = previousConfig;
          this.state.setRuntimeConfig(previousConfig);
        }
        await this.logStore.append("events", {
          type: "readiness.blocked",
          command,
          readiness,
          ...metadata,
        });
        throw new Error("REAL_GUARDED readiness checklist failed");
      }
    }

    let nextState;
    try {
      nextState = this.machine.apply(command);
    } catch (error) {
      if (configChanged) {
        this.runtimeConfig = previousConfig;
        this.state.setRuntimeConfig(previousConfig);
      }
      throw error;
    }

    if (command === "Start" && nextState === STATES.RUNNING && previousState === STATES.STOPPED) {
      this.startFeeds();
    }

    if (command === "Stop" && nextState === STATES.STOPPED) {
      this.fillTracker.handleStopPolicy(this.runtimeConfig.executionPolicy.stopPolicy);
      this.stopFeeds();
    }

    this.state.engineState = nextState;
    const event = await this.logStore.append("events", {
      type: "engine.command",
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
        source: metadata.source || "dashboard",
      });
    }
    await this.writeSnapshot();
    await this.writeDelta({ forceAgingSweep: true });
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
          runMode: command.runMode,
          emergency: command.emergency,
        });
      } catch (error) {
        await this.logStore.append("errors", {
          type: "engine.command.rejected",
          command: command.command,
          commandId: command.commandId,
          message: error.message,
        });
        if (command.commandId) {
          await this.commandStatusStore.write(command.commandId, {
            status: "rejected",
            command: command.command,
            runMode: command.runMode,
            source: command.source || "dashboard",
            message: error.message,
          });
        }
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
  EngineRuntime,
  startEngineRuntime,
  writeJsonAtomic,
};
