const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { EngineRuntime } = require("../src/engine/engineRuntime");
const { DEFAULT_RUNTIME_CONFIG } = require("../src/core/runtimeConfig");
const { AppendOnlyLogStore } = require("../src/core/appendOnlyLog");
const { CommandStatusStore } = require("../src/core/commandStatusStore");

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

test("engine runtime skips commands created before process start", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-engine-old-command-"));
  const logStore = new AppendOnlyLogStore({ logDir: path.join(dir, "logs") });
  await logStore.ensureFiles();
  const oldCommand = await logStore.append("commands", {
    type: "engine.command.requested",
    command: "Start",
    commandId: "11111111-1111-4111-8111-111111111111",
    source: "dashboard",
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
    type: "engine.command.requested",
    command: "Start",
    commandId: "22222222-2222-4222-8222-222222222222",
    runMode: "DRY_RUN",
    source: "dashboard",
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

  assert.equal(runtime.machine.state, "RUNNING");
  assert.equal(runtime.runtimeConfig.runMode, "DRY_RUN");
  assert.equal(observationClient.starts, 1);
  assert.equal(status.status, "accepted");
  assert.equal(delta.type, "delta");
  assert.equal(delta.stateDelta.engineState, "RUNNING");
});
