const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { CommandInbox } = require("../src/core/commandInbox");
const { CommandStatusStore } = require("../src/core/commandStatusStore");
const { DEFAULT_RUNTIME_CONFIG } = require("../src/core/runtimeConfig");
const { createCliContext, filterNewLogs, runOnce } = require("../src/cli/commandRegistry");
const { parseSlashCommand } = require("../src/cli/commandParser");

function memoryOutput() {
  return {
    text: "",
    write(chunk) {
      this.text += chunk;
    },
  };
}

test("CLI status reads snapshot without HTTP or browser objects", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-status-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "RUNNING",
    runtimeConfig: {
      runMode: "DRY_RUN",
      exchange: "upbit",
      liveTradingEnabled: false,
    },
    summary: {
      marketsLoaded: 3,
      uniqueTriangleCount: 1,
      plottedCycleCount: 2,
    },
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    output: memoryOutput(),
    statusPollTimeoutMs: 1,
  });

  const result = await runOnce(parseSlashCommand("/status"), context);

  assert.match(result.output, /Engine\s+RUNNING/);
  assert.match(result.output, /Mode\s+DRY_RUN/);
  assert.match(result.output, /Markets loaded\s+3/);
});

test("CLI latency hides legacy browser metrics from stale snapshots", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-latency-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "RUNNING",
    runtimeConfig: {
      runMode: "DRY_RUN",
    },
    performanceBudget: {
      marketData: {},
      decision: {},
      execution: {},
      displayLatencyAffectsTrading: false,
    },
    metrics: {
      browser: {
        renderSampleCount: 1,
      },
      rates: {
        browserRenderedFramesPerSec: 10,
        recalculatedCyclesPerSec: 2,
      },
      counters: {
        browserRenderedFrames: 10,
        recalculatedCycles: 20,
      },
    },
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    output: memoryOutput(),
  });

  const result = await runOnce(parseSlashCommand("/latency"), context);

  assert.doesNotMatch(result.output, /browser/i);
  assert.match(result.output, /display\s+affectsTrading\s+no/);
  assert.match(result.output, /recalculatedCyclesPerSec/);
});

test("CLI start queues atomic command inbox file", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-start-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "STOPPED",
    runtimeConfig: {
      runMode: "OBSERVE",
    },
    summary: {},
  }));
  const commandStatusStore = new CommandStatusStore({ runtimeDir: dir });
  const commandInbox = new CommandInbox({
    runtimeDir: dir,
    randomUUID: () => "22222222-2222-4222-8222-222222222222",
    now: () => new Date("2026-07-06T12:00:00.000Z"),
  });
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    commandInbox,
    commandStatusStore,
    output: memoryOutput(),
    pollIntervalMs: 1,
    statusPollTimeoutMs: 1,
  });

  const result = await runOnce(parseSlashCommand("/start dry"), context);
  const pending = await commandInbox.listPending();
  const status = await commandStatusStore.read("22222222-2222-4222-8222-222222222222");

  assert.match(result.output, /Command queued: Start/);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].record.runMode, "DRY_RUN");
  assert.equal(pending[0].record.source, "cli");
  assert.equal(status.status, "queued");
});

test("CLI desk exports ranking and opportunity detail without Plotly", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-desk-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "RUNNING",
    runtimeConfig: {
      runMode: "DRY_RUN",
    },
    summary: {},
    cycles: [
      {
        cycleId: "KRW-BTC-ETH-KRW",
        startAsset: "KRW",
        route: ["KRW", "BTC", "ETH", "KRW"],
        netProfitRate: 0.003,
        maxExecutableStartAmount: 10000,
        limitingLeg: "leg2",
        validationStatus: "accepted",
        latency: { decisionAgeMs: 12 },
        executionPlan: { planId: "plan-1" },
      },
      {
        cycleId: "BTC-ETH-KRW-BTC",
        startAsset: "BTC",
        route: ["BTC", "ETH", "KRW", "BTC"],
        netProfitRate: 0.001,
      },
    ],
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    output: memoryOutput(),
  });

  const desk = await runOnce(parseSlashCommand("/desk --start KRW --top 1"), context);
  const detail = await runOnce(parseSlashCommand("/opportunity show #1"), context);
  const csv = await runOnce(parseSlashCommand("/export desk --format csv --start KRW"), context);

  assert.match(desk.output, /KRW->BTC->ETH->KRW/);
  assert.match(detail.output, /KRW-BTC-ETH-KRW/);
  assert.match(csv.output, /rank,startAsset,cycleId,route/);
  assert.match(csv.output, /KRW-BTC-ETH-KRW/);
});

test("CLI strategy select writes active config only when engine is stopped", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-strategy-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  const configPath = path.join(dir, "runtime.json");
  const draftConfigPath = path.join(dir, "runtime.draft.json");
  await fs.writeFile(configPath, JSON.stringify(DEFAULT_RUNTIME_CONFIG));
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "STOPPED",
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    summary: {},
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    configPath,
    draftConfigPath,
    output: memoryOutput(),
  });

  const result = await runOnce(parseSlashCommand("/strategy select depthAwareLimitIoc"), context);
  const active = JSON.parse(await fs.readFile(configPath, "utf8"));

  assert.match(result.output, /Strategy selected/);
  assert.equal(active.activeStrategyId, "depthAwareLimitIoc");
});

test("CLI config draft set validates and avoids active mutation while running", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-config-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  const configPath = path.join(dir, "runtime.json");
  const draftConfigPath = path.join(dir, "runtime.draft.json");
  await fs.writeFile(configPath, JSON.stringify(DEFAULT_RUNTIME_CONFIG));
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "RUNNING",
    runtimeConfig: DEFAULT_RUNTIME_CONFIG,
    summary: {},
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    configPath,
    draftConfigPath,
    output: memoryOutput(),
  });

  const setResult = await runOnce(
    parseSlashCommand("/config draft set candidateValidation.minNetProfitRate 0.0002"),
    context,
  );
  const saveResult = await runOnce(parseSlashCommand("/config draft save"), context);
  const active = JSON.parse(await fs.readFile(configPath, "utf8"));
  const draft = JSON.parse(await fs.readFile(draftConfigPath, "utf8"));

  assert.match(setResult.output, /Draft updated/);
  assert.match(saveResult.output, /active config was not changed/);
  assert.equal(active.candidateValidation.minNetProfitRate, 0);
  assert.equal(draft.candidateValidation.minNetProfitRate, 0.0002);
});

test("CLI log follow helper returns only unseen log records", () => {
  const context = {
    followSeenLogKeys: new Set(),
  };
  const first = [
    { eventId: "a", timestamp: "2026-07-06T00:00:00.000Z", type: "one" },
    { eventId: "b", timestamp: "2026-07-06T00:00:01.000Z", type: "two" },
  ];
  const second = [
    { eventId: "a", timestamp: "2026-07-06T00:00:00.000Z", type: "one" },
    { eventId: "c", timestamp: "2026-07-06T00:00:02.000Z", type: "three" },
  ];

  assert.deepEqual(filterNewLogs(first, context).map((row) => row.eventId), ["a", "b"]);
  assert.deepEqual(filterNewLogs(second, context).map((row) => row.eventId), ["c"]);
});

test("CLI market and execution subcommands render focused views", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-subviews-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "RUNNING",
    runtimeConfig: {
      runMode: "DRY_RUN",
      observationOrderbookUnit: 5,
      validationOrderbookUnit: 30,
    },
    summary: {
      requiredMarketCount: 2,
    },
    wsStatus: {
      status: "open",
      openConnectionCount: 1,
      connections: [{}],
    },
    validationWsStatus: {
      status: "open",
      openConnectionCount: 1,
      connections: [{}],
    },
    privateWsStatus: {
      status: "not_configured",
    },
    orderbookStores: {
      observation: { marketCount: 2, staleCount: 0, oldestAgeMs: 10 },
      validation: { marketCount: 2, staleCount: 1, oldestAgeMs: 20 },
    },
    execution: {
      latestOrders: [
        { timestamp: "t1", market: "KRW-BTC", side: "bid", status: "submitted", identifier: "id-1" },
      ],
      latestFills: [
        { timestamp: "t2", market: "KRW-BTC", side: "bid", volume: 0.1, paidFee: 1 },
      ],
      realBalances: {
        residualBalances: { BTC: 0.01 },
        residualEvents: [
          { timestamp: "t3", asset: "BTC", amount: 0.01, reason: "partial", cycleId: "cycle-1" },
        ],
      },
    },
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    output: memoryOutput(),
  });

  const feeds = await runOnce(parseSlashCommand("/market feeds"), context);
  const orders = await runOnce(parseSlashCommand("/execution orders"), context);
  const residuals = await runOnce(parseSlashCommand("/execution residuals"), context);

  assert.match(feeds.output, /Market Feeds/);
  assert.match(feeds.output, /observation/);
  assert.match(orders.output, /KRW-BTC/);
  assert.match(residuals.output, /BTC/);
  assert.match(residuals.output, /partial/);
});

test("CLI system subcommands and exchange view render without browser services", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-cli-system-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    engineState: "RUNNING",
    runtimeConfig: {
      runMode: "DRY_RUN",
      exchange: "upbit",
    },
    engineProcess: {
      pid: 123,
      runtimeDir: path.join(dir, "runtime"),
      snapshotPath,
      deltaPath: path.join(dir, "latest-delta.json"),
    },
    metrics: {
      cpu: { processCpuPercent: 1.2 },
      memory: { rss: 1000, heapUsed: 500 },
      eventLoop: { utilization: 0.1, delay: { p95Ms: 2 } },
      rates: { recalculatedCyclesPerSec: 3 },
    },
    guardStatus: {
      healthy: true,
      consecutiveFailures: 0,
      maxConsecutiveFailures: 3,
      openOrderCount: 0,
      maxOpenOrders: 2,
      activeRealExecutionCount: 0,
    },
    readiness: {
      passed: true,
    },
    performanceBudget: {
      marketData: {},
      decision: {},
      execution: {},
      displayLatencyAffectsTrading: false,
    },
  }));
  const context = createCliContext({
    runtimeDir: dir,
    logDir: path.join(dir, "logs"),
    snapshotPath,
    output: memoryOutput(),
  });

  const exchanges = await runOnce(parseSlashCommand("/market exchanges"), context);
  const perf = await runOnce(parseSlashCommand("/system perf"), context);
  const guards = await runOnce(parseSlashCommand("/system guards"), context);
  const files = await runOnce(parseSlashCommand("/system files"), context);
  const latency = await runOnce(parseSlashCommand("/system latency"), context);

  assert.match(exchanges.output, /upbit\s+enabled/);
  assert.match(perf.output, /System Performance/);
  assert.match(perf.output, /recalculatedCyclesPerSec/);
  assert.match(guards.output, /System Guards/);
  assert.match(guards.output, /Healthy\s+yes/);
  assert.match(files.output, /System Files/);
  assert.match(files.output, /Command inbox/);
  assert.match(latency.output, /display\s+affectsTrading\s+no/);
});
