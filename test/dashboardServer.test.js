const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const WebSocket = require("ws");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { createDashboardRequestHandler, startDashboardServer } = require("../src/live/dashboardServer");
const { AppendOnlyLogStore } = require("../src/core/appendOnlyLog");

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve(server.address().port);
    });
  });
}

function close(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

test("dashboard command API queues commands and exposes command status", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-dashboard-"));
  const logStore = new AppendOnlyLogStore({ logDir: dir });
  const snapshotPath = path.join(dir, "snapshot.json");
  await fs.writeFile(snapshotPath, JSON.stringify({ engineState: "STOPPED", cycles: [], groups: [] }));
  const server = http.createServer(createDashboardRequestHandler({
    logStore,
    snapshotPath,
    publicDir: path.resolve(process.cwd(), "public"),
  }));
  const port = await listen(server);

  try {
    const accepted = await fetch(`http://127.0.0.1:${port}/api/command`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command: "Pause" }),
    });
    const rejected = await fetch(`http://127.0.0.1:${port}/api/command`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command: "Resync" }),
    });
    const commands = await logStore.readAll("commands");
    const acceptedPayload = await accepted.json();
    const commandStatus = await fetch(`http://127.0.0.1:${port}/api/commands/${acceptedPayload.commandId}`)
      .then((response) => response.json());

    assert.equal(accepted.status, 202);
    assert.equal(rejected.status, 400);
    assert.equal(commands.length, 1);
    assert.equal(commands[0].command, "Pause");
    assert.equal(commandStatus.status.status, "queued");
  } finally {
    await close(server);
  }
});

test("dashboard WebSocket sends one full-state then runtime deltas", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-dashboard-delta-"));
  const snapshotPath = path.join(dir, "latest-snapshot.json");
  const deltaPath = path.join(dir, "latest-delta.json");
  await fs.writeFile(snapshotPath, JSON.stringify({
    type: "full-state",
    engineState: "RUNNING",
    summary: { marketsLoaded: 1 },
    cycles: [],
    groups: [],
  }));
  const dashboard = await startDashboardServer({
    port: 0,
    snapshotPath,
    deltaPath,
    logDir: path.join(dir, "logs"),
    pushIntervalMs: 25,
  });
  const messages = [];
  const ws = new WebSocket(`${dashboard.url.replace("http:", "ws:")}/ws/live`);

  try {
    await new Promise((resolve, reject) => {
      ws.on("message", (data) => {
        messages.push(JSON.parse(data.toString("utf8")));
        if (messages.some((message) => message.type === "full-state")) {
          resolve();
        }
      });
      ws.on("error", reject);
    });

    await fs.writeFile(deltaPath, JSON.stringify({
      type: "delta",
      sentAtEpochMs: Date.now(),
      changedCycles: [],
      summaryDelta: { marketsLoaded: 2 },
      metrics: {},
      stateDelta: { engineState: "RUNNING" },
    }));

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for delta")), 1000);
      ws.on("message", (data) => {
        const message = JSON.parse(data.toString("utf8"));
        if (message.type === "delta") {
          clearTimeout(timer);
          resolve();
        }
      });
    });

    assert.equal(messages[0].type, "hello");
    assert.equal(messages.some((message) => message.type === "full-state"), true);
  } finally {
    ws.close();
    await dashboard.close();
  }
});

test("dashboard exposes filtered logs and dry-run report exports", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "q-gagarin-dashboard-report-"));
  const logStore = new AppendOnlyLogStore({ logDir: dir });
  const snapshotPath = path.join(dir, "snapshot.json");
  await logStore.ensureFiles();
  await logStore.append("decisions", {
    type: "strategy-decision",
    mode: "DRY_RUN",
    accepted: true,
    startAsset: "KRW",
    strategyId: "s1",
    cycleId: "c1",
  });
  await logStore.append("events", {
    type: "cycle.simulated_done",
    mode: "DRY_RUN",
    pnl: 12,
  });
  await fs.writeFile(snapshotPath, JSON.stringify({ engineState: "STOPPED", cycles: [], groups: [] }));
  const server = http.createServer(createDashboardRequestHandler({
    logStore,
    snapshotPath,
    publicDir: path.resolve(process.cwd(), "public"),
  }));
  const port = await listen(server);

  try {
    const logs = await fetch(`http://127.0.0.1:${port}/api/logs?mode=DRY_RUN&type=decision`).then((response) => response.json());
    const report = await fetch(`http://127.0.0.1:${port}/api/dry-run-report`).then((response) => response.json());
    const csv = await fetch(`http://127.0.0.1:${port}/api/dry-run-report?format=csv`).then((response) => response.text());

    assert.equal(logs.logs.length, 1);
    assert.equal(report.summary.simulatedCompleteCycles, 1);
    assert.match(csv, /simulatedNetProfit/);
  } finally {
    await close(server);
  }
});
