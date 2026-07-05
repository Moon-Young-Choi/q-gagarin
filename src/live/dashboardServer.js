const http = require("node:http");
const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { URL } = require("node:url");
const WebSocket = require("ws");
const { AppendOnlyLogStore } = require("../core/appendOnlyLog");
const { CommandStatusStore } = require("../core/commandStatusStore");
const { normalizeCommand } = require("../core/runStateMachine");
const {
  readFilteredLogs,
  summarizeDryRun,
  dryRunReportCsv,
} = require("./logReadModel");
const { formatLocalTimestampForFilename } = require("./liveUtils");

function contentTypeFor(filePath) {
  const extension = path.extname(filePath);

  if (extension === ".html") return "text/html; charset=utf-8";
  if (extension === ".css") return "text/css; charset=utf-8";
  if (extension === ".js") return "application/javascript; charset=utf-8";
  if (extension === ".json") return "application/json; charset=utf-8";
  if (extension === ".png") return "image/png";
  return "application/octet-stream";
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(`${JSON.stringify(payload)}\n`);
}

function sendText(res, statusCode, text) {
  res.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(text);
}

function isLocalRequest(req) {
  return ["127.0.0.1", "::1", "::ffff:127.0.0.1"].includes(req.socket.remoteAddress);
}

async function readJsonBody(req, maxBytes = 1024 * 1024) {
  let body = "";

  for await (const chunk of req) {
    body += chunk;

    if (Buffer.byteLength(body) > maxBytes) {
      throw new Error("Request body is too large");
    }
  }

  return JSON.parse(body || "{}");
}

function resolvePublicPath(publicDir, pathname) {
  const requestPath = pathname === "/" ? "/live-dashboard.html" : pathname;
  const normalized = path.normalize(requestPath).replace(/^(\.\.[/\\])+/, "");
  const resolved = path.join(publicDir, normalized);

  return resolved.startsWith(publicDir) ? resolved : null;
}

async function serveFile(res, filePath) {
  try {
    const data = await fs.readFile(filePath);
    res.writeHead(200, {
      "Content-Type": contentTypeFor(filePath),
      "Cache-Control": "no-store",
    });
    res.end(data);
  } catch (error) {
    sendText(res, error.code === "ENOENT" ? 404 : 500, error.code === "ENOENT" ? "Not found" : error.message);
  }
}

async function readSnapshot(snapshotPath) {
  try {
    return JSON.parse(await fs.readFile(snapshotPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return {
        type: "full-state",
        summary: {
          marketsLoaded: 0,
          uniqueTriangleCount: 0,
          plottedCycleCount: 0,
        },
        cycles: [],
        groups: [],
        engine: {
          state: "STOPPED",
        },
        engineState: "STOPPED",
        eventLog: [],
      };
    }

    throw error;
  }
}

async function readDelta(deltaPath) {
  try {
    return JSON.parse(await fs.readFile(deltaPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }

    throw error;
  }
}

async function saveCapture(capturesDir, payload) {
  const { imageDataUrl, snapshot, timestamp } = payload;

  if (typeof imageDataUrl !== "string" || !imageDataUrl.startsWith("data:image/png;base64,")) {
    throw new Error("imageDataUrl must be a PNG data URL");
  }

  const captureDate = timestamp ? new Date(timestamp) : new Date();
  const stamp = formatLocalTimestampForFilename(Number.isNaN(captureDate.getTime()) ? new Date() : captureDate);
  const baseName = `upbit-triangle-live-${stamp}`;
  const pngPath = path.join(capturesDir, `${baseName}.png`);
  const jsonPath = path.join(capturesDir, `${baseName}.json`);

  await fs.mkdir(capturesDir, { recursive: true });
  await fs.writeFile(pngPath, Buffer.from(imageDataUrl.slice("data:image/png;base64,".length), "base64"));
  await fs.writeFile(jsonPath, `${JSON.stringify({ timestamp: captureDate.toISOString(), snapshot }, null, 2)}\n`);

  return { pngPath, jsonPath };
}

function createDashboardRequestHandler(options) {
  const {
    publicDir = path.resolve(process.cwd(), "public"),
    capturesDir = path.resolve(process.cwd(), "out", "captures"),
    snapshotPath = path.resolve(process.cwd(), "out", "runtime", "latest-snapshot.json"),
    commandStatusStore = new CommandStatusStore({
      runtimeDir: path.dirname(snapshotPath),
    }),
    plotlyPath = require.resolve("plotly.js-dist-min/plotly.min.js"),
    logStore = new AppendOnlyLogStore(),
    sseClients = new Set(),
  } = options;

  return async function requestHandler(req, res) {
    if (!isLocalRequest(req)) {
      sendJson(res, 403, { ok: false, error: "localhost clients only" });
      return;
    }

    const requestUrl = new URL(req.url, "http://localhost");

    try {
      if (req.method === "GET" && requestUrl.pathname === "/api/health") {
        const snapshot = await readSnapshot(snapshotPath);
        sendJson(res, 200, {
          ok: true,
          dashboard: true,
          engineState: snapshot.engineState || (snapshot.engine && snapshot.engine.state) || "UNKNOWN",
        });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/state") {
        sendJson(res, 200, await readSnapshot(snapshotPath));
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/logs") {
        const kind = requestUrl.searchParams.get("kind") || "all";
        const limit = Number.parseInt(requestUrl.searchParams.get("limit") || "200", 10);
        const filters = {
          kind,
          limit: Number.isInteger(limit) ? limit : 200,
          mode: requestUrl.searchParams.get("mode") || "",
          type: requestUrl.searchParams.get("type") || "",
          startAsset: requestUrl.searchParams.get("startAsset") || "",
          strategyId: requestUrl.searchParams.get("strategyId") || "",
          cycleId: requestUrl.searchParams.get("cycleId") || "",
        };
        sendJson(res, 200, {
          ok: true,
          kind,
          logs: await readFilteredLogs(logStore, filters),
        });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/dry-run-report") {
        const logs = await readFilteredLogs(logStore, {
          kind: "all",
          limit: Number.parseInt(requestUrl.searchParams.get("limit") || "5000", 10),
          mode: "DRY_RUN",
        });
        const summary = summarizeDryRun(logs);

        if (requestUrl.searchParams.get("format") === "csv") {
          res.writeHead(200, {
            "Content-Type": "text/csv; charset=utf-8",
            "Cache-Control": "no-store",
            "Content-Disposition": "attachment; filename=\"dry-run-report.csv\"",
          });
          res.end(dryRunReportCsv(summary));
          return;
        }

        sendJson(res, 200, {
          ok: true,
          summary,
          logs,
        });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/command") {
        const payload = await readJsonBody(req);
        let command;

        try {
          command = normalizeCommand(payload.command);
        } catch (error) {
          sendJson(res, 400, { ok: false, error: error.message });
          return;
        }

        const commandId = crypto.randomUUID();
        const record = await logStore.append("commands", {
          type: "engine.command.requested",
          command,
          commandId,
          runMode: payload.runMode,
          emergency: payload.emergency === true,
          source: "dashboard",
        });
        await commandStatusStore.write(commandId, {
          status: "queued",
          command,
          runMode: payload.runMode,
          emergency: payload.emergency === true,
          source: "dashboard",
          queuedAt: record.timestamp,
        });

        sendJson(res, 202, {
          ok: true,
          command: record.command,
          commandId: record.commandId,
          runMode: record.runMode,
          status: "queued",
        });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname.startsWith("/api/commands/")) {
        const commandId = requestUrl.pathname.slice("/api/commands/".length);
        const status = await commandStatusStore.read(commandId);

        if (!status) {
          sendJson(res, 404, { ok: false, error: "Command status not found" });
          return;
        }

        sendJson(res, 200, { ok: true, status });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/events") {
        res.writeHead(200, {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-store",
          Connection: "keep-alive",
          "X-Accel-Buffering": "no",
        });
        sseClients.add(res);
        req.on("close", () => {
          sseClients.delete(res);
        });
        res.write(`event: state\ndata: ${JSON.stringify(await readSnapshot(snapshotPath))}\n\n`);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/capture") {
        const saved = await saveCapture(capturesDir, await readJsonBody(req, 20 * 1024 * 1024));
        sendJson(res, 200, {
          ok: true,
          pngPath: path.relative(process.cwd(), saved.pngPath),
          jsonPath: path.relative(process.cwd(), saved.jsonPath),
        });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/vendor/plotly.min.js") {
        await serveFile(res, plotlyPath);
        return;
      }

      if (req.method === "GET") {
        const resolved = resolvePublicPath(publicDir, requestUrl.pathname);

        if (!resolved) {
          sendText(res, 403, "Forbidden");
          return;
        }

        await serveFile(res, resolved);
        return;
      }

      sendJson(res, 405, { ok: false, error: "Method not allowed" });
    } catch (error) {
      sendJson(res, 500, { ok: false, error: error.message });
    }
  };
}

async function startDashboardServer(options = {}) {
  const port = options.port !== undefined ? options.port : Number.parseInt(process.env.PORT || "3099", 10);
  const host = options.host || "127.0.0.1";
  const snapshotPath = options.snapshotPath || path.resolve(process.cwd(), "out", "runtime", "latest-snapshot.json");
  const deltaPath = options.deltaPath || path.resolve(process.cwd(), "out", "runtime", "latest-delta.json");
  const logStore = options.logStore || new AppendOnlyLogStore({
    logDir: options.logDir || path.resolve(process.cwd(), "out", "logs"),
  });
  const commandStatusStore = options.commandStatusStore || new CommandStatusStore({
    runtimeDir: path.dirname(snapshotPath),
  });
  const sseClients = new Set();
  const liveWsClients = new Set();
  const wsServer = new WebSocket.Server({ noServer: true });
  const server = http.createServer(createDashboardRequestHandler({
    ...options,
    snapshotPath,
    logStore,
    commandStatusStore,
    sseClients,
  }));
  const pushIntervalMs = options.pushIntervalMs ||
    Number.parseInt(process.env.UI_PUSH_INTERVAL_MS || "250", 10);
  let lastPushedDeltaEpochMs = 0;

  wsServer.on("connection", async (socket) => {
    liveWsClients.add(socket);
    socket.send(JSON.stringify({
      type: "hello",
      sentAtEpochMs: Date.now(),
      uiPushIntervalMs: pushIntervalMs,
    }));
    socket.send(JSON.stringify(await readSnapshot(snapshotPath)));
    socket.on("message", (data) => {
      try {
        JSON.parse(data.toString("utf8"));
      } catch (_error) {
        socket.send(JSON.stringify({
          type: "error",
          message: "Invalid client message",
        }));
      }
    });
    socket.on("close", () => {
      liveWsClients.delete(socket);
    });
  });

  server.on("upgrade", (req, socket, head) => {
    const requestUrl = new URL(req.url, "http://localhost");

    if (requestUrl.pathname !== "/ws/live" || !isLocalRequest(req)) {
      socket.destroy();
      return;
    }

    wsServer.handleUpgrade(req, socket, head, (ws) => {
      wsServer.emit("connection", ws, req);
    });
  });

  async function pushDelta() {
    const delta = await readDelta(deltaPath);

    if (!delta) {
      return;
    }

    if (Number(delta.sentAtEpochMs || 0) <= lastPushedDeltaEpochMs) {
      return;
    }

    lastPushedDeltaEpochMs = Number(delta.sentAtEpochMs || 0);

    const message = JSON.stringify(delta);
    const sseMessage = `event: delta\ndata: ${message}\n\n`;

    for (const client of liveWsClients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }

    for (const client of sseClients) {
      client.write(sseMessage);
    }
  }

  const pushTimer = setInterval(() => {
    pushDelta().catch(() => {});
  }, pushIntervalMs);

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });

  return {
    server,
    url: `http://${host}:${server.address().port}`,
    close: async () => {
      clearInterval(pushTimer);
      for (const client of sseClients) client.end();
      for (const client of liveWsClients) client.close(1000, "dashboard shutdown");
      wsServer.close();
      await new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}

module.exports = {
  createDashboardRequestHandler,
  startDashboardServer,
  readDelta,
  readSnapshot,
};
