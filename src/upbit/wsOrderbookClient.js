const EventEmitter = require("node:events");
const crypto = require("node:crypto");
const { performance } = require("node:perf_hooks");
const WebSocket = require("ws");
const { perfNowNs } = require("../core/timingTrace");

const DEFAULT_ENDPOINT = "wss://api.upbit.com/websocket/v1";

function chunk(items, size) {
  const chunks = [];

  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }

  return chunks;
}

function normalizeOrderbookMessage(payload, receivedAt = Date.now(), timings = {}) {
  const normalizeStartPerfNs = perfNowNs();
  const unit = payload && Array.isArray(payload.orderbook_units) && payload.orderbook_units[0];

  if (!payload || payload.type !== "orderbook" || !unit) {
    return null;
  }

  const orderbookUnits = payload.orderbook_units.map((item) => ({
    ask_price: Number(item.ask_price),
    bid_price: Number(item.bid_price),
    ask_size: Number(item.ask_size),
    bid_size: Number(item.bid_size),
  }));

  return {
    market: payload.code || payload.market,
    askPrice: Number(unit.ask_price),
    bidPrice: Number(unit.bid_price),
    askSize: Number(unit.ask_size),
    bidSize: Number(unit.bid_size),
    timestamp: Number(payload.timestamp),
    streamType: payload.stream_type || payload.streamType || "UNKNOWN",
    orderbookUnit: orderbookUnits.length,
    orderbook_units: orderbookUnits,
    receivedAt,
    timings: {
      ...timings,
      normalizeStartPerfNs,
      normalizeDonePerfNs: perfNowNs(),
      upbitTimestampMs: Number(payload.timestamp || payload.tms),
    },
  };
}

class UpbitWsOrderbookClient extends EventEmitter {
  constructor(marketCodes, options = {}) {
    super();
    this.endpoint = options.endpoint || DEFAULT_ENDPOINT;
    this.marketCodes = [...new Set(marketCodes)].sort();
    this.orderbookUnit = options.orderbookUnit || 1;
    this.chunkSize = options.chunkSize || 100;
    this.connectionDelayMs = options.connectionDelayMs || 1000;
    this.reconnectMinMs = options.reconnectMinMs || 3000;
    this.reconnectMaxMs = options.reconnectMaxMs || 30000;
    this.pingIntervalMs = options.pingIntervalMs || 20000;
    this.connections = new Map();
    this.stopped = false;
  }

  start() {
    this.stopped = false;
    const batches = chunk(this.marketCodes, this.chunkSize);

    batches.forEach((markets, index) => {
      setTimeout(() => {
        if (!this.stopped) {
          this.openConnection(index, markets, 0);
        }
      }, index * this.connectionDelayMs);
    });

    this.emitStatus();
  }

  stop() {
    this.stopped = true;

    for (const connection of this.connections.values()) {
      clearInterval(connection.pingTimer);
      clearTimeout(connection.reconnectTimer);

      if (connection.ws && connection.ws.readyState === WebSocket.OPEN) {
        connection.ws.close(1000, "client shutdown");
      } else if (connection.ws) {
        connection.ws.terminate();
      }
    }

    this.connections.clear();
    this.emitStatus();
  }

  openConnection(index, markets, reconnectAttempt) {
    const ws = new WebSocket(this.endpoint);
    const connection = {
      index,
      markets,
      ws,
      status: "connecting",
      reconnectAttempt,
      lastMessageAt: null,
      pingTimer: null,
      reconnectTimer: null,
    };

    this.connections.set(index, connection);
    this.emitStatus();

    ws.on("open", () => {
      connection.status = "open";
      connection.reconnectAttempt = 0;
      const codes = markets.map((market) => `${market}.${this.orderbookUnit}`);
      const ticket = `q-gagarin-live-${index}-${crypto.randomUUID()}`;

      ws.send(
        JSON.stringify([
          { ticket },
          {
            type: "orderbook",
            codes,
          },
        ]),
      );

      connection.pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.ping();
        }
      }, this.pingIntervalMs);

      this.emitStatus();
    });

    ws.on("message", (data) => {
      const serverReceiveEpochMs = Date.now();
      const serverReceivePerfMs = performance.now();
      const parseStartPerfNs = perfNowNs();
      connection.lastMessageAt = serverReceiveEpochMs;

      try {
        const payload = JSON.parse(data.toString("utf8"));
        const parseDonePerfNs = perfNowNs();
        const parseDonePerfMs = performance.now();
        const orderbook = normalizeOrderbookMessage(payload, connection.lastMessageAt, {
          exchangeTimestampEpochMs: Number(payload.timestamp || payload.tms),
          socketReceiveEpochMs: serverReceiveEpochMs,
          socketReceivePerfNs: perfNowNs(),
          parseStartPerfNs,
          parseDonePerfNs,
          serverReceiveEpochMs,
          serverReceivePerfMs,
          parseDonePerfMs,
        });

        if (orderbook && orderbook.market) {
          this.emit("orderbook", orderbook);
        }
      } catch (error) {
        this.emit("error", {
          type: "parse",
          message: error.message,
          connectionIndex: index,
        });
      }
    });

    ws.on("error", (error) => {
      this.emit("error", {
        type: "websocket",
        message: error.message,
        connectionIndex: index,
      });
    });

    ws.on("close", (code, reason) => {
      clearInterval(connection.pingTimer);
      connection.status = "closed";
      connection.closeCode = code;
      connection.closeReason = reason.toString("utf8");
      this.emitStatus();

      if (!this.stopped) {
        const nextAttempt = reconnectAttempt + 1;
        const delayMs = Math.min(this.reconnectMinMs * 2 ** reconnectAttempt, this.reconnectMaxMs);

        connection.status = "reconnecting";
        connection.reconnectAttempt = nextAttempt;
        connection.reconnectTimer = setTimeout(() => {
          if (!this.stopped) {
            this.openConnection(index, markets, nextAttempt);
          }
        }, delayMs);
        this.emitStatus();
      }
    });
  }

  getStatus() {
    const connections = [...this.connections.values()]
      .sort((left, right) => left.index - right.index)
      .map((connection) => ({
        index: connection.index,
        status: connection.status,
        marketCount: connection.markets.length,
        reconnectAttempt: connection.reconnectAttempt,
        lastMessageAt: connection.lastMessageAt,
        closeCode: connection.closeCode,
        closeReason: connection.closeReason,
      }));

    return {
      endpoint: this.endpoint,
      orderbookUnit: this.orderbookUnit,
      marketCount: this.marketCodes.length,
      connectionCount: Math.ceil(this.marketCodes.length / this.chunkSize),
      openConnectionCount: connections.filter((connection) => connection.status === "open").length,
      stopped: this.stopped,
      connections,
    };
  }

  emitStatus() {
    this.emit("status", this.getStatus());
  }
}

module.exports = {
  UpbitWsOrderbookClient,
  normalizeOrderbookMessage,
};
