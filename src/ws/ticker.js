const WebSocket = require("ws");
const crypto = require("node:crypto");

const market = process.env.UPBIT_MARKET || "KRW-BTC";
const endpoint = "wss://api.upbit.com/websocket/v1";
const ticket = `q-gagarin-${crypto.randomUUID()}`;

const ws = new WebSocket(endpoint);

ws.on("open", () => {
  ws.send(
    JSON.stringify([
      { ticket },
      {
        type: "ticker",
        codes: [market],
        isOnlyRealtime: true,
      },
    ]),
  );

  console.log(`Subscribed to ${market} ticker. Press Ctrl+C to stop.`);
});

ws.on("message", (data) => {
  const ticker = JSON.parse(data.toString("utf8"));

  console.log(
    JSON.stringify(
      {
        market: ticker.market || ticker.code || market,
        trade_price: ticker.trade_price,
        signed_change_rate: ticker.signed_change_rate,
        acc_trade_price_24h: ticker.acc_trade_price_24h,
        trade_timestamp: ticker.trade_timestamp,
      },
      null,
      2,
    ),
  );
});

ws.on("error", (error) => {
  console.error(`Upbit WebSocket error: ${error.message}`);
  process.exitCode = 1;
});

ws.on("close", (code, reason) => {
  const reasonText = reason.toString("utf8");
  console.log(`WebSocket closed (${code})${reasonText ? `: ${reasonText}` : ""}`);
});

process.on("SIGINT", () => {
  ws.close(1000, "client shutdown");
});
