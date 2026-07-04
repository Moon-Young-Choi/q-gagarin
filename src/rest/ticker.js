const axios = require("axios");

const market = process.env.UPBIT_MARKET || "KRW-BTC";
const endpoint = "https://api.upbit.com/v1/ticker";

async function main() {
  const response = await axios.get(endpoint, {
    params: { markets: market },
    headers: { Accept: "application/json" },
    timeout: 10000,
  });

  const ticker = response.data && response.data[0];

  if (!ticker) {
    throw new Error(`No ticker data returned for ${market}`);
  }

  console.log(
    JSON.stringify(
      {
        market: ticker.market,
        trade_price: ticker.trade_price,
        signed_change_rate: ticker.signed_change_rate,
        acc_trade_price_24h: ticker.acc_trade_price_24h,
        trade_timestamp: ticker.trade_timestamp,
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  if (error.response) {
    console.error(
      `Upbit REST API error ${error.response.status}: ${JSON.stringify(error.response.data)}`,
    );
  } else {
    console.error(error.message);
  }

  process.exitCode = 1;
});
