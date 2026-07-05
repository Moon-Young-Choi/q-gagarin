# q-gagarin

Upbit API Node.js playground for REST and WebSocket examples.

## Requirements

- nvm
- Node.js 22
- npm

## Setup

```bash
nvm use
npm install
```

## Scripts

```bash
npm run ticker
npm run ticker:ws
npm run triangles
npm run triangles:live
npm run engine
npm run dashboard
npm run baseline
npm run check
npm test
```

The default market is `KRW-BTC`. Override it with `UPBIT_MARKET`:

```bash
UPBIT_MARKET=KRW-ETH npm run ticker
UPBIT_MARKET=KRW-ETH npm run ticker:ws
```

These examples use Upbit public quotation APIs, so no API keys are required.

## Triangular Arbitrage Scanner

Run the read-only universe scanner:

```bash
npm run triangles
```

The scanner loads all Upbit markets dynamically, finds unique 3-asset triangles, builds canonical and reverse route variants for each enabled start asset in a triangle (`KRW`, `BTC`, `USDT` by default), fetches orderbook snapshots, and writes:

- `out/upbit-triangles.json`
- `out/upbit-canonical-cycles.json`
- `out/upbit-canonical-cycle-multipliers.csv`
- `out/upbit-canonical-cycle-multipliers.html`

By default, `netMultiplier` equals `grossMultiplier` because no fee is assumed. To include a taker fee:

```bash
UPBIT_TAKER_FEE_RATE=0.0005 npm run triangles
```

The scanner does not submit orders. Reverse rows are calculated from their own route and current bid/ask snapshots. Report metadata includes `reportSchemaVersion: 2` because cycle rows now include `startAsset`, `endAsset`, and `routeVariantId`.

## Live Dashboard

Recommended live operation uses separate engine and dashboard processes so chart rendering and dashboard JSON work do not share the engine event loop:

```bash
npm run engine
npm run dashboard
```

The dashboard reads `out/runtime/latest-snapshot.json` once for initial state and then follows lightweight deltas from `out/runtime/latest-delta.json`.

For local development you can still start the combined localhost engine and dashboard:

```bash
npm run triangles:live
```

The server prints a URL like:

```text
http://127.0.0.1:3099
```

Use `PORT` to choose another port:

```bash
PORT=4100 npm run triangles:live
```

The browser reads from the local Node.js server only. Upbit market discovery, observation depth-5 and validation depth-30 orderbook WebSocket subscriptions, multiplier calculation, depth-aware candidate validation, caching, latency metrics, and capture saving are handled server-side. Captures are saved under `out/captures/`.

The engine owns exchange connections, orderbook stores, strategy evaluation, dry-run/real execution, private fill tracking, snapshots, deltas, and append-only logs. The dashboard serves static files, reads the engine's runtime files, exposes log APIs, and queues Observe, Dry Run, Real Guarded, Pause, Stop, and Emergency Stop commands for the engine. Each queued command gets a command status record under `out/runtime/command-status/` so the UI can show whether the engine accepted or rejected it.

Append-only NDJSON logs are written under `out/logs/`:

- `events.ndjson`
- `decisions.ndjson`
- `orders.ndjson`
- `fills.ndjson`
- `errors.ndjson`

Authenticated Upbit REST and private MyOrder WebSocket foundations are present. Real order creation is refused unless `liveTradingEnabled=true`, `Q_GAGARIN_ALLOW_LIVE_TRADING=true`, and the `REAL_GUARDED` readiness checklist passes. `DRY_RUN` uses the same execution-plan shape and writes simulated decision/order/fill events without calling Upbit order creation.

The Logs tab can filter append-only logs by mode, type, start asset, strategy, and cycle. It also exposes a dry-run report summary and JSON/CSV export through `/api/dry-run-report`. The Execution tab separates latest real-run orders/fills from dry-run logs and shows guard/cache/readiness status.

The default real execution mode is `LIMIT_IOC_AT_OBSERVED_BEST`: each leg submits an Upbit limit order with `time_in_force=ioc` at the validation orderbook's observed best ask/bid and uses the filled amount for the next leg. Optional `BEST_IOC` remains disabled unless `enabledExecutionModes` includes `BEST_IOC` and `executionPolicy.allowBestIoc=true`.

Selected Arbitrage Desk points include route, depth validation, strategy result, execution feasibility, fee/profit fields, and TimingTrace latency breakdowns. Browser render timing is measured for display only and is not part of engine execution.

## Baseline and Runtime Config

Runtime mode defaults live in `config/runtime.json`; live trading is disabled by default.

Record baseline dashboard metrics:

```bash
npm run baseline
```

Baseline JSON files are saved under `out/baseline/`. See `BASELINE.md` for current runtime assumptions and dashboard behavior.
