# q-gagarin Baseline

This baseline captures the current project before the trading-system architecture refactor.

## Current Commands

```bash
npm run check
npm test
npm run triangles
npm run triangles:live
npm run engine
npm run dashboard
npm run baseline
```

- `npm run check` syntax-checks JavaScript under `src`, `scripts`, `public`, and `test`.
- `npm test` runs `npm run check` and the Node.js test suite.
- `npm run triangles` runs a read-only Upbit public REST scan and writes reports under `out/`.
- `npm run triangles:live` starts the engine and dashboard together for local use.
- `npm run engine` starts the engine process.
- `npm run dashboard` starts the dashboard process against the latest engine snapshot.
- `npm run baseline` starts the live dashboard server long enough to record baseline metrics under `out/baseline/`.

## Runtime Assumptions

- Node.js 22 is expected.
- Upbit is the only implemented exchange.
- The project currently uses public Upbit quotation APIs only; no private order APIs are called.
- No order submission, cancellation, fill tracking, risk guard, or real executor exists in the current implementation.
- Live trading is disabled by default in `config/runtime.json`.
- The runtime config is loaded and frozen at live-server startup.
- The dashboard process follows the engine's runtime snapshot plus lightweight delta files and appends mode-aware command requests for the engine.
- The engine writes latest snapshots under `out/runtime/` and append-only NDJSON logs under `out/logs/`.
- Default start assets are `KRW`, `BTC`, and `USDT`.
- Observation depth is configured as `5`; validation depth is configured as `30`.
- The live scanner generates executable route variants by `startAsset` for enabled start assets present in each triangle.
- Observation orderbooks use depth `5`; validation orderbooks use depth `30`.
- Current top-of-book gross/net multiplier calculation remains available through the baseline strategy.
- Depth-aware candidate validation is conservative and reports validation rejection reasons, but no order submission is implemented.
- Upbit authenticated REST foundations refuse `createOrder()` unless live trading is explicitly enabled.
- Private MyOrder WebSocket and fill tracking foundations normalize order/fill events when credentials are configured.
- The default UI push interval is `250ms`; override with `UI_PUSH_INTERVAL_MS`.
- The dashboard server binds to `127.0.0.1` by default and rejects non-local HTTP/WebSocket clients.

## Dashboard Behavior

- The browser receives full state from `/api/state`, live deltas from `/ws/live`, and can fall back to `/api/events`.
- The dashboard is organized into tabs: Market, Strategy, Arbitrage Desk, Execution, System, Logs, and Settings.
- The live scatter plot is under Arbitrage Desk and is grouped by `KRW Start`, `BTC Start`, `USDT Start`, and an `All` toggle.
- CPU, memory, event-loop, latency, push-rate, and render metrics are under System.
- Upbit is shown as enabled; Binance, Bithumb, and Bybit are shown as not implemented.
- Strategy displays source-managed strategies read-only.
- Logs displays recent in-memory telemetry events, including strategy decisions.
- Execution and Settings remain non-trading surfaces; no order submission is implemented.
- Hover tooltips are disabled.
- Clicking a scatter point updates the external route detail panel.
- Profitable canonical and reverse points are both pink; direction is distinguished by marker symbol and border.
- Fee and stale-threshold controls remain dashboard inputs for the current scanner calculation path.
- Dashboard capture writes PNG/JSON files under `out/captures/`.

## Baseline Metrics

Run:

```bash
npm run baseline
```

The benchmark writes a file like:

```text
out/baseline/baseline-YYYYMMDD-HHmmss.json
```

Recorded fields include:

- live dashboard startup time
- market count
- triangle count
- plotted cycle count
- UI push interval
- average render ms when browser render samples are available

By default the benchmark does not start Upbit WebSocket feeds. Pass `-- --start-feeds` or set `BASELINE_START_FEEDS=1` to include feeds during the benchmark run.
