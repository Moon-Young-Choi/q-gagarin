# Real Run Readiness

Real trading is disabled by default. `REAL_GUARDED` can start only when the readiness checklist passes.

## Required Checks

- Upbit API key and secret are present.
- Required API permissions are verified or explicitly documented.
- Private MyOrder WebSocket is connected.
- `orders/chance` fee policy cache is fresh.
- Account balance cache is fresh.
- Observation feed is healthy.
- Validation feed is healthy.
- Validation orderbook depth is 30 and fresh.
- A dry-run report exists.
- Dry-run minimum sample count is met.
- Dry-run rejection profile is acceptable.
- `liveTradingEnabled` is explicitly `true`.
- `REAL_AUTO` remains disabled unless manually configured.

The engine refreshes private REST permission, account, and `orders/chance` state before evaluating the checklist. The result is written to append-only events and appears in the dashboard Settings tab.

## Safety Defaults

- Default execution mode is `LIMIT_IOC_AT_OBSERVED_BEST`.
- `BEST_IOC` requires `enabledExecutionModes` to include `BEST_IOC` and `executionPolicy.allowBestIoc=true`.
- Dashboard mode buttons queue `Start` with an explicit run mode (`OBSERVE`, `DRY_RUN`, or `REAL_GUARDED`) plus `Pause`, `Stop`, and emergency `Stop`.
- Real order creation is refused while `liveTradingEnabled=false`.
- `Q_GAGARIN_ALLOW_LIVE_TRADING=true` is required before a config with `liveTradingEnabled=true` can load.
- Every real order rejection includes a machine-readable reason such as `MARKET_DATA_STALE`, `ORDER_CHANCE_STALE`, `PRIVATE_WS_DISCONNECTED`, or `ORDER_RATE_LIMIT`.

## Execution Modes

- `LIMIT_IOC_AT_OBSERVED_BEST` is the default. It uses validation depth-30 top of book, `ord_type=limit`, and `time_in_force=ioc`.
- `BEST_IOC` is opt-in only and requires both `enabledExecutionModes: ["LIMIT_IOC_AT_OBSERVED_BEST", "BEST_IOC"]` and `executionPolicy.allowBestIoc=true`.
