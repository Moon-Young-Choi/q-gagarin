const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {
  DEFAULT_RUNTIME_CONFIG,
  loadRuntimeConfig,
  validateRuntimeConfig,
  freezeRuntimeConfig,
} = require("../src/core/runtimeConfig");

test("runtime config accepts the default Upbit observe configuration", () => {
  const config = validateRuntimeConfig(DEFAULT_RUNTIME_CONFIG);

  assert.equal(config.runMode, "OBSERVE");
  assert.equal(config.exchange, "upbit");
  assert.deepEqual(config.enabledStartAssets, ["KRW", "BTC", "USDT"]);
  assert.equal(config.observationOrderbookUnit, 5);
  assert.equal(config.validationOrderbookUnit, 30);
  assert.equal(config.liveTradingEnabled, false);
  assert.equal(config.activeStrategyId, "topOfBookBaseline");
  assert.deepEqual(config.enabledExecutionModes, ["LIMIT_IOC_AT_OBSERVED_BEST"]);
  assert.equal(config.executionPolicy.stopPolicy, "CANCEL_OPEN_ORDERS");
  assert.equal(config.executionPolicy.partialFillPolicy, "CONTINUE_IF_ABOVE_MIN");
  assert.equal(config.executionPolicy.allowBestIoc, false);
  assert.equal(config.executionPolicy.simulatedBalances.KRW, 1000000);
  assert.equal(config.executionPolicy.executionGuards.maxOrderAckMs, 500);
  assert.equal(config.executionPolicy.executionGuards.maxReconciliationMs, 3000);
  assert.equal(config.executionPolicy.readinessGuards.minimumDryRunSamples, 10);
  assert.equal(config.executionPolicy.readinessGuards.minimumDryRunSamplesPerStartAsset, 1);
  assert.equal(config.executionPolicy.readinessGuards.maxDryRunRejectionRate, 0.8);
  assert.equal(config.executionPolicy.readinessGuards.minimumDryRunCompleteRate, 0.5);
  assert.equal(config.candidateValidation.maxTouchRatioPerBestLevel, 0.3);
  assert.equal(config.candidateValidation.maxObservationValidationGapMs, 500);
});

test("runtime config loader backfills newly added defaults for older config files", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "q-gagarin-runtime-config-"));
  const configPath = path.join(dir, "runtime.json");
  const staleConfig = JSON.parse(JSON.stringify(DEFAULT_RUNTIME_CONFIG));
  delete staleConfig.candidateValidation.sizingMode;

  fs.writeFileSync(configPath, `${JSON.stringify(staleConfig, null, 2)}\n`);

  const config = loadRuntimeConfig({ configPath });

  assert.equal(config.candidateValidation.sizingMode, "configured");
});

test("runtime config gates BEST_IOC behind explicit opt-in", () => {
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      executionMode: "BEST_IOC",
    }),
    /BEST_IOC requires explicit enabledExecutionModes/,
  );
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      executionMode: "BEST_IOC",
      enabledExecutionModes: ["LIMIT_IOC_AT_OBSERVED_BEST", "BEST_IOC"],
    }),
    /allowBestIoc=true/,
  );
  assert.equal(validateRuntimeConfig({
    ...DEFAULT_RUNTIME_CONFIG,
    executionMode: "BEST_IOC",
    enabledExecutionModes: ["LIMIT_IOC_AT_OBSERVED_BEST", "BEST_IOC"],
    executionPolicy: {
      ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
      allowBestIoc: true,
    },
  }).executionMode, "BEST_IOC");
});

test("runtime config rejects invalid runMode", () => {
  assert.throws(
    () => validateRuntimeConfig({ ...DEFAULT_RUNTIME_CONFIG, runMode: "REAL" }),
    /Invalid runMode/,
  );
});

test("runtime config rejects invalid partial fill policy", () => {
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      executionPolicy: {
        ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
        partialFillPolicy: "CONTINUE_ALWAYS",
      },
    }),
    /Invalid executionPolicy.partialFillPolicy/,
  );
});

test("runtime config rejects invalid orderbook units", () => {
  assert.throws(
    () => validateRuntimeConfig({ ...DEFAULT_RUNTIME_CONFIG, observationOrderbookUnit: 4 }),
    /observationOrderbookUnit must be 5/,
  );
  assert.throws(
    () => validateRuntimeConfig({ ...DEFAULT_RUNTIME_CONFIG, validationOrderbookUnit: 5 }),
    /validationOrderbookUnit must be 30/,
  );
});

test("runtime config validates dry-run readiness guard thresholds", () => {
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      executionPolicy: {
        ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
        readinessGuards: {
          ...DEFAULT_RUNTIME_CONFIG.executionPolicy.readinessGuards,
          minimumDryRunSamplesPerStartAsset: -1,
        },
      },
    }),
    /executionPolicy\.readinessGuards\.minimumDryRunSamplesPerStartAsset must be a non-negative number/,
  );
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      executionPolicy: {
        ...DEFAULT_RUNTIME_CONFIG.executionPolicy,
        readinessGuards: {
          ...DEFAULT_RUNTIME_CONFIG.executionPolicy.readinessGuards,
          maxDryRunExpectedSimulatedGapRate: 1.2,
        },
      },
    }),
    /executionPolicy\.readinessGuards\.maxDryRunExpectedSimulatedGapRate must be a number between 0 and 1/,
  );
});

test("runtime config validates candidate validation market-data guard thresholds", () => {
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      candidateValidation: {
        ...DEFAULT_RUNTIME_CONFIG.candidateValidation,
        maxObservationValidationGapMs: -1,
      },
    }),
    /candidateValidation\.maxObservationValidationGapMs must be a non-negative number/,
  );
});

test("runtime config rejects invalid start assets", () => {
  assert.throws(
    () => validateRuntimeConfig({ ...DEFAULT_RUNTIME_CONFIG, enabledStartAssets: ["KRW", "ETH"] }),
    /Invalid enabled start asset: ETH/,
  );
});

test("runtime config is deeply frozen and guarded against silent real trading", () => {
  const frozen = freezeRuntimeConfig(DEFAULT_RUNTIME_CONFIG);

  assert.equal(Object.isFrozen(frozen), true);
  assert.equal(Object.isFrozen(frozen.enabledStartAssets), true);
  assert.equal(Object.isFrozen(frozen.executionPolicy), true);
  assert.equal(Object.isFrozen(frozen.candidateValidation), true);
  assert.throws(
    () => validateRuntimeConfig({
      ...DEFAULT_RUNTIME_CONFIG,
      runMode: "REAL_AUTO",
      liveTradingEnabled: true,
    }),
    /explicit allowLiveTrading gate/,
  );
});
