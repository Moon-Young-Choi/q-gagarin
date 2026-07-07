const fs = require("node:fs");
const path = require("node:path");
const {
  DEFAULT_EXECUTION_MODE,
  EXECUTION_MODE_SET,
} = require("../execution/executionModes");
const {
  DEFAULT_PARTIAL_FILL_POLICY,
  PARTIAL_FILL_POLICY_SET,
} = require("../execution/partialFillPolicy");

const DEFAULT_RUNTIME_CONFIG_PATH = path.resolve(process.cwd(), "config", "runtime.json");
const RUN_MODES = new Set(["OBSERVE", "DRY_RUN", "REAL_GUARDED", "REAL_AUTO"]);
const START_ASSETS = new Set(["KRW", "BTC", "USDT"]);
const EXECUTION_MODES = EXECUTION_MODE_SET;
const PARTIAL_FILL_POLICIES = PARTIAL_FILL_POLICY_SET;
const SIZING_MODES = new Set(["configured", "best-level-residual"]);

const DEFAULT_RUNTIME_CONFIG = {
  runMode: "OBSERVE",
  exchange: "upbit",
  enabledStartAssets: ["KRW", "BTC", "USDT"],
  observationOrderbookUnit: 5,
  validationOrderbookUnit: 30,
  executionMode: DEFAULT_EXECUTION_MODE,
  enabledExecutionModes: [DEFAULT_EXECUTION_MODE],
  liveTradingEnabled: false,
  activeStrategyId: "topOfBookBaseline",
  executionPolicy: {
    stopPolicy: "CANCEL_OPEN_ORDERS",
    partialFillPolicy: DEFAULT_PARTIAL_FILL_POLICY,
    allowBestIoc: false,
    simulatedBalances: {
      KRW: 1000000,
      BTC: 0.05,
      USDT: 1000,
    },
    realRunLimits: {
      maxNotionalPerCycleByAsset: {
        KRW: 50000,
        BTC: 0.001,
        USDT: 50,
      },
      maxDailyLossByAsset: {
        KRW: 100000,
        BTC: 0.002,
        USDT: 100,
      },
      maxConsecutiveFailures: 3,
      maxOpenOrders: 3,
      maxCyclesPerMinute: 5,
      maxActiveTriangleExecutions: 1,
    },
    marketDataGuards: {
      maxOldestLegAgeMs: 1000,
      maxLegTimestampSkewMs: 500,
      maxExchangeToServerLatencyMs: 1000,
      maxDecisionAgeMs: 1000,
    },
    executionGuards: {
      orderChanceTtlMs: 30000,
      accountBalanceTtlMs: 30000,
      validationOrderbookTtlMs: 1000,
      orderRateLimitPerSecond: 8,
      maxOrderAckMs: 500,
      maxReconciliationMs: 3000,
    },
    readinessGuards: {
      minimumDryRunSamples: 10,
      minimumDryRunSamplesPerStartAsset: 1,
      maxDryRunRejectionRate: 0.8,
      minimumDryRunCompleteRate: 0.5,
      maxDryRunDepthRejectionRate: 0.8,
      maxDryRunLatencyRejectionRate: 0.2,
      maxDryRunExpectedSimulatedGapRate: 1,
    },
  },
  candidateValidation: {
    sizingMode: "configured",
    startAmountByAsset: {
      KRW: 10000,
      BTC: 0.0002,
      USDT: 10,
    },
    minOrderAmountByAsset: {
      KRW: 5000,
      BTC: 0.00005,
      USDT: 0.5,
    },
    maxTouchRatioPerBestLevel: 0.3,
    minResidualRatioPerBestLevel: 0.1,
    minResidualAbsoluteByAsset: {
      KRW: 0,
      BTC: 0,
      USDT: 0,
    },
    minNetProfitRate: 0,
    maxObservationValidationGapMs: 500,
  },
};

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function mergePlainObjectDefaults(defaults, overrides) {
  if (overrides === undefined) return cloneJson(defaults);
  if (!isPlainObject(defaults) || !isPlainObject(overrides)) return cloneJson(overrides);

  const merged = cloneJson(defaults);
  for (const [key, value] of Object.entries(overrides)) {
    merged[key] = mergePlainObjectDefaults(defaults[key], value);
  }
  return merged;
}

function mergeRuntimeConfigDefaults(input) {
  return mergePlainObjectDefaults(DEFAULT_RUNTIME_CONFIG, input || {});
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) {
    return value;
  }

  Object.freeze(value);

  for (const item of Object.values(value)) {
    deepFreeze(item);
  }

  return value;
}

function validateOrderbookUnit(config, name, expected) {
  const value = config[name];

  if (!Number.isInteger(value) || value !== expected) {
    throw new Error(`${name} must be ${expected}. Received: ${value}`);
  }
}

function validateNonNegativeAssetAmounts(values, name) {
  if (!isPlainObject(values)) {
    throw new Error(`${name} must be an object`);
  }

  for (const [asset, value] of Object.entries(values)) {
    if (!START_ASSETS.has(asset)) {
      throw new Error(`${name} contains unsupported asset: ${asset}`);
    }

    if (!Number.isFinite(Number(value)) || Number(value) < 0) {
      throw new Error(`${name}.${asset} must be a non-negative number`);
    }
  }
}

function validateRatio(value, name) {
  if (!Number.isFinite(Number(value)) || Number(value) < 0 || Number(value) > 1) {
    throw new Error(`${name} must be a number between 0 and 1`);
  }
}

function validateRuntimeConfig(input, options = {}) {
  if (!isPlainObject(input)) {
    throw new Error("Runtime config must be a JSON object");
  }

  const config = {
    ...input,
    enabledStartAssets: Array.isArray(input.enabledStartAssets) ? [...input.enabledStartAssets] : input.enabledStartAssets,
  };

  if (!RUN_MODES.has(config.runMode)) {
    throw new Error(`Invalid runMode: ${config.runMode}`);
  }

  if (config.exchange !== "upbit") {
    throw new Error(`Unsupported exchange: ${config.exchange}`);
  }

  if (!Array.isArray(config.enabledStartAssets) || config.enabledStartAssets.length === 0) {
    throw new Error("enabledStartAssets must be a non-empty array");
  }

  const seenAssets = new Set();
  for (const asset of config.enabledStartAssets) {
    if (!START_ASSETS.has(asset)) {
      throw new Error(`Invalid enabled start asset: ${asset}`);
    }

    if (seenAssets.has(asset)) {
      throw new Error(`Duplicate enabled start asset: ${asset}`);
    }

    seenAssets.add(asset);
  }

  validateOrderbookUnit(config, "observationOrderbookUnit", 5);
  validateOrderbookUnit(config, "validationOrderbookUnit", 30);

  if (!EXECUTION_MODES.has(config.executionMode)) {
    throw new Error(`Invalid executionMode: ${config.executionMode}`);
  }

  if (!Array.isArray(config.enabledExecutionModes) || !config.enabledExecutionModes.every((mode) => EXECUTION_MODES.has(mode))) {
    throw new Error("enabledExecutionModes must contain valid execution modes");
  }

  if (config.executionMode === "BEST_IOC" && !config.enabledExecutionModes.includes("BEST_IOC")) {
    throw new Error("BEST_IOC requires explicit enabledExecutionModes opt-in");
  }

  if (typeof config.liveTradingEnabled !== "boolean") {
    throw new Error("liveTradingEnabled must be a boolean");
  }

  if (typeof config.activeStrategyId !== "string" || config.activeStrategyId.trim() === "") {
    throw new Error("activeStrategyId must be a non-empty string");
  }

  if (!isPlainObject(config.executionPolicy)) {
    throw new Error("executionPolicy must be an object");
  }

  if (!["CANCEL_OPEN_ORDERS", "TRACK_UNTIL_RESOLVED"].includes(config.executionPolicy.stopPolicy)) {
    throw new Error(`Invalid executionPolicy.stopPolicy: ${config.executionPolicy.stopPolicy}`);
  }

  if (!PARTIAL_FILL_POLICIES.has(config.executionPolicy.partialFillPolicy)) {
    throw new Error(`Invalid executionPolicy.partialFillPolicy: ${config.executionPolicy.partialFillPolicy}`);
  }

  if (typeof config.executionPolicy.allowBestIoc !== "boolean") {
    throw new Error("executionPolicy.allowBestIoc must be a boolean");
  }

  if (config.executionMode === "BEST_IOC" && config.executionPolicy.allowBestIoc !== true) {
    throw new Error("BEST_IOC requires executionPolicy.allowBestIoc=true");
  }

  if (!isPlainObject(config.executionPolicy.readinessGuards)) {
    throw new Error("executionPolicy.readinessGuards must be an object");
  }

  validateNonNegativeAssetAmounts(config.executionPolicy.simulatedBalances, "executionPolicy.simulatedBalances");
  validateNonNegativeAssetAmounts(
    config.executionPolicy.realRunLimits.maxNotionalPerCycleByAsset,
    "executionPolicy.realRunLimits.maxNotionalPerCycleByAsset",
  );
  validateNonNegativeAssetAmounts(
    config.executionPolicy.realRunLimits.maxDailyLossByAsset,
    "executionPolicy.realRunLimits.maxDailyLossByAsset",
  );

  for (const [name, value] of Object.entries({
    maxConsecutiveFailures: config.executionPolicy.realRunLimits.maxConsecutiveFailures,
    maxOpenOrders: config.executionPolicy.realRunLimits.maxOpenOrders,
    maxCyclesPerMinute: config.executionPolicy.realRunLimits.maxCyclesPerMinute,
    maxActiveTriangleExecutions: config.executionPolicy.realRunLimits.maxActiveTriangleExecutions,
    maxOldestLegAgeMs: config.executionPolicy.marketDataGuards.maxOldestLegAgeMs,
    maxLegTimestampSkewMs: config.executionPolicy.marketDataGuards.maxLegTimestampSkewMs,
    maxExchangeToServerLatencyMs: config.executionPolicy.marketDataGuards.maxExchangeToServerLatencyMs,
    maxDecisionAgeMs: config.executionPolicy.marketDataGuards.maxDecisionAgeMs,
    orderChanceTtlMs: config.executionPolicy.executionGuards.orderChanceTtlMs,
    accountBalanceTtlMs: config.executionPolicy.executionGuards.accountBalanceTtlMs,
    validationOrderbookTtlMs: config.executionPolicy.executionGuards.validationOrderbookTtlMs,
    orderRateLimitPerSecond: config.executionPolicy.executionGuards.orderRateLimitPerSecond,
    maxOrderAckMs: config.executionPolicy.executionGuards.maxOrderAckMs,
    maxReconciliationMs: config.executionPolicy.executionGuards.maxReconciliationMs,
    "readinessGuards.minimumDryRunSamples": config.executionPolicy.readinessGuards.minimumDryRunSamples,
    "readinessGuards.minimumDryRunSamplesPerStartAsset": config.executionPolicy.readinessGuards.minimumDryRunSamplesPerStartAsset,
  })) {
    if (!Number.isFinite(Number(value)) || Number(value) < 0) {
      throw new Error(`executionPolicy.${name} must be a non-negative number`);
    }
  }

  for (const [name, value] of Object.entries({
    maxDryRunRejectionRate: config.executionPolicy.readinessGuards.maxDryRunRejectionRate,
    minimumDryRunCompleteRate: config.executionPolicy.readinessGuards.minimumDryRunCompleteRate,
    maxDryRunDepthRejectionRate: config.executionPolicy.readinessGuards.maxDryRunDepthRejectionRate,
    maxDryRunLatencyRejectionRate: config.executionPolicy.readinessGuards.maxDryRunLatencyRejectionRate,
    maxDryRunExpectedSimulatedGapRate: config.executionPolicy.readinessGuards.maxDryRunExpectedSimulatedGapRate,
  })) {
    validateRatio(value, `executionPolicy.readinessGuards.${name}`);
  }

  if (!isPlainObject(config.candidateValidation)) {
    throw new Error("candidateValidation must be an object");
  }

  if (!SIZING_MODES.has(config.candidateValidation.sizingMode)) {
    throw new Error(`Invalid candidateValidation.sizingMode: ${config.candidateValidation.sizingMode}`);
  }

  validateNonNegativeAssetAmounts(config.candidateValidation.startAmountByAsset, "candidateValidation.startAmountByAsset");
  validateNonNegativeAssetAmounts(config.candidateValidation.minOrderAmountByAsset, "candidateValidation.minOrderAmountByAsset");
  if (config.candidateValidation.maxStartAmountByAsset !== undefined) {
    validateNonNegativeAssetAmounts(config.candidateValidation.maxStartAmountByAsset, "candidateValidation.maxStartAmountByAsset");
  }
  validateNonNegativeAssetAmounts(
    config.candidateValidation.minResidualAbsoluteByAsset,
    "candidateValidation.minResidualAbsoluteByAsset",
  );
  validateRatio(config.candidateValidation.maxTouchRatioPerBestLevel, "candidateValidation.maxTouchRatioPerBestLevel");
  validateRatio(
    config.candidateValidation.minResidualRatioPerBestLevel,
    "candidateValidation.minResidualRatioPerBestLevel",
  );

  if (
    !Number.isFinite(Number(config.candidateValidation.minNetProfitRate)) ||
    Number(config.candidateValidation.minNetProfitRate) < 0
  ) {
    throw new Error("candidateValidation.minNetProfitRate must be a non-negative number");
  }

  if (
    config.candidateValidation.maxObservationValidationGapMs !== undefined &&
    (
      !Number.isFinite(Number(config.candidateValidation.maxObservationValidationGapMs)) ||
      Number(config.candidateValidation.maxObservationValidationGapMs) < 0
    )
  ) {
    throw new Error("candidateValidation.maxObservationValidationGapMs must be a non-negative number");
  }

  if (config.liveTradingEnabled && !config.runMode.startsWith("REAL_")) {
    throw new Error("liveTradingEnabled requires a REAL_* runMode");
  }

  if (config.liveTradingEnabled && options.allowLiveTrading !== true) {
    throw new Error("liveTradingEnabled requires an explicit allowLiveTrading gate");
  }

  return config;
}

function freezeRuntimeConfig(input, options = {}) {
  return deepFreeze(validateRuntimeConfig(input, options));
}

function loadRuntimeConfig(options = {}) {
  const configPath = options.configPath || DEFAULT_RUNTIME_CONFIG_PATH;
  let parsed = null;

  try {
    parsed = JSON.parse(fs.readFileSync(configPath, "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") {
      throw error;
    }

    parsed = cloneJson(DEFAULT_RUNTIME_CONFIG);
  }

  return freezeRuntimeConfig(mergeRuntimeConfigDefaults(parsed), {
    allowLiveTrading: options.allowLiveTrading === true,
  });
}

module.exports = {
  DEFAULT_RUNTIME_CONFIG,
  DEFAULT_RUNTIME_CONFIG_PATH,
  RUN_MODES,
  START_ASSETS,
  EXECUTION_MODES,
  PARTIAL_FILL_POLICIES,
  SIZING_MODES,
  loadRuntimeConfig,
  mergeRuntimeConfigDefaults,
  validateRuntimeConfig,
  freezeRuntimeConfig,
};
