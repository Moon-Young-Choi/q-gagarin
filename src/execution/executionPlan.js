const { perfNowNs } = require("../core/timingTrace");

function mapGet(source, key) {
  return source instanceof Map ? source.get(key) : source && source[key];
}

function pickValidationOrderbooks(cycle, validationOrderbooks) {
  return new Map(
    (cycle.steps || []).map((step) => [
      step.market,
      mapGet(validationOrderbooks, step.market),
    ]),
  );
}

function legTimestampSkewMs(timestamps = []) {
  const values = timestamps.map(Number).filter(Number.isFinite);
  if (values.length < 2) return 0;
  return Math.max(...values) - Math.min(...values);
}

function executionLogMode(runMode) {
  if (String(runMode || "").startsWith("REAL")) return "REAL";
  return runMode || "OBSERVE";
}

function buildExecutionPlan(options = {}) {
  const {
    cycle,
    row,
    validationOrderbooks,
    runtimeConfig,
    feeRate,
    staleOrderbookMs,
    engineState,
    nowMs = Date.now(),
  } = options;
  const config = runtimeConfig || {};
  const candidateValidation = config.candidateValidation || {};
  const startAsset = row.startAsset || cycle.startAsset;
  const configuredStartAmount = candidateValidation.startAmountByAsset &&
    candidateValidation.startAmountByAsset[startAsset];
  const startAmount = Number(row.executableStartAmount || configuredStartAmount || 0);
  const planId = [
    "plan",
    executionLogMode(config.runMode),
    row.strategyId || config.activeStrategyId || "strategy",
    row.cycleId || cycle.cycleId,
    nowMs,
  ].join(":");
  const exchangeToServerLatencyMs = row.latency && Number.isFinite(Number(row.latency.upbitToServerMs))
    ? Number(row.latency.upbitToServerMs)
    : null;

  return {
    planId,
    mode: executionLogMode(config.runMode),
    executionMode: config.executionMode || "LIMIT_IOC_AT_OBSERVED_BEST",
    strategyId: row.strategyId || config.activeStrategyId,
    strategyVersion: row.strategyVersion || config.activeStrategyVersion || null,
    cycleId: row.cycleId || cycle.cycleId,
    routeVariantId: row.routeVariantId || cycle.routeVariantId,
    startAsset,
    status: row.status,
    marketState: row.marketState || row.status,
    opportunityClass: row.opportunityClass,
    startAmount,
    expectedOutputAmount: row.netMultiplier === null || row.netMultiplier === undefined
      ? null
      : startAmount * Number(row.netMultiplier),
    expectedNetProfit: row.netProfitRate === null || row.netProfitRate === undefined
      ? null
      : startAmount * Number(row.netProfitRate),
    cycle,
    validationOrderbooks: pickValidationOrderbooks(cycle, validationOrderbooks),
    feePolicyByMarket: options.feePolicyByMarket || row.feePolicyByMarket || null,
    marketPolicyByMarket: options.marketPolicyByMarket || row.marketPolicyByMarket || null,
    feeRate,
    useDefaultFeePolicy: options.useDefaultFeePolicy === true || row.useDefaultFeePolicy === true,
    maxDepthLevels: options.maxDepthLevels || row.maxDepthLevels || 1,
    staleOrderbookMs,
    engineState,
    nowMs,
    oldestLegAgeMs: row.oldestLegAgeMs,
    legTimestampSkewMs: legTimestampSkewMs(row.legTimestamps),
    exchangeToServerLatencyMs,
    decisionAgeMs: row.calculatedAtEpochMs ? Math.max(0, nowMs - Number(row.calculatedAtEpochMs)) : 0,
    latencyMs: row.latency && row.latency.estimatedEndToDisplayMs,
    executableStartAmount: row.executableStartAmount,
    maxExecutableStartAmount: row.maxExecutableStartAmount,
    sizingMode: row.sizingMode,
    sizingReason: row.sizingReason,
    sizingLiquidityStartAmount: row.sizingLiquidityStartAmount,
    sizingLegs: row.sizingLegs,
    limitingLeg: row.limitingLeg,
    limitingMarket: row.limitingMarket,
    expectedSlippageBps: row.expectedSlippageBps,
    bestLevelTouchRatio: row.bestLevelTouchRatio,
    validationStatus: row.validationStatus,
    validationReason: row.validationReason,
    observationValidationGapMs: row.observationValidationGapMs,
    observationValidationGapMarket: row.observationValidationGapMarket,
    validationLegTimestampSkewMs: row.validationLegTimestampSkewMs,
    oldestValidationReceivedAgeMs: row.oldestValidationReceivedAgeMs,
    validationOrderbookSources: row.validationOrderbookSources,
    validationOrderbookMetadata: row.validationOrderbookMetadata,
    expectedValidationOrderbookUnit: row.expectedValidationOrderbookUnit,
    executionEnqueuePerfNs: perfNowNs(),
  };
}

module.exports = {
  buildExecutionPlan,
  executionLogMode,
  legTimestampSkewMs,
  pickValidationOrderbooks,
};
