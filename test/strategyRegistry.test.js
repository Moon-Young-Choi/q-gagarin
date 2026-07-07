const test = require("node:test");
const assert = require("node:assert/strict");
const { DEFAULT_RUNTIME_CONFIG } = require("../src/core/runtimeConfig");
const { LiveTriangleState } = require("../src/live/liveState");
const { StrategyRegistry, createStrategyRegistry } = require("../src/strategies/registry");

const cycle = {
  cycleId: "cycle-1",
  routeVariantId: "cycle-1",
  startAsset: "KRW",
  steps: [
    { index: 0, fromAsset: "KRW", toAsset: "BTC", market: "KRW-BTC" },
  ],
};

const acceptedRow = {
  cycleId: "cycle-1",
  routeVariantId: "cycle-1",
  startAsset: "KRW",
  status: "available",
  routeLabel: "KRW -> BTC -> KRW",
  netProfitRate: 0.01,
  netMultiplier: 1.01,
  executableStartAmount: 10000,
  validationStatus: "accepted",
  validationReason: "ACCEPTED",
  calculatedAtEpochMs: 1000,
};

function planContext(strategy, overrides = {}) {
  const row = {
    ...acceptedRow,
    strategyId: strategy.id,
    strategyVersion: strategy.version,
    ...(overrides.row || {}),
  };
  const depthValidation = overrides.depthValidation || {
    validationStatus: row.validationStatus,
    validationReason: row.validationReason,
  };
  const decision = overrides.decision || strategy.evaluate({
    cycle,
    row,
    depthValidation,
    config: strategy.defaultConfig,
  });

  return {
    cycle,
    row,
    depthValidation,
    decision,
    validationOrderbooks: new Map([["KRW-BTC", { market: "KRW-BTC" }]]),
    runtimeConfig: {
      runMode: "DRY_RUN",
      executionMode: "LIMIT_IOC_AT_OBSERVED_BEST",
      activeStrategyId: strategy.id,
      candidateValidation: {
        startAmountByAsset: { KRW: 10000 },
      },
    },
    feeRate: 0,
    staleOrderbookMs: 1000,
    engineState: "RUNNING",
    nowMs: 1500,
  };
}

test("strategy registry exposes baseline and depth-aware strategies", () => {
  const registry = createStrategyRegistry();
  const strategies = registry.list();
  const baseline = registry.get("topOfBookBaseline");

  assert.deepEqual(strategies.map((strategy) => strategy.id).sort(), [
    "bestLevelResidualIoc",
    "depthAwareLimitIoc",
    "topOfBookBaseline",
  ]);
  assert.equal(registry.get("depthAwareBestIoc").id, "depthAwareLimitIoc");
  assert.equal(registry.get("bestLevelResidual").id, "bestLevelResidualIoc");
  assert.equal(baseline.evaluate({
    row: {
      status: "available",
      netProfitRate: 0.001,
    },
  }).accepted, true);
  assert.equal(baseline.evaluate({
    row: {
      status: "available",
      netProfitRate: 0.001,
    },
  }).strategyVersion, baseline.version);
  assert.equal(baseline.evaluate({
    row: {
      status: "available",
      netProfitRate: 0,
    },
  }).reason, "PROFIT_BELOW_THRESHOLD");
});

test("strategy registry rejects incomplete strategy contracts", () => {
  assert.throws(
    () => new StrategyRegistry([{
      id: "broken",
      name: "Broken",
      version: "0.0.0",
      description: "bad",
      defaultConfig: {},
      evaluate: () => ({}),
      rank: () => [],
      buildExecutionPlan: null,
      explain: () => "",
    }]),
    /must implement buildExecutionPlan/,
  );
});

test("strategies build execution plans through the shared contract", () => {
  const registry = createStrategyRegistry();
  const depthAware = registry.get("depthAwareLimitIoc");
  const baseline = registry.get("topOfBookBaseline");
  const residual = registry.get("bestLevelResidualIoc");
  const depthPlan = depthAware.buildExecutionPlan(planContext(depthAware));
  const baselinePlan = baseline.buildExecutionPlan(planContext(baseline));
  const residualPlan = residual.buildExecutionPlan(planContext(residual, {
    row: { sizingMode: "best-level-residual" },
  }));

  assert.equal(depthPlan.strategyId, "depthAwareLimitIoc");
  assert.equal(depthPlan.strategyVersion, depthAware.version);
  assert.equal(depthPlan.validationStatus, "accepted");
  assert.equal(depthPlan.startAmount, 10000);
  assert.equal(baselinePlan.strategyId, "topOfBookBaseline");
  assert.equal(baselinePlan.strategyVersion, baseline.version);
  assert.equal(residualPlan.strategyId, "bestLevelResidualIoc");
  assert.equal(depthAware.explain({ reason: "DEPTH_VALIDATED" }), "DEPTH_VALIDATED");
});

test("strategies do not build execution plans for rejected depth validation", () => {
  const registry = createStrategyRegistry();
  const depthAware = registry.get("depthAwareLimitIoc");
  const baseline = registry.get("topOfBookBaseline");
  const rejected = {
    row: {
      validationStatus: "rejected",
      validationReason: "DEPTH_INSUFFICIENT",
    },
    depthValidation: {
      validationStatus: "rejected",
      validationReason: "DEPTH_INSUFFICIENT",
    },
  };

  assert.equal(depthAware.buildExecutionPlan(planContext(depthAware, rejected)), null);
  assert.equal(baseline.buildExecutionPlan(planContext(baseline, rejected)), null);
});

test("strategy selection is guarded to STOPPED engine state", () => {
  const state = new LiveTriangleState({
    engineState: "STOPPED",
    runtimeConfig: {
      ...DEFAULT_RUNTIME_CONFIG,
      activeStrategyId: "depthAwareBestIoc",
    },
  });

  assert.equal(state.activeStrategy.id, "depthAwareLimitIoc");
  assert.equal(state.runtimeConfig.activeStrategyId, "depthAwareLimitIoc");

  state.selectStrategy("depthAwareBestIoc");
  assert.equal(state.activeStrategy.id, "depthAwareLimitIoc");
  assert.equal(state.runtimeConfig.activeStrategyId, "depthAwareLimitIoc");

  state.engineState = "RUNNING";
  assert.throws(() => state.selectStrategy("topOfBookBaseline"), /STOPPED/);
});
