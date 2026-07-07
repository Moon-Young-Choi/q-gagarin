const crypto = require("node:crypto");
const { buildExecutionPlan } = require("../execution/executionPlan");

const strategy = {
  id: "bestLevelResidualIoc",
  aliases: ["bestLevelResidual", "bestLevelDynamicIoc"],
  name: "Best-level residual IOC",
  version: "0.1.0",
  description: "Sizes limit IOC plans from best-level depth while leaving at least one minimum-order residual.",
  defaultConfig: {
    requireDepthValidation: true,
    sizingMode: "best-level-residual",
  },

  evaluate(context) {
    const validation = context.depthValidation;
    const row = context.row || {};

    if (!validation || validation.validationStatus !== "accepted") {
      return {
        strategyId: this.id,
        strategyVersion: this.version,
        accepted: false,
        reason: validation ? validation.validationReason : "VALIDATION_UNAVAILABLE",
      };
    }

    if (row.sizingMode !== "best-level-residual") {
      return {
        strategyId: this.id,
        strategyVersion: this.version,
        accepted: false,
        reason: "DYNAMIC_SIZING_REQUIRED",
      };
    }

    return {
      strategyId: this.id,
      strategyVersion: this.version,
      accepted: true,
      reason: "BEST_LEVEL_RESIDUAL_SIZED",
    };
  },

  rank(rows) {
    return [...rows].sort((left, right) => {
      const leftProfit = Number.isFinite(left.netProfitRate) ? left.netProfitRate : -Infinity;
      const rightProfit = Number.isFinite(right.netProfitRate) ? right.netProfitRate : -Infinity;
      const leftAmount = Number.isFinite(left.executableStartAmount) ? left.executableStartAmount : -Infinity;
      const rightAmount = Number.isFinite(right.executableStartAmount) ? right.executableStartAmount : -Infinity;

      return rightProfit - leftProfit ||
        rightAmount - leftAmount ||
        String(left.routeLabel).localeCompare(String(right.routeLabel));
    });
  },

  buildExecutionPlan(context = {}) {
    const decision = context.decision || this.evaluate(context);
    const row = context.row || {};
    const validation = context.depthValidation || {};

    if (!decision.accepted || validation.validationStatus !== "accepted") {
      return null;
    }

    return buildExecutionPlan({
      ...context,
      row: {
        ...row,
        strategyId: decision.strategyId,
        strategyVersion: decision.strategyVersion,
        strategyAccepted: decision.accepted,
        strategyReason: decision.reason,
      },
    });
  },

  explain(decision) {
    return decision.reason;
  },
};

strategy.hash = crypto
  .createHash("sha256")
  .update(`${strategy.id}:${strategy.version}:${strategy.description}`)
  .digest("hex")
  .slice(0, 12);

module.exports = strategy;
