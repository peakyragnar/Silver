"""Backtest planning and thin falsifier primitives."""

from silver.backtest.momentum_falsifier import (
    DEFAULT_MIN_TRAIN_SESSIONS,
    DEFAULT_ROUND_TRIP_COST_BPS,
    DEFAULT_STEP_SESSIONS,
    DEFAULT_TEST_SESSIONS,
    MomentumBacktestRow,
    MomentumDateResult,
    MomentumFalsifierInputError,
    MomentumFalsifierResult,
    MomentumHeadlineMetrics,
    MomentumWindowResult,
    run_momentum_falsifier,
)
from silver.backtest.walk_forward import (
    WalkForwardConfig,
    WalkForwardConfigError,
    WalkForwardSplit,
    plan_walk_forward_splits,
)

__all__ = [
    "DEFAULT_MIN_TRAIN_SESSIONS",
    "DEFAULT_ROUND_TRIP_COST_BPS",
    "DEFAULT_STEP_SESSIONS",
    "DEFAULT_TEST_SESSIONS",
    "MomentumBacktestRow",
    "MomentumDateResult",
    "MomentumFalsifierInputError",
    "MomentumFalsifierResult",
    "MomentumHeadlineMetrics",
    "MomentumWindowResult",
    "WalkForwardConfig",
    "WalkForwardConfigError",
    "WalkForwardSplit",
    "plan_walk_forward_splits",
    "run_momentum_falsifier",
]
