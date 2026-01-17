"""Signals module: signal computation and learning outcome application."""
from app.signals.evaluator import (
    SignalConfig,
    get_last_decision_before_run,
    get_current_decision_after_run,
    compute_measurement_delta,
)
from app.signals.applier import (
    classify_signal,
    apply_signal_result,
)

__all__ = [
    "SignalConfig",
    "get_last_decision_before_run",
    "get_current_decision_after_run",
    "compute_measurement_delta",
    "classify_signal",
    "apply_signal_result",
]
