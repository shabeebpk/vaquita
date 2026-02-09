"""Phase-5: Decision & Control

A provider-based decision-making layer that operates post-Phase-4.5.

The system separates concerns into three components:
1. Measurements: pure functions computing signals from artifacts
2. Providers: pluggable decision logic (rule-based, LLM, or hybrid)
3. Controller: orchestrates measurement + provider + persistence

Configuration:
- DECISION_PROVIDER env var: "rule_based" (default), "hybrid", or "llm"
- OPENAI_API_KEY: required only if using LLM provider
- DECISION_*_* env vars: all thresholds and factors are configurable (see config.py)

Public API:
- Decision enum: decision space
- compute_measurements(): signal computation with separated hypothesis populations
- DecisionProvider interface: extensible decision logic
- DecisionController: main orchestrator
- DecisionConfig: loads all thresholds from environment

Example usage:
    from app.decision.controller import get_decision_controller
    from app.graphs.persistence import get_semantic_graph
    from app.path_reasoning.persistence import get_hypotheses
    
    controller = get_decision_controller()  # uses DECISION_PROVIDER env var
    result = controller.decide(
        job_id=123,
        semantic_graph=get_semantic_graph(123),
        hypotheses=get_hypotheses(123),
        job_metadata={"id": 123, "status": "PATH_REASONING_DONE", ...}
    )
    # result = {
    #     "decision_label": "halt_confident",
    #     "provider_used": "rule_based",
    #     "measurements": {...},
    #     "fallback_used": False,
    #     "fallback_reason": None
    # }

Environment Variables (all optional with sensible defaults):
    DECISION_PROVIDER=rule_based                          # or "hybrid" or "llm"
    DECISION_CONFIDENCE_NORM_FACTOR=10                    # divide raw confidence by this
    DECISION_HIGH_CONFIDENCE_THRESHOLD=0.7                # normalized confidence >= this for HALT_CONFIDENT
    DECISION_DOMINANT_GAP_RATIO=0.3                       # 1st-2nd gap > (ratio * max) means dominant
    DECISION_LOW_DIVERSITY_PAIRS_THRESHOLD=2              # < this unique pairs = low diversity
    DECISION_DIVERSITY_RATIO_THRESHOLD=0.3                # diversity_score < this = low diversity
    DECISION_SPARSE_GRAPH_DENSITY_THRESHOLD=0.05          # graph_density < this = sparse
    DECISION_PASSED_TO_TOTAL_RATIO_THRESHOLD=0.2          # diagnostic: low pass rate warning
    DECISION_MINIMUM_HYPOTHESES_THRESHOLD=1               # total hypotheses < this = INSUFFICIENT_SIGNAL
"""

from app.decision.space import Decision, decision_from_string, all_decisions
from app.decision.config import DecisionConfig, get_decision_config
from app.decision.measurements import compute_measurements  # Import from parent measurements.py
from app.decision.providers import DecisionProvider, RuleBasedDecisionProvider, LLMDecisionProvider
from app.decision.controller import DecisionController, get_decision_controller

__all__ = [
    "Decision",
    "decision_from_string",
    "all_decisions",
    "DecisionConfig",
    "get_decision_config",
    "compute_measurements",
    "DecisionProvider",
    "RuleBasedDecisionProvider",
    "LLMDecisionProvider",
    "DecisionController",
    "get_decision_controller",
]
