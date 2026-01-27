"""Predicate mapping for structural compression.

Closed mapping of verb substrings/lemmas to compact relation labels.
This file is intentionally small and curated to remain deterministic and
auditable. Modify with care.
"""
PREDICATE_MAP = {
    "use": "used_for",
    "utilize": "used_for",
    "apply": "used_for",
    "evaluate": "evaluated_by",
    "measure": "measured_by",
    "show": "demonstrates",
    "demonstrate": "demonstrates",
    "lead": "causes",
    "cause": "causes",
    "result": "results_in",
    "improve": "improves",
    "increase": "increases",
    "decrease": "decreases",
    "compare": "compares",
    "propose": "proposes",
    "introduce": "introduces",
    "develop": "develops",
    "train": "training",
    "generate": "generates",
    "produce": "produces",
    "suggest": "suggests",
    "find": "finds",
    "observe": "observes",
    "relate": "related_to",
    "associate": "related_to",
    "contain": "contains",
    "include": "includes",
}
