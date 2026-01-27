"""Object allow-list and object-related rule constants.

This file contains a small, domain-agnostic allow-list of structural
concepts (model, method, dataset, hypothesis, etc.) used to reduce object
phrases to short concept tokens when appropriate.
"""
OBJECT_ALLOW_LIST = {
    "model",
    "method",
    "dataset",
    "algorithm",
    "hypothesis",
    "system",
    "generation",
    "training",
    "evaluation",
    "experiment",
    "metric",
    "result",
}
