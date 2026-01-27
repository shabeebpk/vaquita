"""In-memory cache for interim graph artifacts.

This cache stores Phase-2 structural graph results keyed by job_id so
that subsequent pipeline phases (e.g., Phase-2.5 sanitization) can reuse
the already-built graph instead of recomputing it.

This is intentionally simple and process-local. For multi-worker or
distributed deployments, replace this with a shared cache (Redis, memcached).
"""
from typing import Any, Dict
import threading

_lock = threading.RLock()
_structural_cache: Dict[int, Any] = {}


def set_structural_graph(job_id: int, value: Any) -> None:
    with _lock:
        _structural_cache[int(job_id)] = value


def get_structural_graph(job_id: int):
    with _lock:
        return _structural_cache.get(int(job_id))


def delete_structural_graph(job_id: int) -> None:
    with _lock:
        _structural_cache.pop(int(job_id), None)
