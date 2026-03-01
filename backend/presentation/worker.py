"""
Presentation Worker: LLM-powered narrative enrichment for SSE events.

This Celery worker runs as a dedicated consumer. Its only job:
  1. BLPOP from the Redis presentation event queue.
  2. Load the prompt template for the (phase, status) combination.
  3. Call the LLM to generate a human-readable explanation.
  4. Merge the explanation into the event.
  5. Publish the enriched event to the user's SSE channel via Redis pub/sub.

The worker is intentionally stateless and has no side effects on the DB.
It only reads from the queue and writes to the SSE channel.
"""

import json
import logging
import os
import time
import redis

from celery_app import celery_app
from app.config.admin_policy import admin_policy
from app.config.system_settings import system_settings
from presentation.prompts.loader import load_presentation_prompt

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

# (No longer using a custom Redis queue key; using Celery standard queueing)

# Map (phase, status) -> prompt filename
# All filenames are relative to app/prompts/
_PROMPT_MAP: dict[tuple[str, str | None], str] = {
    ("CREATION",        None):                    "presentation_creation.txt",
    ("INGESTION",       None):                    "presentation_ingestion.txt",
    ("TRIPLES",         None):                    "presentation_triples.txt",
    ("GRAPH",           None):                    "presentation_graph.txt",
    ("PATHREASONING",   None):                    "presentation_path_reasoning.txt",
    ("DECISION",        None):                    "presentation_decision.txt",
    ("DECISION",        "haltconfident"):          "presentation_decision_haltconfident.txt",
    ("DECISION",        "nohypo"):                "presentation_decision_nohypo.txt",
    ("DECISION",        "found"):                 "presentation_decision_found.txt",
    ("DECISION",        "notfound"):              "presentation_decision_notfound.txt",
    ("DECISION",        "insufficientsignal"):    "presentation_decision_insufficientsignal.txt",
    ("FETCH",           None):                    "presentation_fetch.txt",
    ("DOWNLOAD",        None):                    "presentation_download.txt",
}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _get_prompt_filename(phase: str, status: str | None) -> str | None:
    """Return the prompt filename for a given (phase, status) combination."""
    key = (phase, status)
    filename = _PROMPT_MAP.get(key)
    if filename is None and status is not None:
        # Fallback: try without status (for unexpected sub-statuses)
        filename = _PROMPT_MAP.get((phase, None))
    return filename


def _build_prompt(template: str, event: dict) -> str:
    """
    Fill in the prompt template with event data.
    Uses safe .get() calls so missing keys become readable placeholders.
    """
    result  = event.get("result")  or {}
    metric  = event.get("metric")  or {}
    payload = event.get("payload") or {}

    # Flatten all known keys for substitution
    context = {
        "job_id": event.get("job_id", ""),
        "phase": event.get("phase", ""),
        "status": event.get("status") or "",
        "next_action": event.get("next_action") or "pending",
        "error_reason": event.get("error_reason") or "none",
        # CREATION
        "job_type":       event.get("job_type") or result.get("job_type", "discovery"),
        "content_types":  result.get("content_types", ""),
        # INGESTION
        "total_ingested":    result.get("total_ingested", 0),
        "paper_count":       result.get("paper_count", 0),
        "upload_count":      result.get("upload_count", 0),
        "abstract_only_count": result.get("abstract_only_count", 0),
        "total_blocks":      metric.get("total_blocks", 0),
        "llms_used":         metric.get("llms_used", ""),
        # TRIPLES
        "total_triples":         result.get("total_triples", 0),
        "blocks_processed":      result.get("blocks_processed", 0),
        "avg_triples_per_block": result.get("avg_triples_per_block", "N/A"),
        # GRAPH
        "node_count":       result.get("node_count", 0),
        "edge_count":       result.get("edge_count", 0),
        "graph_version":    result.get("graph_version", 1),
        "semantic_merges":  result.get("semantic_merges", 0),
        # PATHREASONING
        "hypothesis_count": result.get("hypothesis_count", 0),
        "passed_count":     result.get("passed_count", 0),
        "hubs_suppressed":  result.get("hubs_suppressed", 0),
        # DECISION (generic)
        "decision_label":        result.get("decision_label", ""),
        "top_hypothesis":        result.get("top_hypothesis", "none"),
        "top_k_count":           result.get("top_k_count", 0),
        "is_dominant":           result.get("is_dominant", False),
        "measurements_summary":  metric.get("measurements_summary", ""),
        # DECISION / haltconfident
        "conclusion":        result.get("conclusion", ""),
        "dominant_hypothesis": result.get("dominant_hypothesis", ""),
        "max_confidence":    result.get("max_confidence", 0),
        "papers_used":       result.get("papers_used", 0),
        "total_cycles":      result.get("total_cycles", 1),
        # DECISION / nohypo
        "reason": result.get("reason", ""),
        # DECISION / found / notfound
        "source":              result.get("source", ""),
        "target":              result.get("target", ""),
        "verification_result": result.get("verification_result", ""),
        # DECISION / insufficientsignal
        "graph_size":    result.get("graph_size", 0),
        "growth_score":  result.get("growth_score", 0),
        "next_step":     result.get("next_step", "need more input"),
        # FETCH
        "searches_run":     result.get("searches_run", 0),
        "queries_created":  result.get("queries_created", 0),
        "papers_retrieved": result.get("papers_retrieved", 0),
        # DOWNLOAD
        "papers_downloaded":   result.get("papers_downloaded", 0),
        "impact_score_range":  metric.get("impact_score_range", "N/A"),
        "other_metrics":       metric.get("other_metrics", ""),
        "error_count":         result.get("error_count", 0),
        # Final evidence snippets for narration
        "final_evidence":      result.get("final_evidence", ""),
    }

    try:
        return template.format_map(context)
    except KeyError as e:
        logger.warning(f"Prompt template missing key {e}; using partial fill")
        # Safe partial fill
        import string
        formatter = string.Formatter()
        filled = template
        for _, field_name, _, _ in formatter.parse(template):
            if field_name and field_name not in context:
                filled = filled.replace("{" + field_name + "}", f"[{field_name}]")
        return filled.format_map(context)


def _call_llm(prompt: str) -> str:
    """Call the LLM using the specialized presentation fallback order.
    
    Returns the generated explanation string.
    Falls back to empty string on failure so the event still goes out.
    """
    from app.llm import get_llm_service
    from app.config.admin_policy import admin_policy
    
    # Use specialized presentation order and parameters if available
    if hasattr(admin_policy, "presentation"):
        order = admin_policy.presentation.llm_order
        temp = admin_policy.presentation.temperature
        max_t = admin_policy.presentation.max_tokens
    else:
        order = None
        temp = 0.4
        max_t = 300
    
    try:
        svc = get_llm_service()
        # Passing 'fallback_order' and parameters as keyword arguments
        response = svc.generate(prompt, fallback_order=order, max_tokens=max_t, temperature=temp)
        if response and response.strip():
            return response.strip()
    except Exception as e:
        logger.error(f"Presentation LLM call failed: {e}")

    return ""


def _publish_to_sse(event: dict) -> None:
    """Publish the enriched event to the user's Redis SSE pub/sub channel."""
    try:
        r = redis.from_url(system_settings.REDIS_URL)
        job_id = event.get("job_id", 0)
        # Default user routing: channel = user:<user_id>
        # Job -> user mapping: we store user_id in the event if available, else default 1
        user_id = event.get("user_id", 1)
        channel = f"user:{user_id}"
        r.publish(channel, json.dumps(event))
        logger.debug(f"Published enriched event to SSE channel {channel} for job {job_id}")
    except Exception as e:
        logger.error(f"Failed to publish SSE event: {e}")


# --------------------------------------------------------------------------
# Celery Task
# --------------------------------------------------------------------------

@celery_app.task(
    name="presentation.process_event",
    bind=True,
    max_retries=0,      # Never retry presentation narration; best-effort only
    ignore_result=True,
)
def process_presentation_event(self, event: dict) -> None:
    """Process a single presentation event: narrate it and push to SSE.
    
    This task is triggered by push_presentation_event via .apply_async() or .delay().
    """
    phase  = (event.get("phase") or "").upper()
    status = event.get("status")
    if status:
        status = status.lower()

    # 1. Find the right prompt file
    prompt_filename = _get_prompt_filename(phase, status)
    explanation = ""

    if prompt_filename:
        template = load_presentation_prompt(prompt_filename)
        if template:
            filled_prompt = _build_prompt(template, event)
            explanation = _call_llm(filled_prompt)
        else:
            logger.warning(f"Empty prompt template for phase={phase} status={status}")
    else:
        logger.warning(f"No prompt found for phase={phase} status={status}")

    # 2. Merge explanation into event
    event["explanation"] = explanation

    # 3. Publish to SSE
    _publish_to_sse(event)


# --------------------------------------------------------------------------
# Blocking Queue Consumer (run as separate process / celery beat alternative)
# --------------------------------------------------------------------------

# (Poller loop removed; using standard Celery queueing instead)
