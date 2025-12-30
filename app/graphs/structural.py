"""Structural compression (deterministic) applied to raw triples.

This module implements Phase 2 in the pipeline: read raw triples directly
from the `triples` table for a job (no aggregation, no thresholding) and
produce an in-memory projected graph using deterministic grammar-based
reductions with spaCy (noun chunks, POS, dependency heads, NER) and
Python/regex rules.

The projected graph is returned as a plain dict and contains provenance
links to original triple IDs, block IDs and source IDs. This module does
not modify the evidence/confidence graph or any database rows.
"""
from typing import Dict, Tuple, Set
import re
import logging

from sqlalchemy.orm import Session

from app.storage.db import engine
from app.storage.models import Triple
from app.graphs.normalizer import normalize_triple_component

from app.graphs.rules.predicates import PREDICATE_MAP
from app.graphs.rules.objects import OBJECT_ALLOW_LIST
from app.graphs.rules.fillers import LEADING_FILLERS, RATHER_THAN, OF_PREFIX, TRAILING_PUNCT

logger = logging.getLogger(__name__)


def _get_nlp():
    """Lazy-load spaCy English model; raise clear error if not installed."""
    try:
        import spacy
    except Exception as e:
        raise RuntimeError(
            "spaCy is required for structural projection. Install it with `pip install spacy` "
            "and a small English model, e.g. `python -m spacy download en_core_web_sm`."
        ) from e

    try:
        return spacy.load("en_core_web_sm")
    except Exception:
        # If model missing, provide actionable error
        raise RuntimeError(
            "spaCy model 'en_core_web_sm' not available. Run: `python -m spacy download en_core_web_sm`."
        )


# Load spaCy pipeline once at module import and reuse (singleton)
NLP = None
try:
    NLP = _get_nlp()
except Exception as e:
    # Do not crash at import time; defer error until projection is used.
    logger.warning("spaCy pipeline not loaded at import: %s", e)


def _extract_acronym(text: str) -> str:
    m = re.search(r"\(([A-Z0-9]{2,})s?\)", text)
    if m:
        return m.group(1)
    return ""


def project_subject(text: str) -> str:
    """Deterministically reduce subject text to a stable handle.

    Steps:
    - parenthetical acronym (LLM) -> acronym
    - named entity (spaCy) -> entity text
    - first noun chunk head lemma -> lemma
    - fallback: cleaned short phrase
    """
    if not text:
        return ""

    acr = _extract_acronym(text)
    if acr:
        return acr

    global NLP
    if NLP is None:
        NLP = _get_nlp()
    doc = NLP(text)

    # Prefer named entity
    if doc.ents:
        return doc.ents[0].text.strip()

    # noun chunk head
    for nc in doc.noun_chunks:
        head = nc.root
        if head.lemma_:
            return head.lemma_.lower()
        return head.text.lower()

    cleaned = re.sub(r"\b(the|a|an)\b", "", text, flags=re.I).strip()
    return cleaned.lower()


def project_predicate(text: str) -> str:
    """Map predicate text to a small closed relation set.

    Strategy:
    - substring match against closed map (longer keys first)
    - spaCy verb lemma fallback
    - default 'related_to'
    """
    if not text:
        return "related_to"

    txt = text.lower()
    # substring match (longest first)
    for key in sorted(PREDICATE_MAP.keys(), key=lambda k: -len(k)):
        if key in txt:
            return PREDICATE_MAP[key]

    global NLP
    if NLP is None:
        NLP = _get_nlp()
    doc = NLP(text)
    for tok in doc:
        if tok.pos_ in ("VERB", "AUX"):
            lemma = tok.lemma_.lower()
            if lemma in PREDICATE_MAP:
                return PREDICATE_MAP[lemma]
            return lemma

    return "related_to"


def _clean_object_phrase(phrase: str) -> str:
    phrase = re.sub(LEADING_FILLERS, "", phrase, flags=re.I)
    phrase = re.sub(RATHER_THAN, "", phrase, flags=re.I)
    phrase = re.sub(r"\s+", " ", phrase).strip()
    phrase = re.sub(TRAILING_PUNCT, "", phrase).strip()
    return phrase


def project_object(text: str) -> str:
    """Deterministically reduce object text to a short concept phrase.

    Steps:
    - clean filler clauses
    - choose longest noun chunk, extract head lemma
    - if head lemma in allow-list return lemma; else return cleaned chunk
    - fallback: first noun lemma or cleaned text
    """
    if not text:
        return ""

    global NLP
    if NLP is None:
        NLP = _get_nlp()
    clean = _clean_object_phrase(text)
    doc = NLP(clean)

    chunks = sorted(list(doc.noun_chunks), key=lambda c: -len(c.text))
    if chunks:
        nc = chunks[0]
        head = nc.root
        head_lemma = (head.lemma_.lower() if head.lemma_ else head.text.lower())
        if head_lemma in OBJECT_ALLOW_LIST:
            return head_lemma
        cleaned = re.sub(OF_PREFIX, "", nc.text.lower()).strip()
        return cleaned

    for tok in doc:
        if tok.pos_ in ("NOUN", "PROPN"):
            lemma = tok.lemma_.lower()
            if lemma in OBJECT_ALLOW_LIST:
                return lemma
            return lemma

    return clean.lower()


def project_structural_graph(job_id: int) -> Dict:
    """Build structural projected graph from raw triples for `job_id`.

    This reads raw `Triple` rows (no aggregation) and returns a dict:
      {
          "job_id": int,
          "total_triples": int,
          "projected_groups": int,
          "graph": {"nodes": [...], "edges": [...]}
      }

    Each edge: {subject, predicate, object, support, triple_ids, block_ids, source_ids}
    """
    projected: Dict[Tuple[str, str, str], Dict] = {}
    total = 0

    with Session(engine) as session:
        triples = session.query(Triple).filter(Triple.job_id == job_id).all()
        total = len(triples)

        for t in triples:
            try:
                # Normalize components for stable processing if needed
                # But we operate on raw text for structural compression
                ps = project_subject(t.subject or "")
                pp = project_predicate(t.predicate or "")
                po = project_object(t.object or "")

                key = (ps, pp, po)
                if key not in projected:
                    projected[key] = {
                        "subject": ps,
                        "predicate": pp,
                        "object": po,
                        "support": 0,
                        "triple_ids": set(),
                        "block_ids": set(),
                        "source_ids": set(),
                    }

                meta = projected[key]
                if t.id not in meta["triple_ids"]:
                    meta["triple_ids"].add(t.id)
                    meta["support"] += 1
                if t.block_id is not None:
                    meta["block_ids"].add(t.block_id)
                if t.source_id is not None:
                    meta["source_ids"].add(t.source_id)

            except Exception as e:
                logger.error("Error projecting triple %s: %s", getattr(t, "id", None), e)
                continue

    # Build deterministic graph
    nodes: Set[str] = set()
    edges = []
    for (s, p, o), m in sorted(projected.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        nodes.add(s)
        nodes.add(o)
        edges.append({
            "subject": s,
            "predicate": p,
            "object": o,
            "support": m["support"],
            "triple_ids": sorted(list(m["triple_ids"])),
            "block_ids": sorted(list(m["block_ids"])),
            "source_ids": sorted(list(m["source_ids"])),
        })

    graph = {"nodes": sorted(list(nodes)), "edges": edges}

    return {
        "job_id": job_id,
        "total_triples": total,
        "projected_groups": len(projected),
        "graph": graph,
    }
