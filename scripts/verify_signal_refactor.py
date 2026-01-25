import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Setup minimal valid SQLAlchemy environment (mocking actual app)
sys.path.append(os.getcwd())

# -------------------------------------------------------------
# MOCK JSONB FOR SQLITE COMPATIBILITY
# Must be done BEFORE importing models to ensure types are swapped
# -------------------------------------------------------------
import sqlalchemy.dialects.postgresql
from sqlalchemy import JSON
sqlalchemy.dialects.postgresql.JSONB = JSON

# Also verify that if models reference it directly, it works
from app.storage import models
models.JSONB = JSON
# -------------------------------------------------------------

from app.storage.models import Base, Job, SearchQuery, SearchQueryRun, DecisionResult
from app.signals.evaluator import find_pending_run_for_evaluation
from app.signals.applier import apply_signal_result

# Use in-memory SQLite for speed and isolation
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def run_verification():
    print("Running verification...")
    
    # 1. Setup Data
    job = Job(status="running")
    session.add(job)
    session.commit()
    
    query = SearchQuery(
        job_id=job.id,
        hypothesis_signature="test_sig",
        query_text="test query",
        status="new",
        config_snapshot={}
    )
    session.add(query)
    session.commit()
    
    # Timeline
    t0 = datetime.utcnow()
    t1 = t0 + timedelta(minutes=1)
    t2 = t0 + timedelta(minutes=2)
    t3 = t0 + timedelta(minutes=3)
    
    # T1: Previous Decision
    d1 = DecisionResult(
        job_id=job.id,
        decision_label="continue",
        provider_used="rule",
        measurements_snapshot={"score": 10},
        created_at=t1
    )
    session.add(d1)
    
    # T2: SearchQueryRun (with fetched IDs, but empty accepted/rejected)
    run = SearchQueryRun(
        search_query_id=query.id,
        job_id=job.id,
        provider_used="test",
        reason="initial",
        fetched_paper_ids=[1, 2, 3],
        accepted_paper_ids=[],
        rejected_paper_ids=[], 
        created_at=t2,
        signal_delta=None
    )
    session.add(run)
    
    # T3: Current Decision
    d2_snapshot = {
        "job_id": job.id,
        "created_at": t3,
        "measurements_snapshot": {"score": 20} # Improved score
    }
    
    session.commit()
    
    # 2. Verify Timing Logic (find_pending_run_for_evaluation)
    found_run = find_pending_run_for_evaluation(job.id, d2_snapshot, session)
    assert found_run is not None
    assert found_run.id == run.id
    print("✓ Timing logic verified: Found pending run between decisions.")
    
    # 3. Verify Attribution Logic (Positive Signal)
    # Apply positive signal (+1, reusable)
    apply_signal_result(run, 1, "reusable", session)
    session.commit()
    
    # Refresh run
    session.refresh(run)
    
    # Expect: accepted_paper_ids = [1, 2, 3], rejected_paper_ids = []
    assert run.signal_delta == 1
    assert run.accepted_paper_ids == [1, 2, 3]
    assert run.rejected_paper_ids == []
    print("✓ Positive attribution verified: Fetched IDs moved to Accepted.")
    
    # 4. Verify Idempotency / Timing again
    # Now run has signal_delta=1, so find_pending_run_for_evaluation should return None
    found_run_again = find_pending_run_for_evaluation(job.id, d2_snapshot, session)
    assert found_run_again is None
    print("✓ Idempotency verified: Run with signal already applied is ignored.")

    print("ALL TESTS PASSED")

if __name__ == "__main__":
    try:
        run_verification()
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
