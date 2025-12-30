from pydantic import BaseModel
from typing import List, Optional

class HypothesisOut(BaseModel):
    id: int
    job_id: int
    source: str
    target: str
    path: List[str]
    predicates: List[str]
    explanation: str
    confidence: int
    mode: str
    query_id: Optional[int] = None
    created_at: Optional[str] = None

class ExploreResponse(BaseModel):
    job_id: int
    hypotheses: List[HypothesisOut]

class QueryRequest(BaseModel):
    query_text: str
    max_hops: Optional[int] = 2
    allow_len3: Optional[bool] = False

class QueryResponse(BaseModel):
    query_id: int
    hypotheses: List[HypothesisOut]

class ReasoningQueryOut(BaseModel):
    id: int
    job_id: int
    query_text: str
    created_at: Optional[str] = None
