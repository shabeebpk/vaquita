"""Ask User Input Handler: Clarification.

Generates a focused clarification question for the user.
Question generation uses the global LLM service if enabled.
Loads prompts via the centralized prompt loader (app.prompts.loader).
Updates job status to WAITING_FOR_USER and stops the pipeline.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import os

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job
from app.llm import get_llm_service
from app.prompts.loader import load_prompt

logger = logging.getLogger(__name__)

# Fallback prompt for clarification question generation
CLARIFICATION_FALLBACK = "Clarify: {user_text}"


class AskUserInputHandler(Handler):
    """Generates a clarification question and pauses for user input."""
    
    def _generate_question(
        self,
        job_id: int,
        job_metadata: Dict[str, Any],
        measurements: Dict[str, Any],
    ) -> str:
        """Generate a focused clarification question.
        
        Optionally uses the global LLM service if CLARIFICATION_USE_LLM=1.
        Otherwise uses template-based generation.
        Loads prompts via the centralized prompt loader.
        """
        """
        # Load job config to check if LLM is enabled
        use_llm = False
        with Session(engine) as session:
             job = session.query(Job).filter(Job.id == job_id).first()
             if job and job.job_config:
                 expert_settings = job.job_config.get("expert_settings", {})
                 handlers_config = expert_settings.get("handlers", {})
                 use_llm = handlers_config.get("clarification_use_llm", False)
        
        if use_llm:
            try:
                # Load prompt template using centralized loader
                from app.config.system_settings import system_settings
                prompt_template = load_prompt(
                    system_settings.CLARIFICATION_PROMPT_FILE,
                    fallback=CLARIFICATION_FALLBACK
                )
                
                # Use the global LLM service
                llm_service = get_llm_service()
                user_text = job_metadata.get("user_text", "")[:500]
                
                # Format the loaded template with actual values
                prompt = prompt_template.format(
                    user_text=user_text,
                    ambiguity_score=measurements.get('ambiguity_score', 0.0)
                )
                
                question = llm_service.generate(prompt).strip()
                if question:
                    logger.info(f"Generated LLM question for job {job_id}: {question}")
                    return question
                else:
                    logger.warning(f"LLM returned empty question for job {job_id}; falling back to template")
            
            except Exception as e:
                logger.warning(f"LLM question generation failed for job {job_id}; falling back to template: {e}")
        
        # Template-based fallback
        from app.config.system_settings import system_settings
        ambiguity_score = measurements.get("ambiguity_score", 0.0)
        
        if ambiguity_score > 0.7:
            return load_prompt(system_settings.CLARIFICATION_HIGH_PROMPT_FILE)
        elif ambiguity_score > 0.5:
            return load_prompt(system_settings.CLARIFICATION_MEDIUM_PROMPT_FILE)
        else:
            return load_prompt(system_settings.CLARIFICATION_LOW_PROMPT_FILE)
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute clarification request.
        
        - Generates one focused question (LLM or template)
        - Updates job status to WAITING_FOR_USER
        - Stops the pipeline
        """
        try:
            measurements = decision_result.get("measurements", {})
            
            # Generate question
            question = self._generate_question(job_id, job_metadata, measurements)
            
            # Update job status
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "WAITING_FOR_USER"
                    session.commit()
                    logger.info(f"Job {job_id} marked WAITING_FOR_USER by AskUserInputHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            clarification_context = {
                "job_id": job_id,
                "question": question,
                "context": {
                    "ambiguity_score": measurements.get("ambiguity_score", 0.0),
                    "diversity_score": measurements.get("diversity_score", 0.0),
                },
                "awaiting_at": datetime.utcnow().isoformat(),
            }
            
            logger.info(f"Job {job_id} awaiting user clarification: {question}")
            
            return HandlerResult(
                status="deferred",
                message=f"Clarification needed: {question}",
                next_action="show_form",
                data=clarification_context,
            )
        
        except Exception as e:
            logger.error(f"AskUserInputHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to generate clarification question: {str(e)}",
                next_action="notify_user",
            )


# Register this handler
register_handler("ask_user_input", AskUserInputHandler)
