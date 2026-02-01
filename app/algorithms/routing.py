import logging

from groq import Groq

from app.utils.config import (
    AVAILABLE_DOMAINS,
    DOMAIN_TO_WORKER_GROUP,
    GROQ_API_KEY,
    GROQ_MODEL,
)
from app.utils.schemas import PostEnvelope

logger = logging.getLogger(__name__)


def _get_groq_client():
    """Lazy initialization of Groq client to avoid import-time errors."""
    if not GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY environment variable is not set")
    return Groq(api_key=GROQ_API_KEY)


async def get_domain_from_llm(text: str) -> str:
    """Use LLM to intelligently classify post into a domain."""
    prompt = f"""Classify the following text into one of these domains: {', '.join(AVAILABLE_DOMAINS)}.

Respond with ONLY the domain name, nothing else.

Text: {text}"""

    try:
        client = _get_groq_client()
        if client:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}],
            )
            domain = response.choices[0].message.content.strip().lower()

            if domain in AVAILABLE_DOMAINS:
                logger.info(f"LLM classified post to domain: {domain}")
                return domain
            else:
                logger.warning(
                    f"LLM returned invalid domain '{domain}', defaulting to 'general'"
                )
                return "general"
            
    except Exception as e:
        logger.error(f"Error calling LLM for domain classification: {e}")
        return "general"


async def choose_worker_group(post: PostEnvelope) -> str:
    # For now, always route to general worker
    return "general"
