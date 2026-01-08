import logging
import os

import httpx
from groq import Groq

from app.utils.config import (
    AVAILABLE_DOMAINS,
    DOMAIN_TO_WORKER_GROUP,
    GROQ_API_KEY,
    GROQ_MODEL,
)
from app.utils.schemas import PostEnvelope

logger = logging.getLogger(__name__)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "ollama")
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "tinyllama")


def _get_groq_client():
    """Lazy initialization of Groq client to avoid import-time errors."""
    if not GROQ_API_KEY:
        logger.warning("GROQ_API_KEY not set, will use Ollama fallback")
        return None
    return Groq(api_key=GROQ_API_KEY)


async def _call_ollama(prompt: str) -> str:
    """Fallback to local Ollama LLM."""
    try:
        url = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/generate"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                }
            )
            if response.status_code == 200:
                result = response.json()
                return result.get("response", "general").strip().lower()
            else:
                logger.error(f"Ollama error: {response.status_code}")
                return "general"
    except Exception as e:
        logger.error(f"Ollama fallback failed: {e}")
        return "general"


async def get_domain_from_llm(text: str) -> str:
    """Use LLM to intelligently classify post into a domain."""
    prompt = f"""Classify the following text into one of these domains: {', '.join(AVAILABLE_DOMAINS)}.

Respond with ONLY the domain name, nothing else.

Text: {text}"""

    # Try Groq first
    try:
        client = _get_groq_client()
        if client:
            message = client.messages.create(
                model=GROQ_MODEL,
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}],
            )
            domain = message.content[0].text.strip().lower()

            if domain in AVAILABLE_DOMAINS:
                logger.info(f"LLM classified post to domain: {domain}")
                return domain
            else:
                logger.warning(
                    f"LLM returned invalid domain '{domain}', defaulting to 'general'"
                )
                return "general"
    except Exception as e:
        logger.warning(f"Groq call failed: {e}, falling back to Ollama")

    # Fallback to Ollama
    domain = await _call_ollama(prompt)
    if domain in AVAILABLE_DOMAINS:
        logger.info(f"Ollama classified post to domain: {domain}")
        return domain
    return "general"


async def choose_worker_group(post: PostEnvelope) -> str:
    # 1. explicit domain from metadata
    domain = post.meta.get("domain")
    if domain in DOMAIN_TO_WORKER_GROUP:
        logger.info(f"Using explicit domain from metadata: {domain}")
        return DOMAIN_TO_WORKER_GROUP[domain]

    # 2. LLM-based intelligent domain routing
    domain = await get_domain_from_llm(post.text)
    return DOMAIN_TO_WORKER_GROUP[domain]
