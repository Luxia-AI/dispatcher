import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTS_TOPIC = os.getenv("POSTS_TOPIC", "posts.inbound")
JOBS_TOPIC = os.getenv("JOBS_TOPIC", "jobs.to_worker")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "jobs.failed")
GROUP_ID = os.getenv("DISPATCHER_GROUP", "dispatcher-posts")
PROM_PORT = int(os.getenv("PROM_PORT", "9600"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# LLM Configuration
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "moonshotai/kimi-k2-instruct")

DOMAIN_TO_WORKER_GROUP = {
    "health": "health-workers",
    "finance": "finance-workers",
    "technology": "tech-workers",
    "education": "edu-workers",
    "entertainment": "entertainment-workers",
    "science": "science-workers",
    "sports": "sports-workers",
    "politics": "politics-workers",
    "business": "business-workers",
    "lifestyle": "lifestyle-workers",
    "travel": "travel-workers",
    "food": "food-workers",
    "fashion": "fashion-workers",
    "art": "art-workers",
    "music": "music-workers",
    "history": "history-workers",
    "nature": "nature-workers",
    "gaming": "gaming-workers",
    "general": "general-workers",
}

# Available domains for LLM to choose from
AVAILABLE_DOMAINS = list(DOMAIN_TO_WORKER_GROUP.keys())
