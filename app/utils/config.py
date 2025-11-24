from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Allow extra fields in .env file
    )

    # Kafka Configuration
    kafka_bootstrap: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers",
    )
    posts_topic: str = Field(
        default="posts.inbound",
        description="Topic for inbound posts",
    )
    jobs_topic: str = Field(
        default="jobs.to_worker",
        description="Topic for job dispatches",
    )
    dlq_topic: str = Field(
        default="jobs.failed",
        description="Topic for failed jobs (Dead Letter Queue)",
    )
    dispatcher_group: str = Field(
        default="dispatcher-posts",
        description="Kafka consumer group ID",
    )

    # Server Configuration
    prom_port: int = Field(
        default=9600,
        description="Prometheus metrics port",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    # LLM Configuration (Groq)
    groq_api_key: str = Field(
        default="",
        description="Groq API key for LLM access",
    )
    groq_model: str = Field(
        default="mixtral-8x7b-32768",
        description="Groq model to use for domain classification",
    )

    # Domain routing configuration
    domain_to_worker_group: dict = Field(
        default_factory=lambda: {
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
        },
        description="Mapping of domains to worker groups",
    )

    @property
    def available_domains(self) -> list:
        """Get list of available domains."""
        return list(self.domain_to_worker_group.keys())


# Create global settings instance
settings = Settings()

# For backward compatibility, export as module-level variables
KAFKA_BOOTSTRAP = settings.kafka_bootstrap
POSTS_TOPIC = settings.posts_topic
JOBS_TOPIC = settings.jobs_topic
DLQ_TOPIC = settings.dlq_topic
GROUP_ID = settings.dispatcher_group
PROM_PORT = settings.prom_port
LOG_LEVEL = settings.log_level
GROQ_API_KEY = settings.groq_api_key
GROQ_MODEL = settings.groq_model
DOMAIN_TO_WORKER_GROUP = settings.domain_to_worker_group
AVAILABLE_DOMAINS = settings.available_domains
