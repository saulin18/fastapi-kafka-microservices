from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_url: str = "localhost:9092"
    kafka_topic: str = "transactions"
    kafka_group_id: str = "antifraude"
    consumer_name: str = "antifraude"
    database_url: str = ""
    log_level: str = "INFO"
    redis_url: str = "redis://localhost:6379"


@lru_cache
def get_settings() -> Settings:
    return Settings()
