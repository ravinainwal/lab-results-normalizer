from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration.

    Values are read from environment variables (prefix ``LABNORM_``) so the
    same build artifact can be promoted across environments.
    """

    model_config = SettingsConfigDict(env_prefix="LABNORM_", env_file=".env", extra="ignore")

    app_name: str = "lab-normalizer"
    log_level: str = "INFO"
    max_upload_bytes: int = 5 * 1024 * 1024


settings = Settings()
