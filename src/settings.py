# src/tfm/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, PositiveInt
from typing import List

class Settings(BaseSettings):
    env: str = Field("dev", alias="TFM_ENV")

    #Configuracion
    data_storage_path: str = Field(..., alias="DATA_STORAGE_PATH")

    # Reddit
    reddit_client_id: str = Field(..., alias="TFM_REDDIT_CLIENT_ID")
    reddit_client_secret: str = Field(..., alias="TFM_REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field(..., alias="TFM_REDDIT_USER_AGENT")
    reddit_username: str = Field(..., alias="TFM_REDDIT_USERNAME")

    # Ingesta
    reddit_subreddits: List[str] = Field(default_factory=list, alias="TFM_REDDIT_SUBREDDITS")
    reddit_limit: PositiveInt = Field(50, alias="TFM_REDDIT_LIMIT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

settings = Settings()
