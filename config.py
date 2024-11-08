from typing import Optional

from pydantic import validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_NAME: str
    DATABASE_USER: str = "postgres"
    DATABASE_PASSWORD: str = "postgres"
    DATABASE_HOST: str = "localhost"
    ADM_DATABASE: str
    PORT: int = 5432

    OPENAI_API_KEY: Optional[str] = None
    ALTERNATIVE_CNPQ_SERVICE: bool = False
    JADE_EXTRATOR_FOLTER: Optional[str] = None
    HOME_SIMCC: Optional[str] = None

    MARIA_DATABASE_PASSWORD: Optional[str] = None
    MARIA_DATABASE_HOST: Optional[str] = None
    MARIA_DATABASE_NAME: Optional[str] = None
    MARIA_DATABASE_PORT: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @validator("OPENAI_API_KEY", pre=True, always=True)
    def check_openai_key(cls, v):
        if v is None:
            raise ValueError("OPENAI_API_KEY is required for production environments")
        return v

    @validator("ALTERNATIVE_CNPQ_SERVICE", pre=True, always=True)
    def check_proxy(cls, v):
        if v:
            print("O Download dos XMLs serão feitos via proxy no Tupi")
        return v


settings = Settings()
