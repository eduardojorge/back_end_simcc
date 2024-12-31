from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROXY_URL: str = 'http://localhost:8080/'


settings = Settings()
