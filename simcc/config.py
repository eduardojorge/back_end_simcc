from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE: str = 'lab'
    PG_USER: str = 'postgres'
    PASSWORD: str = 'postgres'
    HOST: str = 'localhost'
    PORT: int = 5432

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

    def get_connection_string(self) -> str:
        return f'postgresql://{self.PG_USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}'


settings = Settings()
