from pydantic import DirectoryPath
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE: str = 'simcc'
    PG_USER: str = 'postgres'
    PASSWORD: str = 'postgres'
    HOST: str = 'localhost'
    PORT: int = 5432

    ADMIN_DATABASE: str = 'simcc_admin'
    ADMIN_PG_USER: str = 'postgres'
    ADMIN_PASSWORD: str = 'postgres'
    ADMIN_HOST: str = 'localhost'
    ADMIN_PORT: int = 5432

    ROOT_PATH: str = ''
    PROXY_URL: str = 'http://localhost:8080'
    ALTERNATIVE_CNPQ_SERVICE: bool = False

    JADE_EXTRATOR_FOLTER: DirectoryPath

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

    def get_simcc_connection_string(self) -> str:
        return f'postgresql://{self.PG_USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}'

    def get_simcc_admin_connection_string(self) -> str:
        return f'postgresql://{self.ADMIN_PG_USER}:{self.ADMIN_PASSWORD}@{self.ADMIN_HOST}:{self.ADMIN_PORT}/{self.ADMIN_DATABASE}'


settings = Settings()
