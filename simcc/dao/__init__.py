import psycopg
import psycopg.rows

from ..config import settings


class Connection:
    def __init__(
        self,
        database: str = settings.DATABASE,
        user: str = settings.PG_USER,
        password: str = settings.PASSWORD,
        host: str = settings.HOST,
        port: int = settings.PORT,
    ):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def __enter__(self):
        self.connection = psycopg.connect(
            dbname=self.database,
            user=self.user,
            host=self.host,
            password=self.password,
            port=self.port,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                print(f"Erro ao fechar a conexão: {e}")
                raise

    def select(self, script_sql: str, parameters: list = []):
        with self.connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cursor.execute(script_sql, parameters)
            return cursor.fetchall()

    def exec(self, script_sql: str, parameters: list = []):
        with self.connection.cursor() as cursor:
            cursor.execute(script_sql, parameters)
            self.connection.commit()

    def exec_with_result(self, script_sql: str, parameters: list = []):
        with self.connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cursor.execute(script_sql, parameters)
            self.connection.commit()
            return cursor.fetchone()
