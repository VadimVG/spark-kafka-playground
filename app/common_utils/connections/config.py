import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    db_name: str

    @property
    def pg_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db_name}"


pg_config = DBConfig(
    host = os.getenv("POSTGRES_HOST"),
    port = os.getenv("POSTGRES_PORT"),
    user = os.getenv("POSTGRES_USER"),
    password = os.getenv("POSTGRES_PASSWORD"),
    db_name = os.getenv("POSTGRES_DB"),
)