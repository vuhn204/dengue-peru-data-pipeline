from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    sqlserver_db: str = os.getenv("SQLSERVER_DB", "dengue_peru")
    sqlserver_user: str = os.getenv("SQLSERVER_USER", "sa")
    sqlserver_password: str = os.getenv("SQLSERVER_PASSWORD", "DenguePass_2026!")
    sqlserver_host: str = os.getenv("SQLSERVER_HOST", "localhost")
    sqlserver_port: int = int(os.getenv("SQLSERVER_PORT", "1433"))
    sqlserver_driver: str = os.getenv(
        "SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"
    )
    csv_path: Path = PROJECT_ROOT / os.getenv(
        "CSV_PATH", "datos_abiertos_vigilancia_dengue_2000_2024.csv"
    )
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "50000"))
    reports_dir: Path = PROJECT_ROOT / "reports"
    sql_dir: Path = PROJECT_ROOT / "sql"

    @property
    def database_url(self) -> str:
        return str(
            URL.create(
                "mssql+pyodbc",
                username=self.sqlserver_user,
                password=self.sqlserver_password,
                host=self.sqlserver_host,
                port=self.sqlserver_port,
                database=self.sqlserver_db,
                query={
                    "driver": self.sqlserver_driver,
                    "TrustServerCertificate": "yes",
                    "Encrypt": "yes",
                },
            )
        )

    @property
    def admin_database_url(self) -> str:
        return str(
            URL.create(
                "mssql+pyodbc",
                username=self.sqlserver_user,
                password=self.sqlserver_password,
                host=self.sqlserver_host,
                port=self.sqlserver_port,
                database="master",
                query={
                    "driver": self.sqlserver_driver,
                    "TrustServerCertificate": "yes",
                    "Encrypt": "yes",
                },
            )
        )


def get_settings() -> Settings:
    return Settings()
