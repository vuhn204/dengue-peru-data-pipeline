from __future__ import annotations

import re
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine

from dengue_pipeline.config import Settings
from dengue_pipeline.processing import clean_chunk


def get_engine(settings: Settings) -> Engine:
    return create_engine(settings.database_url, fast_executemany=True, pool_pre_ping=True)


def get_admin_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.admin_database_url,
        fast_executemany=True,
        pool_pre_ping=True,
    )


def ensure_database_exists(settings: Settings) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", settings.sqlserver_db):
        raise ValueError(
            "El nombre de la base de datos solo puede tener letras, numeros y guion bajo."
        )

    admin_engine = get_admin_engine(settings)
    try:
        with wait_for_engine(admin_engine).connect() as connection:
            connection = connection.execution_options(isolation_level="AUTOCOMMIT")
            exists = connection.exec_driver_sql(
                "SELECT DB_ID(?)",
                (settings.sqlserver_db,),
            ).scalar()
            if exists is None:
                connection.exec_driver_sql(
                    f"CREATE DATABASE [{settings.sqlserver_db}]"
                )
                print(f"[db] base creada: {settings.sqlserver_db}")
    finally:
        admin_engine.dispose()


def wait_for_engine(engine: Engine, attempts: int = 30, delay_seconds: int = 4) -> Engine:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with engine.connect():
                print(f"[db] conexion disponible en intento {attempt}")
                return engine
        except OperationalError as error:
            last_error = error
            print(f"[db] esperando SQL Server (intento {attempt}/{attempts})...")
            time.sleep(delay_seconds)

    raise RuntimeError("No se pudo conectar a SQL Server a tiempo.") from last_error


def split_sql_script(script: str) -> list[str]:
    normalized = script.replace("\r\n", "\n")
    sections = re.split(r"^\s*GO\s*$", normalized, flags=re.MULTILINE | re.IGNORECASE)
    statements: list[str] = []

    for section in sections:
        current_lines: list[str] = []
        for line in section.split("\n"):
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            current_lines.append(line)

        statement = "\n".join(current_lines).strip()
        if statement:
            statements.append(statement)

    return statements


def execute_sql_file(engine: Engine, sql_path: Path) -> None:
    script = sql_path.read_text(encoding="utf-8")
    with engine.begin() as connection:
        for statement in split_sql_script(script):
            connection.exec_driver_sql(statement)


def recreate_raw_schema(engine: Engine, settings: Settings) -> None:
    execute_sql_file(engine, settings.sql_dir / "01_schema.sql")


def create_marts(engine: Engine, settings: Settings) -> None:
    execute_sql_file(engine, settings.sql_dir / "02_marts.sql")


def load_csv_to_sql_server(settings: Settings, engine: Engine) -> int:
    if not settings.csv_path.exists():
        raise FileNotFoundError(
            f"No se encontro el CSV esperado en: {settings.csv_path}"
        )

    total_rows = 0
    reader = pd.read_csv(
        settings.csv_path,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        chunksize=settings.chunk_size,
        low_memory=False,
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        cleaned_chunk = clean_chunk(chunk)
        cleaned_chunk.to_sql(
            "raw_dengue_cases",
            engine,
            if_exists="append",
            index=False,
            chunksize=1_000,
        )
        total_rows += len(cleaned_chunk)
        print(
            f"[ingest] chunk={chunk_number} rows_loaded={len(cleaned_chunk)} total_rows={total_rows}"
        )

    return total_rows
