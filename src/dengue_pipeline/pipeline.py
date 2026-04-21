from __future__ import annotations

from dengue_pipeline.config import get_settings
from dengue_pipeline.db import (
    create_marts,
    ensure_database_exists,
    get_engine,
    load_csv_to_sql_server,
    recreate_raw_schema,
    wait_for_engine,
)
from dengue_pipeline.reporting import generate_reports


def run_ingestion() -> None:
    settings = get_settings()
    ensure_database_exists(settings)
    engine = wait_for_engine(get_engine(settings))
    recreate_raw_schema(engine, settings)
    total_rows = load_csv_to_sql_server(settings, engine)
    print(f"[ingest] carga completada con {total_rows} filas")


def run_transformations() -> None:
    settings = get_settings()
    ensure_database_exists(settings)
    engine = wait_for_engine(get_engine(settings))
    create_marts(engine, settings)
    print("[transform] vistas analiticas actualizadas")


def run_reporting() -> None:
    settings = get_settings()
    ensure_database_exists(settings)
    engine = wait_for_engine(get_engine(settings))
    generate_reports(engine, settings)
