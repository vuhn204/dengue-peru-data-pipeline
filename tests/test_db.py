from __future__ import annotations

from sqlalchemy.exc import InterfaceError

from dengue_pipeline.db import wait_for_engine
from dengue_pipeline.db import split_sql_script


def test_split_sql_script_handles_go_and_comments() -> None:
    script = """
    -- schema reset
    DROP VIEW IF EXISTS department_weekly_cases;
    GO

    CREATE VIEW department_weekly_cases AS
    SELECT department
    FROM raw_dengue_cases;
    GO
    """

    statements = split_sql_script(script)

    assert len(statements) == 2
    assert statements[0] == "DROP VIEW IF EXISTS department_weekly_cases;"
    assert "CREATE VIEW department_weekly_cases AS" in statements[1]


def test_wait_for_engine_retries_on_interface_error(monkeypatch) -> None:
    monkeypatch.setattr("dengue_pipeline.db.time.sleep", lambda _: None)

    class SuccessfulConnection:
        def __enter__(self) -> "SuccessfulConnection":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class FlakyEngine:
        def __init__(self) -> None:
            self.attempts = 0

        def connect(self) -> SuccessfulConnection:
            self.attempts += 1
            if self.attempts < 3:
                raise InterfaceError("SELECT 1", {}, Exception("login failed"))
            return SuccessfulConnection()

    engine = FlakyEngine()

    assert wait_for_engine(engine, attempts=3, delay_seconds=0) is engine
