from __future__ import annotations

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
