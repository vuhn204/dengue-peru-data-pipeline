from __future__ import annotations

import pandas as pd

from dengue_pipeline.processing import clean_chunk, convert_age_to_years


def test_convert_age_to_years_handles_supported_units() -> None:
    assert convert_age_to_years(24, "M") == 2.0
    assert convert_age_to_years(730, "D") == 2.0
    assert convert_age_to_years(18, "A") == 18.0


def test_clean_chunk_normalizes_columns_and_derives_metrics() -> None:
    raw = pd.DataFrame(
        [
            {
                "\ufeffdepartamento": "Piura",
                "provincia": "Piura",
                "distrito": "Castilla",
                "localidad": "Centro",
                "enfermedad": "Dengue con signos de alarma",
                "ano": "2024",
                "semana": "10",
                "diagnostic": "A97.1",
                "diresa": "20",
                "ubigeo": "200101",
                "localcod": "",
                "edad": "24",
                "tipo_edad": "M",
                "sexo": "f",
            }
        ]
    )

    cleaned = clean_chunk(raw)
    row = cleaned.iloc[0]

    assert row["department"] == "PIURA"
    assert row["severity_level"] == "alert"
    assert row["age_years"] == 2.0
    assert row["age_group"] == "0-4"
    assert row["sex"] == "F"
    assert row["case_count"] == 1
