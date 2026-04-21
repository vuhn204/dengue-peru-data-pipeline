from __future__ import annotations

from typing import Iterable

import pandas as pd

EXPECTED_COLUMNS = [
    "departamento",
    "provincia",
    "distrito",
    "localidad",
    "enfermedad",
    "ano",
    "semana",
    "diagnostic",
    "diresa",
    "ubigeo",
    "localcod",
    "edad",
    "tipo_edad",
    "sexo",
]

SEVERITY_MAP = {
    "DENGUE SIN SIGNOS DE ALARMA": "mild",
    "DENGUE CON SIGNOS DE ALARMA": "alert",
    "DENGUE GRAVE": "severe",
}


def normalize_columns(columns: Iterable[str]) -> list[str]:
    cleaned = []
    for column in columns:
        normalized = column.replace("\ufeff", "").strip().lower()
        cleaned.append(normalized)
    return cleaned


def convert_age_to_years(age_value: float | None, age_unit: str | None) -> float | None:
    if age_value is None or pd.isna(age_value):
        return None

    if age_unit == "A":
        return float(age_value)
    if age_unit == "M":
        return round(float(age_value) / 12, 2)
    if age_unit == "D":
        return round(float(age_value) / 365, 2)
    return None


def classify_age_group(age_years: float | None) -> str | None:
    if age_years is None or pd.isna(age_years):
        return None
    if age_years < 5:
        return "0-4"
    if age_years < 18:
        return "5-17"
    if age_years < 30:
        return "18-29"
    if age_years < 45:
        return "30-44"
    if age_years < 60:
        return "45-59"
    return "60+"


def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = normalize_columns(cleaned.columns)
    cleaned = cleaned.rename(
        columns={
            "departamento": "department",
            "provincia": "province",
            "distrito": "district",
            "localidad": "locality",
            "enfermedad": "disease",
            "ano": "year",
            "semana": "epidemiological_week",
            "diagnostic": "diagnosis_code",
            "diresa": "diresa_code",
            "localcod": "locality_code",
            "edad": "age_value",
            "tipo_edad": "age_unit",
            "sexo": "sex",
        }
    )

    required = {
        "department",
        "province",
        "district",
        "locality",
        "disease",
        "year",
        "epidemiological_week",
        "diagnosis_code",
        "diresa_code",
        "ubigeo",
        "locality_code",
        "age_value",
        "age_unit",
        "sex",
    }
    missing_columns = required.difference(cleaned.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in source data: {sorted(missing_columns)}")

    text_columns = [
        "department",
        "province",
        "district",
        "locality",
        "disease",
        "diagnosis_code",
        "diresa_code",
        "ubigeo",
        "locality_code",
        "age_unit",
        "sex",
    ]
    for column in text_columns:
        cleaned[column] = cleaned[column].astype("string").str.strip()
        cleaned[column] = cleaned[column].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    upper_columns = ["department", "province", "district", "disease", "age_unit", "sex"]
    for column in upper_columns:
        cleaned[column] = cleaned[column].str.upper()

    cleaned["year"] = pd.to_numeric(cleaned["year"], errors="coerce").astype("Int64")
    cleaned["epidemiological_week"] = pd.to_numeric(
        cleaned["epidemiological_week"], errors="coerce"
    ).astype("Int64")
    cleaned["age_value"] = pd.to_numeric(cleaned["age_value"], errors="coerce")

    cleaned["age_years"] = cleaned.apply(
        lambda row: convert_age_to_years(row["age_value"], row["age_unit"]), axis=1
    )
    cleaned["age_group"] = cleaned["age_years"].apply(classify_age_group)
    cleaned["severity_level"] = cleaned["disease"].map(SEVERITY_MAP).fillna("other")
    cleaned["case_count"] = 1

    cleaned = cleaned.dropna(subset=["department", "disease", "year", "epidemiological_week"])
    cleaned["year"] = cleaned["year"].astype(int)
    cleaned["epidemiological_week"] = cleaned["epidemiological_week"].astype(int)

    final_columns = [
        "department",
        "province",
        "district",
        "locality",
        "disease",
        "year",
        "epidemiological_week",
        "diagnosis_code",
        "diresa_code",
        "ubigeo",
        "locality_code",
        "age_value",
        "age_unit",
        "age_years",
        "age_group",
        "sex",
        "severity_level",
        "case_count",
    ]
    return cleaned[final_columns]
