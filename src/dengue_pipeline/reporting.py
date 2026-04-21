from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy.engine import Engine

from dengue_pipeline.config import Settings


def ensure_report_dirs(settings: Settings) -> Path:
    figures_dir = settings.reports_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    return figures_dir


def export_top_departments(engine: Engine, figures_dir: Path) -> pd.DataFrame:
    query = """
        SELECT TOP 10 department, SUM(total_cases) AS total_cases
        FROM department_yearly_cases
        GROUP BY department
        ORDER BY total_cases DESC
    """
    top_departments = pd.read_sql_query(query, engine)

    plt.figure(figsize=(10, 6))
    sns.barplot(data=top_departments, x="total_cases", y="department", palette="viridis")
    plt.title("Top 10 departamentos con mas casos historicos de dengue")
    plt.xlabel("Casos acumulados")
    plt.ylabel("Departamento")
    plt.tight_layout()
    plt.savefig(figures_dir / "top_departments_total_cases.png", dpi=200)
    plt.close()

    return top_departments


def export_latest_year_trend(engine: Engine, figures_dir: Path) -> pd.DataFrame:
    latest_year_query = "SELECT MAX(year) AS latest_year FROM national_weekly_cases"
    latest_year = int(pd.read_sql_query(latest_year_query, engine)["latest_year"].iloc[0])

    # Resolvemos el top de departamentos en pandas para mantener la consulta simple.
    yearly_weekly = pd.read_sql_query(
        f"""
        SELECT department, epidemiological_week, total_cases
        FROM department_weekly_cases
        WHERE year = {latest_year}
        """,
        engine,
    )

    top_departments = (
        yearly_weekly.groupby("department", as_index=False)["total_cases"]
        .sum()
        .nlargest(5, "total_cases")
    )
    filtered = yearly_weekly.merge(top_departments[["department"]], on="department")

    plt.figure(figsize=(11, 6))
    sns.lineplot(
        data=filtered,
        x="epidemiological_week",
        y="total_cases",
        hue="department",
        linewidth=2,
    )
    plt.title(f"Evolucion semanal del dengue en {latest_year} (top 5 departamentos)")
    plt.xlabel("Semana epidemiologica")
    plt.ylabel("Casos")
    plt.tight_layout()
    plt.savefig(figures_dir / "latest_year_top5_trend.png", dpi=200)
    plt.close()

    return filtered


def export_severity_mix(engine: Engine, figures_dir: Path) -> pd.DataFrame:
    query = """
        SELECT
            SUM(mild_cases) AS mild_cases,
            SUM(alert_cases) AS alert_cases,
            SUM(severe_cases) AS severe_cases
        FROM department_yearly_cases
    """
    severity = pd.read_sql_query(query, engine)
    severity_long = severity.melt(var_name="severity", value_name="cases")
    severity_long["severity"] = severity_long["severity"].str.replace("_cases", "", regex=False)

    plt.figure(figsize=(8, 5))
    sns.barplot(data=severity_long, x="severity", y="cases", palette="magma")
    plt.title("Distribucion historica por severidad")
    plt.xlabel("Severidad")
    plt.ylabel("Casos")
    plt.tight_layout()
    plt.savefig(figures_dir / "severity_mix.png", dpi=200)
    plt.close()

    return severity_long


def write_executive_summary(
    engine: Engine,
    settings: Settings,
    top_departments: pd.DataFrame,
    latest_year_trend: pd.DataFrame,
    severity_mix: pd.DataFrame,
) -> Path:
    summary_query = """
        SELECT
            MIN(year) AS first_year,
            MAX(year) AS last_year,
            COUNT(*) AS total_department_weeks,
            SUM(total_cases) AS total_cases
        FROM department_weekly_cases
    """
    summary = pd.read_sql_query(summary_query, engine).iloc[0]
    top_department = top_departments.iloc[0]
    top_latest_department = (
        latest_year_trend.groupby("department", as_index=False)["total_cases"]
        .sum()
        .sort_values("total_cases", ascending=False)
        .iloc[0]
    )
    dominant_severity = severity_mix.sort_values("cases", ascending=False).iloc[0]

    report_path = settings.reports_dir / "executive_summary.md"
    report = f"""# Resumen Ejecutivo

- Cobertura temporal analizada: {int(summary['first_year'])} - {int(summary['last_year'])}
- Casos cargados en la capa analitica: {int(summary['total_cases']):,}
- Observaciones semanales por departamento: {int(summary['total_department_weeks']):,}
- Departamento con mas casos historicos: {top_department['department']} ({int(top_department['total_cases']):,} casos)
- Departamento con mayor concentracion en el ultimo ano disponible: {top_latest_department['department']} ({int(top_latest_department['total_cases']):,} casos)
- Severidad predominante: {dominant_severity['severity']} ({int(dominant_severity['cases']):,} casos)

## Hallazgos sugeridos para GitHub

1. El dengue presenta una carga muy concentrada geograficamente, con departamentos costeros y amazonicos dominando la serie historica.
2. La granularidad semanal permite estudiar estacionalidad y construir alertas tempranas por territorio.
3. La mayor parte de los registros corresponde a cuadros sin signos de alarma, pero la capa SQL deja trazabilidad lista para monitorear eventos graves.
4. La limpieza de `edad` por tipo de unidad (`A`, `M`, `D`) permite comparaciones demograficas consistentes.
"""
    report_path.write_text(report, encoding="utf-8")
    return report_path


def generate_reports(engine: Engine, settings: Settings) -> None:
    sns.set_theme(style="whitegrid")
    figures_dir = ensure_report_dirs(settings)
    top_departments = export_top_departments(engine, figures_dir)
    latest_year_trend = export_latest_year_trend(engine, figures_dir)
    severity_mix = export_severity_mix(engine, figures_dir)
    report_path = write_executive_summary(
        engine, settings, top_departments, latest_year_trend, severity_mix
    )
    print(f"[report] resumen generado en {report_path}")
