-- 1. Departamentos con mayor carga acumulada en toda la serie historica
SELECT TOP 10
    department,
    SUM(total_cases) AS total_cases,
    SUM(severe_cases) AS severe_cases,
    ROUND(100.0 * SUM(severe_cases) / NULLIF(SUM(total_cases), 0), 2) AS severe_share_pct
FROM department_yearly_cases
GROUP BY department
ORDER BY total_cases DESC;

-- 2. Ranking del ultimo ano disponible
WITH latest_year AS (
    SELECT MAX(year) AS year FROM department_yearly_cases
)
SELECT TOP 10
    d.department,
    d.year,
    d.total_cases,
    d.severe_cases,
    d.alert_cases,
    d.mild_cases
FROM department_yearly_cases d
JOIN latest_year y
    ON d.year = y.year
ORDER BY d.total_cases DESC;

-- 3. Semanas pico por departamento
WITH ranked_peaks AS (
    SELECT
        department,
        year,
        epidemiological_week,
        total_cases,
        ROW_NUMBER() OVER (
            PARTITION BY department
            ORDER BY total_cases DESC, year DESC
        ) AS peak_rank
    FROM department_weekly_cases
)
SELECT
    department,
    year,
    epidemiological_week,
    total_cases
FROM ranked_peaks
WHERE peak_rank = 1;

-- 4. Distribucion por sexo del ultimo ano disponible
WITH latest_year AS (
    SELECT MAX(year) AS year FROM department_yearly_cases
)
SELECT TOP 10
    d.department,
    d.male_cases,
    d.female_cases,
    ROUND(
        100.0 * d.female_cases / NULLIF(d.male_cases + d.female_cases, 0),
        2
    ) AS female_share_pct
FROM department_yearly_cases d
JOIN latest_year y
    ON d.year = y.year
ORDER BY d.total_cases DESC;
