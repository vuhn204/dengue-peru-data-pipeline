DROP VIEW IF EXISTS department_weekly_cases;
GO
DROP VIEW IF EXISTS department_yearly_cases;
GO
DROP VIEW IF EXISTS national_weekly_cases;
GO

CREATE VIEW department_weekly_cases AS
SELECT
    department,
    year,
    epidemiological_week,
    SUM(case_count) AS total_cases,
    SUM(CASE WHEN severity_level = 'mild' THEN case_count ELSE 0 END) AS mild_cases,
    SUM(CASE WHEN severity_level = 'alert' THEN case_count ELSE 0 END) AS alert_cases,
    SUM(CASE WHEN severity_level = 'severe' THEN case_count ELSE 0 END) AS severe_cases,
    AVG(age_years) AS avg_age_years,
    SUM(CASE WHEN sex = 'M' THEN case_count ELSE 0 END) AS male_cases,
    SUM(CASE WHEN sex = 'F' THEN case_count ELSE 0 END) AS female_cases
FROM raw_dengue_cases
GROUP BY department, year, epidemiological_week;
GO

CREATE VIEW department_yearly_cases AS
SELECT
    department,
    year,
    SUM(total_cases) AS total_cases,
    SUM(mild_cases) AS mild_cases,
    SUM(alert_cases) AS alert_cases,
    SUM(severe_cases) AS severe_cases,
    AVG(avg_age_years) AS avg_age_years,
    SUM(male_cases) AS male_cases,
    SUM(female_cases) AS female_cases
FROM department_weekly_cases
GROUP BY department, year;
GO

CREATE VIEW national_weekly_cases AS
SELECT
    year,
    epidemiological_week,
    SUM(total_cases) AS total_cases,
    SUM(mild_cases) AS mild_cases,
    SUM(alert_cases) AS alert_cases,
    SUM(severe_cases) AS severe_cases
FROM department_weekly_cases
GROUP BY year, epidemiological_week;
GO
