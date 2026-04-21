DROP TABLE IF EXISTS raw_dengue_cases;
GO

CREATE TABLE raw_dengue_cases (
    department NVARCHAR(120) NOT NULL,
    province NVARCHAR(120) NULL,
    district NVARCHAR(120) NULL,
    locality NVARCHAR(200) NULL,
    disease NVARCHAR(120) NOT NULL,
    year INT NOT NULL,
    epidemiological_week INT NOT NULL,
    diagnosis_code NVARCHAR(20) NULL,
    diresa_code NVARCHAR(20) NULL,
    ubigeo NVARCHAR(20) NULL,
    locality_code NVARCHAR(50) NULL,
    age_value DECIMAL(10, 2) NULL,
    age_unit NVARCHAR(10) NULL,
    age_years FLOAT NULL,
    age_group NVARCHAR(20) NULL,
    sex NVARCHAR(5) NULL,
    severity_level NVARCHAR(20) NOT NULL,
    case_count INT NOT NULL
);
GO

CREATE INDEX idx_raw_department_year_week
    ON raw_dengue_cases (department, year, epidemiological_week);
GO

CREATE INDEX idx_raw_severity
    ON raw_dengue_cases (severity_level);
GO

CREATE INDEX idx_raw_ubigeo
    ON raw_dengue_cases (ubigeo);
GO
