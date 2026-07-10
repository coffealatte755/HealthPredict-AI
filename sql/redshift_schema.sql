-- =============================================================================
-- redshift_schema.sql
-- HealthPredict AI — Amazon Redshift schema
-- Jalankan lewat Redshift Query Editor v2, login sebagai adminuser / healthdb
-- Section 8.1
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS healthpredict;

-- -----------------------------------------------------------------------------
-- TABLE 1: prediction_log
-- Setiap event prediksi dari API. patient_id = distkey, prediction_timestamp = sortkey
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS healthpredict.prediction_log (
    prediction_id          VARCHAR(64)     NOT NULL,
    patient_id             VARCHAR(64)     NOT NULL,
    prediction_timestamp   TIMESTAMP       NOT NULL,
    risk_score             FLOAT8          NOT NULL,
    risk_level             VARCHAR(16)     NOT NULL,
    model_version           VARCHAR(128)    NOT NULL,
    pregnancies             SMALLINT,
    glucose                  FLOAT8,
    blood_pressure           FLOAT8,
    skin_thickness            FLOAT8,
    insulin                   FLOAT8,
    bmi                        FLOAT8,
    diabetes_pedigree          FLOAT8,
    age                        SMALLINT
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (prediction_timestamp);

-- -----------------------------------------------------------------------------
-- TABLE 2: patient_data_processed
-- Dataset penuh hasil Glue ETL (di-load pada Step 9 script ETL). EVEN distribution
-- karena dianalisis secara agregat lintas semua baris.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS healthpredict.patient_data_processed (
    pregnancies                 SMALLINT,
    glucose                      FLOAT8,
    blood_pressure                FLOAT8,
    skin_thickness                 FLOAT8,
    insulin                        FLOAT8,
    bmi                             FLOAT8,
    diabetes_pedigree               FLOAT8,
    age                             SMALLINT,
    outcome                         SMALLINT,
    bmi_category                    VARCHAR(32),
    age_group                       VARCHAR(32),
    glucose_risk                    VARCHAR(32),
    glucose_bmi_interaction         FLOAT8,
    scaled_pregnancies               FLOAT8,
    scaled_glucose                    FLOAT8,
    scaled_blood_pressure              FLOAT8,
    scaled_skin_thickness                FLOAT8,
    scaled_insulin                        FLOAT8,
    scaled_bmi                             FLOAT8,
    scaled_diabetes_pedigree                FLOAT8,
    scaled_age                              FLOAT8
)
DISTSTYLE EVEN;

-- -----------------------------------------------------------------------------
-- TABLE 3: model_registry
-- Katalog versi model, metrik performa, endpoint tempat dideploy
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS healthpredict.model_registry (
    model_version_id    VARCHAR(64)   NOT NULL,
    model_package_arn   VARCHAR(256)  NOT NULL,
    training_job_name   VARCHAR(256),
    auc_score           FLOAT8,
    approval_status     VARCHAR(32),
    endpoint_name        VARCHAR(128),
    registered_at        TIMESTAMP     DEFAULT GETDATE(),
    approved_at           TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (registered_at);

-- -----------------------------------------------------------------------------
-- TABLE 4: daily_summary
-- Agregat harian pre-computed, diisi oleh sp_update_daily_summary
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS healthpredict.daily_summary (
    summary_date        DATE          NOT NULL,
    total_predictions    INT,
    high_risk_count       INT,
    medium_risk_count      INT,
    low_risk_count           INT,
    avg_risk_score            FLOAT8,
    PRIMARY KEY (summary_date)
)
DISTSTYLE ALL
SORTKEY (summary_date);

-- =============================================================================
-- VIEWS
-- =============================================================================

-- v_risk_stats: agregat per risk_level (count, avg score, avg demografi)
CREATE OR REPLACE VIEW healthpredict.v_risk_stats AS
SELECT
    risk_level,
    COUNT(*)                       AS total_predictions,
    ROUND(AVG(risk_score), 4)      AS avg_risk_score,
    ROUND(AVG(age), 1)             AS avg_age,
    ROUND(AVG(bmi), 2)             AS avg_bmi,
    ROUND(AVG(glucose), 2)         AS avg_glucose
FROM healthpredict.prediction_log
GROUP BY risk_level;

-- v_monthly_trend: volume prediksi & rata-rata skor harian, 30 hari terakhir
CREATE OR REPLACE VIEW healthpredict.v_monthly_trend AS
SELECT
    TRUNC(prediction_timestamp)        AS prediction_date,
    COUNT(*)                            AS daily_predictions,
    ROUND(AVG(risk_score), 4)           AS avg_risk_score,
    SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count
FROM healthpredict.prediction_log
WHERE prediction_timestamp >= DATEADD(day, -30, GETDATE())
GROUP BY TRUNC(prediction_timestamp)
ORDER BY prediction_date DESC;

-- v_high_risk_patients: agregat per pasien, diurutkan skor risiko maksimum
CREATE OR REPLACE VIEW healthpredict.v_high_risk_patients AS
SELECT
    patient_id,
    COUNT(*)                          AS total_predictions,
    MAX(risk_score)                    AS max_risk_score,
    MAX(prediction_timestamp)           AS last_prediction_at,
    MAX(risk_level)                      AS latest_risk_level
FROM healthpredict.prediction_log
GROUP BY patient_id
ORDER BY max_risk_score DESC;

-- =============================================================================
-- STORED PROCEDURE
-- sp_update_daily_summary: hitung ulang agregat harian untuk tanggal tertentu
-- =============================================================================
CREATE OR REPLACE PROCEDURE healthpredict.sp_update_daily_summary(target_date DATE)
AS $$
BEGIN
    DELETE FROM healthpredict.daily_summary WHERE summary_date = target_date;

    INSERT INTO healthpredict.daily_summary
        (summary_date, total_predictions, high_risk_count, medium_risk_count,
         low_risk_count, avg_risk_score)
    SELECT
        target_date,
        COUNT(*),
        SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END),
        SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END),
        SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END),
        ROUND(AVG(risk_score), 4)
    FROM healthpredict.prediction_log
    WHERE TRUNC(prediction_timestamp) = target_date;
END;
$$ LANGUAGE plpgsql;

-- Contoh pemanggilan:
-- CALL healthpredict.sp_update_daily_summary(GETDATE()::DATE);
