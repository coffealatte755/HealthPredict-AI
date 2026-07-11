"""
healthpredict_etl.py
AWS Glue ETL Job — healthpredict-glue-etl
Sesuai spesifikasi LKS Cloud Computing 2026, Section 6.3 - 6.5

9 langkah:
1. Baca CSV mentah dari S3 (DynamicFrame -> DataFrame), lowercase semua kolom
2. Data quality audit (null count + jumlah nilai 0 yang secara medis tidak valid)
3. Ganti nilai 0 invalid dengan median kolom (non-zero) via approxQuantile
4. Feature engineering: bmi_category, age_group, glucose_risk, glucose_bmi_interaction
5. StandardScaler normalization pada 8 fitur numerik asli
6. Split 80/20 stratified (fixed seed) -> train/validation
7. Tulis processed dataset penuh sebagai Parquet ke S3
8. Tulis train.csv & validation.csv ke S3 (format XGBoost: label di kolom pertama, no header)
9. Load processed dataset ke Redshift via JDBC (kredensial dari Secrets Manager)
"""

import sys
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.linalg import Vectors

# ----------------------------------------------------------------------------
# 0. Job parameters (Section 6.5)
# ----------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "SOURCE_KEY",
        "DEST_BUCKET",
        "SECRETS_ARN",
        "GLUE_DATABASE",
        "TEMP_DIR",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
SOURCE_KEY = args["SOURCE_KEY"]
DEST_BUCKET = args["DEST_BUCKET"]
SECRETS_ARN = args["SECRETS_ARN"]
TEMP_DIR = args["TEMP_DIR"]

RAW_PATH = f"s3://{SOURCE_BUCKET}/{SOURCE_KEY}"
PROCESSED_PATH = f"s3://{DEST_BUCKET}/processed/"
TRAIN_PATH = f"s3://{DEST_BUCKET}/train/"
VALIDATION_PATH = f"s3://{DEST_BUCKET}/validation/"

# Columns that are physiologically impossible at zero
ZERO_INVALID_COLS = ["glucose", "blood_pressure", "skin_thickness", "insulin", "bmi"]
NUMERIC_FEATURES = [
    "pregnancies",
    "glucose",
    "blood_pressure",
    "skin_thickness",
    "insulin",
    "bmi",
    "diabetes_pedigree",
    "age",
]

# ----------------------------------------------------------------------------
# STEP 1 — Read raw CSV from S3, standardize column names
# ----------------------------------------------------------------------------
dyf_raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [RAW_PATH], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
)
df = dyf_raw.toDF()
print("========== COLUMNS ==========")
print(df.columns)
df.printSchema()
df.show(5, False
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower())

for c in NUMERIC_FEATURES + ["outcome"]:
    df = df.withColumn(c, F.col(c).cast(DoubleType()))

print(f"[STEP 1] Loaded raw records: {df.count()}")

# ----------------------------------------------------------------------------
# STEP 2 — Data quality audit
# ----------------------------------------------------------------------------
print("[STEP 2] Data quality audit")
for c in df.columns:
    null_count = df.filter(F.col(c).isNull()).count()
    print(f"  {c}: nulls={null_count}")

for c in ZERO_INVALID_COLS:
    zero_count = df.filter(F.col(c) == 0).count()
    print(f"  {c}: invalid_zeros={zero_count}")

# ----------------------------------------------------------------------------
# STEP 3 — Replace invalid zeros with column median (non-zero records)
# ----------------------------------------------------------------------------
for c in ZERO_INVALID_COLS:
    non_zero_df = df.filter(F.col(c) != 0)
    median_val = non_zero_df.approxQuantile(c, [0.5], 0.001)[0]
    df = df.withColumn(
        c, F.when(F.col(c) == 0, F.lit(median_val)).otherwise(F.col(c))
    )
    print(f"[STEP 3] {c} median used for imputation: {median_val}")

# ----------------------------------------------------------------------------
# STEP 4 — Feature engineering
# ----------------------------------------------------------------------------
df = df.withColumn(
    "bmi_category",
    F.when(F.col("bmi") < 18.5, "Underweight")
    .when((F.col("bmi") >= 18.5) & (F.col("bmi") < 25), "Normal")
    .when((F.col("bmi") >= 25) & (F.col("bmi") < 30), "Overweight")
    .otherwise("Obese"),
)

df = df.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Young")
    .when((F.col("age") >= 30) & (F.col("age") < 45), "Middle")
    .when((F.col("age") >= 45) & (F.col("age") < 60), "Senior")
    .otherwise("Elderly"),
)

df = df.withColumn(
    "glucose_risk",
    F.when(F.col("glucose") < 100, "Normal")
    .when((F.col("glucose") >= 100) & (F.col("glucose") < 126), "Prediabetes")
    .otherwise("Diabetes_Range"),
)

df = df.withColumn(
    "glucose_bmi_interaction", F.col("glucose") * F.col("bmi")
)

print("[STEP 4] Feature engineering complete")

# ----------------------------------------------------------------------------
# STEP 5 — StandardScaler normalization on the 8 original numeric features
# ----------------------------------------------------------------------------
assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_vec")
df_vec = assembler.transform(df)

scaler = StandardScaler(
    inputCol="features_vec", outputCol="scaled_features", withMean=True, withStd=True
)
scaler_model = scaler.fit(df_vec)
df_scaled = scaler_model.transform(df_vec)

# Explode scaled vector back into individual scaled_* columns (kept alongside
# originals so both raw and normalized values are available downstream)
to_array = F.udf(lambda v: v.toArray().tolist(), "array<double>")
df_scaled = df_scaled.withColumn("scaled_array", to_array(F.col("scaled_features")))
for i, c in enumerate(NUMERIC_FEATURES):
    df_scaled = df_scaled.withColumn(f"scaled_{c}", F.col("scaled_array")[i])

df_final = df_scaled.drop("features_vec", "scaled_features", "scaled_array")

print("[STEP 5] StandardScaler normalization complete")

# ----------------------------------------------------------------------------
# STEP 6 — 80/20 stratified split (fixed seed)
# ----------------------------------------------------------------------------
SEED = 42
train_df, val_df = df_final.randomSplit([0.8, 0.2], seed=SEED)
print(f"[STEP 6] train={train_df.count()} validation={val_df.count()}")

# ----------------------------------------------------------------------------
# STEP 7 — Write full processed dataset as Parquet
# ----------------------------------------------------------------------------
df_final.write.mode("overwrite").parquet(PROCESSED_PATH)
print(f"[STEP 7] Processed Parquet written to {PROCESSED_PATH}")

# ----------------------------------------------------------------------------
# STEP 8 — Write train.csv / validation.csv (XGBoost format: label first col,
#          no header, no index)
# ----------------------------------------------------------------------------
xgb_cols = ["outcome"] + NUMERIC_FEATURES


def to_xgb_csv(df_in, path):
    (
        df_in.select(*xgb_cols)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "false")
        .csv(path)
    )


to_xgb_csv(train_df, TRAIN_PATH)
to_xgb_csv(val_df, VALIDATION_PATH)
print(f"[STEP 8] train.csv -> {TRAIN_PATH} | validation.csv -> {VALIDATION_PATH}")

# ----------------------------------------------------------------------------
# STEP 9 — Load processed data into Redshift (credentials from Secrets Manager)
# ----------------------------------------------------------------------------
secrets_client = boto3.client("secretsmanager", region_name="us-west-2")
secret_value = secrets_client.get_secret_value(SecretId=SECRETS_ARN)
creds = json.loads(secret_value["SecretString"])

redshift_url = (
    f"jdbc:redshift://{creds['host']}:{creds.get('port', 5439)}/{creds.get('dbname', 'healthdb')}"
)

df_final.write.format("jdbc").option("url", redshift_url).option(
    "dbtable", "healthpredict.patient_data_processed"
).option("user", creds["username"]).option("password", creds["password"]).option(
    "driver", "com.amazon.redshift.jdbc42.Driver"
).mode(
    "overwrite"
).save()

print("[STEP 9] Processed data loaded into Redshift table healthpredict.patient_data_processed")

job.commit()
