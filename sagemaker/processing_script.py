"""
processing_script.py
SageMaker Processing Step (SKLearnProcessor) — healthpredict-training-pipeline
Section 10.2 - Step 1

Membaca Parquet hasil Glue ETL, melakukan normalisasi akhir menggunakan
statistik dari split training pada run pipeline ini (mencegah data leakage),
lalu menulis train.csv dan validation.csv ke output channel S3 yang
dikonsumsi oleh Training Step.

Input  : /opt/ml/processing/input/processed  (Parquet dari Glue)
Output : /opt/ml/processing/output/train/train.csv
         /opt/ml/processing/output/validation/validation.csv
"""

import os
import argparse
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

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
LABEL_COL = "outcome"

INPUT_DIR = "/opt/ml/processing/input/processed"
TRAIN_OUT_DIR = "/opt/ml/processing/output/train"
VAL_OUT_DIR = "/opt/ml/processing/output/validation"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    os.makedirs(TRAIN_OUT_DIR, exist_ok=True)
    os.makedirs(VAL_OUT_DIR, exist_ok=True)

    # Read all Parquet part-files produced by the Glue job
    parquet_files = [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.endswith(".parquet")
    ]
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    print(f"Loaded {len(df)} rows from Glue-processed Parquet")

    df = df[NUMERIC_FEATURES + [LABEL_COL]].dropna()

    train_df, val_df = train_test_split(
        df, test_size=args.test_size, random_state=args.seed, stratify=df[LABEL_COL]
    )

    # Fit normalization statistics ONLY on the training split visible to this
    # pipeline run, to avoid leakage from validation data
    scaler = StandardScaler()
    train_scaled = train_df.copy()
    val_scaled = val_df.copy()
    train_scaled[NUMERIC_FEATURES] = scaler.fit_transform(train_df[NUMERIC_FEATURES])
    val_scaled[NUMERIC_FEATURES] = scaler.transform(val_df[NUMERIC_FEATURES])

    # XGBoost built-in algorithm expects label as first column, no header, no index
    train_xgb = train_scaled[[LABEL_COL] + NUMERIC_FEATURES]
    val_xgb = val_scaled[[LABEL_COL] + NUMERIC_FEATURES]

    train_xgb.to_csv(os.path.join(TRAIN_OUT_DIR, "train.csv"), header=False, index=False)
    val_xgb.to_csv(os.path.join(VAL_OUT_DIR, "validation.csv"), header=False, index=False)

    print(f"Wrote {len(train_xgb)} training rows, {len(val_xgb)} validation rows")
    print("Normalization stats (mean):", dict(zip(NUMERIC_FEATURES, scaler.mean_)))
    print("Normalization stats (scale):", dict(zip(NUMERIC_FEATURES, scaler.scale_)))


if __name__ == "__main__":
    main()
