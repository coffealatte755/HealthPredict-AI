"""
predict.py
healthpredict-lambda-predict — POST /predict
Section 14.3
"""

import os
import json
import uuid
import time
import boto3
from datetime import datetime, timedelta

REGION = os.environ.get("AWS_REGION", "us-west-2")
SAGEMAKER_ENDPOINT_NAME = os.environ["SAGEMAKER_ENDPOINT_NAME"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
RISK_THRESHOLD_HIGH = float(os.environ.get("RISK_THRESHOLD_HIGH", 0.7))
RISK_THRESHOLD_LOW = float(os.environ.get("RISK_THRESHOLD_LOW", 0.3))

sagemaker_runtime = boto3.client("sagemaker-runtime", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
secrets_client = boto3.client("secretsmanager", region_name=REGION)

table = dynamodb.Table(DYNAMODB_TABLE_NAME)

REQUIRED_FIELDS = [
    "patient_id",
    "pregnancies",
    "glucose",
    "blood_pressure",
    "skin_thickness",
    "insulin",
    "bmi",
    "diabetes_pedigree",
    "age",
]

# Z-score normalization constants — must match the statistics computed by the
# Glue ETL job (Section 6.3, Step 5). Replace with the actual mean/std printed
# in the Glue job CloudWatch logs after running the ETL job once.
FEATURE_STATS = {
    "pregnancies": {"mean": 3.8, "std": 3.37},
    "glucose": {"mean": 121.7, "std": 30.5},
    "blood_pressure": {"mean": 72.4, "std": 12.1},
    "skin_thickness": {"mean": 29.1, "std": 8.8},
    "insulin": {"mean": 140.7, "std": 86.4},
    "bmi": {"mean": 32.5, "std": 6.9},
    "diabetes_pedigree": {"mean": 0.47, "std": 0.33},
    "age": {"mean": 33.2, "std": 11.8},
}


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _validate(payload):
    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        return f"Missing required fields: {', '.join(missing)}"

    ranges = {
        "glucose": (0, 500),
        "blood_pressure": (0, 250),
        "bmi": (0, 90),
        "age": (0, 120),
    }
    for field, (lo, hi) in ranges.items():
        val = payload[field]
        if not (lo <= val <= hi):
            return f"Field '{field}' out of physiological range ({lo}-{hi})"
    return None


def _normalize(payload):
    ordered = [
        "pregnancies", "glucose", "blood_pressure", "skin_thickness",
        "insulin", "bmi", "diabetes_pedigree", "age",
    ]
    normalized = []
    for f in ordered:
        stats = FEATURE_STATS[f]
        z = (float(payload[f]) - stats["mean"]) / stats["std"]
        normalized.append(z)
    return ",".join(str(v) for v in normalized)


def _invoke_endpoint(csv_payload, retries=3):
    delay = 1
    last_err = None
    for attempt in range(retries):
        try:
            resp = sagemaker_runtime.invoke_endpoint(
                EndpointName=SAGEMAKER_ENDPOINT_NAME,
                ContentType="text/csv",
                Accept="text/csv",
                Body=csv_payload,
            )
            score = float(resp["Body"].read().decode("utf-8").strip())
            return score
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"SageMaker endpoint invocation failed after {retries} attempts: {last_err}")


def _classify(score):
    if score >= RISK_THRESHOLD_HIGH:
        return "HIGH"
    if score >= RISK_THRESHOLD_LOW:
        return "MEDIUM"
    return "LOW"


def _write_dynamodb(payload, score, risk_level, prediction_id, timestamp_iso):
    ttl = int((datetime.utcnow() + timedelta(days=730)).timestamp())
    item = {
        "patient_id": payload["patient_id"],
        "prediction_timestamp": timestamp_iso,
        "prediction_id": prediction_id,
        "risk_score": str(round(score, 4)),
        "risk_level": risk_level,
        "model_version": SAGEMAKER_ENDPOINT_NAME,
        "expiry_time": ttl,
    }
    for f in REQUIRED_FIELDS:
        if f != "patient_id":
            item[f] = str(payload[f])
    table.put_item(Item=item)


def _write_redshift(payload, score, risk_level, prediction_id, timestamp_iso):
    # Import inside function so a missing psycopg2 layer never breaks the
    # primary DynamoDB write path.
    try:
        import psycopg2

        secret = json.loads(secrets_client.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)["SecretString"])
        conn = psycopg2.connect(
            host=secret["host"],
            port=secret.get("port", 5439),
            dbname=secret.get("dbname", "healthdb"),
            user=secret["username"],
            password=secret["password"],
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO healthpredict.prediction_log
            (prediction_id, patient_id, prediction_timestamp, risk_score, risk_level,
             model_version, pregnancies, glucose, blood_pressure, skin_thickness,
             insulin, bmi, diabetes_pedigree, age)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                prediction_id, payload["patient_id"], timestamp_iso, score, risk_level,
                SAGEMAKER_ENDPOINT_NAME, payload["pregnancies"], payload["glucose"],
                payload["blood_pressure"], payload["skin_thickness"], payload["insulin"],
                payload["bmi"], payload["diabetes_pedigree"], payload["age"],
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:  # noqa: BLE001
        # Non-fatal — logged for retry processing, does not affect API response
        print(f"[WARN] Redshift write failed, will retry later: {e}")


def _publish_alert(payload, score, prediction_id):
    message = {
        "prediction_id": prediction_id,
        "patient_id": payload["patient_id"],
        "risk_score": round(score, 4),
        "risk_level": "HIGH",
    }
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="HealthPredict AI — HIGH risk prediction alert",
        Message=json.dumps(message),
    )


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON body"})

    error = _validate(body)
    if error:
        return _response(400, {"error": error})

    csv_payload = _normalize(body)

    try:
        score = _invoke_endpoint(csv_payload)
    except RuntimeError as e:
        return _response(502, {"error": str(e)})

    risk_level = _classify(score)
    prediction_id = str(uuid.uuid4())
    timestamp_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # DynamoDB write is the primary write — a failure here fails the whole request
    try:
        _write_dynamodb(body, score, risk_level, prediction_id, timestamp_iso)
    except Exception as e:  # noqa: BLE001
        return _response(500, {"error": f"Failed to persist prediction: {e}"})

    # Redshift write is secondary/async — failure does not affect the API response
    _write_redshift(body, score, risk_level, prediction_id, timestamp_iso)

    if risk_level == "HIGH":
        try:
            _publish_alert(body, score, prediction_id)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] SNS publish failed: {e}")

    return _response(
        200,
        {
            "prediction_id": prediction_id,
            "patient_id": body["patient_id"],
            "risk_score": round(score, 4),
            "risk_level": risk_level,
            "model_version": SAGEMAKER_ENDPOINT_NAME,
            "timestamp": timestamp_iso,
        },
    )
