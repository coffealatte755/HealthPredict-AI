"""
history.py
healthpredict-lambda-history — GET /predict/{patient_id}  &  GET /history
Section 14.4
"""

import os
import json
import boto3
from boto3.dynamodb.conditions import Key

REGION = os.environ.get("AWS_REGION", "us-west-2")
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]

dynamodb = boto3.resource("dynamodb", region_name=REGION)
secrets_client = boto3.client("secretsmanager", region_name=REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _query_patient(patient_id):
    resp = table.query(
        KeyConditionExpression=Key("patient_id").eq(patient_id),
        ScanIndexForward=False,  # reverse chronological order
    )
    return resp.get("Items", [])


def _query_redshift_analytics():
    """Runs the three analytical views (Section 8.1) for aggregate history requests."""
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

    def fetch(query):
        cur.execute(query)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    result = {
        "risk_stats": fetch("SELECT * FROM healthpredict.v_risk_stats"),
        "monthly_trend": fetch("SELECT * FROM healthpredict.v_monthly_trend LIMIT 30"),
        "high_risk_patients": fetch("SELECT * FROM healthpredict.v_high_risk_patients LIMIT 20"),
    }

    cur.close()
    conn.close()
    return result


def lambda_handler(event, context):
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}
    patient_id = path_params.get("patient_id")

    if patient_id:
        # GET /predict/{patient_id}
        items = _query_patient(patient_id)
        return _response(200, {"patient_id": patient_id, "records": items, "count": len(items)})

    # GET /history
    analytics_requested = str(query_params.get("analytics", "false")).lower() == "true"
    response_body = {"note": "Use ?analytics=true for Redshift-backed aggregate insights"}

    if analytics_requested:
        try:
            response_body["analytics"] = _query_redshift_analytics()
        except Exception as e:  # noqa: BLE001
            response_body["analytics_error"] = str(e)

    return _response(200, response_body)
