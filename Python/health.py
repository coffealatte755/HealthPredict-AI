"""
health.py
healthpredict-lambda-health — GET /health (no auth required)
Checks that the SageMaker endpoint and DynamoDB table are reachable.
"""

import os
import json
import boto3

REGION = os.environ.get("AWS_REGION", "us-west-2")
SAGEMAKER_ENDPOINT_NAME = os.environ["SAGEMAKER_ENDPOINT_NAME"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]

sagemaker_client = boto3.client("sagemaker", region_name=REGION)
dynamodb_client = boto3.client("dynamodb", region_name=REGION)


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    checks = {}
    overall_ok = True

    try:
        desc = sagemaker_client.describe_endpoint(EndpointName=SAGEMAKER_ENDPOINT_NAME)
        checks["sagemaker_endpoint"] = desc["EndpointStatus"]
        if desc["EndpointStatus"] != "InService":
            overall_ok = False
    except Exception as e:  # noqa: BLE001
        checks["sagemaker_endpoint"] = f"ERROR: {e}"
        overall_ok = False

    try:
        desc = dynamodb_client.describe_table(TableName=DYNAMODB_TABLE_NAME)
        checks["dynamodb_table"] = desc["Table"]["TableStatus"]
        if desc["Table"]["TableStatus"] != "ACTIVE":
            overall_ok = False
    except Exception as e:  # noqa: BLE001
        checks["dynamodb_table"] = f"ERROR: {e}"
        overall_ok = False

    return _response(200 if overall_ok else 503, {"status": "ok" if overall_ok else "degraded", "checks": checks})
