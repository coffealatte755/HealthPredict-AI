"""
deploy_endpoint.py
Mengambil versi model 'Approved' terbaru dari healthpredict-model-group
dan men-deploy real-time endpoint healthpredict-endpoint-diabetes.
Section 11.3. Jalankan dari SageMaker Studio terminal setelah kamu
mengubah status model package menjadi Approved di Model Registry console.

INGAT: hapus endpoint setelah selesai testing supaya tidak kena biaya idle.
"""

import boto3
import sagemaker
from sagemaker import ModelPackage

REGION = "us-west-2"
MODEL_PACKAGE_GROUP_NAME = "healthpredict-model-group"
ENDPOINT_NAME = "healthpredict-endpoint-diabetes"
ROLE = None  # will be resolved to LabRole below

session = boto3.Session(region_name=REGION)
sm_client = session.client("sagemaker")
account_id = session.client("sts").get_caller_identity()["Account"]
ROLE = f"arn:aws:iam::{account_id}:role/LabRole"

sagemaker_session = sagemaker.Session(boto_session=session)


def get_latest_approved_package():
    response = sm_client.list_model_packages(
        ModelPackageGroupName=MODEL_PACKAGE_GROUP_NAME,
        ModelApprovalStatus="Approved",
        SortBy="CreationTime",
        SortOrder="Descending",
        MaxResults=1,
    )
    packages = response.get("ModelPackageSummaryList", [])
    if not packages:
        raise RuntimeError(
            f"No Approved model package found in group '{MODEL_PACKAGE_GROUP_NAME}'. "
            "Approve a model version in the Model Registry console first."
        )
    return packages[0]["ModelPackageArn"]


def main():
    model_package_arn = get_latest_approved_package()
    print(f"Deploying model package: {model_package_arn}")

    model = ModelPackage(
        role=ROLE,
        model_package_arn=model_package_arn,
        sagemaker_session=sagemaker_session,
    )

    predictor = model.deploy(
        initial_instance_count=1,
        instance_type="ml.t2.medium",
        endpoint_name=ENDPOINT_NAME,
    )

    print(f"Endpoint '{ENDPOINT_NAME}' deployed and InService.")
    print("Test with predictor.predict(...) or via Lambda/API Gateway.")
    print("Remember to delete the endpoint after testing:")
    print(f'  boto3.client("sagemaker").delete_endpoint(EndpointName="{ENDPOINT_NAME}")')


if __name__ == "__main__":
    main()
