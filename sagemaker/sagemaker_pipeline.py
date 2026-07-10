"""
sagemaker_pipeline.py
Registers healthpredict-training-pipeline (does NOT start execution).
Run from a SageMaker Studio terminal:
    python sagemaker_pipeline.py

Sesuai Section 10.2 - 10.3:
  Step 1: Processing (SKLearnProcessor)  -> processing_script.py
  Step 2: Training   (XGBoost 1.7-1 built-in)
  Step 3: Model Registration -> healthpredict-model-group (PendingManualApproval)
"""

import sagemaker
import boto3
from sagemaker import get_execution_role
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.inputs import TrainingInput
from sagemaker.estimator import Estimator
from sagemaker.workflow.model_step import ModelStep
from sagemaker.model import Model
from sagemaker.image_uris import retrieve

# ----------------------------------------------------------------------------
# Configuration — edit these three values before running
# ----------------------------------------------------------------------------
REGION = "us-west-2"
STUDENT_NAME = "changeme"          # e.g. "budi"  -> matches your S3 bucket suffix
ROLE = "arn:aws:iam::{}:role/LabRole"

session = boto3.Session(region_name=REGION)
account_id = session.client("sts").get_caller_identity()["Account"]
role = ROLE.format(account_id)

pipeline_session = PipelineSession()

DATASET_BUCKET = f"healthpredict-dataset-{STUDENT_NAME}-2026"
MODELS_BUCKET = f"healthpredict-models-{STUDENT_NAME}-2026"

PROCESSED_S3 = f"s3://{DATASET_BUCKET}/processed/"
TRAIN_OUT_S3 = f"s3://{DATASET_BUCKET}/pipeline-train/"
VAL_OUT_S3 = f"s3://{DATASET_BUCKET}/pipeline-validation/"
MODEL_OUT_S3 = f"s3://{MODELS_BUCKET}/training-jobs/"

MODEL_PACKAGE_GROUP_NAME = "healthpredict-model-group"
PIPELINE_NAME = "healthpredict-training-pipeline"

# ----------------------------------------------------------------------------
# Step 1 — Processing Step
# ----------------------------------------------------------------------------
sklearn_processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_type="ml.t3.medium",
    instance_count=1,
    base_job_name="healthpredict-processing",
    sagemaker_session=pipeline_session,
)

step_process = ProcessingStep(
    name="HealthPredictProcessing",
    processor=sklearn_processor,
    code="processing_script.py",
    inputs=[
        ProcessingInput(
            source=PROCESSED_S3,
            destination="/opt/ml/processing/input/processed",
        )
    ],
    outputs=[
        ProcessingOutput(
            output_name="train",
            source="/opt/ml/processing/output/train",
            destination=TRAIN_OUT_S3,
        ),
        ProcessingOutput(
            output_name="validation",
            source="/opt/ml/processing/output/validation",
            destination=VAL_OUT_S3,
        ),
    ],
)

# ----------------------------------------------------------------------------
# Step 2 — Training Step (XGBoost 1.7-1 built-in algorithm)
# ----------------------------------------------------------------------------
xgboost_image_uri = retrieve(framework="xgboost", region=REGION, version="1.7-1")

xgb_estimator = Estimator(
    image_uri=xgboost_image_uri,
    role=role,
    instance_type="ml.m5.large",
    instance_count=1,
    output_path=MODEL_OUT_S3,
    sagemaker_session=pipeline_session,
    hyperparameters={
        "num_round": 100,
        "max_depth": 5,
        "eta": 0.2,
        "objective": "binary:logistic",
        "eval_metric": "auc",
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 6,
        "early_stopping_rounds": 10,
        "seed": 42,
    },
)

step_train = TrainingStep(
    name="HealthPredictTraining",
    estimator=xgb_estimator,
    inputs={
        "train": TrainingInput(
            s3_data=step_process.properties.ProcessingOutputConfig.Outputs[
                "train"
            ].S3Output.S3Uri,
            content_type="text/csv",
        ),
        "validation": TrainingInput(
            s3_data=step_process.properties.ProcessingOutputConfig.Outputs[
                "validation"
            ].S3Output.S3Uri,
            content_type="text/csv",
        ),
    },
)

# ----------------------------------------------------------------------------
# Step 3 — Model Registration Step (PendingManualApproval)
# ----------------------------------------------------------------------------
model = Model(
    image_uri=xgboost_image_uri,
    model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
    role=role,
    sagemaker_session=pipeline_session,
)

register_args = model.register(
    content_types=["text/csv"],
    response_types=["text/csv"],
    inference_instances=["ml.t2.medium", "ml.m5.large"],
    transform_instances=["ml.m5.large"],
    model_package_group_name=MODEL_PACKAGE_GROUP_NAME,
    approval_status="PendingManualApproval",
)

step_register = ModelStep(name="HealthPredictRegisterModel", step_args=register_args)

# ----------------------------------------------------------------------------
# Assemble and register the pipeline (does not execute it)
# ----------------------------------------------------------------------------
pipeline = Pipeline(
    name=PIPELINE_NAME,
    steps=[step_process, step_train, step_register],
    sagemaker_session=pipeline_session,
)

if __name__ == "__main__":
    pipeline.upsert(role_arn=role)
    print(f"Pipeline '{PIPELINE_NAME}' registered.")
    print("Start it from Studio UI, or with: pipeline.start()")
