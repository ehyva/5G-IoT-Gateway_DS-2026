import mlflow
import mlflow.pytorch
import joblib
import tempfile
import os


MLFLOW_URI = os.environ["MLFLOW_TRACKING_URI"]

mlflow.set_tracking_uri(MLFLOW_URI)

MODEL_NAME = "temperature-autoencoder"

def load_model_and_artifacts():
    print(MODEL_NAME)
    client = mlflow.tracking.MlflowClient()

    all_versions = client.get_latest_versions(MODEL_NAME)
    if not all_versions:
        raise ValueError("No model versions found")
    print(all_versions)
    
    latest = client.get_latest_versions(
        MODEL_NAME,
        stages=["Production"]
    )[0]
    run_id = latest.run_id
    print(latest)
    # Load model
    model = mlflow.pytorch.load_model(
        f"models:/{MODEL_NAME}/Production"
    )
    # Download scaler artifact
    with tempfile.TemporaryDirectory() as tmpdir:
        scaler_path = client.download_artifacts(
            run_id,
            "scaler.pkl",
            tmpdir
        )
        scaler = joblib.load(scaler_path)

    # Load threshold metric
    run = client.get_run(run_id)
    threshold = float(run.data.metrics["threshold"])

    return model, scaler, threshold