from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import boto3
import uuid
import mlflow
import torch
import joblib
import numpy as np
import pandas as pd
import os
from preprocessing import preprocess_dataframe
from model_loader import load_model_and_artifacts
from sqlalchemy.orm import Session
from database import engine, SessionLocal
import models
import schemas

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Environment Config
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


S3_ENDPOINT = os.environ["MLFLOW_S3_ENDPOINT_URL"]
AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]
MLFLOW_URI = os.environ["MLFLOW_TRACKING_URI"]

BUCKET = "sensor-data"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
)

mlflow.set_tracking_uri(MLFLOW_URI)

# Startup

@app.on_event("startup")
def startup_event():
    global model, scaler, threshold

    try:
        model, scaler, threshold = load_model_and_artifacts()
        model.eval()
        print("Model loaded successfully")
    except Exception as e:
        print("Model loading failed:", e)
        model = None

# Prediction Schema

class Measurement(BaseModel):
    Month: int
    Day: int
    Time_UTC: str
    Average_temperature: float

# Prediction Endpoint

@app.post("/predict")
def predict(measurement: Measurement):

    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    df = pd.DataFrame([{
        "Month": measurement.Month,
        "Day": measurement.Day,
        "Time [UTC]": measurement.Time_UTC,
        "Average temperature [°C]": measurement.Average_temperature
    }])

    features = preprocess_dataframe(df)
    x = scaler.transform(features)
    x_tensor = torch.tensor(x, dtype=torch.float32)

    with torch.no_grad():
        recon = model(x_tensor)
        error = torch.mean((recon - x_tensor)**2).item()

    return {
        "reconstruction_error": error,
        "threshold": threshold,
        "anomaly": error > threshold
    }

# Model Info

@app.get("/v1/models/latest")
def get_latest_model():

    client = mlflow.tracking.MlflowClient()
    latest = client.get_latest_versions(
        "temperature-autoencoder",
        stages=["Production"]
    )

    if not latest:
        raise HTTPException(status_code=404, detail="No production model")

    version = latest[0]

    return {
        "version": version.version,
        "run_id": version.run_id
    }

# Model File Download

@app.get("/v1/models/download")
def download_model():

    client = mlflow.tracking.MlflowClient()

    latest = client.get_latest_versions(
        "temperature-autoencoder",
        stages=["Production"]
    )

    if not latest:
        raise HTTPException(status_code=404, detail="No production model")

    version = latest[0]

    # Get the real artifact location
    run = client.get_run(version.run_id)
    artifact_uri = run.info.artifact_uri  

    path = artifact_uri.replace("s3://", "")
    bucket, key_prefix = path.split("/", 1)

    model_key = f"{key_prefix}/model/model_scripted.pt"
    scaler_key = f"{key_prefix}/scaler.pkl"
    threshold_key = f"{key_prefix}/threshold.pkl"

    model_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": model_key},
        ExpiresIn=600
    )

    scaler_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": scaler_key},
        ExpiresIn=600
    )

    threshold_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": threshold_key},
        ExpiresIn=600
    )

    return {
        "version": version.version,
        "model_url": model_url,
        "scaler_url": scaler_url,
        "threshold_url": threshold_url,
        "expires_in": 600
    }


# End points for the database

@app.post("/measurements/", response_model=schemas.MeasurementResponse)
def create_measurement(measurement: schemas.MeasurementCreate, db: Session = Depends(get_db)):
    db_measurement = models.Measurement(**measurement.model_dump())
    db.add(db_measurement)
    db.commit()
    db.refresh(db_measurement)
    return db_measurement


@app.get("/measurements/")
def get_measurements(db: Session = Depends(get_db)):
    return db.query(models.Measurement).all()

# Health

@app.get("/health")
def health():
    return {"status": "running"}