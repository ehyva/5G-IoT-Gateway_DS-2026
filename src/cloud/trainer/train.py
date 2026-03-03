import os
import boto3
import mlflow
import mlflow.pytorch
import torch
import numpy as np
import joblib
import pandas as pd
from sklearn.preprocessing import StandardScaler
from model import PointAutoencoder

# -----------------------------
# MLflow config
# -----------------------------
mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])

# -----------------------------
# S3 download
# -----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

DATA_BUCKET = "datasets"
DATA_KEY = "training_v1.csv"

s3.download_file(DATA_BUCKET, DATA_KEY, "training.csv")

# -----------------------------
# Preprocessing
# -----------------------------
FEATURE_COLUMNS = [
    "Month_sin", "Month_cos",
    "Day_sin", "Day_cos",
    "Hour_sin", "Hour_cos",
    "Average temperature [°C]"
]

def preprocess_dataframe(df: pd.DataFrame):

    df = df.copy()

    df["Hour"] = df["Time [UTC]"].str.split(":").str[0].astype(int)

    df["Month_sin"] = np.sin(2 * np.pi * df["Month"] / 12)
    df["Month_cos"] = np.cos(2 * np.pi * df["Month"] / 12)

    df["Day_sin"] = np.sin(2 * np.pi * df["Day"] / 31)
    df["Day_cos"] = np.cos(2 * np.pi * df["Day"] / 31)

    df["Hour_sin"] = np.sin(2 * np.pi * df["Hour"] / 24)
    df["Hour_cos"] = np.cos(2 * np.pi * df["Hour"] / 24)

    return df[FEATURE_COLUMNS]

df = pd.read_csv("training.csv")

df_processed = preprocess_dataframe(df)

# -----------------------------
# Scaling
# -----------------------------
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_processed)

X_tensor = torch.tensor(X_scaled, dtype=torch.float32)

# -----------------------------
# Model
# -----------------------------
input_dim = X_scaled.shape[1]
model = PointAutoencoder(input_dim)

optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
criterion = torch.nn.MSELoss()

# -----------------------------
# Training loop
# -----------------------------
EPOCHS = 512
loss_history = []

with mlflow.start_run():

    mlflow.log_param("input_dim", input_dim)
    mlflow.log_param("epochs", EPOCHS)
    mlflow.log_param("learning_rate", 0.001)

    for epoch in range(EPOCHS):
        optimizer.zero_grad()
        recon = model(X_tensor)
        loss = criterion(recon, X_tensor)
        loss.backward()
        optimizer.step()

        loss_value = loss.item()
        loss_history.append(loss_value)

        # Log loss per epoch
        mlflow.log_metric("train_loss", loss_value, step=epoch)

        if epoch % 50 == 0:
            print(f"Epoch {epoch}/{EPOCHS} - Loss: {loss_value:.6f}")

    # -----------------------------
    # Threshold computation
    # -----------------------------
    with torch.no_grad():
        errors = torch.mean((model(X_tensor) - X_tensor) ** 2, dim=1)

    threshold = float(errors.mean() + 4 * errors.std())

    mlflow.log_metric("threshold", threshold)

    # -----------------------------
    # Save model
    # -----------------------------
    mlflow.pytorch.log_model(
        model,
        name="model",  
        registered_model_name="temperature-autoencoder"
    )

    # Save scaler
    joblib.dump(scaler, "scaler.pkl")
    mlflow.log_artifact("scaler.pkl")

    # Save threshold
    joblib.dump(threshold, "threshold.pkl")
    mlflow.log_artifact("threshold.pkl")   

    # Save model
    scripted = torch.jit.script(model)
    scripted.save("model_scripted.pt")
    mlflow.log_artifact("model_scripted.pt")
    
    
    client = mlflow.tracking.MlflowClient()
    latest_versions = client.get_latest_versions("temperature-autoencoder")
    new_version = max([v.version for v in latest_versions], key=int)

    client.transition_model_version_stage(
        name="temperature-autoencoder",
        version=new_version,
        stage="Production", 
        archive_existing_versions=True 
    )
