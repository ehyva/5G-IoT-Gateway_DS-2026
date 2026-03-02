import pandas as pd
import numpy as np

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