import paho.mqtt.client as mqtt
import time
import datetime
import os
import signal
from datetime import datetime
import socket
import json
import torch
import joblib
import numpy as np
import pandas as pd


class EdgeGateway:
    def __init__(self): 

        # Server state
        self.running = False
        self.start_time = None

        self.messages_handled = 0
        self.message_load = 0

        # Load ML model
        self.ml_model = torch.jit.load("model_scripted.pt")
        self.ml_scaler = joblib.load("scaler.pkl")
        self.ml_threshold = joblib.load("threshold.pkl")

        # MQTT client configuration
        self.mqtt_topic_list = []
        self.mqtt_broker = os.getenv('MQTT_SERVER', "localhost")
        self.mqtt_port = int(os.getenv('MQTT_PORT', "1883"))

        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)  # Also Ctrl+C

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.log(f"Connected with result code {reason_code}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        for topic in self.mqtt_topic_list: 
            client.subscribe(topic)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        #Get message info and run ML inference
        sensor_id = int(msg.topic.split("/")[1])
        measurement = json.loads(msg.payload)
        ml_result = self.run_prediction(measurement)

        measurement_result = {
            "temperature": measurement["temperature"],
            "timestamp": int(measurement["timestamp"]),
            "sensor_id": sensor_id,
            "anomaly": ml_result["anomaly"]
        }
        
        #TODO: Send result to database

        self.messages_handled += 1
        self.message_load += 1

    def run_prediction(self, measurement):

        # Preprocess measurement
        date = datetime.fromtimestamp(measurement["timestamp"])
        df = pd.DataFrame([{
            "Month": date.month,
            "Day": date.day,
            "Hour": date.hour,
            "Average temperature [°C]": measurement["temperature"]
        }])
        
        features = self.preprocess_dataframe(df)
        x = self.ml_scaler.transform(features)
        x_tensor = torch.tensor(x, dtype=torch.float32)

        with torch.no_grad():
            recon = self.ml_model(x_tensor)
            error = torch.mean((recon - x_tensor)**2).item()

        return {
            "reconstruction_error": error,
            "threshold": self.ml_threshold,
            "anomaly": error > self.ml_threshold
        }

    def preprocess_dataframe(self, df: pd.DataFrame):

        FEATURE_COLUMNS = [
            "Month_sin", "Month_cos",
            "Day_sin", "Day_cos",
            "Hour_sin", "Hour_cos",
            "Average temperature [°C]"
        ]

        df = df.copy()

        #df["Hour"] = df["Time [UTC]"].str.split(":").str[0].astype(int)
        df["Hour"] = df["Hour"]

        df["Month_sin"] = np.sin(2 * np.pi * df["Month"] / 12)
        df["Month_cos"] = np.cos(2 * np.pi * df["Month"] / 12)

        df["Day_sin"] = np.sin(2 * np.pi * df["Day"] / 31)
        df["Day_cos"] = np.cos(2 * np.pi * df["Day"] / 31)

        df["Hour_sin"] = np.sin(2 * np.pi * df["Hour"] / 24)
        df["Hour_cos"] = np.cos(2 * np.pi * df["Hour"] / 24)

        return df[FEATURE_COLUMNS]

    def log(self, message):
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] {message}", flush=True)  # flush=True for immediate output

    def handle_shutdown(self, signum, frame):
        # Handle SIGTERM/SIGINT for graceful shutdown.

        self.log(f"Received signal {signum}, shutting down gracefully...")
        self.running = False  # This will cause main loop to exit

    def start(self):

        self.running = True
        self.start_time = time.time()
        
        # Connect to mqtt
        try:
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
        except Exception as e:
            print("MQTT connection failed: " + str(e))
            exit(1)

        self.mqtt_client.loop_start()
        
        # Log startup info (captured by docker logs)
        self.log(f"Process ID: {os.getpid()}")
        self.log(f"Hostname: {os.environ.get('HOSTNAME', socket.gethostname())}")
        
        logging_time = time.monotonic()

        self.mqtt_topic_list.append("$share/gateways/sensor/#")
        self.mqtt_client.subscribe("$share/gateways/sensor/#")
    
        while self.running:

            # Check if coordinator is running. Shut down gateway if coordinator is not reachable for 3 times
            if logging_time < time.monotonic():
                logging_time = time.monotonic() + 10
                self.log(f"Total messages handled: {self.messages_handled}\nMessages handled in last 10 seconds: {self.message_load}")
                self.message_load = 0

        self.log("Server stopped")


if __name__ == '__main__':
    server = EdgeGateway()
    server.start()
