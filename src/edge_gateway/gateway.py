from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.util import uuid_from_time, datetime_from_uuid1
import textwrap
import uuid

import paho.mqtt.client as mqtt
import time
import datetime
import os
import signal
from datetime import datetime, timezone, date
import socket
import json
import torch
import joblib
import numpy as np
import pandas as pd

def convert_epoch_ms_to_cassandra(epoch_ms: int):
    """
    Converts epoch milliseconds to cassandra DATE and TIMEUUID
    """
    # Convert milliseconds → seconds
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)

    # For clustering column
    time = uuid_from_time(dt)

    # For partition bucket
    day_bucket = dt.date()

    return time, day_bucket


def timeuuid_to_epoch_ms(timeuuid_value):
    """
    Converts Cassandra TIMEUUID to epoch milliseconds (UTC).
    """
    dt = datetime_from_uuid1(timeuuid_value) # returns timezone-aware datetime (UTC)
    return int(dt.timestamp() * 1000)


class NoSession(Exception):
    """Exception raised when there is no session established."""

    def __init__(self, message="No session established"):
        super().__init__(message)

    def __str__(self):
        return f"{self.message}"


class Database:
    def __init__(self, cluster: Cluster, replication_factor: int = 1, consistency_level: ConsistencyLevel = ConsistencyLevel.ONE):
        self.cluster = cluster
        self.RF = replication_factor
        # (Consistency) Sets how many nodes need to respond Quorum for ceil(RF/2)+1 nodes required. One for 1 node
        self.consistency_level = consistency_level
        self.session = None
        self.INSERT_MEASUREMENTS_STMT = None
        self.GET_DAY_MEASUREMENTS_STMT = None


    def connect(self):
        self.session = self.cluster.connect(wait_for_all_pools=True)

    def close_connection(self):
        self.cluster.shutdown()
        self.session = None

    def log(self, message):
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] {message}", flush=True)

    def log_insert_success(self, result):
        self.log(f"Database: Insert completed successfully", flush=True)

    def log_error(self, exc):
        self.log(f"Database: Operation failed: {type(exc).__name__}: {exc}", flush=True)
    

    def create_structure(self):
        """
        Creates the necessary keyspace(s) and table(s) if they don't exist.
        """
        if not self.session:
            raise NoSession()
        # Create keyspace with replication factor of 1, 3-5 when more nodes
        self.session.execute(textwrap.dedent(
            f"""
            CREATE KEYSPACE IF NOT EXISTS measurements
            WITH replication = {{
                'class': 'NetworkTopologyStrategy',
                'replication_factor': {self.RF}
            }}
            """
        ))
        # Activate keyspace
        self.session.set_keyspace("measurements")
        # Create table
        self.session.execute(textwrap.dedent(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                sensor_id TEXT,
                location TEXT,
                sensor_type TEXT,
                sensor_value FLOAT,
                anomaly BOOLEAN,
                time TIMEUUID,
                day_bucket DATE,
                db_id UUID,
                PRIMARY KEY ((sensor_id, sensor_type, day_bucket), time, db_id)
            ) WITH CLUSTERING ORDER BY (time DESC)
            """
        ))

    
    def insert_measurements(
        self,
        sensor_id: str,
        location: str,
        sensor_type: str,
        sensor_value: float,
        anomaly: bool,
        epoch_ms: int
    ):
        """
        Insert measurements to the database async. Failures are logged
        """
        if not self.session:
            raise NoSession()
        
        time, day_bucket = convert_epoch_ms_to_cassandra(epoch_ms)
        if self.INSERT_MEASUREMENTS_STMT is None:
            prepared = self.session.prepare(textwrap.dedent(
                """
                INSERT INTO measurements (sensor_id, location, sensor_type, sensor_value, anomaly, time, day_bucket, db_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
            ))
            prepared.consistency_level = self.consistency_level
            self.INSERT_MEASUREMENTS_STMT = prepared
        
        future = self.session.execute_async(
            self.INSERT_MEASUREMENTS_STMT,
            (sensor_id, location, sensor_type, sensor_value, anomaly, time, day_bucket, uuid.uuid4()),
        )

        future.add_callbacks(self.log_insert_success, self.log_error)


    def get_day_measurements(
        self,
        sensor_id: str,
        sensor_type: str,
        day_bucket: datetime.date = None,
    ):
        """
        Query measurements for a given sensor_id, sensor_type, and day_bucket.
        Returns a list of dicts with event_time in epoch ms.
        Synchronous
        """
        if not self.session:
            raise NoSession()

        if not day_bucket:
            day_bucket = datetime.now(timezone.utc).date()

        if self.GET_DAY_MEASUREMENTS_STMT is None:
            prepared = self.session.prepare(textwrap.dedent(
                """
                SELECT sensor_id, sensor_type, location, sensor_value, anomaly, time, day_bucket, db_id
                FROM measurements
                WHERE sensor_id = ?
                    AND sensor_type = ?
                    AND day_bucket = ?
                """
            ))
            prepared.consistency_level = self.consistency_level
            self.GET_DAY_MEASUREMENTS_STMT = prepared

        rows = self.session.execute(
            self.GET_DAY_MEASUREMENTS_STMT,
            (sensor_id, sensor_type, day_bucket),
        )

        results = []
        for row in rows:
            # Convert TIMEUUID to epoch ms
            epoch_ms = timeuuid_to_epoch_ms(row.time)

            results.append({
                "sensor_id": row.sensor_id,
                "location": row.location,
                "sensor_type": row.sensor_type,
                "sensor_value": row.sensor_value,
                "anomaly": row.anomaly,
                "time": epoch_ms,
                "db_id": str(row.db_id)
            })

        return results



class EdgeGateway:
    def __init__(self): 

        # Server state
        self.running = False
        self.start_time = None

        self.messages_handled = 0
        self.message_load = 0

        # Apache Cassandra database configuration
        self.cassandra_ip = os.getenv("CASSANDRA_IP", "cassandra1")
        self.cassandra_port = int(os.getenv("CASSANDRA_PORT", 9042))
        # ip-addresses of all cassandra seed nodes, currently just run on single node so this is enough
        self.cassandra_node_ips = [self.cassandra_ip]
        self.rf = int(os.getenv("REPLICATION_FACTOR", 1))
        self.cl = os.getenv("CONSISTENCY_LEVEL", "one").strip().lower()
        if self.cl == "quorum":
            self.cl = ConsistencyLevel.QUORUM
        else:
            self.cl = ConsistencyLevel.ONE
        self.DB = None

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
            client.subscribe(topic, qos=1)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        #Get message info and run ML inference
        try:
            sensor_id = str(msg.topic.split("/")[1])
            measurement = json.loads(msg.payload)
            ml_result = self.run_prediction(measurement)
            sensor_value = float(measurement["temperature"])
            epoch_ms = int(measurement["timestamp"] * 1000)
            anomaly = ml_result["anomaly"]
        except Exception as e:
            self.log(f"On message handler exception: {type(e).__name__}: {str(e)}")

        try:
            self.DB.insert_measurements(
                sensor_id=sensor_id,
                location="Dummy location",
                sensor_type ="Dummy temperature sensor",
                sensor_value=sensor_value,
                anomaly=anomaly,
                epoch_ms=epoch_ms
            )
        except Exception as e:
            self.log(f"Database insertion exception: {type(e).__name__}: {str(e)}")
        

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
        
        # Connect to the database before mqtt, can't start consuming messages before cassandra database is up
        attempts = 10
        for i in range(1, attempts+1):
            try:
                cluster = Cluster(
                    contact_points=self.cassandra_node_ips,
                    port=self.cassandra_port,
                )
                # Replication factor: copy data to rf nodes, consistency_level: Sets how many nodes need to respond for writes/reads, Quorum for ceil(RF/2)+1 nodes required. One for 1 node
                self.DB = Database(cluster, replication_factor=self.rf, consistency_level=self.cl)
                # It takes a while to connect to cassandra if its created at the same time as this container
                self.DB.connect()
                self.DB.create_structure()
                # Sanity check that something is stored in the database
                # self.log(self.DB.get_day_measurements("1", "Dummy temperature sensor", date(2026, 1, 1)))
                break
            except Exception as e:
                self.log(f"Can't connect to Cassandra database: {type(e).__name__}: {str(e)}. Retry: {i}/{attempts}.")
                if attempts == i:
                    exit(1)
                time.sleep(3)

        # Connect to mqtt
        try:
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
        except Exception as e:
            self.log("MQTT connection failed: " + str(e))
            exit(1)

        self.mqtt_client.loop_start()
        
        # Log startup info (captured by docker logs)
        self.log(f"Process ID: {os.getpid()}")
        self.log(f"Hostname: {os.environ.get('HOSTNAME', socket.gethostname())}")
        
        logging_time = time.monotonic()

        self.mqtt_topic_list.append("$share/gateways/sensor/#")
        self.mqtt_client.subscribe("$share/gateways/sensor/#", qos=1)
    
        while self.running:

            # Check if coordinator is running. Shut down gateway if coordinator is not reachable for 3 times
            if logging_time < time.monotonic():
                logging_time = time.monotonic() + 10
                self.log(f"Total messages handled: {self.messages_handled}\nMessages handled in last 10 seconds: {self.message_load}")
                self.message_load = 0
        
        self.mqtt_client.disconnect()
        self.DB.close_connection()
        self.log("Server stopped")



if __name__ == '__main__':
    server = EdgeGateway()
    server.start()

