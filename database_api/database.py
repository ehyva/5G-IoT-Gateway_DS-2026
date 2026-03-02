from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.util import uuid_from_time, datetime_from_uuid1
from datetime import datetime, timezone

import asyncio
from aiohttp import web
import signal
import os
import sys

import textwrap
import uuid

def log(message):
    timestamp = datetime.now().isoformat()
    print(f"[{timestamp}] {message}", flush=True)

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
        
        self.session.execute(
            self.INSERT_MEASUREMENTS_STMT,
            (sensor_id, location, sensor_type, sensor_value, anomaly, time, day_bucket, uuid.uuid4()),
        )


    def get_day_measurements(
        self,
        sensor_id: str,
        sensor_type: str,
        day_bucket: datetime.date = None,
    ):
        """
        Query measurements for a given sensor_id, sensor_type, and day_bucket.
        Returns a list of dicts with event_time in epoch ms.
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

def check_boolean(value):
    if not isinstance(value, bool):
        raise ValueError("Value is not boolean")
    return value


class HTTPDatabase:
    def __init__(self, db: Database):
        self.db = db

    def close_connection(self):
        self.db.close_connection()

    async def get_day_measurements(self, request):
        """
        request:
        {
            "sensor_id": "str",
            "sensor_type": "str",
            "day_bucket": int; epoch ms for the day in utc timezone
        }

        return: http code 200 for success, 400 for bad json, 500 for server failure
        {
            results:
            [
                {
                "sensor_id": "str",
                "location": "str",
                "sensor_type": "str",
                "sensor_value": float,
                "anomaly": bool,
                "time": int,
                "db_id": "str"
                }
                ,
                ...
            ]
        }
        """
        try:
            data = await request.json()
        except Exception as e:
            log(f"Invalid JSON: {str(e)}")
            return web.Response(status=400)
        
        try:
            sensor_id = str(data["sensor_id"])
            sensor_type = str(data["sensor_type"])
        except Exception as e:
            log(f"Failed to convert values from strings or key missing: {str(e)}")
            return web.Response(status=400)

        if "day_bucket" not in data:
            day_bucket = datetime.now(timezone.utc).date()
        else:
            try:
                day_bucket = datetime.fromtimestamp(int(data["day_bucket"]) / 1000, tz=timezone.utc).date()
            except Exception as e:
                log(f"Get day measurements failed: {str(e)}")
                return web.Response(status=400)

        loop = asyncio.get_running_loop()
        try:
            results = await loop.run_in_executor(
                None,  # default thread pool
                lambda: self.db.get_day_measurements(
                    sensor_id,
                    sensor_type,
                    day_bucket
                )
            )
        except Exception as e:
            log(f"Get day measurements failed: {str(e)}")
            return web.Response(status=500)
        return web.json_response({"results": results})
    
    async def insert_measurements(self, request):
        """
        request:
        {
            "sensor_id": "str",
            "location": "str",
            "sensor_type": "str",
            "sensor_value": float,
            "anomaly": bool,
            "epoch_ms": int
        }

        return: http code 200 for success, 400 for bad json, 500 for server failure
        """
        try:
            data = await request.json()
        except Exception as e:
            log(f"Invalid JSON: {str(e)}")
            return web.Response(status=400)
        
        try:
            sensor_id = str(data["sensor_id"])
            location = str(data["location"])
            sensor_type = str(data["sensor_type"])
            sensor_value = float(data["sensor_value"])
            anomaly = check_boolean(data["anomaly"])
            epoch_ms = int(data["epoch_ms"])
        except Exception as e:
            log(f"Failed to convert values from strings or key missing: {str(e)}")
            return web.Response(status=400)

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,  # default thread pool
                lambda: self.db.insert_measurements(
                    sensor_id,
                    location,
                    sensor_type,
                    sensor_value,
                    anomaly,
                    epoch_ms
                )
            )
        except Exception as e:
            log(f"Measurement insertion failed: [sid:{sensor_id}, st:{sensor_type}, loc:{location}, {epoch_ms}] {str(e)}")
            return web.Response(status=500)
        return web.Response(status=200)


async def main():

    cassandra_ip = os.getenv("CASSANDRA_IP", "cassandra1")
    cassandra_port = int(os.getenv("CASSANDRA_PORT", 9042))
    host = os.getenv("SERVER_HOST", "localhost")
    port = int(os.getenv("PORT", 8000))
    rf = int(os.getenv("REPLICATION_FACTOR", 1))
    cl = os.getenv("CONSISTENCY_LEVEL", "one").strip().lower()
    
    if cl == "quorum":
        cl = ConsistencyLevel.QUORUM
    else:
        cl = ConsistencyLevel.ONE

    # Multiple contact points for fault tolerance and horizontal scaling
    # ip-addresses of all cassandra nodes, currently just run on single node so this is enough
    cassandra_node_ips = [cassandra_ip]
    try:
        cluster = Cluster(
            contact_points=cassandra_node_ips,
            port=cassandra_port,
        )
        # Replication factor: copy data to rf nodes, consistency_level: Sets how many nodes need to respond for writes/reads, Quorum for ceil(RF/2)+1 nodes required. One for 1 node
        db = Database(cluster, replication_factor=rf, consistency_level=cl)
        db.connect()
        db.create_structure()
    except Exception as e:
        log(f"Can't connect to database: {str(e)}")
        sys.exit(1)
    httpDB = HTTPDatabase(db)
    app = web.Application()
    app.add_routes(
        [
            web.get("/day_measurements", httpDB.get_day_measurements),
            web.post("/insert_measurements", httpDB.insert_measurements),
        ]
    )

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    
    # ---- Cleanup hook ----¨
    stop_event = asyncio.Event()
    def graceful_shutdown_callback():
        stop_event.set()

    # Register the signal handlers
    signals_to_handle = ["SIGINT", "SIGBREAK", "SIGTERM"] # CTRL+C, CTRL+Break, Sigterm
    # All signal are not supported on all OS so we need to check whether that signal has
    # been implemented
    loop = asyncio.get_running_loop()
    for sig_name in signals_to_handle:
        if hasattr(signal, sig_name):
            sig = getattr(signal, sig_name)
            try:
                loop.add_signal_handler(sig, graceful_shutdown_callback)
            except NotImplementedError:
                pass
    # Wait until shutdown signal, do proper shutdown
    try:
        await stop_event.wait()
    finally:
        log("Shutting down gracefully...")
        await runner.cleanup()
        httpDB.close_connection()
        log("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass