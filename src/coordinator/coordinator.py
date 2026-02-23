import paho.mqtt.client as mqtt
import time
import os
import signal
from datetime import datetime
import socket
import json
import random
from concurrent.futures import ThreadPoolExecutor

class Coordinator:
    def __init__(self): 

        # Server state
        self.running = False
        self.request_count = 0
        self.start_time = None
        self.sensor_list = set()
        self.gateway_list = {1, 2}
        self.gateway_load = {}
        self.sensor_gateway_map = {}

        self.host = os.environ.get('SERVER_HOST', '0.0.0.0')
        self.port = int(os.environ.get('SERVER_PORT', '8001'))

        self.mqtt_broker = os.getenv('MQTT_SERVER', "localhost")
        self.mqtt_port = int(os.getenv('MQTT_PORT', "1883"))
        
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)  # Also Ctrl+C

    def sensor_join_request(self):
        # Handle sensors joining to network. Return sensor a unique ID

        while True:
            sensor_id = random.randint(0, 10000)
            if sensor_id not in self.sensor_list:
                self.sensor_list.add(int(sensor_id))
                self.log(f'Sensor: {sensor_id} joined')

                response = {"sensor_id": sensor_id}

                return response


    def add_sensor_to_gateway(self, sensor_id):
        # Find gateway with least load. Load is measured by number of sensors served

        min_sensors = min(list(self.gateway_load.values()))
        min_index = list(self.gateway_load.values()).index(min_sensors)
        gateway = list(self.gateway_load.keys())[min_index]

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('iot_gateway-gateway-' + str(gateway), 8000))
        
        request = {
        'sensor_id': sensor_id
        }
        request_body = json.dumps(request, indent=2)
        http_request = (
            "POST /sensors HTTP/1.1\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(request_body)}\r\n"
            "\r\n"
            f"{request_body}"
        )
        sock.send(http_request.encode())
        sock.close()

        self.gateway_load[gateway] = self.gateway_load[gateway] + 1
        self.sensor_gateway_map[sensor_id] = gateway

        self.log(f"Sensor: {sensor_id} added to gateway: {gateway}")

    def check_gateway_status(self):
        # Check if all gateways are available. 
        # TODO: We need to discover what gateways are running and update the list

        self.gateway_load = {}

        for gateway in self.gateway_list:

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                sock.connect(('iot_gateway-gateway-' + str(gateway), 8000))
                sock.send(b'GET /health HTTP/1.1\r\n\r\n')
            
                response = sock.recv(1024).decode()
                body = json.loads(response.split('\r\n\r\n', 1)[-1])

                if body['status'] == 'healthy':
                    self.gateway_load[gateway] = int(body['sensor_count'])

            except Exception as e:
                self.log("JSON parsing failed: " + str(e))

            sock.close()

            
    def scale_gateways(self):
        # Check if gateways need to be scaled up or down.
        #TODO: implement

        #os.system("docker compose scale gateway=5")

        pass

    def log(self, message):
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] {message}", flush=True)  # flush=True for immediate output

    def handle_shutdown(self, signum, frame):
        # Handle SIGTERM/SIGINT for graceful shutdown.

        self.log(f"Received signal {signum}, shutting down gracefully...")
        self.running = False  # This will cause main loop to exit

    def health_check(self):
        return {
            'status': 'healthy',
            'uptime_seconds': time.time() - self.start_time if self.start_time else 0
        }

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.log(f"Connected with result code {reason_code}")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.log(msg.topic+" "+str(msg.payload))

    def handle_request(self, client_socket, address):
        """
        Interface for sensors to join. Endpoints:
        POST    /sensor_join
        GET     /
        """
        
        try:
            data = client_socket.recv(1024).decode()
            
            if not data:
                return
            
            # Parse simple HTTP-like request
            lines = data.split('\n')
            request_line = lines[0] if lines else ''

            
            # ─────────────────────────────────────────────────────────────────
            # Route request to appropriate handler
            # ─────────────────────────────────────────────────────────────────
            if 'GET /health' in request_line:
                # Health endpoint for orchestration (Kubernetes, Docker Swarm)
                response = self.health_check()
            
            elif 'POST /sensor_join' in request_line:
                # Health endpoint for sensor joining network
                response = self.sensor_join_request()

            else:
                # Default: show available endpoints (API discovery)
                response = {
                    'endpoints': [
                        'GET /health - Get coordinator status',
                        'POST /sensor_join - Join sensor to network',
                    ]
                }
            
            # ─────────────────────────────────────────────────────────────────
            # Send HTTP response
            # ─────────────────────────────────────────────────────────────────
            response_body = json.dumps(response, indent=2)
            http_response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(response_body)}\r\n"
                "\r\n"
                f"{response_body}"
            )
            client_socket.send(http_response.encode())
            
            # Log to stdout (captured by docker logs)
            self.log(f"Request from {address[0]}: {request_line.strip()}")
            
        except Exception as e:
            self.log(f"Error handling request: {e}")
        finally:
            client_socket.close()

    def start(self):

        self.running = True
        self.start_time = time.time()

        # Connect to mqtt
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client.on_connect = self.on_connect
        mqtt_client.on_message = self.on_message
        
        for attempt in range(5):
            try:
                mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
            except Exception as e:
                self.log("MQTT connection failed: " + str(e))
                if attempt == 4:
                    exit(1)
                time.sleep(5)

        mqtt_client.loop_start()  

        # Create and configure socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)
        #server_socket.settimeout(1)  # Allow checking self.running
        server_socket.setblocking(False)
        
        # Log startup info (captured by docker logs)
        self.log(f"Server starting on {self.host}:{self.port}")
        self.log(f"Process ID: {os.getpid()}")
        self.log(f"Hostname: {os.environ.get('HOSTNAME', socket.gethostname())}")

        gateway_check_time = time.monotonic()

        with ThreadPoolExecutor(max_workers=2) as executor:
            while self.running:

                # Handle sensors joining
                try:
                    client_socket, address = server_socket.accept()
                    executor.submit(self.handle_request, client_socket, address)
                except BlockingIOError as e:
                    pass

                # Check gateways every 5 seconds
                if gateway_check_time < time.monotonic():
                    gateway_check_time = time.monotonic() + 5

                    self.check_gateway_status()

                    if self.gateway_load:
                        for sensor in self.sensor_list:
                            if not sensor in self.sensor_gateway_map:
                                self.add_sensor_to_gateway(sensor)

        server_socket.close()
        self.log("Server stopped")


if __name__ == '__main__':
    server = Coordinator()
    time.sleep(2)
    server.start()
