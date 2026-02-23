import paho.mqtt.client as mqtt
import time
import os
import signal
from datetime import datetime
import socket
import json
from concurrent.futures import ThreadPoolExecutor

class EdgeGateway:
    def __init__(self): 

        # Server state
        self.running = False
        self.start_time = None
        self.sensor_list = set()
        self.gateway_id = None

        self.host = os.environ.get('SERVER_HOST', '0.0.0.0')
        self.port = int(os.environ.get('SERVER_PORT', '8000'))

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
        # client.subscribe("sensor/#")
        for sensor in self.sensor_list:
            client.subscribe("sensor/" + str(sensor) + "/#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.log(msg.topic+" "+str(msg.payload))

    def handle_coordinator(self, client_socket, address):
        """
        Management interface for coordinator. Endpoints:

        GET     /health
        GET     /info
        GET     /sensors
        POST    /sensors
        DELETE  /sensors
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
                
            elif 'GET /sensors' in request_line:
                # Actual application logic
                self.get_sensors(request)

            elif 'POST /sensors' in request_line:
                # Actual application logic. Get JSON request from HTTP header.
                body = data.split('\r\n\r\n', 1)[-1]
                try:
                    request = json.loads(body)
                    self.log(type(request))
                    response = self.add_sensor(request)
                except json.JSONDecodeError:
                    response = {'error': 'Invalid JSON'}

            elif 'DELETE /sensors' in request_line:
                # Actual application logic. Get JSON request from HTTP header.
                body = data.split('\r\n\r\n', 1)[-1]
                try:
                    request = json.loads(body)
                    response = self.remove_sensor(request)
                except json.JSONDecodeError:
                    response = {'error': 'Invalid JSON'}
                    
            else:
                # Default: show available endpoints (API discovery)
                response = {
                    'endpoints': [
                        'GET /health - Health check for orchestration',
                        'GET /sensors - Get sensors handled by gateway',
                        'POST /sensors - Add sensor to gateway',
                        'DELETE /sensors - Remove sensor from gateway'
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

    def get_sensors(self):
        sensors = list(self.sensor_list)
        return {
            'sensors': sensors 
        }

    def add_sensor(self, request):
        sensor_id = request.get('sensor_id', None)
        if sensor_id is not None and not int(sensor_id) in self.sensor_list:
            self.sensor_list.add(int(sensor_id))
            self.mqtt_client.subscribe("sensor/" + str(sensor_id) + "/#")
            
            self.log(f"Added sensor: {sensor_id} to gateway: {self.gateway_id}")
            return {
                'sensor_id_added': sensor_id,
                'gateway': self.gateway_id,
                'processed_by': os.environ.get('HOSTNAME', 'unknown')
            }
        elif int(sensor_id) in self.sensor_list:
            return {'error': f'Sensor allready added: {sensor_id}'}
        else:
            return {'error: sensor_id not provided'}

    def remove_sensor(self, request):
        sensor_id = request.get('sensor_id', None)
        if sensor_id is not None and int(sensor_id) in self.sensor_list:
            self.sensor_list.discard(int(sensor_id))
            self.mqtt_client.unsubscribe("sensor/" + str(sensor_id) + "/#")
        elif not int(sensor_id) in self.sensor_list:
            return {'error: sensor_id not found in this gateway'}
        else:
            return {'error: sensor_id not provided'}

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
            'uptime_seconds': time.time() - self.start_time if self.start_time else 0,
            'sensor_count': len(self.sensor_list)
        }

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

        # Create and configure socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)
        server_socket.settimeout(1)  # Allow checking self.running
        
        # Log startup info (captured by docker logs)
        self.log(f"Server starting on {self.host}:{self.port}")
        self.log(f"Process ID: {os.getpid()}")
        self.log(f"Hostname: {os.environ.get('HOSTNAME', socket.gethostname())}")
        
        coordinator_check = 0
        coordinator_check_time = time.monotonic()

        with ThreadPoolExecutor(max_workers=2) as executor:
            while self.running:
                # Handle coordinator health checks and handling sensors
                try:
                    client_socket, address = server_socket.accept()
                    executor.submit(self.handle_coordinator, client_socket, address)
                except socket.timeout:
                    continue  # Check if still running

                # Check if coordinator is running. Shut down gateway if coordinator is not reachable for 3 times
                if coordinator_check_time < time.monotonic():
                    coordinator_check_time = time.monotonic() + 10
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect(('coordinator', 8001))
                        sock.send(b'GET /health HTTP/1.1\r\n\r\n')
                    
                        response = sock.recv(1024).decode()
                        body = json.loads(response.split('\r\n\r\n', 1)[-1])

                        if body['status'] == "healthy":
                            coordinator_check = 0
                        else:
                            coordinator_check += 1


                    except Exception as e:
                        coordinator_check += 1
                    
                    if coordinator_check >= 3:
                        self.running = False

        
        server_socket.close()
        self.log("Server stopped")


if __name__ == '__main__':
    server = EdgeGateway()
    server.start()
