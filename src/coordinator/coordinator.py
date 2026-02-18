import paho.mqtt.client as mqtt
import time
import os
import signal
from datetime import datetime
import socket
import json

class Coordinator:
    def __init__(self): 

        # Server state
        self.running = False
        self.request_count = 0
        self.start_time = None
        self.sensor_list = {1, 2}
        self.gateway_list = {1, 2}
        self.gateway_load = {}
        self.sensor_gateway_map = {}

        self.mqtt_broker = os.getenv('MQTT_SERVER', "localhost")
        self.mqtt_port = int(os.getenv('MQTT_PORT', "1883"))
        
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)  # Also Ctrl+C

    def sensor_join_request(self, sensor_id):
        #TODO: How sensors are added?

        if sensor_id in self.sensor_list:
            self.sensor_list.add(int(sensor_id))
            self.log(f'Sensor: {sensor_id} joined')
        else:
            self.log('error: sensor_id not provided')

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
        # Check if all gateways are available. Check which sensors they are handling.

        self.gateway_load = {}

        for gateway in self.gateway_list:

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                sock.connect(('iot_gateway-gateway-' + str(gateway), 8000))
                sock.send(b'GET /health HTTP/1.1\r\n\r\n')
            
                response = sock.recv(1024).decode()
                body = json.loads(response.split('\r\n\r\n', 1)[-1])
                
                self.log(str(body))

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

    def start(self):

        self.running = True
        self.start_time = time.time()

        # Connect to mqtt
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client.on_connect = self.on_connect
        mqtt_client.on_message = self.on_message
        
        try:
            mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
        except Exception as e:
            self.log("MQTT connection failed: " + str(e))
            exit(1)

        mqtt_client.loop_start()  

        while self.running:

            self.check_gateway_status()

            if self.gateway_load:
                for sensor in self.sensor_list:
                    if not sensor in self.sensor_gateway_map:
                        self.add_sensor_to_gateway(sensor)

            time.sleep(5)

        
        self.log("Server stopped")


if __name__ == '__main__':
    server = Coordinator()
    time.sleep(2)
    server.start()
