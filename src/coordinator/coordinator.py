import paho.mqtt.client as mqtt
import time
import os
import signal
from datetime import datetime
import socket

class Coordinator:
    def __init__(self): 

        # Server state
        self.running = False

        self.mqtt_broker = os.getenv('MQTT_SERVER', "localhost")
        self.mqtt_port = int(os.getenv('MQTT_PORT', "1883"))
        
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)  # Also Ctrl+C

        # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.log(f"Connected with result code {reason_code}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("$SYS/broker/bytes/#")
        client.subscribe("$SYS/broker/load/bytes/#")
        client.subscribe("$SYS/broker/messages/#")
        client.subscribe("$SYS/broker/load/messages/#")


        #client.subscribe("$share/gateways/sensor/#")
        #for sensor in self.sensor_list:
        #    client.subscribe("sensor/" + str(sensor) + "/#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.log(msg.topic+" "+str(msg.payload))

            
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

        # Log startup info (captured by docker logs)
        self.log(f"Server starting")
        self.log(f"Process ID: {os.getpid()}")
        self.log(f"Hostname: {os.environ.get('HOSTNAME', socket.gethostname())}")

        print_check_time = time.monotonic()

        while self.running:

            # print info every 5 seconds
            if print_check_time < time.monotonic():
                print_check_time = time.monotonic() + 5

        self.log("Server stopped")


if __name__ == '__main__':
    server = Coordinator()
    time.sleep(2)
    server.start()
