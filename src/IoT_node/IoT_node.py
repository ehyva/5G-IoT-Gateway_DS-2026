import paho.mqtt.client as mqtt
import time
import os
import socket
import random
import json
import math
from datetime import datetime

class IoTSensor:

    def __init__(self): 

        # Sensor state
        self.sensor_id = None

        self.host = os.environ.get('COORDINATOR', 'coordinator')
        self.port = int(os.environ.get('COORDINATOR_PORT', '8001'))

        self.mqtt_broker = os.getenv('MQTT_SERVER', "localhost")
        self.mqtt_port = int(os.getenv('MQTT_PORT', "1883"))
        
    def join_network(self):
            
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.connect(('coordinator', 8001))
            sock.send(b'POST /sensor_join HTTP/1.1\r\n\r\n')
        
            response = sock.recv(1024).decode()
            body = json.loads(response.split('\r\n\r\n', 1)[-1])
            
            print(str(body))

            if body['sensor_id']:
                self.sensor_id = int(body['sensor_id'])

        except Exception as e:
            print("JSON parsing failed: " + str(e))

        sock.close()

    def log(self, message):
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] {message}", flush=True)  # flush=True for immediate output

    def start(self):

        # Connect to mqtt
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        for attempt in range(5):
            try:
                if mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60) == 0:
                    break
            except Exception as e:
                self.log("MQTT connection failed: " + str(e))
            except TimeoutError as e:
                self.log("MQTT connection timed out: " + str(e))
                
            if attempt == 4:
                exit(1)
            time.sleep(5)

        mqtt_client.loop_start()

        # Join network, get sensor ID
        self.sensor_id = random.randint(0,10000)
        
        # Initialize data generation
        random.seed(self.sensor_id)

        self.n = 0
        temp_min = -15.0
        temp_max = 25.0
        self.a = temp_min + (temp_max - temp_min) / 2.0
        self.b = (temp_max - temp_min) / 2.0

        while True:
            timestamp, temperature = self.hourly_data()
            #temperature = int(a + b * math.cos(time.time() / 5.0) + 5.0 * random.gauss())
            mqtt_message = {
                "timestamp": timestamp,
                "temperature": temperature
            }

            mqtt_client.publish(f"sensor/{self.sensor_id}/temperature", json.dumps(mqtt_message))
            
            print(f"Published temp: {temperature} @ {datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}")

            time.sleep(1)

    # Generate noisy date, using real timestamp
    def noisy_data(self):
        timestamp = int(time.time())
        temperature = random.uniform(temp_min, temp_max)
        self.n += 1
        return timestamp, temperature
    
    # Generate hourly data
    def hourly_data(self):
        hour = self.n % 24
        day = 1 + int(self.n / 24)
        datetime = (2026, 1, day, hour, 00, 00, 0, 0, -1)
        timestamp = time.mktime(datetime)
        
        temperature = self.temperature_model(timestamp)

        self.n += 1
        return timestamp, temperature
    
    # Generate daily data
    def daily_data(self):
        
        datetime = (2026, 1, self.n, 00, 00, 00, 0, 0, -1)
        timestamp = time.mktime(datetime)
    
        temperature = self.temperature_model(timestamp)

        self.n += 1
        return timestamp, temperature


    def temperature_model(self, timestamp):
        start = time.mktime((2026, 1, 1, 00, 00, 00, 0, 0, -10))
        phase = (timestamp - start)  / (time.mktime((2027, 1, 1, 00, 00, 00, 0, 0, -1)) - start)
        offset = math.pi * (11.0 / 12.0)
        amplitude = math.cos(math.tau * phase + offset) + 0.2 * math.cos(math.tau * phase * 24.0) + random.gauss(0.0, 0.05)
        return self.a + self.b * amplitude
        




if __name__ == '__main__':
    sensor = IoTSensor()
    sensor.start()
