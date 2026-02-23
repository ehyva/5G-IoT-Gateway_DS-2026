import paho.mqtt.client as mqtt
import time
import os
import socket
import random
import json

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

    def start(self):

        # Connect to mqtt
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        for attempt in range(5):
            try:
                mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
            except Exception as e:
                self.log("MQTT connection failed: " + str(e))
                if attempt == 4:
                    exit(1)
                time.sleep(5)

        mqtt_client.loop_start()

        # Join network
        while self.sensor_id is None:
            self.join_network()
            time.sleep(5)
                

        while True:
            temperature = str(random.randint(-40,40))
            mqtt_message = {
                "timestamp": time.time(),
                "temperature": temperature
                }

            mqtt_client.publish(f"sensor/{self.sensor_id}/temperature", json.dumps(mqtt_message))
            
            print(f"Published temp: {temperature}")

            time.sleep(1)



if __name__ == '__main__':
    sensor = IoTSensor()
    sensor.start()
