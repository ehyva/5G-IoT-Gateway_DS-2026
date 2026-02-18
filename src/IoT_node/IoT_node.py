import paho.mqtt.client as mqtt
import time
import random
import os

def main():

    # Get unique Sensor ID
    sensor_id = int(os.getenv('SENSOR_ID', random.randint(0, 10000)))
    mqtt_broker = os.getenv('MQTT_SERVER', "localhost")
    mqtt_port = int(os.getenv('MQTT_PORT', "localhost"))
    
    # Connect to mqtt
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    #mqtt_client.on_connect = on_connect
    #mqtt_client.on_message = on_message
    try:
        mqtt_client.connect(mqtt_broker, mqtt_port, 60)
    except Exception as e:
        print("MQTT connection failed: " + str(e))
        exit(1)
    mqtt_client.loop_start()    

    print(str(sensor_id))

    while True:
        temperature = str(random.randint(-40,40))
        mqtt_client.publish(f"sensor/{sensor_id}/temperature", temperature)
        
        print(f"Published temp: {temperature}")

        time.sleep(1)



if __name__ == '__main__':
    main()
