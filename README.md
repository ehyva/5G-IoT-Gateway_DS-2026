
Distributed system for collecting, transmitting, and storing environmental data

![System architecture](architecture.svg)

- IoT sensors generature temperature and timestamp data and publish then publish it to to MQTT broker.
- MQTT broker forward the messages to the gateway, using shared subscription the balance load for the gateways
- Gateways preprocess the sensor data, they detect anomalous readings, add sensor metadata, and push the processed data to database  

To deploy the system 

```
docker compose up -d --build gateway iot_node cassandra1 grafana mqtt
```

to force rebuild of custom images and start services. Shut down system by running 

```
docker compose down
```
To access Grafana open `localhost:3000` in web browser and enter 'admin' as both username and password.
