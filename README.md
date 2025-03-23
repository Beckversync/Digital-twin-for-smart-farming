docker run -d --name influxdb -p 8086:8086 -e INFLUXDB_DB=sensor_data -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD=admin123 influxdb:2.0
