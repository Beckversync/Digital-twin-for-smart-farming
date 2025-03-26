
# 🌱 Digital Twin For Smart Farming

## 📌 Project Overview

<p align="center">
<img width=800 src="https://github.com/user-attachments/assets/d70c889d-c52c-44a5-b8e3-7d648bd594ec"/>
</p>


## 📂 Project Structure

```
Smart-Farm-IoT/
├── gateway/
│   ├── gateWay.py         # Gateway Edge Node (Farm 1)
│   ├── gateWay_2.py       # Gateway Edge Node (Farm 2)
│   ├── consumer.py        # Kafka Consumer Fog Node
│   ├── Dockerfile         # Dockerfile for gateways
│   └── requirements.txt   # Python dependencies
├── docker-compose.yaml    # Deploy services
└── README.md              # Project description
```

## 💻 Deployment Instructions

### 🐳 Installation Requirements
- Docker and Docker Compose

### ▶️ Run the System
```bash
docker-compose up -d --build
```

### 🖥️ Monitoring the System

- Kafka Control Center: [http://localhost:9021](http://localhost:9021)
- InfluxDB UI: [http://localhost:8086](http://localhost:8086)

## ⚙️ Configuration
- Kafka and InfluxDB configurations are located in the `docker-compose.yaml` file and as environment variables within each script.


<p align="center">
<img width=800 src="https://github.com/user-attachments/assets/2dfb8fe1-59d7-455b-8d84-e3fdf7c05df2"/>
</p>

