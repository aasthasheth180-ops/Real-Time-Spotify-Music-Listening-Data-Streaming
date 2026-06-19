# 🎵 Spotify Wrapped: Real-Time Streaming & Analytics Dashboard

An end-to-end, real-time data streaming and metrics analytics pipeline that collects continuous user listening streams, processes dimension-specific KPIs under low latency, and serves a live analytical dashboard.

---

## Architecture Overview

The system architecture splits data production, message ingestion, analytical stream computation, and state visualization into four cleanly separated layers:

1. **Ingestion Layer (Flask Backend & Producer):** Simulates real-time Spotify stream events via `app.py`, pushing raw user behavior events asynchronously into Kafka topics using `producer.py`.
2. **Message Broker Layer (Apache Kafka):** Manages high-throughput, decoupled event streaming queues across partitioned streaming topics via a local Docker container network.
3. **Stream Processing Tier (PySpark Structured Streaming):** Four isolated, dedicated Spark consumer workers execute ongoing rolling aggregation matrices grouped by target business dimensions (`album`, `artist`, `item`, `song`).
4. **Visualization Layer (Streamlit Dashboard):** Reads the calculated state matrices directly to render a real-time, interactive "Spotify Wrapped" analytics monitoring experience via `dashboard.py`.

---

##  Repository Structure

Based on your project workspace, the directory is organized as follows:

```text
SPOTIFY/
├── consumers/                  # PySpark Structured Streaming engines
│   ├── spark-consumer-album.py # Tracks real-time top-performing albums
│   ├── spark-consumer-artist.py# Calculates continuous artist metrics
│   ├── spark-consumer-item.py  # Computes distinct generic catalog items
│   └── spark-consumer-song.py  # Aggregates song listening volumes live
├── dashboard/                  
│   └── dashboard.py            # Streamlit dashboard script for real-time visualization
├── flask_backend/              # Flask authentication and token manager
│   ├── app.py                  # Core backend app interface
│   ├── models.py               # Database schemas and telemetry definitions
│   ├── token_info.json         # Encrypted mock token state tracker
│   └── utils.py                # Helper utilities and data formatters
├── producer/                   # Event-driven streaming layer
│   ├── dataset.csv             # Source user listening tracking raw seed logs
│   └── producer.py             # Kafka event producer pushing streaming payloads
├── .env                        # Local infrastructure configurations
├── docker-compose.yml          # Single-command multi-node Kafka orchestrator
└── README.md
               
#  Technical Highlights & Infrastructure Decisions
Multi-Consumer Parallel Processing: Instead of utilizing a single monolithic consumer processing script, the processing tier splits metrics calculation into 4 separate, dedicated dimension paths (album, artist, item, song). This prevents compute bottlenecks and isolates processing failures.

Asynchronous Decoupling: The Flask app backend serves web clients and streams background user metadata logs asynchronously into Kafka brokers via producer.py. This design protects user interaction layers from downstream analytical bottlenecks or failures.

Production-Grade Infrastructure Management: The complete streaming infrastructure environment is cleanly containerized via docker-compose.yml. Developers can initialize local multi-node cluster platforms safely without needing to manually configure intricate system runtimes locally.

## Getting Started
1. Environment Initialization
Clone this repository locally, move into the workspace directory, and construct your local Python isolated dependencies environment:

Bash
git clone [https://github.com/aasthasheth180-ops/Spotify-Realtime-Pipeline.git](https://github.com/aasthasheth180-ops/Spotify-Realtime-Pipeline.git)
cd Spotify-Realtime-Pipeline
python -m venv venv
source venv/Scripts/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt

2. Boot up Kafka Infrastructure
Spin up your background message broker environment instantly via Docker Compose:

Bash
docker-compose up -d

3. Launch Stream Ingestion (Producer)
Initialize the internal application backend tier to trigger simulated user track event streaming loops:

Bash
python flask_backend/app.py
# In a separate terminal session, start publishing streaming payloads:
python producer/producer.py

4. Initialize PySpark Computation Layers
Run your dedicated Spark processing scripts concurrently to begin aggregating live metrics:

Bash
python consumers/spark-consumer-song.py
python consumers/spark-consumer-artist.py

5. Launch the Visual Analytics Product
Initialize your data dashboard layer to monitor real-time user listening metrics and track performance metrics live:

Bash
streamlit run dashboard/dashboard.py
