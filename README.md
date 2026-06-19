<div align="center">

<!-- DEMO GIF PLACEHOLDER — replace src with your actual GIF path -->
<!-- <img src="assets/demo.gif" alt="Spotify Wrapped Demo" width="720"/> -->

# 🎵 Spotify Wrapped: Real-Time Streaming & Analytics Pipeline

**An end-to-end, production-grade data streaming system** that ingests live Spotify listening events, processes dimension-specific KPIs across parallel PySpark consumers, stores results in a PostgreSQL star schema, and serves a real-time "Spotify Wrapped" analytics dashboard — with AI-generated weekly listening insights delivered via email.


---

## 📋 Table of Contents

- [What This Does](#-what-this-does)
- [Architecture Overview](#-architecture-overview)
- [Pipeline Layers](#-pipeline-layers)
- [Tech Stack](#-tech-stack)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
- [Key Engineering Decisions](#-key-engineering-decisions)
- [Sample Output](#-sample-output)
- [Improvements Over Original](#-improvements-over-original)
- [What I Learned](#-what-i-learned)

---

## 🎯 What This Does

Most Spotify analytics projects stop at a static Jupyter notebook. This project ships a **fully operational streaming pipeline** that:

1. Authenticates with the Spotify API and continuously polls your recently played tracks
2. Publishes raw listening events into four partitioned Kafka topics (by dimension)
3. Processes each topic in parallel using dedicated PySpark Structured Streaming consumers
4. Writes aggregated KPIs into a PostgreSQL star schema in real time
5. Displays live metrics on a Streamlit dashboard
6. Generates a weekly AI-written "Wrapped" summary using Gemini and sends it to your inbox

The entire infrastructure starts with a **single command**: `docker compose up`

---

## 🏗️ Architecture Overview

The pipeline is split into four cleanly separated layers — ingestion, message brokering, stream processing, and visualization — so each layer can fail, scale, or be replaced independently.

<br/>

<div align="center">

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│                                                                     │
│   Spotify API  ──►  Flask (OAuth + /recently-played)  ──►  Producer │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  publishes JSON events
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     MESSAGE BROKER LAYER                            │
│                                                                     │
│   song_topic   album_topic   artist_topic   item_topic              │
│   (Kafka — Confluent KRaft, Docker)                                 │
└───────┬──────────────┬───────────────┬──────────────┬──────────────┘
        │              │               │              │
        ▼              ▼               ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   STREAM PROCESSING LAYER                           │
│                                                                     │
│   PySpark      PySpark        PySpark       PySpark                 │
│   song         album          artist        item                    │
│   consumer     consumer       consumer      consumer                │
│        │              │               │              │              │
│        └──────────────┴───────────────┴──────────────┘             │
│                              │                                      │
│                              ▼                                      │
│                    PostgreSQL (star schema)                         │
│             dim_song · dim_album · dim_artist · fact_history        │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    VISUALIZATION LAYER                              │
│                                                                     │
│   Streamlit Dashboard  ←──  Flask /insight  ──►  Gemini AI  ──►  📧 │
└─────────────────────────────────────────────────────────────────────┘
```

</div>

<br/>

<!-- ARCHITECTURE DIAGRAM (SVG) -->
<div align="center">

<svg width="100%" viewBox="0 0 680 420" role="img" xmlns="http://www.w3.org/2000/svg">
<title>Weekly Spotify Wrapped — data pipeline architecture</title>
<desc>A flowchart showing data flowing from the Spotify API through Flask, to a Kafka producer, through four Kafka topics, processed by PySpark Spark Streaming consumers, stored in PostgreSQL, and finally queried by Flask and Gemini AI to send an email.</desc>
<defs>
  <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M2 1L8 5L2 9" fill="none" stroke="#73726C" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
  </marker>
</defs>

<!-- Row 1: Spotify API → Flask → Producer -->
<rect x="30" y="30" width="130" height="52" rx="8" fill="#E1F5EE" stroke="#0F6E56" stroke-width="0.5"/>
<text x="95" y="53" text-anchor="middle" dominant-baseline="central" fill="#085041" font-size="14" font-weight="500" font-family="system-ui,sans-serif">Spotify API</text>
<text x="95" y="70" text-anchor="middle" dominant-baseline="central" fill="#0F6E56" font-size="12" font-family="system-ui,sans-serif">recently played</text>

<rect x="220" y="30" width="130" height="52" rx="8" fill="#EEEDFE" stroke="#534AB7" stroke-width="0.5"/>
<text x="285" y="53" text-anchor="middle" dominant-baseline="central" fill="#3C3489" font-size="14" font-weight="500" font-family="system-ui,sans-serif">Flask</text>
<text x="285" y="70" text-anchor="middle" dominant-baseline="central" fill="#534AB7" font-size="12" font-family="system-ui,sans-serif">web service / auth</text>

<rect x="410" y="30" width="130" height="52" rx="8" fill="#FAEEDA" stroke="#854F0B" stroke-width="0.5"/>
<text x="475" y="53" text-anchor="middle" dominant-baseline="central" fill="#633806" font-size="14" font-weight="500" font-family="system-ui,sans-serif">Kafka producer</text>
<text x="475" y="70" text-anchor="middle" dominant-baseline="central" fill="#854F0B" font-size="12" font-family="system-ui,sans-serif">Python script</text>

<!-- arrows row 1 -->
<line x1="160" y1="56" x2="218" y2="56" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<line x1="350" y1="56" x2="408" y2="56" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>

<!-- Row 2 label -->
<text x="340" y="120" text-anchor="middle" dominant-baseline="central" fill="#73726C" font-size="12" font-family="system-ui,sans-serif">Kafka topics (Docker / Confluent KRaft)</text>

<!-- Row 2: Kafka topics -->
<rect x="30" y="135" width="130" height="44" rx="8" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/>
<text x="95" y="157" text-anchor="middle" dominant-baseline="central" fill="#7A2510" font-size="13" font-weight="500" font-family="system-ui,sans-serif">song_topic</text>

<rect x="180" y="135" width="130" height="44" rx="8" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/>
<text x="245" y="157" text-anchor="middle" dominant-baseline="central" fill="#7A2510" font-size="13" font-weight="500" font-family="system-ui,sans-serif">album_topic</text>

<rect x="330" y="135" width="130" height="44" rx="8" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/>
<text x="395" y="157" text-anchor="middle" dominant-baseline="central" fill="#7A2510" font-size="13" font-weight="500" font-family="system-ui,sans-serif">artist_topic</text>

<rect x="480" y="135" width="160" height="44" rx="8" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/>
<text x="560" y="157" text-anchor="middle" dominant-baseline="central" fill="#7A2510" font-size="13" font-weight="500" font-family="system-ui,sans-serif">item_topic</text>

<!-- producer → topics -->
<path d="M475 82 L475 110 L95 110 L95 133" fill="none" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<path d="M475 110 L245 110 L245 133" fill="none" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<path d="M475 110 L395 110 L395 133" fill="none" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<path d="M475 110 L560 110 L560 133" fill="none" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>

<!-- Row 3 label -->
<text x="340" y="220" text-anchor="middle" dominant-baseline="central" fill="#73726C" font-size="12" font-family="system-ui,sans-serif">PySpark Structured Streaming consumers</text>

<!-- Row 3: PySpark consumers -->
<rect x="30" y="233" width="130" height="44" rx="8" fill="#E6F1FB" stroke="#185FA5" stroke-width="0.5"/>
<text x="95" y="255" text-anchor="middle" dominant-baseline="central" fill="#0C447C" font-size="13" font-weight="500" font-family="system-ui,sans-serif">song consumer</text>

<rect x="180" y="233" width="130" height="44" rx="8" fill="#E6F1FB" stroke="#185FA5" stroke-width="0.5"/>
<text x="245" y="255" text-anchor="middle" dominant-baseline="central" fill="#0C447C" font-size="13" font-weight="500" font-family="system-ui,sans-serif">album consumer</text>

<rect x="330" y="233" width="130" height="44" rx="8" fill="#E6F1FB" stroke="#185FA5" stroke-width="0.5"/>
<text x="395" y="255" text-anchor="middle" dominant-baseline="central" fill="#0C447C" font-size="13" font-weight="500" font-family="system-ui,sans-serif">artist consumer</text>

<rect x="480" y="233" width="160" height="44" rx="8" fill="#E6F1FB" stroke="#185FA5" stroke-width="0.5"/>
<text x="560" y="255" text-anchor="middle" dominant-baseline="central" fill="#0C447C" font-size="13" font-weight="500" font-family="system-ui,sans-serif">history consumer</text>

<!-- topics → consumers -->
<line x1="95" y1="179" x2="95" y2="231" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<line x1="245" y1="179" x2="245" y2="231" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<line x1="395" y1="179" x2="395" y2="231" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<line x1="560" y1="179" x2="560" y2="231" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>

<!-- Row 4: PostgreSQL -->
<rect x="220" y="320" width="220" height="52" rx="8" fill="#EAF3DE" stroke="#3B6D11" stroke-width="0.5"/>
<text x="330" y="343" text-anchor="middle" dominant-baseline="central" fill="#173404" font-size="14" font-weight="500" font-family="system-ui,sans-serif">PostgreSQL</text>
<text x="330" y="360" text-anchor="middle" dominant-baseline="central" fill="#3B6D11" font-size="12" font-family="system-ui,sans-serif">star schema (dim + fact)</text>

<!-- consumers → postgres -->
<path d="M95 277 L95 300 L330 300 L330 318" fill="none" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<path d="M245 277 L245 300 L330 300" fill="none" stroke="#73726C" stroke-width="1.5"/>
<path d="M395 277 L395 300 L330 300" fill="none" stroke="#73726C" stroke-width="1.5"/>
<path d="M560 277 L560 300 L330 300" fill="none" stroke="#73726C" stroke-width="1.5"/>
<line x1="330" y1="300" x2="330" y2="318" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>

<!-- Row 5: Flask + Gemini → Email -->
<rect x="30" y="390" width="110" height="40" rx="8" fill="#EEEDFE" stroke="#534AB7" stroke-width="0.5"/>
<text x="85" y="410" text-anchor="middle" dominant-baseline="central" fill="#3C3489" font-size="13" font-weight="500" font-family="system-ui,sans-serif">Flask /insight</text>

<rect x="160" y="390" width="110" height="40" rx="8" fill="#FAEEDA" stroke="#854F0B" stroke-width="0.5"/>
<text x="215" y="410" text-anchor="middle" dominant-baseline="central" fill="#633806" font-size="13" font-weight="500" font-family="system-ui,sans-serif">Gemini AI</text>

<rect x="290" y="390" width="90" height="40" rx="8" fill="#E1F5EE" stroke="#0F6E56" stroke-width="0.5"/>
<text x="335" y="410" text-anchor="middle" dominant-baseline="central" fill="#085041" font-size="13" font-weight="500" font-family="system-ui,sans-serif">📧 Email</text>

<!-- postgres → flask insight -->
<path d="M220 346 L85 346 L85 388" fill="none" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<line x1="140" y1="410" x2="158" y2="410" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
<line x1="270" y1="410" x2="288" y2="410" stroke="#73726C" stroke-width="1.5" marker-end="url(#arrow)"/>
</svg>

*Figure 1 — Full data pipeline: ingestion → Kafka → PySpark consumers → PostgreSQL → dashboard & email*

</div>

---

## 🔧 Pipeline Layers

### 1. Ingestion Layer — Flask Backend & Producer

`flask_backend/app.py` serves as the Spotify OAuth handler and data gateway. It authenticates via the Authorization Code flow, fetches the 50 most recently played tracks from the Spotify API, and feeds the raw event payloads into `producer/producer.py`. The producer serializes each event as JSON and publishes it asynchronously to the appropriate Kafka topic based on data dimension (song, album, artist, item). This design ensures the web layer never blocks on downstream processing.

### 2. Message Broker Layer — Apache Kafka

A local Confluent KRaft cluster runs inside Docker with no external Zookeeper dependency. Four partitioned topics (`song_topic`, `album_topic`, `artist_topic`, `item_topic`) decouple event production from consumption, allowing each dimension's processing to be independently scaled, replayed, or paused. Events are retained for 7 days by default, enabling reprocessing if a consumer fails.

### 3. Stream Processing Layer — PySpark Structured Streaming

Four dedicated PySpark consumers (`spark-consumer-song.py`, `spark-consumer-album.py`, `spark-consumer-artist.py`, `spark-consumer-item.py`) each run as an independent Structured Streaming job. They use `readStream` to consume from their Kafka topic, apply rolling aggregations (play count, top tracks, listening time), and write results to PostgreSQL via `foreachBatch` and JDBC. Separating consumers per dimension prevents processing bottlenecks — a slow album aggregation never delays song KPI computation.

### 4. Storage Layer — PostgreSQL (Star Schema)

Aggregated results land in a Kimball-style star schema:

| Table | Type | Description |
|---|---|---|
| `dim_song` | Dimension | Song metadata — title, duration, explicit flag |
| `dim_album` | Dimension | Album metadata — name, release date, image URL |
| `dim_artist` | Dimension | Artist metadata — name, genres, popularity |
| `fact_history` | Fact | Listening events — `played_at`, `song_id`, `album_id`, `artist_id` |

A `UNIQUE (played_at, song_id)` constraint on `fact_history` with `ON CONFLICT DO NOTHING` prevents duplicate inserts if the pipeline restarts.

### 5. Visualization & Insight Layer — Streamlit + Flask + Gemini

`dashboard/dashboard.py` queries PostgreSQL directly and renders live listening metrics — top tracks, most-played artists, listening hours by day. The Flask `/insight` endpoint aggregates the week's data, constructs a prompt, and sends it to Gemini AI, which generates a personalized narrative summary delivered to the user's email via `smtplib`.

---

## 🛠️ Tech Stack

| Category | Technology | Role in this project |
|---|---|---|
| **Data Source** | Spotify Web API | Live recently-played track events |
| **Backend** | Flask + Python | OAuth authentication, REST endpoints, event dispatch |
| **Message Broker** | Apache Kafka (Confluent KRaft) | Decoupled, partitioned topic streaming |
| **Stream Processing** | PySpark Structured Streaming | Parallel, dimension-isolated aggregation |
| **Database** | PostgreSQL + SQLAlchemy | Star schema storage — facts and dimensions |
| **AI Insight** | Google Gemini API | Natural language weekly listening summary |
| **Dashboard** | Streamlit | Real-time KPI visualization |
| **Infrastructure** | Docker + Docker Compose | Single-command cluster orchestration |
| **Language** | Python 3.10+ | End-to-end |

---

## 📁 Repository Structure

```
spotify-wrapped/
├── consumers/
│   ├── spark-consumer-album.py     # Tracks real-time top-performing albums
│   ├── spark-consumer-artist.py    # Calculates continuous artist metrics
│   ├── spark-consumer-item.py      # Computes distinct catalog item KPIs
│   └── spark-consumer-song.py      # Aggregates song listening volumes live
├── dashboard/
│   └── dashboard.py                # Streamlit real-time analytics dashboard
├── flask_backend/
│   ├── app.py                      # Core Flask app — OAuth + /insight endpoint
│   ├── models.py                   # SQLAlchemy schema definitions
│   ├── token_info.json             # Spotify token state (gitignored)
│   └── utils.py                    # Data formatters, DB helpers, email sender
├── producer/
│   ├── dataset.csv                 # Seed listening data for offline testing
│   └── producer.py                 # Kafka event producer
├── .env.example                    # Environment variable template
├── docker-compose.yml              # Multi-service Kafka cluster orchestration
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Docker Desktop installed and running
- Python 3.10+
- Spotify Developer account (for API credentials)
- Google Gemini API key (for AI insights)
- Gmail account with App Password enabled (for email delivery)

### 1. Clone and configure

```bash
git clone https://github.com/aasthasheth180-ops/Spotify-Realtime-Pipeline.git
cd Spotify-Realtime-Pipeline
cp .env.example .env
```

Edit `.env` with your credentials:

```env
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:5000/redirect
GEMINI_API_KEY=your_gemini_key
EMAIL_SENDER=your_gmail@gmail.com
EMAIL_APP_PASSWORD=your_app_password
EMAIL_RECIPIENT=destination@example.com
```

### 2. Start the Kafka infrastructure

```bash
docker compose up -d
```

Wait ~30 seconds, then verify all services are healthy:

```bash
docker compose ps
```

Open Confluent Control Center at `http://localhost:9021` and confirm the broker is running. Manually create the four topics if they were not auto-created: `song_topic`, `album_topic`, `artist_topic`, `item_topic`.

### 3. Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate         # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Authenticate with Spotify

```bash
python flask_backend/app.py
```

Navigate to `http://localhost:5000/login` and complete the OAuth flow. A `token_info.json` file will be created in `flask_backend/`.

### 5. Start the producer

Open a new terminal (with venv activated):

```bash
python producer/producer.py
```

You should see `Produced: <event>` messages. Verify events appear in Control Center under Topics → Messages.

### 6. Start the PySpark consumers

Open four terminals (or use `tmux`), one per consumer:

```bash
python consumers/spark-consumer-song.py
python consumers/spark-consumer-album.py
python consumers/spark-consumer-artist.py
python consumers/spark-consumer-item.py
```

Verify rows appear in PostgreSQL:

```sql
SELECT * FROM fact_history LIMIT 5;
```

### 7. Launch the dashboard

```bash
streamlit run dashboard/dashboard.py
```

Open `http://localhost:8501` to see your live listening dashboard.

### 8. Generate your weekly insight

```bash
curl http://localhost:5000/insight
```

Check your inbox for the AI-generated weekly summary.

---

## 💡 Key Engineering Decisions

**Multi-consumer parallel processing** — Instead of a single monolithic consumer, the pipeline runs four isolated Spark jobs, one per business dimension. This prevents compute bottlenecks (a slow artist aggregation never delays song KPI computation), isolates processing failures, and makes the codebase easier to extend.

**Asynchronous decoupling via Kafka** — The Flask layer and the processing layer are fully decoupled. The producer publishes events and returns immediately; downstream consumers process at their own pace. If a consumer crashes, events queue in Kafka and are consumed on restart — no data is lost.

**Deduplication at the database layer** — A `UNIQUE (played_at, song_id)` constraint on `fact_history` combined with `ON CONFLICT DO NOTHING` prevents duplicate rows if the producer or consumer restarts and re-processes already-seen events.

**Star schema over flat table** — Separating dimension data into `dim_song`, `dim_album`, and `dim_artist` tables reduces storage redundancy, makes analytical queries faster, and follows the Kimball model used in production data warehouses.

**Containerized infrastructure** — `docker-compose.yml` orchestrates the entire Kafka cluster. Developers initialize the environment with one command and never manually configure Zookeeper, broker ports, or topic retention policies.

---

## 📚 What I Learned

- **Event streaming architecture** — how Kafka decouples producers from consumers and why that matters for fault tolerance and scalability
- **PySpark Structured Streaming** — `readStream`, `foreachBatch`, JDBC sinks, and the difference between micro-batch and continuous processing modes
- **Star schema design** — Kimball-style fact/dimension modelling for analytical workloads versus OLTP normalization
- **OAuth 2.0 flow** — implementing the Spotify Authorization Code flow with token refresh handling in Flask
- **Containerized infrastructure** — writing production-grade `docker-compose.yml` files with health checks and ordered service startup

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

Built with ☕ and too many late nights.

If this helped you, consider leaving a ⭐

</div>
