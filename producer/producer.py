"""
producer/producer.py  —  Kaggle CSV version
Reads Spotify tracks dataset and streams to Kafka exactly like the API version.
Pipeline (Kafka → PySpark → PostgreSQL) is completely unchanged.
"""

import os, json, time, random
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime, timezone


load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_PATH      = "producer/dataset.csv"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def load_dataset():
    df = pd.read_csv(CSV_PATH)
    # Keep only clean rows
    df = df.dropna(subset=["track_id","track_name","artists","album_name"])
    print(f"[✓] Loaded {len(df)} tracks from dataset")
    return df


def simulate_play_history(df, n=200):
    """
    Pick n random tracks and assign fake played_at timestamps
    spread over the last 7 days — simulates real listening history.
    """
    sample    = df.sample(n=min(n, len(df)), replace=True).reset_index(drop=True)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    plays     = []

    for i, row in sample.iterrows():
        # Spread plays across last 7 days randomly
        offset     = timedelta(
            days   =random.randint(0, 6),
            hours  =random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        played_at  = (now - offset).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        plays.append((row, played_at))

    # Sort by played_at descending (most recent first)
    plays.sort(key=lambda x: x[1], reverse=True)
    return plays


def publish_batch(plays, producer):
    published = 0
    for row, played_at in plays:
        track     = row
        song_id   = str(track["track_id"])
        # Use track_id as album/artist IDs too (dataset doesn't have separate IDs)
        album_id  = f"album_{abs(hash(str(track['album_name']))) % 100000}"
        artist_id = f"artist_{abs(hash(str(track['artists']))) % 100000}"

        # ── song_topic ──────────────────────────────────────
        producer.send("song_topic", key=song_id, value={
            "song_id"   : song_id,
            "song_name" : str(track["track_name"]),
            "duration"  : int(track.get("duration_ms", 200000)),
            "explicit"  : str(track.get("explicit", False)),
            "popularity": int(track.get("popularity", 50)),
        })

        # ── album_topic ─────────────────────────────────────
        producer.send("album_topic", key=album_id, value={
            "album_id"    : album_id,
            "album_name"  : str(track["album_name"]),
            "album_type"  : "album",
            "release_date": str(track.get("release_date", "2023-01-01")),
            "total_tracks": int(track.get("track_number", 10)),
        })

        # ── artist_topic ─────────────────────────────────────
        producer.send("artist_topic", key=artist_id, value={
            "artist_id"   : artist_id,
            "artist_name" : str(track["artists"]).strip("[]'"),
            "external_url": f"https://open.spotify.com/artist/{artist_id}",
        })

        # ── item_topic (fact) ────────────────────────────────
        producer.send("item_topic", key=song_id, value={
            "played_at"  : played_at,
            "song_id"    : song_id,
            "album_id"   : album_id,
            "artist_id"  : artist_id,
            "duration_ms": int(track.get("duration_ms", 200000)),
        })

        published += 1

    producer.flush()
    print(f"[✓] Published {published} plays to all 4 Kafka topics")
    return published


def run_producer(batch_size=50, interval_seconds=30):
    print(f"[*] Kafka Producer (CSV mode) → {KAFKA_SERVERS}")
    producer = create_producer()
    df       = load_dataset()

    print(f"[*] Streaming {batch_size} tracks every {interval_seconds}s")

    while True:
        try:
            plays = simulate_play_history(df, n=batch_size)
            publish_batch(plays, producer)
        except Exception as e:
            print(f"[!] Error: {e}")

        print(f"[*] Waiting {interval_seconds}s...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_producer(batch_size=50, interval_seconds=30)