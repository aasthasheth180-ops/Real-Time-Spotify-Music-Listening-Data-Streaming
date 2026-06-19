"""
PySpark Structured Streaming Consumer — song_topic → PostgreSQL dim_song
Run:
    python consumers/spark-consumer-song.py
"""

import os

# ─────────────────────────────────────────────────────────────
# WINDOWS + HADOOP FIXES
# ─────────────────────────────────────────────────────────────

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

JDBC_URL = "jdbc:postgresql://localhost:5432/spotify_db"

DB_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ─────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────

SONG_SCHEMA = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_name", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("explicit", StringType(), True),
    StructField("popularity", IntegerType(), True),
])

# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("SongConsumer")

    # Kafka package
    .config(
        "spark.jars.packages",
        (
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3"
        )
    )

    # WINDOWS FIXES
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[*]")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("[*] Spark Session Started")

# ─────────────────────────────────────────────────────────────
# READ FROM KAFKA
# ─────────────────────────────────────────────────────────────

stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "song_topic")
    .option("startingOffsets", "latest")
    .load()
)

print("[*] Connected to Kafka")

# ─────────────────────────────────────────────────────────────
# PARSE JSON
# ─────────────────────────────────────────────────────────────

songs_df = (
    stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), SONG_SCHEMA).alias("data"))
    .select("data.*")
)

# ─────────────────────────────────────────────────────────────
# WRITE TO POSTGRES
# ─────────────────────────────────────────────────────────────

def write_to_postgres(batch_df, batch_id):

    if batch_df.count() == 0:
        print(f"[*] Batch {batch_id}: empty")
        return

    print(f"[✓] Batch {batch_id}: writing {batch_df.count()} rows")

    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_song")
        .option("user", DB_PROPS["user"])
        .option("password", DB_PROPS["password"])
        .option("driver", DB_PROPS["driver"])
        .mode("append")
        .save()
    )

# ─────────────────────────────────────────────────────────────
# STREAM QUERY
# ─────────────────────────────────────────────────────────────

query = (
    songs_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")

    # IMPORTANT: NEW CLEAN CHECKPOINT
    .option("checkpointLocation", "./checkpoint_song")

    .start()
)

print("[*] Song consumer running...")

query.awaitTermination()