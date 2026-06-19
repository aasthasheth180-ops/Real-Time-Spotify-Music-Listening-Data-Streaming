"""
PySpark Structured Streaming Consumer — item_topic → fact_history table
This is the most important consumer — creates the fact table.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dotenv import load_dotenv

load_dotenv()



os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
JDBC_URL      = "jdbc:postgresql://localhost:5432/spotify_db"
JDBC_DRIVER   = os.path.expanduser("~/spark-jars/postgresql-42.7.3.jar")

ITEM_SCHEMA = StructType([
    StructField("played_at",  StringType(),  True),
    StructField("song_id",    StringType(),  True),
    StructField("album_id",   StringType(),  True),
    StructField("artist_id",  StringType(),  True),
    StructField("duration_ms",IntegerType(), True),
])

# Add this BEFORE your SparkSession builder
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--conf spark.hadoop.io.native.lib.available=false pyspark-shell"
)
spark = (SparkSession.builder
         .appName("ItemConsumer")
         .config("spark.jars", JDBC_DRIVER)
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
         .config("spark.hadoop.io.native.lib.available", "false")
         .config("spark.hadoop.fs.file.impl.disable.cache", "true")
         .getOrCreate())


spark.sparkContext.setLogLevel("WARN")

stream = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_SERVERS)
          .option("subscribe", "item_topic")
          .option("startingOffsets", "latest")
          .load())

items_df = (stream
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), ITEM_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("played_at", to_timestamp(col("played_at")))
            .dropDuplicates(["played_at", "song_id"]))


def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    print(f"[item] Batch {batch_id}: {batch_df.count()} rows → fact_history")
    (batch_df.write.format("jdbc")
     .option("url", JDBC_URL)
     .option("dbtable", "fact_history")
     .option("user", "postgres")
     .option("password", "postgres")
     .option("driver", "org.postgresql.Driver")
     .mode("append").save())


(items_df.writeStream
 .foreachBatch(write_to_postgres)
 .outputMode("append")
 .option("checkpointLocation", "./checkpoint_song")
 .start()
 .awaitTermination())