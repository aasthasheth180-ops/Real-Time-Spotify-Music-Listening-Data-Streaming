"""
PySpark Structured Streaming Consumer — artist_topic → dim_artist table
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv

load_dotenv()



os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
JDBC_URL      = "jdbc:postgresql://localhost:5432/spotify_db"
JDBC_DRIVER   = os.path.expanduser("~/spark-jars/postgresql-42.7.3.jar")

ARTIST_SCHEMA = StructType([
    StructField("artist_id",    StringType(), True),
    StructField("artist_name",  StringType(), True),
    StructField("external_url", StringType(), True),
])
# Add this BEFORE your SparkSession builder
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--conf spark.hadoop.io.native.lib.available=false pyspark-shell"
)
spark = (SparkSession.builder
         .appName("ArtistConsumer")
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
          .option("subscribe", "artist_topic")
          .option("startingOffsets", "latest")
          .load())

artists_df = (stream
              .selectExpr("CAST(value AS STRING) as json_str")
              .select(from_json(col("json_str"), ARTIST_SCHEMA).alias("data"))
              .select("data.*")
              .dropDuplicates(["artist_id"]))


def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    print(f"[artist] Batch {batch_id}: {batch_df.count()} rows → dim_artist")
    (batch_df.write.format("jdbc")
     .option("url", JDBC_URL)
     .option("dbtable", "dim_artist")
     .option("user", "postgres")
     .option("password", "postgres")
     .option("driver", "org.postgresql.Driver")
     .mode("append").save())


(artists_df.writeStream
 .foreachBatch(write_to_postgres)
 .outputMode("append")
 .option("checkpointLocation", "./checkpoint_song")
 .start()
 .awaitTermination())