import os
import sys

IN_DOCKER = os.path.exists("/.dockerenv") or "/opt/spark" in os.environ.get("SPARK_HOME", "")

KAFKA_SERVER = "redpanda:29092" if IN_DOCKER else "localhost:9092"
ICEBERG_URI = "http://iceberg-rest:8181" if IN_DOCKER else "http://localhost:8181"
S3_ENDPOINT = "http://minio:9000" if IN_DOCKER else "http://localhost:9000"

local_checkpoint_raw = os.path.abspath('./checkpoints/market_trades').replace('\\', '/')
CHECKPOINT_PATH = "/opt/spark/work-dir/checkpoints/market_trades" if IN_DOCKER else f"file:///{local_checkpoint_raw}"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoLakehouseProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", ICEBERG_URI) \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.demo.s3.endpoint", S3_ENDPOINT) \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.catalog.demo.s3.access-key-id", "admin") \
    .config("spark.sql.catalog.demo.s3.secret-access-key", "password123") \
    .config("spark.sql.catalog.demo.client.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema Definition
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("timestamp", StringType())

# 3. Create Namespace & Table
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.market")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.market.trades (
        symbol STRING,
        price DOUBLE,
        volume DOUBLE,
        event_time TIMESTAMP
    ) USING iceberg
""")

# 4. Stream from Redpanda
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", "crypto_raw") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Transform
silver_df = raw_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .drop("timestamp")

# 6. Stream directly to Iceberg
print(f"Starting Stream -> Iceberg ({S3_ENDPOINT})...")
query = silver_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "demo.market.trades") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()