from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from consumer.quality_checks import run_all_checks
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, OUTPUT_DIR

# Schema — defines expected shape of incoming JSON (schema evolution handled here)
SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("merchant",       StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("status",         StringType(),  True),
])

def run():
    spark = SparkSession.builder \
        .appName("TransactionStreamingPipeline") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON payload
    parsed = raw_stream.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # Add processing timestamp
    parsed = parsed.withColumn("processed_at", current_timestamp())
    parsed = parsed.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Run data quality checks
    def process_batch(batch_df, batch_id):
        print(f"\n[BATCH {batch_id}] Records received: {batch_df.count()}")
        clean_df = run_all_checks(batch_df)
        print(f"[BATCH {batch_id}] Records after quality checks: {clean_df.count()}")

        # Write to Parquet partitioned by date
        clean_df.write \
            .mode("append") \
            .partitionBy("currency") \
            .parquet(OUTPUT_DIR)

        print(f"[BATCH {batch_id}] Written to Parquet: {OUTPUT_DIR}")

    # Trigger every 30 seconds
    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "./checkpoints") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("Spark Streaming job running. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    run()