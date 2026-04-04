from pyspark.sql.functions import col

# 1. Kafka Connection
kafka_options = {
    "kafka.bootstrap.servers": "your-kafka-broker:9092",
    "subscribe": "production_user_events",
    "startingOffsets": "latest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": dbutils.secrets.get(scope="kafka", key="jaas_config")
}

# 2. Read the Stream from Kafka
raw_kafka_df = (spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load())

# 3. Cast binary to String/JSON
# We keep it 'Raw' here so Bronze can handle schema evolution later
translated_df = raw_kafka_df.select(
    col("value").cast("string").alias("json_payload"),
    col("timestamp").alias("kafka_ingest_time")
)

# 4. Write to the UC Volume (Landing Zone)
# Path to DLT 'cloudFiles' (Auto Loader) is watching
volume_path = "/Volumes/lakehouse_dev/cloudwatch_metrics_pull/user_raw_data/dev/users_json"

checkpoint_path = "/Volumes/lakehouse_dev/cloudwatch_metrics_pull/user_raw_data/dev/_checkpoints/kafka_to_volume"

query = (translated_df.writeStream
    .trigger(processingTime='1 minute')
    .format("json")
    .option("checkpointLocation", checkpoint_path)
    .start(volume_path))