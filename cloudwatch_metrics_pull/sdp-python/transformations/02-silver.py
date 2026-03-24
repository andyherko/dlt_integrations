import os
import sys

from pyspark import pipelines as dp
from pyspark.sql.functions import col, sha1, to_timestamp, current_timestamp, lit

from config import get_rules

# Serverless DLT runs from the file location.
# We need to find the 'src' directory relative to 'sdp-python/transformations/'
# Path: ../../src
current_path = os.getcwd()
project_root = os.path.abspath(os.path.join(current_path, "..", ".."))
src_path = os.path.join(project_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)


from cloudwatch_metrics_pull.kpi_spend import calculate_spending_efficiency

# Clean and anonymize User data
@dp.table(comment="User data cleaned and anonymized for analysis.")
@dp.expect_all_or_drop(get_rules("user_silver_sdp"))
def user_silver_sdp():
    return spark.readStream.table("user_bronze_sdp").select(
        col("id").cast("int"),
        sha1("email").alias("email"),
        to_timestamp(col("creation_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"),
        to_timestamp(col("last_activity_date"), "MM-dd-yyyy HH:mm:ss").alias("last_activity_date"),
        "firstname",
        "lastname",
        "address",
        "city",
        "last_ip",
        "postcode",
    )

# Ingest user spending score
@dp.table(comment="Spending score from raw data")
@dp.expect_all_or_drop(get_rules("spend_silver_sdp"))
def spend_silver_sdp():
    source_df = spark.readStream.table("raw_spend_data")
    return calculate_spending_efficiency(source_df)


@dp.table(name='user_quarantine_sdp', comment="User quarantine data")
@dp.expect_all_or_drop(get_rules("user_quarantine_sdp"))
def user_quarantine_sdp():
    return (spark.readStream.table("user_bronze_sdp")
        .withColumn("quarantine_reason", lit("invalid_activity_sequence"))
        .withColumn("quarantine_ts", current_timestamp()))