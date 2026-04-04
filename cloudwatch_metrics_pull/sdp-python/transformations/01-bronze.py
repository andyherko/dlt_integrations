from pyspark import pipelines as dp

from config import get_rules

spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")
catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
env = spark.conf.get("env")


@dp.table(comment="Raw user data")
def raw_user_data():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaHints", "id int")
      # Switches from listing to event-based discovery, muted because of access restrictions to s3 storage from serverless
      # .option("cloudFiles.useNotifications", "true")
      .load(f"/Volumes/{catalog}/{schema}/raw_data/{env}/users_json/*.json"))


@dp.table(comment="Raw spend data")
def raw_spend_data():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaHints", "id int, age int, annual_income float, spending_score float")
    # .option("cloudFiles.useNotifications", "true")
    .load(f"/Volumes/{catalog}/{schema}/raw_data/{env}/spend_csv/*.csv"))

  # Ingest raw User stream data in incremental mode

@dp.table(comment="Raw user data")
@dp.expect_all_or_drop(('user_bronze_sdp')) #get the rules from our centralized table.
def user_bronze_sdp():
  return spark.readStream.table("raw_user_data")
