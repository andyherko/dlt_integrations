from pyspark import pipelines as dp

from config import get_rules


# Join both data to create our final table
@dp.table(comment="Final user table with all information for Analysis / ML")
@dp.expect_all_or_drop(get_rules('user_gold_sdp'))
def user_gold_sdp():
  user_df = spark.table("user_silver_sdp")
  spend_df = spark.read.table("spend_silver_sdp")
  return user_df.join(spend_df, ["id"], "left")