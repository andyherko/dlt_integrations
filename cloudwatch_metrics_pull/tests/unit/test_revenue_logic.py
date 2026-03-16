import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from src.cloudwatch_metrics_pull.transformations.revenue_logic import calculate_net_revenue

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
        .master("local[*]")
        .appName("cloudwatch_metrics_pull")
        .getOrCreate())

def test_calculate_net_revenue(spark):
    source_data = [
        ("order_1", 100.0, 10.0),
        ("order_2", 50.5, 5.05)
    ]
    source_df = spark.createDataFrame(source_data, ["order_id", "gross_revenue", "tax_amount"])

    expected_data = [
        ("order_1", 100.0, 10.0, 90.0),
        ("order_2", 50.5, 5.05, 45.45)
    ]
    expected_df = spark.createDataFrame(expected_data, ["order_id", "gross_revenue", "tax_amount", "net_revenue"])

    actual_df = calculate_net_revenue(source_df)

    assertDataFrameEqual(actual_df, expected_df)
