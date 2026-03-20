import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from src.cloudwatch_metrics_pull.revenue_logic import calculate_net_revenue


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture to create a local SparkSession for testing."""
    return (SparkSession.builder
        .master("local[*]")
        .appName("cloudwatch_metrics_pull")
        .getOrCreate())

def test_calculate_net_revenue(spark) -> None:
    """Test that net_revenue is correctly calculated as gross_revenue minus tax_amount."""
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
