import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

from src.cloudwatch_metrics_pull.kpi_spend import calculate_spending_efficiency


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
        .master("local[1]")
        .appName("unit-tests")
        .getOrCreate())


def test_calculate_spending_efficiency_kpi(spark):
    # 1. Arrange: Create mock data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("annual_income", FloatType(), True),
        StructField("spending_score", FloatType(), True)
    ])

    data = [
        (1, 100.0, 50.0),  # Expected: 50.0
        (2, 200.0, 20.0),  # Expected: 10.0
        (3, 0.0, 10.0),  # Expected: None (Division by zero check)
    ]

    input_df = spark.createDataFrame(data, schema)

    # 2. Act: Run the transformation
    result_df = calculate_spending_efficiency(input_df)
    results = result_df.collect()

    # 3. Assert: Verify the KPI values
    assert results[0]["spending_efficiency"] == 50.0
    assert results[1]["spending_efficiency"] == 10.0
    assert results[2]["spending_efficiency"] is None

def test_fail():
    assert False