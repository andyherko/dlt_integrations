import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def calculate_net_revenue(df: DataFrame) -> DataFrame:
    """
    Pure transformation function to calculate net revenue.
    Decoupled from SDP/DLT decorators for unit testing.
    """
    return df.withColumn(
        "net_revenue",
        F.round(F.col("gross_revenue") - F.col("tax_amount"), 2)
    )

def calculate_spending_efficiency(df: DataFrame) -> DataFrame:
    """
    Calculates the 'spending_efficiency' KPI.
    Formula: (spending_score / annual_income) * 100
    Handles division by zero by returning null.
    """
    return df.withColumn(
        "spending_efficiency",
        F.when(F.col("annual_income") > 0,
               (F.col("spending_score") / F.col("annual_income")) * 100)
        .otherwise(None)
    )