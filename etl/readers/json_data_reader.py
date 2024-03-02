from utils.config import JSON_TABLE_NAME


def read_json_data(spark):
    """Extract data from a Hive table."""
    df = spark.table(JSON_TABLE_NAME)
    return df
