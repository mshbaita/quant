from utils.config import CSV_TABLE_NAME

def read_csv_data(spark):
    """Extract data from a Hive table."""
    df = spark.table(CSV_TABLE_NAME)
    return df
