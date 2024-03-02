def read_delta_file(spark):
    df = spark.read.format("delta").load("s3://your-bucket/path/to/delta-table/")
    return df
