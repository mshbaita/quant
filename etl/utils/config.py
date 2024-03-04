import os


class Config:
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    # API table name
    JSON_TABLE_NAME = "ad_api_data"  # delta table
    DATA_LAKE_S3_BUCKET_URL = "s3a://quant-data-lake"
    DATA_WAREHOUSE_S3_BUCKET_URL = "s3a://quantdata-warehouse"


config = Config()