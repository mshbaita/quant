import os


class Config:
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    # The bucket where we initially upload our csv files
    S3_CSV_SOURCE_BUCKET_URL = "s3a://real-state-csv-files"
    # The bucket that represents our data lake
    DATA_LAKE_S3_BUCKET_URL = "s3a://quant-data-lake"
    # The API Endpoint to fetch the ads data
    ADS_API_ENDPOINT = "https://api.dealapp.sa/production/ad"
    ADS_API_CALL_PAGE_SIZE = 10


config = Config()
