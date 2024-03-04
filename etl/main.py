from pyspark.sql import SparkSession

from extractors.ad_csv_extractor import AdCSVExtractor
from extractors.ad_api_extractor import AdAPIExtractor
from pipeline.ads_data_pipeline import AdsDataPipeline
from utils.config import config


def execute_readers(spark):
    csv_dataframe = AdCSVExtractor(spark=spark).execute()
    api_dataframe = AdAPIExtractor(spark=spark).execute()
    return csv_dataframe, api_dataframe


def main():
    spark = (SparkSession.builder
             .appName("quant-etl")
             .master("local[*]")
             .config("spark.driver.memory", "10g")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
             .config("spark.sql.debug.maxToStringFields", 1000)
             .config(
                    "spark.jars.packages",
                    "io.delta:delta-core_2.12:2.4.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.670"
                )
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID)
             .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
             .config("spark.sql.warehouse.dir", config.DATA_LAKE_S3_BUCKET_URL)
             .getOrCreate())

    AdsDataPipeline(spark=spark).execute()


if __name__ == "__main__":
    main()

# TODO:
#  1. Finalize the extractors, unify the dataframe returned
#  2. Set the cols for tables, including partitioning cols
#  3. Implement the merge criteria in the table
