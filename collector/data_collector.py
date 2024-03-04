from pyspark.sql import SparkSession

from collectors.ad_api_collector import AdAPICollector
from collectors.ad_csv_collector import AdCSVCollector
from utils.config import config

spark = (
        SparkSession.builder.appName("quant-data-collector")
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
        .getOrCreate()
)


def execute_collectors():
    # The file path can be passed as a parameter or an environment variable, keeping it here for simplicity
    (
        AdCSVCollector(spark=spark, source_file_path=f"{config.S3_CSV_SOURCE_BUCKET_URL}/realstate_data_2014_2016.csv")
        .execute()
    )

    (
        AdAPICollector(spark=spark)
        .execute()
    )


if __name__ == "__main__":
    try:
        execute_collectors()
    except Exception:
        raise
    finally:
        spark.stop()
