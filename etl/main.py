from pyspark.sql import SparkSession
from readers.csv_data_reader import read_csv_data
from readers.json_data_reader import read_json_data
from readers.warehouse_reader import read_delta_file

from tables.tl import upsert_data, transform_dataframe, merge_delta



# spark-submit \
#   --packages io.delta:delta-core_2.12:1.0.0 \
#   main.py

def main():
    spark = SparkSession.builder \
        .appName("deltaLakeQuant") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
        .getOrCreate()

    # Read from Hive Data Lake tables
    df_csv = read_csv_data(spark)
    df_json = read_json_data(spark)

    # Transform columns based on mappings
    df_csv_transformed = transform_dataframe(df_csv)
    df_json_transformed = transform_dataframe(df_json)

    # Additional transformations can be applied here as needed

    # Load transformed data back into Hive Warehouse tables
    # upsert_data(spark, df_csv_transformed)
    # upsert_data(spark, df_json_transformed)

    # Load data into delta warehouse
    merge_delta(spark, df_csv_transformed)
    merge_delta(spark, df_json_transformed)

    # do the analysis here, after getting the warehouse data
    return read_delta_file(spark)



if __name__ == "__main__":
    main()
