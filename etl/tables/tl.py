# from abc import ABC
# from datetime import datetime
#
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col
# from utils.config import config
# from delta.tables import DeltaTable
# from pyspark.sql import SparkSession
#
#
# class TL(ABC):
#     def __init__(self, spark: SparkSession):
#         self._spark = spark
#         self.__start_time = datetime.now().strftime('%Y-%m-%d-%H-%M')
#
#     def __transform_columns(df: DataFrame):
#         """Apply column name transformations based on mappings."""
#         for original, translated in config.COLUMN_TRANSLATIONS.items():
#             df = df.withColumnRenamed(original, translated)
#         return df
#
#
#     def transform_dataframe(df: DataFrame):
#
#         # Apply column translations
#         df_transformed = __transform_columns(df)
#
#         # Additional transformation steps can be applied here
#
#         return df_transformed
#
#
#     def upsert_data(spark, new_data_df):
#         """
#         Perform an upsert operation - update existing records and insert new ones, for none delta table.
#
#         :param spark: Spark session
#         :param new_data_df: DataFrame containing new data
#         """
#         try:
#             # Try to read the existing data
#             existing_df = spark.read.parquet(config.S3_PATH)
#
#             # Perform a full outer join between existing data and new data on the primary key
#             combined_df = existing_df.join(new_data_df, config.PRIMARY_KEY, 'outer')
#
#             # Columns to be updated/considered. This needs to be dynamic based on your schema
#             update_cols = [c for c in combined_df.columns if c not in [config.PRIMARY_KEY, "existing", "new"]]
#
#             # Resolve each column: if new data is present, choose new data, else keep existing
#             for col_name in update_cols:
#                 combined_df = combined_df.withColumn(col_name,
#                                                      col(f"new.{col_name}").alias(col_name))
#
#             # Select only the columns from the new data frame to maintain the schema
#             final_df = combined_df.select([config.PRIMARY_KEY] + update_cols)
#
#         except Exception as e:
#             # If the existing data does not exist, set new data as the final DataFrame to be written
#             final_df = new_data_df
#
#         # Write (or overwrite) the final DataFrame back to the S3 path
#         final_df.write.mode("overwrite").parquet(config.S3_PATH)
#
#
#     def merge_delta(spark, new_data_df):
#         # Create a DeltaTable object for the existing Delta table
#         deltaTable = DeltaTable.forPath(spark, config.PARQUET_PATH)
#
#         # Assume `new_data_df` is the DataFrame with new data to merge
#         # Perform the merge operation
#         (deltaTable.alias("old_data")
#          .merge(
#             new_data_df.alias("new_data"),
#             "old_data.id = new_data.id")  # Merge condition
#          .whenMatchedUpdate(set={"value": "new_data.value"})  # Update in case of match
#          .whenNotMatchedInsert(values={"id": "new_data.id", "value": "new_data.value"})  # Insert in case of no match
#          .execute())
#
#
#
#
#
