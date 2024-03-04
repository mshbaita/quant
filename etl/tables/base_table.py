from abc import ABC, abstractmethod
from typing import List
from delta.tables import DeltaTable

from pyspark.sql import SparkSession, DataFrame

from utils.config import config


class BaseTable(ABC):
    def __init__(self, spark: SparkSession, table_name: str, source_df: DataFrame,
                 cols: List[str], id_col: str, partitioning_cols: List[str] = None):
        self._spark = spark
        self.__source_dataframe = source_df
        self.__table_name = table_name
        self.__cols = cols
        self.__id_col = id_col
        self.__partitioning_cols = partitioning_cols


    def __project_cols(self, df: DataFrame) -> DataFrame:
        return df.select(self.__cols)

    def _pre_save_process(self, df: DataFrame) -> DataFrame:
        return df

    def __load_table(self, df: DataFrame):
        target_path = f"{config.DATA_WAREHOUSE_S3_BUCKET_URL}/{self.__table_name}"

        # # Check if the Delta table exists at the target path
        # if DeltaTable.isDeltaTable(self._spark, target_path):
        #     # Load the DeltaTable object for the existing Delta table
        #     deltaTable = DeltaTable.forPath(self._spark, target_path)
        #
        #     # Perform the merge operation
        #     # "source" is the alias for the incoming DataFrame
        #     # "target" is the alias for the existing Delta table
        #     deltaTable.alias("target").merge(
        #         source=df.alias("source"),
        #         condition="target._id = source._id"
        #     ).whenMatchedUpdateAll(  # Update all matching rows
        #     ).whenNotMatchedInsertAll(  # Insert new rows that do not match
        #     ).execute()
        # else:
        #     # If the Delta table does not exist, write the DataFrame as a new Delta table
        #     (df
        #      .coalesce(1)  # Coalesce to potentially reduce the number of files created
        #      .write
        #      .partitionBy(*self.__partitioning_cols)  # Use the class attribute for partitioning columns
        #      .format("delta")
        #      .mode("append")  # Mode is set to append to ensure the table is created if not exists
        #      .option("mergeSchema", "true")  # Enable schema merging
        #      .option("delta.columnMapping.mode","name")  # To support column naming flexibility, including non-English names
        #      .option("path", target_path)  # Specify the target path for the Delta table
        #      .save()
        #      )

        # todo: implement the merge methodology
        if self.__partitioning_cols:
            (df
             .coalesce(1)
             .write
             .partitionBy(*self.__partitioning_cols)
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .option("delta.columnMapping.mode", "name")  # To support Arabic column naming
             .option("path", f"{config.DATA_WAREHOUSE_S3_BUCKET_URL}/{self.__table_name}")
             .save()
             )
        else:
            (df
             .coalesce(1)
             .write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .option("delta.columnMapping.mode", "name")  # To support Arabic column naming
             .option("path", f"{config.DATA_WAREHOUSE_S3_BUCKET_URL}/{self.__table_name}")
             .save()
             )

    def execute(self):
        dataframe = self.__project_cols(df=self.__source_dataframe)
        dataframe = dataframe.dropDuplicates([self.__id_col])
        dataframe.cache()
        dataframe = self._pre_save_process(dataframe)
        self.__load_table(df=dataframe)
