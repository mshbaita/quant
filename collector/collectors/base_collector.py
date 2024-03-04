from abc import abstractmethod, ABC
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from utils.config import config

import pyspark.sql.functions as F


class BaseDataCollector(ABC):

    def __init__(self, spark: SparkSession, table_name: str):
        self._spark = spark
        self.__dataframe = None
        self.__table_name = table_name
        self.__start_time = datetime.now().strftime('%Y-%m-%d-%H-%M')

    @property
    def dataframe(self) -> DataFrame:
        return self.__dataframe

    @dataframe.setter
    def dataframe(self, v):
        self.__dataframe = v

    @abstractmethod
    def _extract(self) -> DataFrame:
        pass

    def _process(self, df: DataFrame) -> DataFrame:
        return df

    def __handle_ingestion_time_partitioning(self):
        # Set to the start time to avoid gaps in the data in sources with incremental pulling
        self.dataframe = self.__dataframe.withColumn("ingestion_time", F.lit(self.__start_time))

    def load_to_s3(self):
        (self.__dataframe
         .coalesce(1)
         .write
         .partitionBy("ingestion_time")
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .option("delta.columnMapping.mode", "name")  # To support Arabic column naming
         .option("path", f"{config.DATA_LAKE_S3_BUCKET_URL}/{self.__table_name}")
         .save()
         )

    def execute(self):
        self.dataframe = self._extract()
        self.dataframe = self._process(self.dataframe)
        self.dataframe.cache()
        self.__handle_ingestion_time_partitioning()
        self.dataframe.show()
        self.load_to_s3()
