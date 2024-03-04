from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from utils.config import config


class BaseExtractor(ABC):
    """
    This Class do the extraction and unified the schema from both sources in order to use one schema in tables
    """
    def __init__(self, spark: SparkSession, table_name: str = ""):
        self._spark = spark
        self.__table_name = table_name

    def __read_data(self) -> DataFrame:
        return self._spark.read.format("delta").load(f"{config.DATA_LAKE_S3_BUCKET_URL}/{self.__table_name}")

    @abstractmethod
    def _process(self, dataframe: DataFrame) -> DataFrame:
        pass

    def execute(self):
        dataframe = self.__read_data()
        return self._process(dataframe)
