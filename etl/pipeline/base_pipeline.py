from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession, DataFrame

from extractors.base_extractor import BaseExtractor
from tables.base_table import BaseTable
from utils.config import config


class BasePipeline(ABC):
    """
    This Class do the extraction and unified the schema from both sources in order to use one schema in tables
    """
    def __init__(self, spark: SparkSession, extractors: List[type[BaseExtractor]], tables: List[type[BaseTable]]):
        self._spark = spark
        self.__extractors = extractors
        self.__tables = tables

    def __execute_extractors(self) -> List[DataFrame]:
        extracted_sources = list()
        for extractor in self.__extractors:
            extracted_sources.append(
                extractor(self._spark).execute()
            )
        return extracted_sources

    @abstractmethod
    def _process_extracted_sources(self, sources_dataframes: List[DataFrame]) -> DataFrame:
        raise NotImplementedError

    def __execute_tables(self, processed_df):
        for table in self.__tables:
            table(spark=self._spark, source_df=processed_df).execute()

    def execute(self):
        extracted_sources_dfs = self.__execute_extractors()
        processed_df = self._process_extracted_sources(sources_dataframes=extracted_sources_dfs)
        processed_df.dropDuplicates()
        self.__execute_tables(processed_df=processed_df)