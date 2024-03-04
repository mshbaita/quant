from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from extractors.ad_api_extractor import AdAPIExtractor
from extractors.ad_csv_extractor import AdCSVExtractor
from pipeline.base_pipeline import BasePipeline
from functools import reduce, partial

from tables.ad_table import AdsTable
from tables.district_table import LocationTable
from tables.property_type_table import PropertyTable
from utils.udfs import hash_values


class AdsDataPipeline(BasePipeline):
    """
    This Class do the extraction and unified the schema from both sources in order to use one schema in tables
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark, extractors=[AdCSVExtractor, AdAPIExtractor], tables=[AdsTable,
                                                                                     LocationTable,
                                                                                     PropertyTable])

    def _process_extracted_sources(self, sources_dataframes: List[DataFrame]) -> DataFrame:
        union_by_name = partial(DataFrame.unionByName, allowMissingColumns=True)
        unified_df = reduce(union_by_name, sources_dataframes)
        unified_df = self.__standardize_foreign_keys(unified_df)
        unified_df = unified_df.dropDuplicates()
        return unified_df

    @staticmethod
    def __standardize_foreign_keys(df: DataFrame) -> DataFrame:
        return (
            df
            .drop("district_id", "property_type_id")
            .withColumn("district_id",
                        F.when((F.col("district_name_en").isNull()) | (F.col("district_name_en") == ""), -1)
                        .otherwise(hash_values(F.col("district_name_en")))
                        )
            .withColumn("property_type_id",
                        F.when((F.col("property_type").isNull()) | (F.col("property_type") == ""), -1)
                        .otherwise(hash_values(F.col("property_type")))
                        )
            .withColumnRenamed("created_by_id", "advertiser_id")
        )
