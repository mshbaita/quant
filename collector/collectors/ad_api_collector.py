import math

from collectors.base_collector import BaseDataCollector
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils.config import config

import requests

from utils.types import infer_json_schema


class AdAPICollector(BaseDataCollector):
    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, table_name="ad_api_data")
        self.api_endpoint = config.ADS_API_ENDPOINT

    def _extract(self):
        # hit the first API call to get the total number of pages, limit=1 to reduce networking but to use the value
        # in inferring the json schema
        result = self.make_custom_api_call(api_endpoint=self.api_endpoint, page=1, limit=1)
        # calculate the total number of pages
        n_pages = math.ceil(result['total'] / config.ADS_API_CALL_PAGE_SIZE)
        # create udf with the proper dynamically inferred return type
        make_custom_api_call_udf = F.udf(f=self.make_custom_api_call,
                                         returnType=infer_json_schema(spark=self._spark, example_json=result))

        # Get all pages
        df = (
            self._spark
            .range(n_pages)  # Creates a new dataframe with 1 column, "id"
            .withColumn("page_number", F.col("id") + 1)
            .withColumn("data", make_custom_api_call_udf(
                F.lit(self.api_endpoint), F.lit(config.ADS_API_CALL_PAGE_SIZE), F.col("page_number")
            ))
            .drop("id", "page_number")
        )
        return df

    def _process(self, df: DataFrame) -> DataFrame:
        # Organize the data and metadata
        # 1. Cast the metadata of the API request into a map
        # 2. Cast the array carrying the response data into array of maps
        df = (df
              .withColumn("metadata", F.struct(F.col("data.total"),
                                               F.col("data.page"),
                                               F.col("data.limit"),
                                               F.col("data.totalPage"),
                                               F.col("data.hasNextPage"),
                                               F.col("data.hasPrevPage"),
                                               F.col("data.nextPage")))
              .withColumn("data", F.col("data.data"))
              .select("data", "metadata")
              )
        return df

    @staticmethod
    def make_custom_api_call(api_endpoint, limit, page):
        """
        Makes an API call to the ads API
        :param api_endpoint:
        :param limit: How many entries to return in one page
        :param page: page number
        :return: Json response resulted from the API call
        """
        params = {
            'page': f'{page}',
            'limit': f'{limit}',
            'sort': '-1',
            'sortBy': 'refreshed.at',
            'city': '6009d941950ada00061eeeab'
        }

        response = requests.get(api_endpoint, params=params).json()
        return response
