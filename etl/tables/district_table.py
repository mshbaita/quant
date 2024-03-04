from pyspark.sql import SparkSession, DataFrame
from tables.base_table import BaseTable


class LocationTable(BaseTable):

    def __init__(self, spark: SparkSession, source_df: DataFrame):
        super().__init__(
            spark,
            table_name="district_detail",
            source_df=source_df,
            cols=["district_id",
                  "district_name_ar",
                  "district_name_en",
                  "city_name_ar",
                  "city_name_en",
                  "region_name_ar",
                  "region_name_en",
                  "province_name",
                  "nearest_city_name_ar",
                  "nearest_city_name_en"
                  ]
        )
