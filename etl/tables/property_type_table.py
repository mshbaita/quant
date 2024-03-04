from pyspark.sql import SparkSession, DataFrame
from tables.base_table import BaseTable
from utils.config import config


class PropertyTable(BaseTable):

    def __init__(self, spark: SparkSession, source_df: DataFrame):
        super().__init__(
            spark,
            table_name="property_type",
            source_df=source_df,
            cols=["property_type_id",
                  "property_type_ar",
                  "property_type",
                  ],
            id_col="property_type_id"

        )
