from pyspark.sql import SparkSession, DataFrame
from tables.base_table import BaseTable


class AdsTable(BaseTable):

    def __init__(self, spark: SparkSession, source_df: DataFrame):
        super().__init__(
            spark,
            table_name="ad_detail",
            source_df=source_df,
            cols=["id",
                  "district_id",
                  "property_type_id",
                  "district_name_en",
                  "property_age_less_than",
                  "number_of_apartments",
                  "number_of_bedrooms",
                  "floor",
                  "number_of_kitchens",
                  "is_closed",
                  "residential_or_commercial",
                  "driver_room",
                  "is_duplex",
                  "families_or_singles",
                  "is_furnished",
                  "halls_Num",
                  "maid_room",
                  "price_per_meter",
                  "advertiser_type",
                  "has_swimming_pool",
                  "is_paid",
                  "price",
                  "rental_term",
                  "rooms_num",
                  "space",
                  "street_direction",
                  "street_width_range",
                  "for_sale_or_rent",
                  "toilets_num",
                  "latitude",
                  "longitude",
                  "property_age_range",
                  "created_at",
                  "updated_at"
                  ],
            partitioning_cols=["district_name_en"],
        )
