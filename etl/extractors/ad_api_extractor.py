from extractors.base_extractor import BaseExtractor
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utils.udfs import struct_get

COLUMNS_MAP = {
    "data._id",
    "data.area",
    "data.city._id as city_id",
    "data.city.name_ar as city_name_ar",
    "data.city.name_en as city_name_en",
    "data.code",
    "data.createdAt as created_at",
    "data.createdBy._id as created_by_id",
    "data.createdBy.agentType as createdBy_agent_type",
    "data.district._id as district_id",
    "data.district.name_ar as district_name_ar",
    "data.district.name_en as district_name_en",
    "data.id",
    "data.isCorrectLocation as is_correct_location",
    "data.isPromoted as is_promoted",
    "data.location.type as location_type",
    "data.location.value.coordinates[0] as latitude",
    "data.location.value.coordinates[1] as longitude",
    "data.media.main.type as media_main_type",
    "data.media.main.url as media_main_url",
    "data.paymentMethod as payment_method",
    "data.price",
    "data.priceType as price_type",
    "data.propertyPurpose",
    "data.propertyType._id as property_type_id",
    "data.propertyType.cardIcon as property_type_card_icon",
    "data.propertyType.category._id as category_id",
    "data.propertyType.category.cardIcon as category_card_icon",
    "data.propertyType.category.filterIcon as category_filter_icon",
    "data.propertyType.category.name_ar as category_name_ar",
    "data.propertyType.category.name_en as category_name_en",
    "data.propertyType.filterIcon as property_type_filter_icon",
    "data.propertyType.propertyType as property_type",
    "data.propertyType.propertyType_ar as property_type_ar",
    "data.published",
    "data.purpose",
    "rent_type",
    "data.refreshed.at as refreshed_at",
    "data.relatedQuestions.hallsNum as halls_num",
    "data.relatedQuestions.isAnnex as is_annex",
    "data.relatedQuestions.propertyAgeRange as property_age_range",
    "data.relatedQuestions.roomsNum as rooms_num",
    "data.relatedQuestions.streetWidthRange as street_width_range",
    "data.relatedQuestions.surroundingStreetsNum as surrounding_streets_num",
    "data.relatedQuestions.toiletsNum as toilets_num",
    "data.status",
    "data.title",
    "data.updatedAt as updated_at",
    "ingestion_time"
}


class AdAPIExtractor(BaseExtractor):
    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, table_name="ad_api_data")

    def _process(self, dataframe: DataFrame):
        # First, explode the 'data' array to flatten it
        columns_map_list = list(COLUMNS_MAP)
        return (
            dataframe
            .withColumn("data", F.explode(F.col("data")))
            .withColumn("rent_type", struct_get(F.col("data"), F.lit("data.rentType"),F.lit(None)))
            .withColumn("data_source", F.lit("ad_api_data"))
            .selectExpr(*columns_map_list)
        )
