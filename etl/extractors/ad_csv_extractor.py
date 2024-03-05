from extractors.base_extractor import BaseExtractor
from utils.config import config
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

COLUMNS_MAP = {
    'رقم الإعلان': 'id',
    'رقم المستخدم': 'created_by_id',
    'وقت الإنشاء': 'created_at',
    'وقت اخ تحديث': 'updated_at',
    'العمر اقل من': 'property_age_less_than',
    'عدد الشقق': 'number_of_apartments',
    'عدد غرف النوم': 'number_of_bedrooms',
    'الدور': 'floor',
    'عدد المطبخ': 'number_of_kitchens',
    'مغلق': 'is_closed',
    'سكني أو تجاري': 'residential_or_commercial',
    'نوع العقار': 'property_type',
    'غرفة سائق': 'driver_room',
    'دوبلكس': 'is_duplex',
    'عوائل أم عزاب': 'families_or_singles',
    'مؤثثة': 'is_furnished',
    'السعر': 'price',
    'عدد الصالات': 'halls_Num',
    'غرفة خادمة': 'maid_room',
    'مدة الإيجار': 'rent_type',
    'سعر المتر': 'price_per_meter',
    'نوع المعلن': 'advertiser_type',
    'مسبح': 'has_swimming_pool',
    'مدفوع': 'is_paid',
    'عدد الغرف': 'rooms_num',
    'المساحة': 'space',
    'اتجاه الشارع': 'street_direction',
    'عرض الشارع': 'street_width_range',
    'للبيع أم الإيجار': 'purpose',
    'عدد دورات المياة': 'toilets_num',
    'Latitude': 'latitude',
    'Longitude': 'longitude',
    'region_name_ar': 'region_name_ar',
    'region_name_en': 'region_name_en',
    'province_name': 'province_name',
    'nearest_city_name_ar': 'nearest_city_name_ar',
    'nearest_city_name_en': 'nearest_city_name_en',
    'district_name_ar': 'district_name_ar',
    'district_name_en': 'district_name_en',
    'Zip Code No': 'zip_code',
    'ingestion_time': 'ingestion_time'
}


class AdCSVExtractor(BaseExtractor):
    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, table_name="ad_csv_data")

    def __translate_columns(self, dataframe: DataFrame) -> DataFrame:
        """Apply column name transformations based on mappings."""
        for original, translated in COLUMNS_MAP.items():
            dataframe = dataframe.withColumnRenamed(original, translated)
        return dataframe

    def _process(self, dataframe: DataFrame):
        dataframe = self.__translate_columns(dataframe)
        dataframe = dataframe.dropna(subset=['price', 'space'])
        # Convert data types - Example: Ensuring 'Price' and 'Space' are of type float
        dataframe = (dataframe
                     .withColumn('price', F.col('price').cast('float'))
                     .withColumn('space', F.col('space').cast('float'))
                     .withColumn("data_source", F.lit("ad_csv_data"))
                     )



        return dataframe
