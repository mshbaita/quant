# Column translations
COLUMN_TRANSLATIONS = {
    'رقم الإعلان': 'Ad Number',
    'رقم المستخدم': 'User Number',
    'وقت الإنشاء': 'Creation Time',
    'وقت اخ تحديث': 'Last Update Time',
    'العمر اقل من': 'Age Less Than',
    'عدد الشقق': 'Number of Apartments',
    'عدد غرف النوم': 'Number of Bedrooms',
    'الدور': 'Floor Number',
    'مغلق': 'Is Available',
    'عدد المطبخ': 'Number of Kitchens',
    'سكني أو تجاري': 'Residential or Commercial',
    'نوع العقار': 'Type of Property',
    'غرفة سائق': 'Driver\'s Room',
    'دوبلكس': 'Duplex',
    'عوائل أم عزاب': 'Families or Singles',
    'مؤثثة': 'Furnished',
    'السعر': 'Price',
    'عدد الصالات': 'Number of Halls',
    'غرفة خادمة': 'Maid\'s Room',
    'مدة الإيجار': 'Rental Term',
    'سعر المتر': 'Price per Meter',
    'نوع المعلن': 'Advertiser Type',
    'مسبح': 'Swimming Pool',
    'مدفوع': 'Paid',
    'عدد الغرف': 'Number of Rooms',
    'المساحة': 'Space',
    'اتجاه الشارع': 'Street Direction',
    'عرض الشارع': 'Street Width',
    'عدد دورات المياة': 'Number of Bathrooms',
    'للبيع أم الإيجار': 'For Sale or Rent',
    'district_name_ar': 'District Name AR',
    'district_name_en': 'District Name EN'
}

# CSV table name
CSV_TABLE_NAME = "datalake_csv" # delta table

# JSON table name
JSON_TABLE_NAME = "datalake_json" # delta table

# warehouse path
PARQUET_PATH = ""

# S3 warehouse path
S3_PATH = ""

# Merging Criteria will be on Id, because no date is available
PRIMARY_KEY = "Id"




