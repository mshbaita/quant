from collectors.base_collector import BaseDataCollector
from pyspark.sql import SparkSession


class AdCSVCollector(BaseDataCollector):
    def __init__(self, spark: SparkSession, source_file_path: str):
        super().__init__(spark=spark, table_name="ad_csv_data")
        self.source_file_path = source_file_path

    def _extract(self):
        return self._spark.read.csv(self.source_file_path, header=True)
