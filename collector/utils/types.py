from typing import Dict

from pyspark.sql import (
    SparkSession,
    functions as F
)

import json


def infer_json_schema(spark: SparkSession, example_json: Dict):
    return (spark.
            range(1)
            .select(F.schema_of_json(F.lit(json.dumps(example_json))).alias("schema"))
            .collect()[0]
            .schema)
