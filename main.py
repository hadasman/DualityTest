import json

from pyspark.sql import SparkSession

from global_variables import TRANSACTIONS_FILENAME, PARTITION_COLUMN
from ingest import ingest
from save_output import save_output
from transform import transform


def load_config_as_json(filename: str):
    with open(filename, 'r', encoding='utf-8') as f:
        json_content = f.read()
    return json.loads(json_content)


if __name__ == '__main__':
    duality_query = load_config_as_json('inputs/duality_query.json')
    duality_schema = load_config_as_json('inputs/duality_schema.json')
    country_code_mapping = load_config_as_json('inputs/country_code_mapping.json')

    spark = SparkSession.builder \
        .appName("DualityDataPipeline") \
        .getOrCreate()

    ingested_df = ingest(spark, TRANSACTIONS_FILENAME, duality_query, country_code_mapping)
    transformed_df = transform(ingested_df, duality_schema, country_code_mapping['column_names'],
                               country_code_mapping['countries'], 'country', duality_query)
    save_output(transformed_df, 'output', PARTITION_COLUMN)

    spark.stop()
