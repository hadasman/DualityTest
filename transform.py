import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, when

from global_variables import TYPE_MAP


def transform(df: DataFrame, duality_schema: dict, column_names_mapping: dict,
              specific_column_mapping: dict, mapped_column: str, duality_query: dict):
    """casting, mapping"""
    table_schema = duality_schema['duality_schemas'][0]['table_schema']

    df = _cast_columns(df, column_names_mapping, table_schema)
    df = _make_mapping_replacements(df, specific_column_mapping, mapped_column, duality_query)
    return df


def _make_mapping_replacements(df: DataFrame, column_mapping: dict, mapped_column: str, duality_query: dict) -> DataFrame:
    if mapped_column in duality_query['retrieve_fields']:
        for code, country_name in column_mapping.items():
            df = df.withColumn(mapped_column,
                               when(col(mapped_column) == code, lit(country_name)).otherwise(col(mapped_column)))
    return df


def _cast_columns(df: DataFrame, column_mapping: dict, table_schema: dict) -> DataFrame:
    column_name_mapping = {key.lower(): value.lower() for key, value in column_mapping.items()}
    for column in df.columns:
        duality_name = column_name_mapping[column]
        data_type = table_schema[duality_name]
        df = df.withColumn(duality_name, F.col(column).cast(TYPE_MAP[data_type]))
        df = df.drop(column)
    return df
