from pyspark.sql import SparkSession, DataFrame

from duality_operator import Operator
from global_variables import PARTITION_COLUMN


def ingest(spark: SparkSession, connection_string: str, duality_query: dict, column_mapping: dict) -> DataFrame:
    """
    Ingests data from a specified connection string into a Spark DataFrame. Supported formats: CSV, JSON
    Args:
        spark: the spark session
        connection_string (str): The path to the data source. This can be a local file path or a distributed file system
                                 path (e.g., HDFS, S3) accessible by Spark. Example S3:
                                    s3a://your-s3-bucket-name/path/to/your/data/file.csv
    """
    reverse_mapping = {value: key for key, value in column_mapping['column_names'].items()}
    if connection_string.endswith('.csv'):
        df = spark.read.csv(connection_string, header=True, inferSchema=True)
    elif connection_string.endswith('.jsonl'):
        df = spark.read.json(connection_string)
    elif connection_string.endswith('.parquet'):
        df = spark.read.parquet(connection_string)
    else:
        raise ValueError('Only CSV and JSON file formats are currently supported!')

    df = _filter_rows(df, duality_query, reverse_mapping)

    # Filter columns last since row filtering might use filtered column
    df = _filter_columns(df, reverse_mapping, duality_query)

    return df


def _filter_columns(df: DataFrame, reverse_mapping: dict, duality_query: dict):
    original_name_select_fields = set(duality_query['retrieve_fields'] + [PARTITION_COLUMN])
    select_fields = [reverse_mapping[field].lower() for field in original_name_select_fields]
    return df.select(select_fields)


def _filter_rows(df: DataFrame, duality_query: dict, reverse_mapping: dict):
    for predicate in duality_query['predicates']:
        current_operator = Operator(predicate, reverse_mapping)
        df = current_operator.transform(df)
    return df
