from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth


def save_output(df: DataFrame, output_path: str, partition_column: str = None, save_mode: str = "overwrite"):
    """partition"""
    # TODO: use id
    if partition_column:
        _save_by_datetime_partition(df, partition_column, output_path, save_mode)
    else:
        df.write.mode(save_mode).json(output_path)


def _save_by_datetime_partition(df: DataFrame, partition_column: str, output_path: str, save_mode: str):
    df = df.withColumn("year", year(col(partition_column))) \
        .withColumn("month", month(col(partition_column))) \
        .withColumn("day", dayofmonth(col(partition_column)))
    df.write.partitionBy("year", "month", "day").mode(save_mode).json(output_path)
