from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType

TRANSACTIONS_FILENAME = 'inputs/mock_transactions.csv'

TYPE_MAP = {
    'string': StringType(),
    'integer': IntegerType(),
    'double': DoubleType(),
    'float': DoubleType(),
    'datetime': TimestampType(),
    'timestamp': TimestampType(),
    'date': TimestampType()
}
PARTITION_COLUMN = 'time_of_transaction'
