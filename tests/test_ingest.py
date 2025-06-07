from unittest import TestCase

from pyspark.sql import SparkSession

from ingest import ingest


class TestIngest(TestCase):
    def setUp(self):
        # I could also patch spark
        self.spark = SparkSession.builder \
            .appName("DualityDataPipeline") \
            .getOrCreate()
        self.column_mapping = {
            "countries": {
                "1": "US",
                "2": "FR",
                "3": "DE",
                "4": "IN",
                "5": "GB"
            },
            "column_names": {
                "txn_id": "transaction_id",
                "cust_id": "customer_id",
                "city_name": "city",
                "country_code": "country",
                "Tx_time": "time_of_transaction"
            }
        }
        self.schema = {
            "duality_schemas": [
                {
                    "name": "transactions",
                    "table_schema": {
                        "transaction_id": "string",
                        "customer_id": "string",
                        "city": "string",
                        "country": "string",
                        "time_of_transaction": "datetime"
                    }
                }
            ]
        }

    def test_equal_query(self):
        query = {
            "id": "retrieve_mapping_postgres",
            "query_type": "RETRIEVE",
            "retrieve_fields": [
                "customer_id",
                "city",
                "time_of_transaction"
            ],
            "table": "transactions",
            "predicates": [
                {
                    "field_name": "country",
                    "operator": "eq",
                    "value": "1"
                }
            ]
        }

        expected_df = self.spark.createDataFrame(
            [{"cust_id": "C10", "city_name": "New York", "tx_time": "2023-01-15T10:00:00"}])
        actual_df = ingest(self.spark, 'mock_df.jsonl', query, self.column_mapping)

        self.compare_dataframes(expected_df, actual_df)

    def test_last_time_period_query(self):
        query = {
            "id": "retrieve_mapping_postgres",
            "query_type": "RETRIEVE",
            "retrieve_fields": [
                "customer_id",
                "city",
                "time_of_transaction"
            ],
            "table": "transactions",
            "predicates": [
                {
                    "field_name": "time_of_transaction",
                    "operator": "last_time_period",
                    "time_period_type": "year",
                    "value": 1
                }
            ]
        }

        expected_df = self.spark.createDataFrame(
            [{"cust_id": "C14", "city_name": "London", "tx_time": "2024-12-20T05:00:00"}])
        actual_df = ingest(self.spark, 'mock_df.jsonl', query, self.column_mapping)

        self.compare_dataframes(expected_df, actual_df)

    def compare_dataframes(self, df1, df2):
        df1_dict = [row.asDict() for row in df1.collect()]
        df2_dict = [row.asDict() for row in df2.collect()]
        self.assertEqual(df1_dict, df2_dict)
