from datetime import datetime
from unittest import TestCase

from pyspark.sql import SparkSession

from transform import transform


class TestIngest(TestCase):
    def setUp(self):
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
                        "transaction_id": "integer",
                        "customer_id": "string",
                        "city": "string",
                        "country": "string",
                        "time_of_transaction": "datetime"
                    }
                }
            ]
        }

        self.df = self.spark.createDataFrame([{"txn_id": "1", "cust_id": "C10", "city_name": "New York",
                                               "country_code": "1", "tx_time": "2023-01-15T10:00:00"},
                                              {"txn_id": "2", "cust_id": "C11", "city_name": "Paris",
                                               "country_code": "2", "tx_time": "2023-01-15T11:30:00"}
                                              ])

        self.query = {"retrieve_fields": [
            "customer_id",
            "city",
            "time_of_transaction",
            "country"
        ]}

    def test_transform(self):
        expected_df = self.spark.createDataFrame([{"transaction_id": 1, "customer_id": "C10", "city": "New York",
                                                   "country": "US", "time_of_transaction": datetime.strptime("2023-01-15T10:00:00", "%Y-%m-%dT%H:%M:%S")},
                                                  {"transaction_id": 2, "customer_id": "C11", "city": "Paris",
                                                   "country": "FR", "time_of_transaction": datetime.strptime("2023-01-15T11:30:00", "%Y-%m-%dT%H:%M:%S")}
                                                  ])
        actual_df = transform(self.df, self.schema, self.column_mapping['column_names'],
                              self.column_mapping['countries'],
                              'country', self.query)

        self.compare_dataframes(expected_df, actual_df)

    def compare_dataframes(self, df1, df2):
        df1_dict = [row.asDict() for row in df1.collect()]
        df2_dict = [row.asDict() for row in df2.collect()]
        self.assertEqual(df1_dict, df2_dict)
