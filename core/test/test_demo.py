import unittest

from core.util.main import *
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

class MyTestCase(unittest.TestCase):

    def test_explode(self):
        schema = StructType([
            StructField("version", IntegerType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("numFiles", StringType(), nullable=True),
            StructField("id_period_end_date", StringType(), nullable=False),
            StructField("id_scenario_version", StringType(), nullable=False),
            StructField("end_date_data_format", DateType(), nullable=True),
        ])

        data = [
            (129, datetime(2023, 12, 15, 20, 3, 28), "1", "20231231", "0", datetime(2023, 12, 31)),
            (128, datetime(2023, 12, 15, 20, 0, 28), "1", "20231231", "0", datetime(2023, 12, 31)),
            (127, datetime(2023, 12, 15, 16, 42, 6), "3", "20231130", "0", datetime(2023, 11, 30)),
            (127, datetime(2023, 12, 15, 16, 42, 6), "3", "20231031", "0", datetime(2023, 10, 31)),
        ]

        # Create the DataFrame
        expected_df = spark.createDataFrame(data, schema=schema)

        actual_df = explode_df(df,operation_parameters_schema,operation_metrics_schema)

        df_actual=actual_df.limit(4)

        self.assertEqual(df_actual.show(),expected_df.show())  # add assertion here


if __name__ == '__main__':
    unittest.main()
