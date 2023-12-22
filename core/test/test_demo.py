import unittest
from core.util.main import *

class MyTestCase(unittest.TestCase):

    def test_explode_df(self):
        # Create a Spark session
        spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

        # Define the schema
        schema = StructType([
            StructField("version", StringType(), nullable=True),
            StructField("timestamp", StringType(), nullable=True),
            StructField("numFiles", StringType(), nullable=True),
            StructField("id_period_end_date", StringType(), nullable=False),
            StructField("id_scenario_version", StringType(), nullable=False),

        ])

        # Input Csv data
        csv_path = "../../resource/delta_log.csv"  # Replace with your actual path
        df = read_csv(csv_path, options = {'header': True, 'delimiter': ',', 'escape': '"'})

        # Create the expected DataFrame
        data = [
            (129, '2023-12-15T14:33:28.000+0000', "1", "20231231", "0"),
            (128, '2023-12-15T14:30:28.000+0000', "1", "20231231", "0"),
            (127, '2023-12-15T11:12:06.000+0000', "3", "20231130", "0"),
            (127, '2023-12-15T11:12:06.000+0000', "3", "20231031", "0"),
        ]
        expected_df = spark.createDataFrame(data, schema=schema)


        # Call the explode_df function with proper schemas
        actual_df = explode_df(df,"id_period_end_date","id_scenario_version")


        # Limit the actual DataFrame to match the expected DataFrame
        df_actual = actual_df.limit(4)

        # Assert the DataFrames are equal
        self.assertEqual(df_actual.collect(), expected_df.collect())  # add assertion here
if __name__ == '__main__':
    unittest.main()
