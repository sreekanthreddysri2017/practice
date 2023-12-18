from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("delta") \
    .getOrCreate()

options = {'header': True,
           'inferschema': True,
           'delimiter': ',',
           'escape': '"'}


def read_csv(path, options):
    return spark.read.options(**options).csv(path)


df = read_csv("../../resource/delta_log.csv", options)
df.show()


operation_parameters_schema = StructType([
    StructField("mode", StringType(), True),
    StructField("partitionBy", StringType(), True),
    StructField("predicate", StringType(), True)
])

operation_metrics_schema = StructType([
    StructField("numFiles", StringType(), True),
    StructField("numOutputRows", StringType(), True),
    StructField("numOutputBytes", StringType(), True)
])


df2 = df.withColumn("operationParameters", from_json(col("operationParameters"), operation_parameters_schema)) \
        .withColumn("operationMetrics", from_json(col("operationMetrics"), operation_metrics_schema))


df2.display()

final_data_df = df2.select("version", "timestamp", "operationParameters.predicate", "operationMetrics.numFiles")
final_data_df.display()
