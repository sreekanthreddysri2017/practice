from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split, explode,regexp_extract
from pyspark.sql.types import *

from pyspark.sql.types import MapType, StringType, StructType, StructField
from pyspark.sql.functions import from_json

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

def from_json_df(df,operation_parameters_schema,operation_metrics_schema):
    df = df.withColumn("operationParameters", from_json(col("operationParameters"), operation_parameters_schema)) \
        .withColumn("operationMetrics", from_json(col("operationMetrics"), operation_metrics_schema))
    return df

def explode_df(df,operation_parameters_schema,operation_metrics_schema):
    df = from_json_df(df,operation_parameters_schema,operation_metrics_schema)
    filtered_data_df = df.select("version", "timestamp", "operationParameters.predicate", "operationMetrics.numFiles")
    splited_df = filtered_data_df.select("version","timestamp",explode(split("predicate", " OR ")).alias("predicate"),"numFiles")
    explode_df = splited_df.withColumn("id_period_end_date", split(splited_df["predicate"], "AND")[0]) \
                     .withColumn("id_scenario_version", split(splited_df["predicate"], "AND")[1])
    final_df = explode_df.withColumn("id_period_end_date", regexp_extract(explode_df["predicate"], "id_period_end_date='(\\d+)'", 1)) \
       .withColumn("id_scenario_version", regexp_extract(explode_df["predicate"], "id_scenario_version='(\\d+)'", 1))

    final_df = final_df.drop("predicate")
    return final_df

df = read_csv("resource/delta_log.csv", options)

final_df = explode_df(df,operation_parameters_schema,operation_metrics_schema)

final_df.show()

