
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create a Spark session
spark = SparkSession.builder.appName("delta").getOrCreate()
path = "../../resource/delta_log.csv"
options = {'header': True, 'delimiter': ',', 'escape': '"'}

def read_csv(path, options):
    return spark.read.options(**options).csv(path)

def from_json_df(df):
    df = df.withColumn("operationParameters", from_json(col("operationParameters"), MapType(StringType(), StringType()))) \
          .withColumn("operationMetrics", from_json(col("operationMetrics"), MapType(StringType(), StringType())))
    return df

def explode_df(df):
    df = from_json_df(df)
    filtered_data_df = df.select("version", "timestamp", "operationParameters.predicate", "operationMetrics.numFiles")
    splited_df = filtered_data_df.select("version", "timestamp", explode(split("predicate", " OR ")).alias("predicate"), "numFiles")
    explode_df = splited_df.withColumn("id_period_end_date", split(splited_df["predicate"], "AND")[0]) \
                          .withColumn("id_scenario_version", split(splited_df["predicate"], "AND")[1])
    final_df = explode_df.withColumn("id_period_end_date", regexp_extract(explode_df["predicate"], "id_period_end_date='(\\d+)'", 1)) \
                         .withColumn("id_scenario_version", regexp_extract(explode_df["predicate"], "id_scenario_version='(\\d+)'", 1))

    final_df = final_df.drop("predicate")
    return final_df



