from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create a Spark session
spark = SparkSession.builder.appName("delta").getOrCreate()
path = "../../resource/delta_log.csv"
options = {'header': True, 'delimiter': ',', 'escape': '"'}

def read_csv(path, options):
    return spark.read.options(**options).csv(path)



def explode_df(df,id_period_end_date_col,id_scenario_version_col):
    df1 = df.withColumn("operationParameters", from_json(col("operationParameters"), MapType(StringType(), StringType()))) \
          .withColumn("operationMetrics", from_json(col("operationMetrics"), MapType(StringType(), StringType())))
    filtered_data_df = df1.select("version", "timestamp", "operationParameters.predicate", "operationMetrics.numFiles")
    splited_df = filtered_data_df.select("version", "timestamp", explode(split("predicate", " OR ")).alias("predicate"), "numFiles")
    explode_df = splited_df.withColumn(id_period_end_date_col, split(splited_df["predicate"], "AND")[0]) \
                          .withColumn(id_scenario_version_col, split(splited_df["predicate"], "AND")[1])
    final_df = explode_df.withColumn(id_period_end_date_col, regexp_extract(explode_df["predicate"], f"{id_period_end_date_col}='(\\d+)'", 1)) \
                         .withColumn(id_scenario_version_col, regexp_extract(explode_df["predicate"], f"{id_scenario_version_col}='(\\d+)'", 1))

    final_df = final_df.drop("predicate")
    return final_df