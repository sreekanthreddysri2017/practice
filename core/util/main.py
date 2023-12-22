
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create a Spark session
spark = SparkSession.builder.appName("delta").getOrCreate()
path = "../../resource/delta_log.csv"
options = {'header': True, 'delimiter': ',', 'escape': '"'}

def read_csv(path, options):
    return spark.read.options(**options).csv(path)



def explode_df(df, id_columns):
    # Ensure at least two columns are provided
    if len(id_columns) < 2:
        raise ValueError("Please provide at least two column names.")

    df1 = df.withColumn("operationParameters",
                        from_json(col("operationParameters"), MapType(StringType(), StringType()))) \
        .withColumn("operationMetrics", from_json(col("operationMetrics"), MapType(StringType(), StringType())))

    filtered_data_df = df1.select("version", "timestamp", "operationParameters.predicate", "operationMetrics.numFiles")

    splited_df = filtered_data_df.select("version", "timestamp", explode(split("predicate", " OR ")).alias("predicate"),
                                         "numFiles")

    explode_df = splited_df
    for column in id_columns:
        explode_df = explode_df.withColumn(column, split(splited_df["predicate"], "AND")[id_columns.index(column)])

    final_df = explode_df
    for column in id_columns:
        final_df = final_df.withColumn(column, regexp_extract(explode_df["predicate"], f"{column}='(\\d+)'", 1))

    final_df = final_df.drop("predicate")

    return final_df


# Example usage with dynamic column names
id_columns = ["id_period_end_date", "id_scenario_version"]




