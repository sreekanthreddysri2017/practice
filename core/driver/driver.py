from core.util.main import *

df = read_csv("../../resource/delta_log.csv", options)
df.show()

final_df = explode_df(df,operation_parameters_schema,operation_metrics_schema)
final_df.show()