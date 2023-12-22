from core.util.main import *

df = read_csv("../../resource/delta_log.csv", options)
df.show()

final_df = explode_df(df,"id_period_end_date","id_scenario_version")
final_df.show(truncate=False)