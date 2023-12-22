from core.util.main import *

df = read_csv(path, options)
df.show()

final_df = explode_df(df,id_columns)
final_df.show(truncate=False)