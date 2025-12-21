import duckdb
import pathlib

con = duckdb.connect('../data/tiktok_dw.db')

df = con.sql("SHOW TABLES").df()

df2 = con.sql("SELECT * FROM  dwd_behavior_logs").df()
df3 = con.sql("SELECT * FROM  ods_behavior_logs").df()

print(df3)
print(df2)

con.close()
