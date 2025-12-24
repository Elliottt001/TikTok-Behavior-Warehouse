import duckdb
import pandas as pd
import os

# 设置 pandas 显示选项，防止列被省略
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def view_ads_tables():
    # 确定数据库路径
    if os.path.exists('data/tiktok_dw.db'):
        db_path = 'data/tiktok_dw.db'
    elif os.path.exists('../data/tiktok_dw.db'):
        db_path = '../data/tiktok_dw.db'
    else:
        print("Error: Could not find database file.")
        return

    print(f"Connecting to database at: {db_path}")
    con = duckdb.connect(db_path)

    # 需要检查的 ADS 表
    tables = ['ads_hot_videos', 'daily_active_users', 'retention_rate']

    for table in tables:
        print(f"\n{'='*30} {table} (Top 10) {'='*30}")
        try:
            # 检查表是否存在
            table_exists = con.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'").fetchone()[0]
            
            if table_exists:
                df = con.sql(f"SELECT * FROM {table} LIMIT 10").df()
                if df.empty:
                    print("Table is empty.")
                else:
                    print(df)
            else:
                print(f"Table '{table}' does not exist.")
                
        except Exception as e:
            print(f"Error querying {table}: {e}")

    con.close()

if __name__ == "__main__":
    view_ads_tables()
