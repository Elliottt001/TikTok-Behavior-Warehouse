from scripts.data_gen import DataGenerator
from scripts.etl_job import SQLRunner
from scripts.parquet_torage import ToParquet
import scripts.db_connector
import pathlib


def run_pipeline():
    # 生成数据
    user_header_list = ['user_id', 'nickname', 'age', 'ip', 'fans_num',
                        'likes_num', 'phone_type', 'register_date']
    video_header_list = ['user_id', 'video_id', 'video_title', 'tags',
                         'duration', 'upload_time']
    
    DataGenerator.parallel_generate(total_csv_record_count=5000000,
                                    total_jsonl_record_count=30000000,
                                    user_csv_headers=user_header_list,
                                    video_csv_headers=video_header_list)

    # ODS and DWD 层
    db_path = '../data/tiktok_dw.db'
    sql_dir = '../sql/'

    runner = SQLRunner(db_path)  # 实例化 Runner

    ods_sql_path = sql_dir + 'ods_load.sql'
    runner.execute_sql(ods_sql_path)

    dwd_sql_path = sql_dir + 'dwd_cleansing.sql'
    runner.execute_sql(dwd_sql_path)

    # 转存为 parquet 格式
    ToParquet.store_to_parquet('')



if __name__ == "__main__":
    run_pipeline()