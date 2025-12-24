from scripts.data_gen import DataGenerator
from scripts.etl_job import SQLRunner
from scripts.parquet_torage import ToParquet
import pathlib
import time


def run_pipeline():
    # 生成数据
    user_header_list = ['user_id', 'nickname', 'age', 'ip', 'fans_num',
                        'likes_num', 'phone_type', 'register_date']
    video_header_list = ['user_id', 'video_id', 'video_title', 'tags',
                         'duration', 'upload_time']
    
    generator = DataGenerator()
    generator.parallel_generate(total_csv_record_count=5000000,
                                total_jsonl_record_count=30000000,
                                user_csv_headers=user_header_list,
                                video_csv_headers=video_header_list)

    # ODS and DWD 层
    db_path = pathlib.Path('data/tiktok_dw.db')
    sql_dir = pathlib.Path('sql/')

    runner = SQLRunner(db_path)  # 实例化 Runner

    ods_sql_path = sql_dir / 'ods_load.sql'
    runner.execute_sql(ods_sql_path)

    dwd_sql_path = sql_dir / 'dwd_cleansing.sql'
    runner.execute_sql(dwd_sql_path)

    # 转存为 parquet 格式
    parquet_storage = ToParquet()
    parquet_storage.store_to_parquet('dwd_users',
                                     'data/dwd_users_parquet.parquet',
                                     'data/tiktok_dw.db')
    parquet_storage.store_to_parquet('dwd_video',
                                     'data/dwd_videos_parquet.parquet',
                                     'data/tiktok_dw.db')
    parquet_storage.store_to_parquet('dwd_behavior_logs',
                                     'data/dwd_behavior_logs_parquet.parquet',
                                     'data/tiktok_dw.db')

    # ADS 层
    ads_sql_path = sql_dir / 'ads_report.sql'
    runner.execute_sql(ads_sql_path)


if __name__ == "__main__":
    start_time = time.perf_counter()

    run_pipeline()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"程序运行耗时: {elapsed_time:.4f} 秒")