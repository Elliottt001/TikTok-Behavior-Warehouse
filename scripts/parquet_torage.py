from . import db_connector


class ToParquet:

    def store_to_parquet(self, table_name, output_path, db_path):
        if 'behavior_logs' in table_name:
            sql = f'''
                COPY (SELECT *, CAST(event_time AS DATE) as event_date FROM {table_name})
                TO '{output_path}'
                (FORMAT PARQUET, PARTITION_BY (event_date), OVERWRITE 1);
            '''
        else:
            sql = f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET, OVERWRITE 1);"
            
        con = db_connector.get_connection(db_path)
        con.execute(sql)
