from pathlib import Path
import db_connector


def execute_sql(db_path, sql_file):

    sql = Path(sql_file).read_text(encoding="UTF-8")
    con = db_connector.get_connection(db_path)

    con.execute(sql)


if __name__ == "__main__":

    db_path = '../data/tiktok_dw.db'
    sql_dir = '../sql/'

    # ods_sql_path = sql_dir + 'ods_load.sql'
    # execute_sql(db_path, ods_sql_path)

    dwd_sql_path = sql_dir + 'dwd_cleansing.sql'

    execute_sql(db_path, dwd_sql_path)
