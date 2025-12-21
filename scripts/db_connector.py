import duckdb
import pathlib


def get_connection(db_path="data/tiktok_dw.db"):
    try:
        path = pathlib.Path(db_path)

        path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(path))
        return con
    except Exception as e:
        print(f"database connection failed, {e}")
        return None
