from postgresql_setting import con
import pandas as pd

def read_sql(con, select_sql):
    with con.cursor() as cur:
        df = pd.read_sql(select_sql, con)
        return df
