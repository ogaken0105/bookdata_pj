def read_sql(con, select_sql):
    with con.cursor() as cur:
        df = pd.read_sql(select_sql, con)
        return df
