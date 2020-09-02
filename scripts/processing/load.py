import mariadb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import os.path as path

try:
    conn = mariadb.connect(
        user="guest",
        password="relational",
        host="relational.fit.cvut.cz",
        port=3306,
        database="ftp"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cur = conn.cursor()

SQL_Query_session = pd.read_sql('''select * from session''', conn)
SQL_Query_product = pd.read_sql('''select * from product''', conn)

df_session = pd.DataFrame(SQL_Query_session)
df_product = pd.DataFrame(SQL_Query_product)

df_session.to_parquet('data/external/df_session.gzip',
              compression='gzip')
df_product.to_parquet('data/external/df_product.gzip',
              compression='gzip')

df_general = df_product.merge(df_session, on='session_id', validate='many_to_one')
df_general.to_parquet('data/interim/df_general.gzip',
              compression='gzip')