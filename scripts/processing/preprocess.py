import pandas as pd

df_general = pd.read_parquet('data/raw/df_general.gzip')

df_gen_train = df_general.dropna()
duration = (pd.to_datetime(df_gen_train['end_time'])-
                            pd.to_datetime(df_gen_train['start_time'])).apply(lambda i: 
                                                                              i.total_seconds() / 60.0)
df_gen_train['duration'] = duration

df_orders = df_gen_train.groupby('session_id')\
                        .nunique()\
                        .drop(labels=['start_time', 'end_time', 'gender',
                                      'duration', 'sequence_order'], axis=1)

df_duration = df_gen_train.groupby('session_id').mean()['duration']

df_final = df_orders.merge(df_duration, on='session_id')
df_final = df_final.merge(df_gen_train[['session_id','gender']]\
                          .drop_duplicates(), on='session_id')

df_final.to_parquet('data/preprocessed/df_final.gzip',
              compression='gzip')