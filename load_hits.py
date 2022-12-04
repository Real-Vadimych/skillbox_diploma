import pandas as pd
from sqlalchemy import create_engine

df = pd.read_pickle('data/ga_hits-prep.pkl')

engine = create_engine(
    'postgresql://datagrip:datagrip@localhost:5432/diploma_db')

df.to_sql('hits',
          engine,
          schema='dbo',
          if_exists='replace',
          index=False,
          chunksize=500000)
