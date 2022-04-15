# IMPORT MODULE
import json
import pandas as pd
import numpy as np

# IMPORT DBCONNECTION
from dbconnection.mysql_connect import MySQL

# IMPORT SQL
#from sql.query import create_table_dim, create_table_fact

with open ("credential.json", "r") as cred:
       credential = json.load(cred)

def insert_raw_data():
  mysql_auth = MySQL(credential['mysql_lake'])
  engine, engine_conn = mysql_auth.connect()

  with open ('./data/data_covid.json', "r") as data:
    data = json.load(data)

  df = pd.DataFrame(data['data']['content'])

  df.columns = [x.lower() for x in df.columns.to_list()]
  df.to_sql(name='desi_raw_covid_data', con=engine, if_exists="replace", index=False)
  engine.dispose()

  if __name__ == '__main__':
    insert_raw_data()
