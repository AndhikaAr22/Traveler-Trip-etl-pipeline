import pandas as pd
import json
import os
from connector.mysql_connection import MYSQL
from model.query import get_raw_data
from connector.postgre_connection import PostgreSQL

# Baca konfigurasi dari file JSON
with open('config.json', 'r') as cred:
    credential = json.load(cred)

path = os.getcwd() + '//' + 'data' + '//'
file_name = 'Travel details dataset.csv'
file_path = os.path.join(path, file_name)

def ingest_raw_data():
    mysql_aunt = MYSQL(credential['mysql_lake'])
    engine_mysql = mysql_aunt.connect()
    df = pd.read_csv(file_path)
    # print(df)
    df.to_sql(name='raw_data_trip', con=engine_mysql, if_exists='replace', index=False)
    print(df)
    engine_mysql.dispose()
    print('success ingest data to mysql')

