from package.transform import data_dim_traveler, data_dim_transportation, data_dim_accommodation, data_dim_destination, data_dim_time, data_fact_trip
from connector.postgre_connection import PostgreSQL
import json
from model.query import insert_dim_traveler, insert_dim_accommodation, insert_dim_transportation, insert_dim_destination, insert_dim_time, insert_fact_trip

with open('config.json', 'r') as cred:
    credential = json.load(cred)


# dim_traveler
def insert_data_dim_traveler():
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')
    curr = conn.cursor()
    query = insert_dim_traveler()
    df_data = data_dim_traveler()
    for i, row in df_data.iterrows():
        curr.execute(query, list(row))
    conn.commit()

    conn.close()
    curr.close()
    return print('success insert data dim traveler')

# dim_transpotation
def insert_data_dim_transpotation():
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')
    curr = conn.cursor()
    query = insert_dim_transportation()
    df_data = data_dim_transportation()
    for i, row in df_data.iterrows():
        curr.execute(query, list(row))
    conn.commit()

    conn.close()
    curr.close()
    return print('success insert data dim transpotation')

# dim_transpotation
def insert_data_dim_accommodation():
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')
    curr = conn.cursor()
    query = insert_dim_accommodation()
    df_data = data_dim_accommodation()
    for i, row in df_data.iterrows():
        curr.execute(query, list(row))
    conn.commit()

    conn.close()
    curr.close()
    return print('success insert data dim accommodation')

# dim_time
def insert_data_dim_destination():
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')
    curr = conn.cursor()
    query = insert_dim_destination()
    df_data = data_dim_destination()
    for i, row in df_data.iterrows():
        curr.execute(query, list(row))
    conn.commit()

    conn.close()
    curr.close()
    return print('success insert data dim destination')


# dim_destination
def insert_data_dim_time():
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')
    curr = conn.cursor()
    query = insert_dim_time()
    df_data = data_dim_time()
    for i, row in df_data.iterrows():
        curr.execute(query, list(row))
    conn.commit()

    conn.close()
    curr.close()
    return print('sukses insert data time')





# fact_trip
def insert_data_fact_trip():
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')
    curr = conn.cursor()
    query = insert_fact_trip()
    df_data = data_fact_trip()
    for i, row in df_data.iterrows():
        curr.execute(query, list(row))
    conn.commit()

    conn.close()
    curr.close()
    return print('success insert data fact trip')

