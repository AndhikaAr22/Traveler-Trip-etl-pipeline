import pandas as pd
import numpy as np
from sqlalchemy import  text
import json
import os
from connector.mysql_connection import MYSQL
from connector.postgre_connection import PostgreSQL
from model.query import get_raw_data, create_dim_table, create_fact_table_trip

# Baca konfigurasi dari file JSON
with open('config.json', 'r') as cred:
    credential = json.load(cred)

output_directory = '/home/andhika/project_trip_bp/data'

def get_data():
    mysql_aunt = MYSQL(credential['mysql_lake'])
    engine_mysql = mysql_aunt.connect()
    query = get_raw_data()
    df_data = pd.read_sql(query, con=engine_mysql)
    df_data['Traveler nationality'].replace({'USA':'American', 'Canada':'Canadian', 'Korean':'South Korean', 'United Kingdom':'British', 'UK':'British',
                                            'China':'Chinese', 'German':'Germany','Spain':'Spanish', 'Japan':'Japanese', 'Italy':'Italian', 'South Korea':'South Korean', 'United Arab Emirates':'Emirati'}, inplace=True)
    df_data['Transportation type'].replace({"Flight":"Plane", "Airplane": "Plane"}, inplace=True)
    
    def format_currency(amount):
        if amount is None:
            return None  
        amount = str(amount)  # Konversi ke string 
        amount = amount.replace('$', '')  # Hapus tanda dolar yang mungkin sudah ada
        amount = amount.replace(',', '')  # Hapus koma yang mungkin sudah ada
        amount = ''.join(filter(str.isdigit, amount))  # Hanya ambil karakter angka
        amount = '${:,}'.format(int(amount))  # Format dengan tanda dolar dan pemisah ribuan koma
        return amount
    df_data['Accommodation cost'] = df_data['Accommodation cost'].apply(format_currency)
    df_data['Transportation cost'] = df_data['Transportation cost'].apply(format_currency)

    df_data[['city','country']] = df_data['Destination'].str.split(', ',expand=True)
    df_data.drop(columns=['Destination'], inplace=True)
    df_data['city'].replace({'Japan':'Tokyo','Thailand':'Bangkok','Australia':'Sydney', 'Brazil':'Rio de Janeiro','Greece':'Athens','Mexico':'Cancun','Italy':'Rome',
                                        'Spain':'Barcelona','Canada':'Vancouver','Hawaii':'Honolulu','Egypt':'Cairo','France':'Paris', 'New York City':'New York'}, inplace=True)
        # Mengganti nilai yang tidak tepat di kolom 'country'
    df_data['country'].replace({'AUS':'Australia','Aus':'Australia','Thai':'Thailand','SA':'South Aflica'}, inplace=True)

    # Isi nilai kosong di kolom 'country' berdasarkan kota yang ada
    city_to_country_mapping = {
        'Bali':'Indonesia',
        'Paris':'France',
        'London':'UK',
        'New York':'USA',
        'Tokyo': 'Japan',
        'Bangkok': 'Thailand',
        'Sydney': 'Australia',
        'Rio de Janeiro': 'Brazil',
        'Athens': 'Greece',
        'Cancun': 'Mexico',
        'Rome': 'Italy',
        'Barcelona': 'Spain',
        'Vancouver': 'Canada',
        'Seoul':'South Korea',
        'Phuket':'Thailand',
        'Dubai':'United Arab Emirates',
        'Phnom Penh':'Cambodia',
        'Santorini':'Greece',
        'Amsterdam':'Netherlands',
        'Cairo':'Egypt',
        'Cape Town':'South Africa',
        'Honolulu':'Hawaii'


    }
    df_data['country'] = df_data.apply(
        lambda row: city_to_country_mapping.get(row['city'], row['country']), axis=1
    )
    df_data['Start date'] = pd.to_datetime(df_data['Start date'])
    df_data['End date'] = pd.to_datetime(df_data['End date'])
    output_file = os.path.join(output_directory, 'df_data.csv')

    df_data.to_csv(output_file, index=False)
    engine_mysql.dispose()
    return df_data

def data_dim_traveler():
    column_end = ['traveler_id' ,'name', 'age', 'gender', 'nationality']
    column_start = ['Traveler name', 'Traveler age', 'Traveler gender', 'Traveler nationality']
    df = get_data()
    df_dim_traveler = df[column_start]
    df_dim_traveler = df_dim_traveler.rename(columns={'Traveler name':'name', 'Traveler age':'age', 'Traveler gender':'gender', 'Traveler nationality':'nationality'})
    


    df_dim_traveler.dropna(inplace=True)
    
    
    # mengubah tipe data kolom age menjadi int
    df_dim_traveler['age'] = df_dim_traveler['age'].astype(int)
    # Menambahkan kolom 'traveler_id'
    df_dim_traveler['traveler_id'] = 'TR_' + (df_dim_traveler.groupby(['name', 'age', 'gender', 'nationality']).ngroup() + 1).astype(str)

    df_dim_traveler = df_dim_traveler[column_end]
    df_dim_traveler = df_dim_traveler.drop_duplicates()


    df_dim_traveler['traveler_number'] = df_dim_traveler['traveler_id'].str.extract(r'(\d+)').astype(int)
    df_dim_traveler = df_dim_traveler.sort_values('traveler_number', ascending=True)
    df_dim_traveler = df_dim_traveler.drop(columns=['traveler_number'])
    print(df_dim_traveler.columns)
    print(df_dim_traveler)
    print(df_dim_traveler.dtypes)

    output_file = os.path.join(output_directory, 'data_dim_traveler.csv')

    df_dim_traveler.to_csv(output_file, index=False)
   
    
    return df_dim_traveler

def data_dim_accommodation():
    column_end = ['Accommodation_id', 'type', 'cost']
    column_start = ['Accommodation type', 'Accommodation cost']
    df = get_data()
    df_dim_accommodation = df[column_start]
    df_dim_accommodation = df_dim_accommodation.rename(columns={'Accommodation type':'type', 'Accommodation cost':'cost'})

    # Menghapus baris dengan nilai kosong
    df_dim_accommodation.dropna(subset=['type', 'cost'], inplace=True)
    


    
    df_dim_accommodation['Accommodation_id'] = 'AC_' + (df_dim_accommodation.groupby(['type', 'cost']).ngroup() + 1).astype(str)
    df_dim_accommodation = df_dim_accommodation[column_end]
    df_dim_accommodation = df_dim_accommodation.drop_duplicates()

    
    # Mendapatkan nomor urutan dari Accommodation_id
    df_dim_accommodation['Accommodation_number'] = df_dim_accommodation['Accommodation_id'].str.extract(r'(\d+)').astype(int)
    
    # Mengurutkan berdasarkan nomor urutan
    df_dim_accommodation = df_dim_accommodation.sort_values('Accommodation_number', ascending=True)
    
    # Menghapus kolom sementara yang digunakan untuk menghitung nomor urutan
    df_dim_accommodation = df_dim_accommodation.drop(columns=['Accommodation_number'])
    
    output_file = os.path.join(output_directory, 'data_dim_accommodation.csv')

    df_dim_accommodation.to_csv(output_file, index=False)


    return df_dim_accommodation

def data_dim_transportation():
    column_end = ['Transportation_id', 'type', 'cost']
    column_start = ['Transportation type', 'Transportation cost']
    df = get_data()
    df_dim_trasportation = df[column_start]
    df_dim_trasportation = df_dim_trasportation.rename(columns={'Transportation type':'type', 'Transportation cost':'cost'})
    print(df_dim_trasportation)
    df_dim_trasportation.dropna(subset=['type', 'cost'], inplace=True)


    df_dim_trasportation['Transportation_id'] = 'TRAN_' + (df_dim_trasportation.groupby(['type', 'cost']).ngroup() + 1).astype(str)

    df_dim_trasportation = df_dim_trasportation[column_end]
    df_dim_trasportation = df_dim_trasportation.drop_duplicates()

    # Mendapatkan nomor urutan dari Transportation_id
    df_dim_trasportation['Transportation_number'] = df_dim_trasportation['Transportation_id'].str.extract(r'(\d+)').astype(int)
    
    # Mengurutkan berdasarkan nomor urutan
    df_dim_trasportation = df_dim_trasportation.sort_values('Transportation_number', ascending=True)
    
    # Menghapus kolom sementara yang digunakan untuk menghitung nomor urutan
    df_dim_trasportation = df_dim_trasportation.drop(columns=['Transportation_number'])
    print(df_dim_trasportation.columns)
    

    output_file = os.path.join(output_directory, 'data_dim_transportation.csv')

    df_dim_trasportation.to_csv(output_file, index=False)
    
    return df_dim_trasportation

def data_dim_destination():
    column_end = ['destination_id', 'country', 'city']
    column_start = ['country', 'city']

    df = get_data()  # Anda perlu mengganti ini dengan pemanggilan fungsi yang mengambil data Anda
    df_dim_destination = df[column_start]
    

    
    print(df_dim_destination)
    df_dim_destination.dropna(inplace=True)
    df_dim_destination = df_dim_destination.drop_duplicates()
    df_dim_destination['destination_id'] = 'DEST_' + (df_dim_destination.groupby(['city', 'country']).ngroup() + 1).astype(str)
    df_dim_destination = df_dim_destination[column_end]
    df_dim_destination = df_dim_destination.drop_duplicates()

    # Mendapatkan nomor urutan dari destination_id
    df_dim_destination['Transportation_number'] = df_dim_destination['destination_id'].str.extract(r'(\d+)').astype(int)
    
    # Mengurutkan berdasarkan nomor urutan
    df_dim_destination = df_dim_destination.sort_values('Transportation_number', ascending=True)
    
    # Menghapus kolom sementara yang digunakan untuk menghitung nomor urutan
    df_dim_destination = df_dim_destination.drop(columns=['Transportation_number'])
    print(df_dim_destination.columns)

    output_file = os.path.join(output_directory, 'data_dim_destination.csv')

    df_dim_destination.to_csv(output_file, index=False)
    
    return df_dim_destination

def data_dim_time():
    column_end = ['time_id','start_date', 'end_date', 'duration']
    column_start = ['Start date', 'End date', 'Duration (days)']
    df = get_data()
    df_dim_time = df[column_start]
    df_dim_time = df_dim_time.rename(columns={'Start date':'start_date', 'End date':'end_date','Duration (days)':'duration'})
    df_dim_time.dropna(subset=['start_date', 'end_date', 'duration'], inplace=True)

    df_dim_time['start_date'] = pd.to_datetime(df_dim_time['start_date'])
    df_dim_time['end_date'] = pd.to_datetime(df_dim_time['end_date'])

    unik_time = df_dim_time['start_date'].unique()
    print(len(unik_time))
    df_dim_time['time_id'] = 'TIM_' + (df_dim_time.groupby(['start_date', 'end_date', 'duration']).ngroup() + 1).astype(str)
    df_dim_time = df_dim_time[column_end]
    df_dim_time = df_dim_time.drop_duplicates()

    # Mendapatkan nomor urutan dari Transportation_id
    df_dim_time['time_number'] = df_dim_time['time_id'].str.extract(r'(\d+)').astype(int)
    
    # Mengurutkan berdasarkan nomor urutan
    df_dim_time = df_dim_time.sort_values('time_number', ascending=True)
    
    # Menghapus kolom sementara yang digunakan untuk menghitung nomor urutan
    df_dim_time = df_dim_time.drop(columns=['time_number'])

    output_file = os.path.join(output_directory, 'data_dim_time.csv')

    df_dim_time.to_csv(output_file, index=False)



    print(df_dim_time)
    return df_dim_time

def create_star_schema(schema):
    postgre_aunt = PostgreSQL(credential['postgresql_warehouse'])
    conn, cursor = postgre_aunt.connect(conn_type='cursor')

    tabel_dim = create_dim_table(schema)
    cursor.execute(tabel_dim)
    conn.commit()
    table_fact = create_fact_table_trip(schema)
    cursor.execute(table_fact)
    conn.commit()

    cursor.close()
    conn.close()
    print('succes create table')



def data_fact_trip():
    column_end = ['id', 'traveler_id', 'Accommodation_id', 'Transportation_id', 'destination_id', 'time_id']
    data_trip = get_data()
    
    dim_traveler = data_dim_traveler()

    dim_accommodation = data_dim_accommodation()

    dim_transportation = data_dim_transportation()

    dim_destination = data_dim_destination()

    dim_time = data_dim_time()

    df_fact = pd.merge(data_trip, dim_traveler, left_on=['Traveler name', 'Traveler age', 'Traveler nationality'], right_on=['name', 'age', 'nationality'], how='inner')
    df_fact = pd.merge(df_fact, dim_accommodation, left_on=['Accommodation type', 'Accommodation cost'], right_on=['type', 'cost'], how='inner')
    df_fact = pd.merge(df_fact, dim_transportation, left_on=['Transportation type', 'Transportation cost'], right_on=['type', 'cost'], how='inner')
    df_fact = pd.merge(df_fact, dim_destination, left_on=['country', 'city'], right_on=['country', 'city'], how='inner')
    df_fact = pd.merge(df_fact, dim_time, left_on=['Start date', 'End date', 'Duration (days)'], right_on=['start_date', 'end_date', 'duration'], how='inner')

    df_fact['id'] = np.arange(1, df_fact.shape[0]+1)
    df_fact = df_fact.drop_duplicates().reset_index()
    df_fact = df_fact.dropna(axis=0)
    df_fact = df_fact[column_end]

    output_file = os.path.join(output_directory, 'data_fact_trip.csv')

    df_fact.to_csv(output_file, index=False)

    return df_fact
    

    


