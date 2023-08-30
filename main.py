
from package.transform import create_star_schema
from package.load import insert_data_dim_traveler, insert_data_dim_transpotation, insert_data_dim_accommodation, insert_data_dim_destination, insert_data_dim_time, insert_data_fact_trip

if __name__=='__main__':
    create_star_schema('public')
    insert_data_dim_traveler()
    insert_data_dim_transpotation()
    insert_data_dim_accommodation()
    insert_data_dim_destination()
    insert_data_dim_time()
    insert_data_fact_trip()