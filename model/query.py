def get_raw_data():
    return """
    SELECT * FROM raw_data_trip
    """

def create_dim_table(schema):
    return """    
    CREATE TABLE IF NOT EXISTS {schema}.dim_traveler (
        traveler_id VARCHAR(64) primary key,
        name text,
        age int,
        gender VARCHAR(64),
        nationality text
        );

    CREATE TABLE IF NOT EXISTS {schema}.dim_accommodation (
        Accommodation_id VARCHAR(64) primary key,
        type VARCHAR(64),
        cost VARCHAR(64)
        );

    CREATE TABLE IF NOT EXISTS {schema}.dim_transportation (
        Transportation_id VARCHAR(64) primary key,
        type VARCHAR(64),
        cost VARCHAR(64)
        );

    CREATE TABLE IF NOT EXISTS {schema}.dim_destination (
        destination_id VARCHAR(64) primary key,
        country text,
        city text
        );

    CREATE TABLE IF NOT EXISTS {schema}.dim_time (
        time_id VARCHAR(64) primary key,
        start_date date,
        end_date date,
        duration int
        );


    """.format(schema=schema)

def create_fact_table_trip(schema):
    return """
    CREATE TABLE IF NOT EXISTS {schema}.fact_trip (
        id SERIAL PRIMARY KEY,
        traveler_id VARCHAR(64) REFERENCES dim_traveler(traveler_id),
        accommodation_id VARCHAR(64) REFERENCES dim_accommodation(accommodation_id),
        transportation_id VARCHAR(64) REFERENCES dim_transportation(transportation_id),
        destination_id VARCHAR(64) REFERENCES dim_destination(destination_id),
        time_id VARCHAR(64) REFERENCES dim_time(time_id)
        );

    """.format(schema=schema)


# table dim
def insert_dim_traveler():
    return """
    INSERT INTO dim_traveler(
        traveler_id,
        name,
        age,
        gender,
        nationality)
        VALUES (%s,%s,%s,%s,%s);
        """
def insert_dim_accommodation():
    return """
    INSERT INTO dim_accommodation(
        accommodation_id,
        type,
        cost)
        VALUES (%s,%s,%s);
    """
def insert_dim_transportation():
    return """
    INSERT INTO dim_transportation(
        transportation_id,
        type,
        cost)
        VALUES (%s,%s,%s);

    """
def insert_dim_destination():
    return """
    INSERT INTO dim_destination(
        destination_id,
        country,
        city)
        VALUES (%s,%s,%s);

    """
def insert_dim_time():
    return """
    INSERT INTO dim_time(
        time_id,
        start_date,
        end_date,
        duration)
        VALUES (%s,%s,%s,%s);

    """   
# table fact trip
def insert_fact_trip():
    return """
    INSERT INTO fact_trip(
        id,
        traveler_id,
        accommodation_id,
        transportation_id,
        destination_id,
        time_id
        )
        VALUES (%s,%s,%s,%s,%s,%s);

    """

# report
def create_dashboard():
    return """
    select
        to_char(start_date, 'month') as bulan,
        count(*) as visit_month
    from dim_time dt  
    group by 1
    order by 2 desc ;
    """

