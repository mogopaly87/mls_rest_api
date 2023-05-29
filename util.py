from psycopg2._psycopg import connection
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

from dotenv import load_dotenv
import os
import pandas as pd
import collections
from transform_data_to_df import transform

# Load environment variables from the .env file. 
load_dotenv(dotenv_path='.env')

# Assign variables to each environment variable
DB_NAME = os.getenv('DB_NAME2')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def get_sql_alchemy_engine(conn_string) -> Connection:
    """Gets an SQL Alchemy engine connection object

    Args:
        conn_string (string): a connection string to connect to PostgreSQL database

    Returns:
        Connection: A connection object using SqlAlchemy
    """
    db = create_engine(conn_string)
    conn = db.connect()
    conn.autocommit = True
    
    return conn


def get_postgresql_conn(db_name, 
                        db_user, 
                        db_pass, 
                        db_host, 
                        db_port) -> connection:
    """Gets a Postgresql connection object

    Args:
        db_name (string): Database Name
        db_user (string): Database User
        db_pass (string): Database Password
        db_host (string): Host
        db_port (string): Port number

    Returns:
        connection: A connection object using pyscopg2
    """
    try:
        conn = psycopg2.connect(database = db_name, 
                            user = db_user, 
                            password = db_pass, 
                            host = db_host,
                            port = db_port)
        print("Database connected successfully!")
    except:
        print("Database not connected")
    
    return conn



def create_mls_listing_table(conn) -> None:
    """Creates a table called 'listing' in the database. MLS listings will be recored
        in this table.
    
    Args:
        conn (connection) : psycopg2 connection object
    
    """
    sql_create_table = """
        CREATE TABLE listing(mls_num CHAR(50) PRIMARY KEY,
                            address_street VARCHAR(200),
                            num_of_beds int,
                            num_of_baths int,
                            sqft int,
                            property_type CHAR(200),
                            price int,
                            last_updated DATE,
                            city VARCHAR(200),
                            province CHAR(50),
                            postal_code CHAR(50),
                            link VARCHAR(255))
        """
    
    with conn as conn:
        try:
            cur = conn.cursor()
            cur.execute(sql_create_table)
            
            conn.commit()
            print("Table created successfully!")
        except Exception as e:
            print("Something went wrong!\n", e)
        

def load_transformed_data_to_sql_table(dataframe, table_name, conn) -> None:
    """Loads transformed data in a dataframe into a sql table

    Args:
        dataframe (pandas dataframe): Dataframe of transformed data
        table_name (string): Destination table name
    
    conn (Connection): 
                    SqlAlchemy connection object
    """
    with conn as conn:
        dataframe.to_sql(table_name, conn, if_exists='replace')
    

def execute_initial_data_ingestion():
    
    postgres_conn = get_postgresql_conn(DB_NAME,
                                        DB_USER,
                                        DB_PASSWORD,
                                        DB_HOST,
                                        DB_PORT)
    
    create_mls_listing_table(postgres_conn)
    
    sql_alc_conn_string = os.getenv('SQL_ALCHEMY_CONN_STRING')
    sql_alc_conn = get_sql_alchemy_engine(sql_alc_conn_string)
    
    
    df = transform('mls_listing.json')
    # print(f"\n{df.head(5)}")
    
    load_transformed_data_to_sql_table(df, 'listing', sql_alc_conn)
    


def get_all_mls_num_from_db():
    list_of_listing = []
    with get_postgresql_conn(DB_NAME,
                            DB_USER,
                            DB_PASSWORD,
                            DB_HOST,
                            DB_PORT) as conn:
        
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM listing")
            record = cur.fetchall()
            for row in record:
                list_of_listing.append(row[1].strip())
            return list_of_listing


def get_current_mls_listing_on_remax_by_number():
    df2 = pd.read_json('mls_temp.json')
    df2[['mls_escape','mls_num']] = df2.mls_num.str.split(":", expand=True)
    mls_num_list = df2['mls_num'].to_list()
    mls_num_list = [i.strip() for i in mls_num_list]
    
    return mls_num_list


def is_mls_num_data_unchanged() -> bool:
    
    db_mls_listing = get_all_mls_num_from_db()
    mls_num_list = get_current_mls_listing_on_remax_by_number()
    status = False
    if collections.Counter(db_mls_listing) == collections.Counter(mls_num_list):
        status = True
    
    return status



if __name__ == '__main__':
    
    execute_initial_data_ingestion()
