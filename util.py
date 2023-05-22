# from psycopg2._psycopg import connection
# import psycopg2
# from sqlalchemy import create_engine
# from transform_data_to_df import transform


# def get_sql_alchemy_engine(conn_string):
    
#     db = create_engine(conn_string)
#     conn = db.connect()
#     conn.autocommit = True
    
#     return conn


# def get_postgresql_conn(db_name, 
#                         db_user, 
#                         db_pass, 
#                         db_host, 
#                         db_port) -> connection:
    
#     try:
#         conn = psycopg2.connect(database = db_name, 
#                             user = db_user, 
#                             password = db_pass, 
#                             host = db_host,
#                             port = db_port)
#         print("Database connected successfully!")
#     except:
#         print("Database not connected")
    
#     return conn



# def create_mls_listing_table(conn):
    
#     sql_create_table = """
#         CREATE TABLE listing(mls_num CHAR(50) PRIMARY KEY,
#                             address_street VARCHAR(200),
#                             num_of_beds int,
#                             num_of_baths int,
#                             sqft int,
#                             property_type CHAR(200),
#                             price int,
#                             last_updated DATE,
#                             city VARCHAR(200),
#                             province CHAR(50),
#                             postal_code CHAR(50))
#         """
    
#     try:
#         cur = conn.cursor()
#         cur.execute(sql_create_table)
#         cur.close()
#         conn.commit()
#         print("Table created successfully!")
#     except:
#         print("Something went wrong!")
        

# def load_transformed_data_to_sql_table(dataframe, table_name, conn):
    
#     dataframe.to_sql(table_name, conn, if_exists='replace')
    

# def main():
    
#     postgres_conn = get_postgresql_conn('mls_listing',
#                                         'nonso',
#                                         'OkeMog2014',
#                                         'localhost',
#                                         '5432')
    
#     create_mls_listing_table(postgres_conn)
    
#     sql_alc_conn_string = 'postgresql://nonso:OkeMog2014@127.0.0.1/mls_listing'
#     sql_alc_conn = get_sql_alchemy_engine(sql_alc_conn_string)
    
    
#     df = transform('mls_listing.json')
#     # print(f"\n{df.head(5)}")
    
#     load_transformed_data_to_sql_table(df, 'listing', sql_alc_conn)
    

# if __name__ == '__main__':
    
#     main()
