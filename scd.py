import util
import os
import transform_data_to_df as trf


def add_new_or_update_listing_status():
    
    sql_alc_conn_string = os.getenv('SQL_ALCHEMY_CONN_STRING')
    sql_alc_conn = util.get_sql_alchemy_engine(sql_alc_conn_string)
    
    # Get the status of the data difference between what is in the database versus
    # what is on Remax website.
    status = util.get_data_difference_by_mls_number()
    print("The data difference is: \n", status)
    # If there is new data on Remax website then do this:
    if len(status['new_data']) >= 1:
        new_data = status['new_data']
        df = trf.transform('mls_temp_data.json')
        df_filtered = df.query("mls_num in @new_data")
        
        util.load_new_data(df_filtered, 'listing', sql_alc_conn)
        
    if len(status['stale_data']) >= 1:
        db_name = util.DB_NAME
        db_user = util.DB_USER
        db_pass = util.DB_PASSWORD
        db_host = util.DB_HOST
        db_port = util.DB_PORT
        
        conn = util.get_postgresql_conn(db_name, db_user, db_pass, db_host, db_port)
        
        with conn as conn:
            cur = conn.cursor()
            for mls_num in status['stale_data']:
                cur.execute(f"""
                            UPDATE listing
                            SET status = 'Inactive'
                            WHERE mls_num = '{mls_num}'
                            """)
            conn.commit()

