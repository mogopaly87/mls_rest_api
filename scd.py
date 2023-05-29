import util
import os
import transform_data_to_df as trf


def update_table():
    
    sql_alc_conn_string = os.getenv('SQL_ALCHEMY_CONN_STRING')
    sql_alc_conn = util.get_sql_alchemy_engine(sql_alc_conn_string)
    
    # Get the status of the data difference between what is in the database versus
    # what is on Remax website.
    status = util.get_data_difference_by_mls_number()
    
    # If there is new data on Remax website then do this:
    if len(status['new_data']) >= 1:
        new_data = status['new_data']
        df = trf.transform('mls_temp.json')
        df_filtered = df.query("mls_num in @new_data")
        
        util.load_new_data(df_filtered, 'listing', sql_alc_conn)
        
    if len(status['stale_data']) >= 1:
        pass
    return util.get_data_difference_by_mls_number()