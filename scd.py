import util


def update_table_with_new_listing():
    # If the mls numbers in the database is not the same as the ones on Remax website:
    # This could mean two things:
    # 1) The database has an mls record that does not exist on remax which indicates a listing has been removed.
    # 2) Remax website has an mls record that does not exist in the database which indicates a listing has been added.
    if not util.is_mls_num_data_unchanged():
        print('False')
    else:
        print('True')
        
update_table_with_new_listing()