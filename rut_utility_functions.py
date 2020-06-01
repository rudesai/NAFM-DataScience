import datascience as ds
import pandas as pd

def data_from_RS(query, want_csv_file = False, csv_file_name=None): 
    '''
    Parameters
    -----------
    query: write the query you want 
    want_csv_file: If the value is True, then will output a csv file of the queried data. Default is False
    csv_file_name: Name of the CSV file, will use this parameter only if csv output is requested. default csv file name is None
    '''
    data = ds.query_RS(query)
    if want_csv_file is True:
        data.to_csv(csv_file_name)
    return data 