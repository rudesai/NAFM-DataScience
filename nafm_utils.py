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
        csv_file_name=csv_file_name+'.csv'
        data.to_csv(csv_file_name)
    return data 


def create_quote_comma_text():
    '''
    This function is created so that simple space separated texts can be converted to quotes-comma text so that they can be used   as filters in SQL or any other usage.

    It requires no parameters, just input your text data which may be Email Addresses, offerIDs or any else. 
    '''
    raw_data=input('Input your data which is space separated: ')
    texts = raw_data.split()
    return texts
