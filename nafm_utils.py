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

def create_quote_comma_text(spaced_values='',values_have_space=False,csv_output=False, csv_name='quotes_comma'):
    '''
    This function is created so that simple space separated texts can be converted to quotes-comma text so that they can be used   as filters in SQL or any other usage.
    -------
    Parameters
    -------
    spaced_values: This parameter is required if you setted the parameter 'values_have_space=True'. 
    values_have_space: Set this to true if your values have space within the main value. e.g., Lead Developer               
    csv_output: Default is False and will print a list in Jupyter Notebook. 
                If user inputs True, then a CSV will be created as output.
                
    csv_name: This parameter will be used only if csv_output is True.
              Default CSV name is 'quotes_comma'. User can input other name as desired
THIS is a demo line from domino
    '''
    if values_have_space==True:
        cleaned_string=spaced_values.replace(' ','_')
        split_list=cleaned_string.split()
        cleaned_texts=[]
        for text in split_list:
            if "'" in text:
                text=text.replace("'", "''")
                cleaned_texts.append(text)
            else:
                cleaned_texts.append(text)       
        if csv_output==False:
            for text in cleaned_texts:
                print("'{}',".format(text.replace('_',' ')),end=' ')
        else:
            final_text_list=[]
            for text in cleaned_texts:
                ## if the item is the last item in the list then don't add the final comma
                if text == cleaned_texts[-1]:
                    text="'"+text+"'"
                    final_text_list.append(text.replace('_',' '))
                else:
                    text="'"+text+"'"+","
                    final_text_list.append(text.replace('_',' '))

            ## return the final list as a csv for easy use if the user wants it

            df=pd.DataFrame(final_text_list)
            df.to_csv(path_or_buf= csv_name+'.csv')
            return print('See the downloaded file titled "{}.csv"'.format(csv_name))
    elif  values_have_space==False:
        ## inputing the raw data
        raw_data=input('Input your data which is space separated: ')
        raw_texts = raw_data.split()

        ## cleaning the text if it has a quote in it
        cleaned_texts=[]
        for text in raw_texts:
            if "'" in text:
                text=text.replace("'", "''")
                cleaned_texts.append(text)
            else:
                cleaned_texts.append(text)

        if csv_output==False:
            print (cleaned_texts, end=' ')
        else:
            final_text_list=[]
            for text in cleaned_texts:
                ## if the item is the last item in the list then don't add the final comma
                if text == cleaned_texts[-1]:
                    text="'"+text+"'"
                    final_text_list.append(text)
                else:
                    text="'"+text+"'"+","
                    final_text_list.append(text)

            ## return the final list as a csv for easy use if the user wants it

            df=pd.DataFrame(final_text_list)
            df.to_csv(path_or_buf= csv_name+'.csv')
            return print('See the downloaded file titled "{}.csv"'.format(csv_name))
