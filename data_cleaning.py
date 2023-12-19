import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


class DataCleaning:
    def __init__(self):
        pass
        '''
        Initialise the DataCleaning class
        '''

    def clean_user_data(self, df):
        '''
        Clean the user data from the specified DataFrame.
        '''
        print("DataFrame cleaning operation initiated.")
        
        # Set 'index' as index of the DataFrame
        df.set_index('index', inplace=True)

        # Drop rows with NULL values
        df = df.dropna()

        # Change columns containing strings into 'string' dtype
        string_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'user_uuid']
        df[string_columns] = df[string_columns].astype('string')

        # Change columns containing dates into 'datetime64' dtype
        date_columns = ['date_of_birth', 'join_date']
        for column in date_columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')
        for column in date_columns:
            df[column] = df[column].dt.date     # Extract only the date part
        
        # Drop rows with invalid dates
        df = df.dropna(subset=date_columns)

        # Replace incorrect values in 'country_code' column
        df['country_code'] = df['country_code'].replace({'GGB': 'GB'})

        print("DataFrame cleaned successfully.")

        return df
    
    def clean_card_data(self, df):
        '''
        Clean the card data from the specified DataFrame.
        '''
        # if not df:
        #     print("Failed to find DataFrame")
        #     return pd.DataFrame() # Return empty DataFrame
        print("DataFrame cleaning operation initiated.")

        # Drop rows with NULL values
        df = df.dropna()

        # Change 'card_number' column into 'int64' dtype
        df['card_number'] = df['card_number'].astype('int64')

        # Change 'card_provider' column into 'str' dtype
        df['card_provider'] = df['card_provider'].astype('string')

        # # Change columns containing dates into 'datetime64' dtype
        # date_columns = ['expiry_date', 'date_payment_confirmed']
        # for column in date_columns:
        #     df[column] = pd.to_datetime(df[column], errors='coerce')
        # for column in date_columns:
        #     df[column] = df[column].dt.date     # Extract only the date part
        
        # # Drop rows with invalid dates
        # #df = df.dropna(subset=date_columns)
        print("DataFrame cleaning operation completed.")
        return df
