import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


class DataCleaning:
    def __init__(self, df):
        self.df = df
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
    
    def clean_card_data(self):
        '''
        Clean the card data from the specified DataFrame.
        '''
        print("DataFrame cleaning operation initiated.")

        # Drop rows with NULL values
        self.df.dropna(subset=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'], inplace=True)

        # Remove non-numeric characters from 'card_number' column
        self.df['card_number'] = self.df['card_number'].str.replace('[^0-9]', '', regex=True)
        # Convert 'card_number' to numeric, coerce errors to NaN
        self.df['card_number'] = pd.to_numeric(self.df['card_number'], errors='coerce') # From 15254 to 2464 non-nulls - I suspect it's removing more rows than expected... return to this
        # Drop any rows that now have NaN in 'card_number'
        self.df.dropna(subset=['card_number'], inplace=True)
        # Change 'card_number' column into 'int64' dtype
        self.df['card_number'] = self.df['card_number'].astype('int64')

        # Change 'card_provider' column into 'str' dtype
        self.df['card_provider'] = self.df['card_provider'].astype('string')

        # Change columns containing dates into 'datetime64' dtype
        date_columns = ['expiry_date', 'date_payment_confirmed']
        for column in date_columns:
            self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
        for column in date_columns:
            self.df[column] = self.df[column].dt.date     # Extract only the date part
        
        # Drop rows with invalid dates
        self.df.dropna(subset=date_columns, inplace=True)

        self.df.drop(columns=['card_number expiry_date', 'Unnamed: 0'], inplace=True)

        print("DataFrame cleaning operation completed.")
        return self.df
    
    def clean_store_data(self):
        '''
        Clean the store data from the specified DataFrame
        '''
        print("DataFrame cleaning operation initiated.")

        # Drop rows with NULL values
        self.df.dropna(subset=['address', 'longitude', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'], inplace=True)

        # Change longitude and latitude columns into 'float' dtype
        coordinate_columns = ['longitude', 'latitude']
        for column in coordinate_columns:
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        
        print(self.df.info())
        print("Commencing 'opening_date' column conversion into 'datetime64' dtype")
        # Change 'opening_date' column into 'datetime64' dtype
        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce')

        # Change 'staff_numbers' column into a numeric dtype
        self.df['staff_numbers'] = pd.to_numeric(self.df['staff_numbers'], errors='coerce')

        # Drop NaN rows in 'staff_numbers' column
        self.df.dropna(subset=['staff_numbers'], inplace=True)
        # Convert 'staff_numbers' column into 'int' from float
        self.df['staff_numbers'] = self.df['staff_numbers'].astype(int)
        
        # Change columns containing text into string dtype
        text_columns = ['address', 'locality', 'store_code', 'store_type', 'country_code', 'continent']
        for column in text_columns:
            self.df[column] = self.df[column].astype('string')
        
        # Replace incorrect values in 'country_code' column
        self.df['continent'] = self.df['continent'].replace({'eeEurope': 'Europe'})
        self.df['continent'] = self.df['continent'].replace({'eeAmerica': 'America'})
        
        # Drop the 'lat' column
        self.df.drop('lat', axis=1, inplace=True)

        # Drop rows with NULL values
        self.df.dropna(subset=['address', 'longitude', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'], inplace=True)

        
        return self.df


