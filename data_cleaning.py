import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import re
import numpy as np


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

        # Drop rows filled with NULL values
        df = df.dropna(how='all')

        df = df.drop_duplicates()

        # Define a list of date formats to try
        date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%Y %B %d", "%B %Y %d"]

        # Lists to store entries that couldn't be parsed
        date_of_birth_errors = []
        join_date_errors = []

        # Function to try parsing date with different formats
        def parse_date(date_str, error_list):
            for fmt in date_formats:
                try:
                    return pd.to_datetime(date_str, format=fmt)
                except (ValueError, TypeError):
                    continue
            error_list.append(date_str)
            return np.nan

        # Apply the custom parsing to 'date_of_birth' and 'join_date'
        df['date_of_birth'] = df['date_of_birth'].apply(lambda x: parse_date(x, date_of_birth_errors))
        df['join_date'] = df['join_date'].apply(lambda x: parse_date(x, join_date_errors))

        # Replace incorrect values in 'country_code' column
        df['country_code'] = df['country_code'].replace({'GGB': 'GB'})
        # List of valid country codes
        valid_country_codes = ['GB', 'DE', 'US']

        # Replace invalid country codes with NaN
        df['country_code'] = df['country_code'].apply(lambda x: x if x in valid_country_codes else np.nan)

        # Convert entire rows to NaN if any cell in the row is NaN
        df = df.apply(lambda row: row if not row.isna().any() else [np.nan]*len(row), axis=1)

        # Drop rows filled with NULL values
        df = df.dropna(how='all')

        print("DataFrame cleaned successfully.")

        return df


    def clean_card_data(self):
        '''
        Clean the card data from the specified DataFrame.
        '''
        print("DataFrame cleaning operation initiated.")
        cleaned_df = self.df

        # Drop rows filled with NULL values
        cleaned_df = cleaned_df.dropna(how='all')

        # Define a list of date formats to try
        date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%Y %B %d", "%B %Y %d"]

        # Lists to store entries that couldn't be parsed
        date_payment_confirmed_errors = []

        # Function to try parsing date with different formats
        def parse_date(date_str, error_list):
            for fmt in date_formats:
                try:
                    return pd.to_datetime(date_str, format=fmt)
                except (ValueError, TypeError):
                    continue
            error_list.append(date_str)
            return np.nan
        
        # List of invalid payment dates
        invalid_payment_dates = ['XTD27ANR5Q', 'WCK463ZO1Z', 'UZGSD0AEBT', 'T995FX2C7W', 'T008RE1ZR6', 
                             'RLQYRRYHPU', 'OE3KONN2V6', 'H2PCQP4W50', 'GTC9KBWJO9', 'GD9PHJXQR4', 
                             'EVVMMB3QYV', 'DJIXF1AFAZ', '7VGB4DA1WI', '7FL8EU9GBF', 'NULL']
        
        # Set 'date_payment_confirmed' to None for invalid values
        cleaned_df.loc[cleaned_df['date_payment_confirmed'].isin(invalid_payment_dates), 'date_payment_confirmed'] = None

        # Drop rows with NULL 'date_payment_confirmed'
        cleaned_df.dropna(subset=['date_payment_confirmed'], inplace=True)

        # Process rows where 'card_number' is null and 'card_number expiry_date' is not null
        card_mask = cleaned_df['card_number'].isnull() & cleaned_df['card_number expiry_date'].notnull()

        # Extract card number from 'card_number expiry_date' and assign it to 'card_number'
        cleaned_df.loc[card_mask, 'card_number'] = cleaned_df.loc[card_mask, 'card_number expiry_date'].str.split().str[0]

        # Process rows where 'expiry_date' is null and 'card_number expiry_date' is not null
        expiry_mask = cleaned_df['expiry_date'].isnull() & cleaned_df['card_number expiry_date'].notnull()

        # Extract expiry_date from 'card_number expiry_date' and assign it to 'expiry_date'
        cleaned_df.loc[expiry_mask, 'expiry_date'] = cleaned_df.loc[expiry_mask, 'card_number expiry_date'].str.split().str[1]

        # Convert all entries in 'card_number' column into string
        cleaned_df['card_number'] = cleaned_df['card_number'].astype(str)

        # Remove question marks at the beginning of card numbers
        cleaned_df['card_number'] = cleaned_df['card_number'].str.lstrip('?')

        cleaned_df.drop(columns=['card_number expiry_date', 'Unnamed: 0'], inplace=True)              

        print("DataFrame cleaning operation completed.")
        return cleaned_df
    
    def clean_store_data(self):
        '''
        Clean the store data from the specified DataFrame
        '''
        print("DataFrame cleaning operation initiated.")

        # Set the index column
        self.df.set_index('index', inplace=True)
        
        # Replace incorrect values in 'continent' column
        self.df['continent'] = self.df['continent'].replace({'eeEurope': 'Europe'})
        self.df['continent'] = self.df['continent'].replace({'eeAmerica': 'America'})

        # Replace any alphabetical characters in 'staff_numbers' with an empty string
        self.df['staff_numbers'] = self.df['staff_numbers'].str.replace('[a-zA-Z]', '', regex=True)

        # Set 'store_code' to None where the length of the string is smaller than 11
        self.df.loc[self.df['store_code'].str.len() < 11, 'store_code'] = None

        # Drop rows with NULL values
        self.df.dropna(subset=['store_code'], inplace=True)

        
        return self.df
    
    def clean_products_data(self):

        print("Commencing DataFrame cleaning.")

        # Drop rows filled with NULL values
        self.df = self.df.dropna(how='all')
        
        # Drop the 'Unnamed: 0' column
        self.df.drop('Unnamed: 0', axis=1, inplace=True)

        # Drop rows where 'product_name' is 'VLPCU81M30'
        self.df = self.df[self.df['product_name'] != 'VLPCU81M30']

        # Update 'weight' column where 'product_code' is 'A8-4686892S'
        self.df.loc[self.df['product_code'] == 'A8-4686892S', 'weight'] = 0.4535

        # Drop rows with NULL values in weight
        self.df.dropna(subset=['weight'], inplace=True)

        # Drop rows filled with NULL values
        self.df = self.df.dropna(how='all')

        print('DataFrame has now been cleaned.')

        return self.df

    def convert_product_weights(self):
        print("Initiating weight conversion for 'weight' column.")
        # Iterate through the weight column
        for i, weight in enumerate(self.df['weight']):
            # Initialize the converted value as None
            converted_value = None

            # Check if the weight is a string and contains a number
            if isinstance(weight, str) and re.search(r'\d', weight):
                # Handling compound formats like '12 x 100g'
                if 'x' in weight:
                    try:
                        quantity, unit_weight = weight.split('x')
                        quantity = float(quantity.strip())
                        unit_weight = unit_weight.strip()
                    except ValueError:
                        pass  # Skip to the next item if parsing fails

                    # Extract the numeric value and unit from unit_weight
                    match = re.match(r'(\d+(\.\d+)?)(\D*)', unit_weight)
                    if match:
                        value, unit = match.group(1), match.group(3).lower().strip()
                        value = float(value) * quantity  # Multiply by the quantity
                else:
                    # Handling simpler formats like '100g'
                    match = re.match(r'(\d+(\.\d+)?)(\D*)', weight)
                    if match:
                        value, unit = match.group(1), match.group(3).lower().strip()
                        value = float(value)

                # Convert to kg based on the unit
                if match:
                    if 'g' in unit or 'ml' in unit:  # Assuming 'g' and 'ml' are equivalent
                        converted_value = value / 1000  # Convert to kg
                    elif 'kg' in unit:
                        converted_value = value
            
            # Assign the converted value or None
            self.df.at[i, 'weight'] = converted_value

        # Ensure the weight column is of type float
        self.df['weight'] = self.df['weight'].astype(float)
        # Round the 'weight' column to 8 decimal places
        self.df['weight'] = self.df['weight'].round(8)
        print("Weight conversion for 'weight' column complete.")

        return self.df
    
    def clean_orders_data(self):
        remove_columns = ['first_name', 'last_name', '1']  # Adjust if needed
        self.df.drop(columns=remove_columns, inplace=True, errors='ignore')

        # Convert columns to string dtype
        string_columns = ['date_uuid', 'user_uuid', 'store_code', 'product_code', 'card_number']
        for column in string_columns:
            self.df[column] = self.df[column].astype('string')

        # Drop level_0 column
        self.df.drop('level_0', axis=1, inplace=True)
            
        # Set index column as index
        self.df = self.df.set_index('index', inplace=False)


        return self.df
    
    def clean_date_details_data(self):

        print("Initiating DataFrame cleaning.")
        # Valid time periods
        valid_time_periods = ['Midday', 'Evening', 'Morning', 'Late_Hours']

        # Replace values in 'time_period' that are not in valid_time_periods with NaN
        self.df['time_period'] = self.df['time_period'].where(self.df['time_period'].isin(valid_time_periods), other=pd.NA)

        # Convert date_columns into numeric dtypes
        date_columns = ['month', 'year', 'day']
        for column in date_columns:
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        
        # Drop rows with NULL values
        self.df.dropna(inplace=True)

        # Convert date_columns into int dtypes
        for column in date_columns:
            self.df[column] = self.df[column].astype('int')

        # Convert time_period into string dtype
        self.df['time_period'] = self.df['time_period'].astype('string')

        # Convert date_uuid into string dtype
        self.df['date_uuid'] = self.df['date_uuid'].astype('string')

        print("DataFrame cleaning complete.")

        # print("Hi World")

        return self.df
