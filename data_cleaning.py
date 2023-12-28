import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import re


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

        # Set the index column
        self.df.set_index('index', inplace=True)

        # # Drop rows with NULL values
        # self.df.dropna(subset=['address', 'longitude', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'], inplace=True)

        # # Change longitude and latitude columns into 'numeric' values
        coordinate_columns = ['longitude', 'latitude', 'lat']
        for column in coordinate_columns:
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        
        # Change 'opening_date' column into 'datetime64' dtype
        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce')
        self.df['opening_date'] = self.df['opening_date'].dt.date

        # Change 'staff_numbers' column into a numeric dtype
        self.df['staff_numbers'] = pd.to_numeric(self.df['staff_numbers'], errors='coerce')

        # # Drop NaN rows in 'staff_numbers' column
        # self.df.dropna(subset=['staff_numbers'], inplace=True)
        # Convert 'staff_numbers' column into 'int' from float
        # self.df['staff_numbers'] = self.df['staff_numbers'].astype(int)
        
        # # Change columns containing text into string dtype
        # text_columns = ['address', 'locality', 'store_code', 'store_type', 'country_code', 'continent']
        # for column in text_columns:
        #     self.df[column] = self.df[column].astype('string')
        
        # Replace incorrect values in 'country_code' column
        self.df['continent'] = self.df['continent'].replace({'eeEurope': 'Europe'})
        self.df['continent'] = self.df['continent'].replace({'eeAmerica': 'America'})
        
        # Drop the 'lat' column
        # self.df.drop('lat', axis=1, inplace=True)

        # Drop rows with NULL values
        # self.df.dropna(subset=['address', 'longitude', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'], inplace=True)

        
        return self.df
    
    def clean_products_data(self):

        print("Commencing DataFrame cleaning!")
        # Change 'date_added' column into 'datetime64' dtype
        self.df['date_added'] = pd.to_datetime(self.df['date_added'], errors='coerce')
        self.df['date_added'] = self.df['date_added'].dt.date
        
        # Change 'product_price' to float
        # Remove the '£' symbol and any commas
        self.df['product_price'] = self.df['product_price'].str.replace('£', '').str.replace(',', '')
        # Convert the column to float, setting unparseable strings to NaN
        self.df['product_price'] = pd.to_numeric(self.df['product_price'], errors='coerce')

        # Convert EAN to numeric type
        self.df['EAN'] = pd.to_numeric(self.df['EAN'], errors='coerce')

        # Drop rows with NULL values
        self.df.dropna(subset=['product_price', 'weight', 'EAN', 'date_added', 'product_name', 'category', 'uuid', 'removed', 'product_code'], inplace=True)

        # Convert specified columns to string dtype
        string_columns = ['product_name', 'category', 'uuid', 'removed', 'product_code']
        for column in string_columns:
            self.df[column] = self.df[column].astype('string')
        
        # Drop the 'Unnamed: 0' column
        self.df.drop('Unnamed: 0', axis=1, inplace=True)
        print('DataFrame has now been cleaned!')

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

        # Convert 'card_number' into numeric
        # self.df['card_number'] = pd.to_numeric(self.df['card_number'], errors='coerce')

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