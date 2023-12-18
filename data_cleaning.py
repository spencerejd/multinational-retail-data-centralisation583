import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


class DataCleaning:
    def __init__(self, data_extractor):
        self.data_extractor = data_extractor
        '''
        Initialise the DataCleaning class
        '''

    def clean_user_data(self, table_name):
        '''
        Clean the user data from the specified database table.
        '''
        
        table_name_df = self.data_extractor.read_rds_table(table_name)
        print("DataFrame cleaning operation initiated")
        
        # Set 'index' as index of the DataFrame
        table_name_df.set_index('index', inplace=True)

        # Drop rows with NULL values
        table_name_df = table_name_df.dropna()

        # Change columns containing strings into 'string' dtype
        string_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'user_uuid']
        table_name_df[string_columns] = table_name_df[string_columns].astype('string')

        # Change columns containing dates into 'datetime64' dtype
        date_columns = ['date_of_birth', 'join_date']
        for column in date_columns:
            table_name_df[column] = pd.to_datetime(table_name_df[column], errors='coerce')
        
        # Drop rows with invalid dates
        table_name_df = table_name_df.dropna(subset=date_columns)

        # Replace incorrect values in 'country_code' column
        table_name_df['country_code'] = table_name_df['country_code'].replace({'GGB': 'GB'})

        print("DataFrame cleaned successfully")

        return table_name_df




# # Create an instance of the DatabaseConnector class
# connector = DatabaseConnector()
# # Initialise the database engine
# connector_engine = connector.init_db_engine()
    
# # Create an instance of the DataExtractor class, passing the DatabaseConnector as an argument
# data_extractor = DataExtractor(db_connector=connector)

# # Create an instance of the DataCleaning class, passing the DatabaseExtractor as an argument
# data_cleaner = DataCleaning(data_extractor=data_extractor)

# # The table that contains user data
# legacy_users_table = 'legacy_users'

# # Clean the user data
# cleaned_users_df = data_cleaner.clean_user_data(legacy_users_table)


