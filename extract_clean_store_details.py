from scripts.data_extraction import DataExtractor
from scripts.data_cleaning import DataCleaning
import pandas as pd
from scripts.database_utils import DatabaseConnector

# Usage example

# Define the URL for the API endpoint that returns the number of stores
url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
# Define the headers for the API request, including the API key for authentication
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Create an instance of the DataExtractor class
data_extractor = DataExtractor()

# Use the list_number_of_stores method to get the total number of stores
# This method uses the provided URL and headers to make an API call
number_of_stores = data_extractor.list_number_of_stores(url, headers)

# Print the total number of stores retrieved from the API
print(f"Number of Stores: {number_of_stores}")

# Define the base URL for the API endpoint that retrieves details of a specific store
base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'

# Use the retrieve_stores_data method to fetch data for all stores
# This method iterates over store numbers, makes API calls for each, and collects the data
stores_df = data_extractor.retrieve_stores_data(base_url, headers)
stores_df.info()

# Create an instance of the DataCleaning class, passing our DataFrame as an argument
cleaner = DataCleaning(stores_df)

# Clean the stores_df DataFrame
clean_stores_df = cleaner.clean_store_data()
print(clean_stores_df.head())
clean_stores_df.info()

# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_df' to our initialised engine
database.upload_to_db(clean_stores_df, table_name='dim_store_details')
