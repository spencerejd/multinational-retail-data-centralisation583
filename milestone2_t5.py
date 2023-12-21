from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

# Usage example

# # Define the URL for the API endpoint that returns the number of stores
# url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
# # Define the headers for the API request, including the API key for authentication
# headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# # Create an instance of the DataExtractor class
# extractor = DataExtractor()

# # Use the list_number_of_stores method to get the total number of stores
# # This method uses the provided URL and headers to make an API call
# number_of_stores = extractor.list_number_of_stores(url, headers)

# # Print the total number of stores retrieved from the API
# print(f"Number of Stores: {number_of_stores}")

# # Define the base URL for the API endpoint that retrieves details of a specific store
# base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'

# # Use the retrieve_stores_data method to fetch data for all stores
# # This method iterates over store numbers, makes API calls for each, and collects the data
# stores_df = extractor.retrieve_stores_data(base_url, headers)

# Print the DataFrame containing all the stores' data
#print(stores_df)

stores_df = pd.read_csv('stores_data.csv')
#stores_df.info()

# Create an instance of the DataCleaning class, passing our DataFrame as an argument
cleaner = DataCleaning(stores_df)

# Clean the stores_df DataFrame
clean_stores_df = cleaner.clean_store_data()

clean_stores_df.info()
clean_stores_df
