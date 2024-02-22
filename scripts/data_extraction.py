import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
import tabula
import boto3
import requests
from io import StringIO, BytesIO

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_engine, table_name):
        """
        Extract data from the specified RDS database table to a pandas DataFrame.

        Parameters:
            db_connector (DatabaseConnector):
                An instance of the DatabaseConnector class.
            db_engine:
                A SQLAlchemy database engine
            table_name (str):
                The name of the table to extract data from.
        
        Returns:
            pd.DataFrame:
                A pandas DataFrame containing the data from the specified table.
        """

        if not db_engine:
            print("Failed to initialise the database engine.")
            return pd.DataFrame() # Return empty DataFrame if connection fails
        
        try:
            # Use pandas to read table into a DataFrame
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql_query(query, db_engine)

            print(f"Data extracted from the '{table_name}' table and into a DataFrame.")
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return pd.DataFrame() # Return an empty DataFrame if error
        

    def retrieve_pdf_data(self, pdf_path):
        '''
        Retrieve data from a pdf and return a pandas DataFrame.
        '''

        if not pdf_path:
            print("Failed to find pdf")
            return pd.DataFrame()   # Return empty DataFrame
        
        try:
            # Set JVM options for tabula-py
            java_options = "-Djava.awt.headless=true -XX:ReservedCodeCacheSize=96mb"
            tabula._java_options = java_options.split()

            # read_pdf returns list of DataFrames
            # pages='all' to extract tables from all pages
            df = tabula.read_pdf(pdf_path, stream=True, pages='all')

            # Concatenate the list of DataFrames into a single DataFrame
            result_df = pd.concat(df, ignore_index=True)
            print("Returning DataFrame from the given pdf")
            return result_df
        except Exception as e:
            print(f"Error reading pdf: {e}")
            return pd.DataFrame() # Return empty DataFrame if error
        
    def list_number_of_stores(self, url, headers):
        response = requests.get(url, headers=headers)
        print("Status Code:", response.status_code)
        if response.status_code == 200:
            # Assuming the API returns a JSON object with the number of stores under a key
            number_stores = response.json().get('number_stores')
            print(response.json())
            return number_stores
        else:
            # Handle errors (e.g., API not reachable, wrong credentials, etc.)
            print(f"Error: {response.status_code}")
            return None
        
    def retrieve_stores_data(self, base_url, headers):
        number_of_stores = self.list_number_of_stores(url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers=headers)
        all_stores_data = []

        for store_number in range(0, number_of_stores + 1):
            store_url = f"{base_url}/{store_number}"  # Construct the URL for each store
            response = requests.get(store_url, headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                all_stores_data.append(store_data)
            else:
                print(f"Error retrieving store {store_number}: {response.status_code}")

        df = pd.DataFrame(all_stores_data)
        return df
    
    def extract_from_s3(self, s3_path):
        # Split the S3 path to get the bucket name and file key
        bucket_name, file_key = s3_path.replace("s3://", "").split("/", 1)

        # Create an S3 client using boto3
        s3_client = boto3.client('s3')

        # Retrieve the file from the specified S3 bucket
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)

        # Determine the file type (extension)
        if s3_path.endswith('.csv'):
            # Read CSV file content into a pandas DataFrame
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        elif s3_path.endswith('.json'):
            # Read JSON file content into a pandas DataFrame
            df = pd.read_json(BytesIO(obj['Body'].read()))
        else:
            raise ValueError("Unsupported file format")
        
        return df
    