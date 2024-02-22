import boto3
import pandas as pd
from scripts.data_extraction import DataExtractor
from scripts.data_cleaning import DataCleaning
from scripts.database_utils import DatabaseConnector

# Extract product information from s3 address and return a pandas DataFrame

extractor = DataExtractor()
s3_path = "s3://data-handling-public/products.csv"
products_df = extractor.extract_from_s3(s3_path)

# Create an instance of the DataCleaning class
cleaner = DataCleaning(products_df)
# Convert the product weights into kg
clean_1_products_df = cleaner.convert_product_weights()

# Clean the 'products_df'
clean2_products_df = cleaner.clean_products_data()

# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'clean2_products_df' to our initialised engine
database.upload_to_db(clean2_products_df, table_name='dim_products')