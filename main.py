# Import the three project Classes for extracting, cleaning and uploading data to local database and other necessary modules

from scripts.database_utils import DatabaseConnector
from scripts.data_cleaning import DataCleaning
from scripts.data_extraction import DataExtractor
import pandas as pd


# Create the DataFrame which will contain the user data

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# Initialise engine with yaml_file_path set to 'default'
database_engine = database.init_db_engine()
# Create an instance of the DataExtractor class
data_extractor = DataExtractor()
# Extract data from table 'legacy_users' and store it in a DataFrame
df = data_extractor.read_rds_table(database_engine, 'legacy_users')

# Clean the user DataFrame

# Create an instance of the DataCleaning class, with our DataFrame passed as an argument
data_cleaner = DataCleaning(df)
# Clean the DataFrame
cleaned_df = data_cleaner.clean_user_data(df)

# Upload the cleaned user DataFrame to the local database

# Now let's initialise the engine that we will upload our DataFrame to
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_df' to our initialised engine
database.upload_to_db(cleaned_df, table_name='dim_users')


# Create the DataFrame that will contain card_details

# Create an instance of the DataExtractor class
data_extractor = DataExtractor()

# Set file path of pdf
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# Convert pdf into DataFrame
pdf_df = data_extractor.retrieve_pdf_data(pdf_path)

# Create an instance of the DataCleaning class, passing DataFrame as an argument
data_cleaner = DataCleaning(pdf_df)
# Clean the DataFrame
clean_pdf_df = data_cleaner.clean_card_data()

# Now initialise the engine that we will upload DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()

# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned__pdf_df' to our initialised engine
database.upload_to_db(clean_pdf_df, table_name='dim_card_details')

