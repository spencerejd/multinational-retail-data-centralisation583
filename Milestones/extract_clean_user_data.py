import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# Create an instance of the DataExtractor class
data_extractor = DataExtractor()

# Set file path of pdf
pdf_path = "/Users/spencer/Documents/AiCore/Courses/Projects/retail_data/card_details.pdf"
# Convert pdf into DataFrame
pdf_df = data_extractor.retrieve_pdf_data(pdf_path)
# Create an instance of the DataCleaning class, passing DataFrame as an argument
data_cleaner = DataCleaning(pdf_df)
# Clean the DataFrame
clean_pdf_df = data_cleaner.clean_card_data()
clean_pdf_df.head()
clean_pdf_df.info()

# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()

# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_df' to our initialised engine
database.upload_to_db(clean_pdf_df, table_name='dim_card_details')