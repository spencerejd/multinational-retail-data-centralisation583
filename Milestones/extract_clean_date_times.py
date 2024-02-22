from scripts.data_extraction import DataExtractor
from scripts.data_cleaning import DataCleaning
from scripts.database_utils import DatabaseConnector

# Initialise the DataExtractor class
extractor = DataExtractor()

s3_path = "s3://data-handling-public/date_details.json"
date_details_df = extractor.extract_from_s3(s3_path)

# Initialise DataCleaning class
cleaner = DataCleaning(date_details_df)
# Clean date_details_df DataFrame
cleaned_date_details_df = cleaner.clean_date_details_data()

# Now let's initialise the engine that we will upload the cleaned_date_details_df DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_date_details_df' to our initialised engine
database.upload_to_db(date_details_df, table_name='dim_date_times')