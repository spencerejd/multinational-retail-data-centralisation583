from data_extraction import DataExtractor
import pandas as pd
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

extractor = DataExtractor()

# s3_path = "s3://data-handling-public/date_details.json"
# date_details_df = extractor.extract_from_s3(s3_path)

# # Convert DataFrame into csv file stored locally
# date_details_df.to_csv('date_details_data.csv', index=False)

# Convert csv into DataFrame
date_details_df = pd.read_csv('date_details_data.csv')
print(date_details_df)
#print(date_details_df['time_period'].unique())

# Initialise our DataCleaning class
cleaner = DataCleaning(date_details_df)
# Clean our DataFrame
cleaned_date_details_df = cleaner.clean_date_details_data()
date_details_df.info()
print(date_details_df)
print(date_details_df['month'].unique())
date_details_df.info()

# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'clean2_products_df' to our initialised engine
database.upload_to_db(date_details_df, table_name='dim_date_times')