from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# Initialise engine with yaml_file_path set to 'default'
database_engine = database.init_db_engine()
# Create an instance of the DataExtractor class, with our initialised engine passed as an argument
data_extractor = DataExtractor(database_engine)
# Extract data from table 'legacy_users' and store it in a DataFrame
df = data_extractor.read_rds_table(database_engine, 'legacy_users')
# Create an instance of the DataCleaning class, with our DataFrame passed as an argument
data_cleaner = DataCleaning(df)
# Clean the DataFrame
cleaned_df = data_cleaner.clean_user_data(df)

# Now let's initialise the engine that we will upload our DataFrame to

# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_df' to our initialised engine
database.upload_to_db(cleaned_df, table_name='dim_users')
