from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# You need to reach out to AiCore requesting help on creating an instance to check your code

# Create an instance of the DatabaseConnector class
connector = DatabaseConnector()
# Initialise the database engine
connector_engine = connector.init_db_engine()
    
# Create an instance of the DataExtractor class, passing the DatabaseConnector as an argument
data_extractor = DataExtractor(db_connector=connector)

# Create an instance of the DataCleaning class, passing the DatabaseExtractor as an argument
data_cleaner = DataCleaning(data_extractor=data_extractor)

# The table that contains user data
legacy_users_table = 'legacy_users'

# Clean the user data
cleaned_users_df = data_cleaner.clean_user_data(legacy_users_table)
print(cleaned_users_df)

# I think it is only after this step, that you will then utilise a new variable which contains
# the object that will contain the database engine that we are uploading to!
# See below:
# Initialise the engine which we will upload our df to
# upload_engine = connector.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload cleaned user data to the 'dim_users' table
# This bit I'm stuck on now...
# connector.upload_to_db(cleaned_users_df, table_name='dim_users')


# Upload cleaned user data to the 'dim_users' table
# connector.upload_to_db(cleaned_users_df, table_name='dim_users')

