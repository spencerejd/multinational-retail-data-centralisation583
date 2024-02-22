from scripts.database_utils import DatabaseConnector
from scripts.data_extraction import DataExtractor
from scripts.data_cleaning import DataCleaning

# Initialise a DatabaseConnector instance
database = DatabaseConnector()

# Initialise engine with yaml_file_path set to 'default'
db_engine = database.init_db_engine()
# Retrieve list of tables contained within database
table_names = database.list_db_tables()

# Extract orders data from table to return Pandas DataFrame

# Initialise a DataExtractor instance
extractor = DataExtractor()

orders_df = extractor.read_rds_table(db_engine, 'orders_table')

# Initialise DataCleaning, passing df as an argument
cleaner = DataCleaning(orders_df)
cleaned_orders_df = cleaner.clean_orders_data()

# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_orders_df' to our initialised engine
database.upload_to_db(cleaned_orders_df, table_name='orders_table')