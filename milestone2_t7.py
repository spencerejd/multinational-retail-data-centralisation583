from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd


# # Initialise a DatabaseConnector instance
# db_connector = DatabaseConnector()

# # Initialise engine with yaml_file_path set to 'default'
# db_engine = db_connector.init_db_engine()
# # Retrieve list of tables contained within database
# table_names = db_connector.list_db_tables()

# Extract orders data from table to return Pandas DataFrame

# # Initialise a DataExtractor instance
# data_extractor = DataExtractor()

# orders_df = data_extractor.read_rds_table(db_engine, 'orders_table')

# orders_df.to_csv('orders_data.csv', index=False)

orders_df = pd.read_csv('orders_data.csv')

# Initialise DataCleaning, passing df as an argument
cleaner = DataCleaning(orders_df)
cleaned_orders_df = cleaner.clean_orders_data()

print(cleaned_orders_df)
cleaned_orders_df.info()
print("Unique product quantities:", cleaned_orders_df['product_quantity'].unique())


# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'clean2_products_df' to our initialised engine
database.upload_to_db(cleaned_orders_df, table_name='orders_table')