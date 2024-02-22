# Import the three project Classes for extracting, cleaning and uploading data to local database and other necessary modules

from scripts.database_utils import DatabaseConnector
from scripts.data_cleaning import DataCleaning
from scripts.data_extraction import DataExtractor
import yaml
import psycopg2


# Create and upload the DataFrame which will contain the user data

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# Initialise engine with yaml_file_path set to 'default'
database_engine = database.init_db_engine()
# Create an instance of the DataExtractor class
extractor = DataExtractor()
# Extract data from table 'legacy_users' and store it in a DataFrame
df = extractor.read_rds_table(database_engine, 'legacy_users')

# Clean the user DataFrame

# Create an instance of the DataCleaning class, with our DataFrame passed as an argument
cleaner = DataCleaning(df)
# Clean the DataFrame
cleaned_df = cleaner.clean_user_data(df)

# Upload the cleaned user DataFrame to the local database

# Now let's initialise the engine that we will upload our DataFrame to
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_df' to our initialised engine
database.upload_to_db(cleaned_df, table_name='dim_users')


# Create and upload the DataFrame that will contain card_details

# Create an instance of the DataExtractor class
extractor = DataExtractor()

# Set file path of pdf
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# Convert pdf into DataFrame
pdf_df = extractor.retrieve_pdf_data(pdf_path)

# Create an instance of the DataCleaning class, passing DataFrame as an argument
cleaner = DataCleaning(pdf_df)
# Clean the DataFrame
clean_pdf_df = cleaner.clean_card_data()

# Now initialise the engine that we will upload DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()

# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned__pdf_df' to our initialised engine
database.upload_to_db(clean_pdf_df, table_name='dim_card_details')


# Create and upload the DataFrame which will contain the store_details

# Define the base URL for the API endpoint that retrieves details of a specific store
base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
# Define the headers for the API request, including the API key for authentication
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Use the retrieve_stores_data method to fetch data for all stores and save to DataFrame
# This method iterates over store numbers, makes API calls for each, and collects the data
stores_df = extractor.retrieve_stores_data(base_url, headers)

# Create an instance of the DataCleaning class, passing store_details DataFrame as an argument
cleaner = DataCleaning(stores_df)

# Clean the stores_df DataFrame
clean_stores_df = cleaner.clean_store_data()

# Now let's initialise the engine that we will upload our store_details DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'clean_stores_df' to our initialised engine
database.upload_to_db(clean_stores_df, table_name='dim_store_details')


# Create and upload the DataFrame which will contain the products data

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

# Create and upload the DataFrame which will contain the orders data

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

# Initialise DataCleaning, passing orders_df as an argument
cleaner = DataCleaning(orders_df)
cleaned_orders_df = cleaner.clean_orders_data()

# Now let's initialise the engine that we will upload our DataFrame to

# Create an instance of the DatabaseConnector class
database = DatabaseConnector()
# We will set the 'yaml_file_path' to ensure the connection is made to the upload database engine
upload_engine = database.init_db_engine(yaml_file_path='rds_upload_db_creds.yaml')

# Upload 'cleaned_orders_df' to our initialised engine
database.upload_to_db(cleaned_orders_df, table_name='orders_table')


# Create and upload the DataFrame which will contain date_times data

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



# Create the database schema

# Cast the columns of the orders_table to correct data types
# Load database credentials from YAML file
with open('rds_upload_db_creds.yaml', 'r') as file:
    creds = yaml.safe_load(file)

try:
    # Establish a database connection
    connection = psycopg2.connect(
        host=creds['RDS_HOST'],
        user=creds['RDS_USER'],
        password=creds['RDS_PASSWORD'],
        database=creds['RDS_DATABASE']
    )
    # Check if the connection was successful
    if connection:
        print (f"Connected to {creds['RDS_DATABASE']} database")
    
    # Modify orders_table
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT MAX(LENGTH(card_number)) AS max_card_number_length,
                   MAX(LENGTH(store_code)) AS max_store_code_length,
                   MAX(LENGTH(product_code)) AS max_product_code_length
            FROM orders_table
        """)
        max_lengths = cursor.fetchone()
        print(max_lengths)

    with connection.cursor() as cursor:
        # Execute ALTER TABLE commands
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;")
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;")
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(%s);", (max_lengths[0],))
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(%s);", (max_lengths[1],))
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(%s);", (max_lengths[2],))
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;")


    # Modify dim_users table

    with connection.cursor() as cursor:
        # Execute ALTER TABLE commands
        alter_commands = [
            "ALTER TABLE dim_users ALTER COLUMN first_name TYPE VARCHAR(255);",
            "ALTER TABLE dim_users ALTER COLUMN last_name TYPE VARCHAR(255);",
            "ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;",
            "ALTER TABLE dim_users ALTER COLUMN country_code TYPE VARCHAR(2);",
            "ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;",
            "ALTER TABLE dim_users ALTER COLUMN join_date TYPE DATE USING join_date::DATE;"
        ]
        for command in alter_commands:
            cursor.execute(command)

    # Modify dim_store_details table
        
    with connection.cursor() as cursor:
        # Merge latitude columns and drop 'lat' column
        #
        cursor.execute("""
            UPDATE dim_store_details
            SET latitude = COALESCE(latitude, lat);
            ALTER TABLE dim_store_details DROP COLUMN lat;
        """)

        # Alter column data types
        cursor.execute("""
            ALTER TABLE dim_store_details
            ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
            ALTER COLUMN locality TYPE VARCHAR(255),
            ALTER COLUMN store_code TYPE VARCHAR(255),
            ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
            ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
            ALTER COLUMN store_type TYPE VARCHAR(255),
            ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
            ALTER COLUMN country_code TYPE VARCHAR(255),
            ALTER COLUMN continent TYPE VARCHAR(255);
        """)

    # Handle NULL Values
    with connection.cursor() as cursor:
        # Update string columns
        text_columns_to_update = ['locality', 'store_code', 'store_type', 'country_code', 'continent']
        for column in text_columns_to_update:
            cursor.execute(f"""
                UPDATE dim_store_details
                SET {column} = COALESCE({column}, 'N/A')
                WHERE {column} IS NULL;
            """)
        
        # Update numeric columns with a default value
        cursor.execute("""
            UPDATE dim_store_details
            SET longitude = COALESCE(longitude, 0.0),
                latitude = COALESCE(latitude, 0.0)
            WHERE longitude IS NULL OR latitude IS NULL;
        """)

        # Commit the transaction
        connection.commit()
        print("Task 1: Data types in orders_table altered successfully.")
    

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the connection
    if connection:
        connection.close()
        
        print("Connection closed")


