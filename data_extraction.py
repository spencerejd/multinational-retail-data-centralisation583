import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """
        Extract data from the specified RDS database table to a pandas DataFrame.

        Parameters:
            db_connector (DatabaseConnector):
                An instance of the DatabaseConnector class.
            table_name (str):
                The name of the table to extract data from.
        
        Returns:
            pd.DataFrame:
                A pandas DataFrame containing the data from the specified table.
        """

        db_engine = self.db_connector.init_db_engine()

        if not db_engine:
            print("Failed to initialise the database engine.")
            return pd.DataFrame() # Return empty DataFrame if connection fails
        
        try:
            # Use pandas to read table into a DataFrame
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql_query(query, db_engine)

            print(f"Data extracted from the '{table_name}' table.")
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return pd.DataFrame() # Return an empty DataFrame if error


# Create an instance of the DatabaseConnector class
connector = DatabaseConnector()

    
# Create an instance of the DataExtractor class, passing the DatabaseConnector as an argument
data_extractor = DataExtractor(db_connector=connector)
    
# Use the DatabaseConnector to get the list of tables
tables = connector.list_db_tables()

# The table that contains user data
legacy_users_table = 'legacy_users'
# Use the DataExtractor class to read data from the 'legacy_user' table
legacy_users_df = data_extractor.read_rds_table(legacy_users_table)

#print(legacy_users_df.head())

legacy_users_df.info()
