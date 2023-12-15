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

            print(f"Data extracted from the '{table_name}' table and into a DataFrame.")
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return pd.DataFrame() # Return an empty DataFrame if error