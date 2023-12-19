import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
import tabula

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_engine, table_name):
        """
        Extract data from the specified RDS database table to a pandas DataFrame.

        Parameters:
            db_connector (DatabaseConnector):
                An instance of the DatabaseConnector class.
            db_engine:
                A SQLAlchemy database engine
            table_name (str):
                The name of the table to extract data from.
        
        Returns:
            pd.DataFrame:
                A pandas DataFrame containing the data from the specified table.
        """

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
        

    def retrieve_pdf_data(self, pdf_path):
        '''
        Retrieve data from a pdf and return a pandas DataFrame.
        '''

        if not pdf_path:
            print("Failed to find pdf")
            return pd.DataFrame()   # Return empty DataFrame
        
        try:
            # Set JVM options for tabula-py
            java_options = "-Djava.awt.headless=true -XX:ReservedCodeCacheSize=1000m"
            tabula._java_options = java_options.split()

            # read_pdf returns list of DataFrames
            # pages='all' to extract tables from all pages
            df = tabula.read_pdf(pdf_path, stream=True, pages='all')

            # Concatenate the list of DataFrames into a single DataFrame
            result_df = pd.concat(df, ignore_index=True)
            print("Returning DataFrame from the given pdf")
            return result_df
        except Exception as e:
            print(f"Error reading pdf: {e}")
            return pd.DataFrame() # Return empty DataFrame if error


if __name__ == '__main__':
    data_extractor = DataExtractor()
    pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    pdf_df = data_extractor.retrieve_pdf_data(pdf_path)
    #print(pdf_df)
    print(pdf_df.describe())
