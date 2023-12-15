import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self, yaml_file_path='db_creds.yaml'):
        '''
        Initialise the DatabaseConnector class.
        
        '''
        self.yaml_file_path = yaml_file_path
        self.connection = None
        self.credentials = self.read_db_creds()

    
    def read_db_creds(self):
        '''
        Read database credentials from the 'db_creds.yaml file'
        and return a dictionary.

        Returns:
            dict: a dictionary containing all database credentials        
        '''

        yaml_file_path = 'db_creds.yaml'
        try:
            with open(self.yaml_file_path, 'r') as file:
                credentials = yaml.safe_load(file)
                return credentials
        except FileNotFoundError:
            print(f"Error: YAML file not found at {yaml_file_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return {}
    
    def init_db_engine(self):
        '''
        Read the credentials returned from read_db_creds
        and initialise and return an SQLAlchemy database engine

        Returns:
        Database engine
        '''
        credentials = self.read_db_creds()

        if not credentials:
            print("Failed to initialise database engine. Missing or invalid credentials.")
            return None
        
        try:
            engine = create_engine(f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")
            print("Database engine initialised successfully.")
            self.connection = engine
            return engine
        except Exception as e:
            print(f"Error initialising database engine: {e}")
            return None
    
    def list_db_tables(self):
        '''
        List all the tables found in the database
        Returns:
            list: a list of all tables found in database
        '''
        if not self.connection:
            print("Error: Database connection not established.")
            return []
        
        try:
            #Create an Inspector and bind it to existing engine
            inspector = inspect(self.connection)
            table_names = inspector.get_table_names()

            # Get a list of all table names in the connected database
            if not table_names:
                print("No tables found in the database.")
            else:
                print("Tables in the database:")
                for table_name in table_names:
                    print(f"- {table_name}")

            return table_names
        
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []
        
    def upload_to_db(self, df, table_name):
        '''
        Uploads a Pandas DataFrame to the specified table in the database.
        
        Parameters:
            df (pd.DataFrame):
                The DataFrame to be uploaded.
            table_name (str):
                The name of the table to upload the data to.
        '''
        if not self.connection:
            print("Error: Database connection not established.")
            return
        
        try:
            # Use the Pandas to_sql method to upload DataFrame to the specified table
            df.to_sql(name=table_name, con=self.connection, index=False, if_exists='replace')
            print(f"Data uploaded to the '{table_name}' table successfully.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")


        






connector = DatabaseConnector()
credentials = connector.init_db_engine()
credentials = connector.list_db_tables()