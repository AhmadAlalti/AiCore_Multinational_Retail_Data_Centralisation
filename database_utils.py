import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    
    def read_db_credentials(self, file):
        
        '''It reads a YAML file and returns a dictionary
        
        Parameters
        ----------
        file
            The path to the YAML file containing the database credentials.
        
        Returns
        -------
            A dictionary of the credentials.
        '''
        
        with open(file, 'r') as file:
            creds_dict = yaml.safe_load(file)
        
        return creds_dict
    
    
    
    def init_db_engine(self):
        
        '''This function reads the database credentials from a yaml file, and then uses those credentials to
        create a database engine
        
        Returns
        -------
            An engine object.
        '''
        
        creds_dict = self.read_db_credentials('db_creds.yaml')
        database_type = 'postgresql'
        dbapi = 'psycopg2'
        host = creds_dict['RDS_HOST']
        user = creds_dict['RDS_USER']
        password = creds_dict['RDS_PASSWORD']
        database = creds_dict['RDS_DATABASE']
        port = creds_dict['RDS_PORT']
        engine = create_engine(f'{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}')
        engine.connect()
        
        return engine
    
    
    
    def list_db_tables(self):
        
        '''This function returns a list of all the tables in the database
        
        Returns
        -------
            A list of all the tables in the database.
        '''
        
        engine = self.init_db_engine()
        inspector = inspect(engine)
        
        return inspector.get_table_names()



    def upload_to_db(self, df, table_name):
        
        '''This function takes a dataframe and a table name as arguments, reads the database credentials from a
        yaml file, creates an engine, and uploads the dataframe to the database.
        
        Parameters
        ----------
        df
            the dataframe you want to upload
        table_name
            the name of the table you want to create in the database
        '''
        
        creds_dict = self.read_db_credentials('local_db_creds.yaml')
        database_type = 'postgresql'
        dbapi = 'psycopg2'
        host = creds_dict['HOST']
        user = creds_dict['USER']
        password = creds_dict['PASSWORD']
        database = creds_dict['DATABASE']
        port = 5432
        engine = create_engine(f'{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}')
        engine.connect()
        df.to_sql(name=table_name, con=engine, if_exists='replace')