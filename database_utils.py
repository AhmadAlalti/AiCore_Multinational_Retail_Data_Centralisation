import psycopg2
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    
    def read_db_credentials(self, file):
        with open(file, 'r') as file:
            creds_dict = yaml.safe_load(file)
        return creds_dict
    
    def init_db_engine(self):
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
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
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

if __name__=="__main__":
    databse_connector = DatabaseConnector()
    creds_dict = databse_connector.read_db_credentials('db_creds.yaml')
    engine = databse_connector.init_db_engine()
    db_tables = databse_connector.list_db_tables()
    print(db_tables)    