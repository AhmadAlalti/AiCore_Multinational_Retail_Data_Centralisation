import boto3
import requests
import tabula
import pandas as pd

class DataExtractor:
    
    def read_rds_table(self, db_connector, table_name):
        
        '''This function reads a table from a database and returns a pandas dataframe
        
        Parameters
        ----------
        db_connector
            the database connector object
        table_name
            The name of the table you want to read from.
        
        Returns
        -------
            A dataframe
        '''
        
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine, index_col='index')
        
        return df
    
    
    
    def retrieve_pdf_data(self, link):
        
        '''This function takes a link to a PDF file and returns a pandas dataframe of the data in the PDF
        file
        
        Parameters
        ----------
        link
            the link to the pdf file
        
        Returns
        -------
            A dataframe
        '''
        
        df = pd.concat(tabula.read_pdf(link, pages='all'), ignore_index=True)
        
        return df
    
    
    
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        
        '''This function takes in a URL and a dictionary of headers, and returns the number of stores in the
        database.
        
        Parameters
        ----------
        num_stores_endpoint
            The endpoint for the number of stores.
        header_dict
            a dictionary of the headers that will be sent with the request
        
        Returns
        -------
            The number of stores in the database.
        '''
        
        response = requests.get(num_stores_endpoint, headers=header_dict)
        data = response.json()
        number_of_stores = data['number_stores']
        
        return number_of_stores
    
    
    
    def retrieve_stores_data(self, num_stores_endpoint, stores_endpoint, header_dict):
        
        '''This function takes in the number of stores endpoint, the stores endpoint, and the header
        dictionary, and returns a dataframe of all the stores data.
        
        Parameters
        ----------
        num_stores_endpoint
            the endpoint that returns the number of stores
        stores_endpoint
            the endpoint for the stores data
        header_dict
            a dictionary of the headers that are required to access the API
        
        Returns
        -------
            A dataframe with all the stores data.
        '''
        
        number_of_stores = self.list_number_of_stores(num_stores_endpoint, header_dict)
        
        for store_number in range(number_of_stores):
            
            if store_number == 0:
                response = requests.get(f'{stores_endpoint}/{store_number}', headers=header_dict)
                data = response.json()
                columns = list(data.keys())
                df = pd.DataFrame(data, columns=columns, index=[0])
                df.set_index("index", inplace=True)
                dfs = []
                dfs.append(df)
            else:
                response = requests.get(f'{stores_endpoint}/{store_number}', headers=header_dict)
                data = response.json()                
                df = pd.DataFrame(data, index=[0])
                df.set_index("index", inplace=True)
                dfs.append(df)
        
        df = pd.concat(dfs, ignore_index=True)
        
        return df
    
    
    
    def extract_from_s3(self, address):
        
        '''This function takes in a string that is the address of a csv file in an S3 bucket, and returns a
        pandas dataframe of the csv file
        
        Parameters
        ----------
        address
            the address of the file you want to extract
        
        Returns
        -------
            A dataframe
        '''
        
        client = boto3.client('s3')
        df = pd.read_csv(address)
        
        return df
    
    def extract_json_data(self, link):
        
        '''It takes a link as an input, makes a request to that link, and returns a dataframe of the json data.
        
        Parameters
        ----------
        link
            the link to the API
        
        Returns
        -------
            A dataframe
        '''
        
        response = requests.get(link)
        data = response.json()
        df = pd.DataFrame.from_dict(data)
        
        return df
        