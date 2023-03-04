import re
import pandas as pd
import numpy as np

class DataCleaner:
    
    def replace_and_drop_null(self, df):
        
        '''It takes a dataframe as an argument, replaces the string 'NULL' with a null value, and then drops
        all rows with null values.
        
        Parameters
        ----------
        df
            the dataframe to be cleaned
        
        Returns
        -------
            The dataframe with the null values replaced with NaN and then dropped.
        '''
        
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        
        return df
    
    
    
    def drop_rows_containing_mask(self, df, column, exp):
        
        '''It takes a dataframe, a column name, and a regular expression, and returns a dataframe with the rows
        containing the regular expression dropped
        
        Parameters
        ----------
        df
            the dataframe you want to clean
        column
            the column name of the column you want to search
        exp
            the regular expression to search for
        
        Returns
        -------
            A dataframe with the rows containing the mask dropped. 
        '''
        
        mask = df[column].str.contains(exp)
        rows_to_drop = mask.index[mask].tolist()    
        df.drop(rows_to_drop, inplace=True)
        
        return df
    
    
    
    def clean_user_data(self, user_df):
        
        '''It replaces null values with the string 'unknown', drops rows containing digits in the first name
        column, converts the date of birth column to datetime, replaces '@@' with '@' in the email address
        column, replaces 'GG' with 'G' in the country code column, converts the country code column to a
        category, replaces certain characters in the phone number column, drops rows containing letters in
        the phone number column, converts the join date column to datetime, and replaces the country code in
        the phone number column with the appropriate country code
        
        Parameters
        ----------
        user_df
            the dataframe containing the user data
        
        Returns
        -------
            A dataframe with cleaned data.
        '''

        user_df = self.replace_and_drop_null(user_df)
        user_df = self.drop_rows_containing_mask(user_df, "first_name", "\d+")
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'])
        user_df['email_address'] = user_df['email_address'].str.replace('@@', '@')
        user_df['country_code'] = user_df['country_code'].str.replace('GG', 'G')
        user_df['country_code'] = user_df['country_code'].astype('category')
        replacements = {'\(0\)': '', '[\)\(\.\- ]' : '', '^\+': '00'}
        user_df['phone_number'] = user_df['phone_number'].replace(replacements, regex=True)
        user_df = self.drop_rows_containing_mask(user_df, "phone_number", "[a-zA-Z]")
        user_df['join_date'] = pd.to_datetime(user_df['join_date'])
        user_df['phone_number'] = user_df['phone_number'].str.replace('^00\d{2}', '', regex=True)
        code_dict = {'GB': '0044', 'US': '001', 'DE': '0049'}
        user_df['phone_number'] = user_df['phone_number'].apply(lambda x: code_dict.get(user_df.loc[user_df['phone_number']==x, 'country_code'].values[0], '') + x)
        user_df = user_df.reset_index(drop=True)
        
        return user_df        
    
    
    
    def clean_card_data(self, card_df):
        
        '''It takes a dataframe of credit card data, replaces null values with 'Unknown', drops null values,
        removes leading question marks from card numbers, removes non-numeric card numbers, converts the
        date column to a datetime object, converts the card number column to an integer, and converts the
        card provider column to a category
        
        Parameters
        ----------
        card_df
            the dataframe of card data
        
        Returns
        -------
            A dataframe with the cleaned data.
        '''

        card_df = self.replace_and_drop_null(card_df)
        card_df.dropna(inplace=True)
        card_df['card_number'] = card_df['card_number'].apply(lambda x: re.sub(r'^\?+', '', x) if isinstance(x, str) else x)
        card_df = card_df[card_df['card_number'].apply(lambda x: str(x).isdigit())]
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'])
        card_df['card_number'] = card_df['card_number'].astype('int')
        card_df['card_provider'] = card_df['card_provider'].astype('category')        
        card_df = card_df.reset_index(drop=True)
        
        return card_df



    def clean_store_data(self, store_df):
        
        '''The above function cleans the store data. It drops the lat column, replaces the NULL values with
        NaN, drops the NaN values, drops the rows that contain letters in the staff_numbers column, removes
        the ee from the continent column, converts the opening_date column to a datetime object, moves the
        latitude column to the third column, converts the longitude and latitude columns to floats, converts
        the staff_numbers column to an integer, converts the store_type and country_code columns to
        categories, and resets the index.
        
        Parameters
        ----------
        store_df
            the dataframe containing the store data
        
        Returns
        -------
            A dataframe with the cleaned data.
        '''
        
        store_df.drop('lat', axis=1, inplace=True)
        store_df = self.replace_and_drop_null(store_df)
        store_df = self.drop_rows_containing_mask(store_df, "staff_numbers", "[a-zA-Z]")
        store_df['continent'] = store_df['continent'].str.replace('ee', '')
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'])
        column_to_move = store_df.pop('latitude')
        store_df.insert(2, 'latitude', column_to_move)
        store_df['longitude'] = store_df['longitude'].astype('float')
        store_df['latitude'] = store_df['latitude'].astype('float')
        store_df['staff_numbers'] = store_df['staff_numbers'].astype('int')
        store_df['store_type'] = store_df['store_type'].astype('category')
        store_df['country_code'] = store_df['country_code'].astype('category')
        store_df = store_df.reset_index(drop=True)
        
        return store_df
    
        
    def convert_product_weights(self, weight):
        
        '''It takes a string as an input, and if it finds a certain pattern, it will convert the string into a
        float
        
        Parameters
        ----------
        weight
            the weight of the product
        
        Returns
        -------
            the weight of the product in kg.
        '''
        
        weight = str(weight)
        if re.search(r'kg\b', weight):
            x = re.sub("[\s,'kg']", "", weight)
            return float(x)
        elif re.search ('x', weight):
            weight = re.sub("x", "*", weight)
            y = re.sub("[\s,'g']", "", weight)
            z = eval(y)
            return float(z)/1000
        elif re.search (r'ml\b',weight):
            x = re.sub("[\s,'ml']", "", weight)
            return float(x)/1000
        elif re.search(r'g\b', weight):
            x = re.sub("[\s,'g']", "", weight)
            return float(x)/1000
        elif re.search(r'oz\b', weight):
            x = re.sub("[\s,'oz']", "", weight)
            return float(x)*0.0283495
        
        return weight
    
    def clean_products_data(self, product_df):
        
        '''It takes a dataframe of products, replaces all instances of 'NULL' with NaN, drops all rows with
        NaN, drops all rows with non-numeric characters in the product_price column, drops all rows with EAN
        codes longer than 13 characters, converts the date_added column to datetime, converts the weight
        column to float, removes the £ symbol from the product_price column, converts the product_price
        column to float, converts the category and removed columns to category, renames the weight and
        product_price columns to weight_kg and price_£, drops the Unnamed: 0 column, and resets the index
        
        Parameters
        ----------
        product_df
            The dataframe of products
        
        Returns
        -------
            A dataframe with the cleaned products data.
        '''
        
        product_df = self.replace_and_drop_null(product_df)
        product_df = self.drop_rows_containing_mask(product_df, "product_price", "[a-zA-Z]")
        product_df = product_df[product_df['EAN'].str.len() <= 13]
        product_df['date_added'] = pd.to_datetime(product_df['date_added'])
        product_df['weight'] = product_df['weight'].apply(self.convert_product_weights)
        product_df['weight'] = product_df['weight'].astype('float')
        product_df['product_price'] = product_df['product_price'].str.replace('£', '')
        product_df['product_price'] = product_df['product_price'].astype('float')
        product_df['category'] = product_df['category'].astype('category')
        product_df['removed'] = product_df['removed'].astype('category')
        product_df.rename(columns={'weight': 'weight_kg', 'product_price': 'price_£'}, inplace=True)
        product_df.drop('Unnamed: 0', axis=1, inplace=True)
        product_df = product_df.reset_index(drop=True)
        
        return product_df
        
        
        
    def clean_orders_data(self, order_df):
        
        '''It drops the first_name, last_name, and 1 columns from the order_df dataframe.
        
        Parameters
        ----------
        order_df
            the dataframe of orders
        
        Returns
        -------
            A dataframe with the columns first_name, last_name, and 1 dropped.
        '''
        
        order_df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        
        return order_df
    
    
    
    def clean_date_times_data(self, date_times_df):
        
        '''This function takes in a dataframe and returns a dataframe with the following changes:
        
        1. Replaces null values with the string "unknown"
        2. Drops rows containing the string "month"
        
        Parameters
        ----------
        date_times_df
            the dataframe containing the date and time columns
        
        Returns
        -------
            A dataframe with the cleaned data.
        '''
        
        date_times_df = self.replace_and_drop_null(date_times_df)
        date_times_df = self.drop_rows_containing_mask(date_times_df, "month", "[a-zA-Z]")  
        
        return date_times_df            