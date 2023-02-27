import re
import pandas as pd
import numpy as np

class DataCleaner:
    
    def clean_user_data(self, user_df):
        
        '''It takes a dataframe as input, replaces all the NULL values with NaN, drops all the NaN values,
        drops all the rows where the first name contains a number, converts the date of birth column to
        datetime, replaces all the @@ in the email address column with @, replaces all the GG in the country
        code column with G, converts the country code column to a category, replaces all the phone numbers
        with the correct format, drops all the rows where the phone number contains a letter, converts the
        join date column to datetime, replaces all the phone numbers with the correct country code, and
        returns the cleaned dataframe.
        
        Parameters
        ----------
        user_df
            the dataframe containing the user data
        
        Returns
        -------
            A dataframe with cleaned data.
        '''
        
        user_df.replace('NULL', np.nan, inplace=True)
        user_df.dropna(inplace=True)
        mask = user_df["first_name"].str.contains('\d+')
        rows_to_drop = mask.index[mask].tolist()    
        user_df.drop(rows_to_drop, inplace=True)
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'])
        user_df['email_address'] = user_df['email_address'].str.replace('@@', '@')
        user_df['country_code'] = user_df['country_code'].str.replace('GG', 'G')
        user_df['country_code'] = user_df['country_code'].astype('category')
        replacements = {'\(0\)': '', '[\)\(\.\- ]' : '', '^\+': '00'}
        user_df['phone_number'] = user_df['phone_number'].replace(replacements, regex=True)
        mask2 = user_df['phone_number'].str.contains('[a-zA-Z]')
        rows_to_drop2 = mask2.index[mask2].tolist()
        user_df.drop(rows_to_drop2, inplace=True)
        user_df['join_date'] = pd.to_datetime(user_df['join_date'])
        user_df['phone_number'] = user_df['phone_number'].str.replace('^00\d{2}', '', regex=True)
        code_dict = {'GB': '0044', 'US': '001', 'DE': '0049'}
        user_df['phone_number'] = user_df['phone_number'].apply(lambda x: code_dict.get(user_df.loc[user_df['phone_number']==x, 'country_code'].values[0], '') + x)
        user_df = user_df.reset_index(drop=True)
        
        return user_df        
    
    
    
    def clean_card_data(self, card_df):
        
        '''It takes a dataframe of credit card data, replaces all instances of 'NULL' with NaN, drops all rows
        with NaN, removes all non-digit characters from the card number column, removes all rows with
        non-digit characters in the card number column, converts the date_payment_confirmed column to
        datetime, converts the card_number column to int, and converts the card_provider column to category
        
        Parameters
        ----------
        card_df
            the dataframe containing the card data
        
        Returns
        -------
            A dataframe with the cleaned data.
        '''
        
        card_df.replace('NULL', np.nan, inplace=True)
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
        store_df.replace('NULL', np.nan, inplace=True)
        store_df.dropna(inplace=True)
        mask = store_df['staff_numbers'].str.contains('[a-zA-Z]')
        rows_to_drop = mask.index[mask].tolist()
        store_df.drop(rows_to_drop, inplace=True)
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
        
        product_df.replace('NULL', np.nan, inplace=True)
        product_df.dropna(inplace=True)
        mask = product_df['product_price'].str.contains('[a-zA-Z]')
        rows_to_drop = mask.index[mask].tolist()
        product_df.drop(rows_to_drop, inplace=True)
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
        date_times_df.replace('NULL', np.nan, inplace=True)
        date_times_df.dropna(inplace=True)        
        mask = date_times_df['month'].str.contains('[a-zA-Z]')
        rows_to_drop = mask.index[mask].tolist()
        date_times_df.drop(rows_to_drop, inplace=True)
        return date_times_df            