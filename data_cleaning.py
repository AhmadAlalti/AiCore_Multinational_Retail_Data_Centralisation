import datetime as dt
import pandas as pd
import numpy as np
import re
from data_extraction import DataExtractor

class DataCleaner:
    def clean_user_data(self, user_df):
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
        order_df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        return order_df
    
    def clean_date_times_data(self, date_times_df):
        date_times_df.replace('NULL', np.nan, inplace=True)
        date_times_df.dropna(inplace=True)        
        mask = date_times_df['month'].str.contains('[a-zA-Z]')
        rows_to_drop = mask.index[mask].tolist()
        date_times_df.drop(rows_to_drop, inplace=True)
        return date_times_df            