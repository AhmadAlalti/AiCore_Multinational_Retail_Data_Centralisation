from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaner

def main():
    dbcon = DatabaseConnector()
    dbex = DataExtractor()
    dbclean = DataCleaner()
    
    user_df = dbex.read_rds_table(dbcon, 'legacy_users')
    clean_user_df = dbclean.clean_user_data(user_df)
    dbcon.upload_to_db(clean_user_df, 'dim_users')
    
    card_df = dbex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    clean_card_df = dbclean.clean_card_data(card_df)
    dbcon.upload_to_db(clean_card_df, 'dim_card_details')
    
    api_creds = dbcon.read_db_credentials('api_creds.yaml')
    store_df = dbex.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', api_creds)
    clean_store_df = dbclean.clean_store_data(store_df)
    dbcon.upload_to_db(clean_store_df, 'dim_store_details')
    
    product_df = dbex.extract_from_s3('s3://data-handling-public/products.csv')
    clean_product_df = dbclean.clean_products_data(product_df)
    dbcon.upload_to_db(clean_product_df, 'dim_products')

    order_df = dbex.read_rds_table(dbcon, 'orders_table')
    clean_order_df = dbclean.clean_orders_data(order_df)
    dbcon.upload_to_db(clean_order_df, 'orders_table')
    
    date_times_df = dbex.extract_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    clean_date_times_df = dbclean.clean_date_times_data(date_times_df)
    dbcon.upload_to_db(clean_date_times_df, 'dim_date_times')
    
main()
    