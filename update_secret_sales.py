import pandas as pd
import numpy as np
import requests
import copy
import json
import math
import concurrent.futures
import ulta_functions as ulta
import google_api_functions as gapi
import google_sheets_credentials as creds
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

session = requests.Session()
all_url_info = ulta.get_url_dict(session)
urls = all_url_info.keys()

products = {}
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(ulta.scrape_url, url, session, products, all_url_info): url for url in urls}
    for future in concurrent.futures.as_completed(futures):
        url = futures[future]
        try:
            data = future.result()
        except Exception as exc:
            print(url, ':', exc)
        else:
            products = data
            
session.close()

#loading in old data from yesterday
old_ulta_df = (
    pd.read_csv('data/ulta_df.csv')
    .rename(columns={'price' : 'old_price', 'sale' : 'old_sale', 'secret_sale' : 'old_secret_sale', 'options' : 'old_options'})
    .set_index('id')
    .loc[:, ['old_price', 'old_sale', 'old_secret_sale', 'old_options']]
)
old_secret_sales_in_stock = (
    pd.read_csv('data/secret_sales_in_stock.csv')
    .set_index('id')
    .groupby('id')
    .first()
)

#checking for products whose price has changed since yesterday
changed_prices_df = (
    pd.merge(ulta_df, old_ulta_df, on='id', how='inner')
    .dropna(subset=['price', 'old_price'])
    .query('price != old_price')
    .query('sale == 0 & old_sale == 0')
    .fillna(value={'old_options': ' ', 'options': ' '})
    .pipe(ulta.clean_changed_prices_df)
)

#getting products with different color options and more than one price listed
ulta_df_t = ulta_df.dropna(subset=['price', 'options'])
check_prices_df = (
    ulta_df_t[ulta_df_t['options'].str.contains("Colors") & ulta_df_t['price'].str.contains("-")]
    .query('sale == 0 & secret_sale == 0')
)

#getting products with .97 in their price
price_97_df = (
    ulta_df
    .query('secret_sale == 1 & sale == 0')
    .pipe(copy.deepcopy)
)

#putting them all together removing duplicates
secret_sales_df = (
    pd.concat([changed_prices_df, check_prices_df, price_97_df])
    .groupby('id')
    .first()
)

#making sure I'm not excluding any products in the google sheet
not_in_secret_sales_df = ulta.get_secret_sales_not_in_df(secret_sales_df, old_secret_sales_in_stock, ulta_df)
secret_sales_df = (
    pd.concat([secret_sales_df, not_in_secret_sales_df])
    .groupby('id')
    .first()
    .query('sale == 0')
)
secret_sales = (
    secret_sales_df
    .transpose()
    .pipe(pd.DataFrame.to_dict)
)

#finding out which products are in stock
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(r'C:\Users\elerm\Downloads\chromedriver_win32\chromedriver.exe', options = chrome_options)
products_in_stock, secret_sales = ulta.get_products_in_stock(secret_sales, driver)
driver.close()
driver.quit()

#df of products that are in stock
products_in_stock_df = (
    pd.DataFrame.from_dict(products_in_stock)
    .transpose()
    .reset_index()
    .rename(columns={'index' : 'id'})
    .pipe(pd.melt, id_vars=['id'], var_name='price2', value_name='options2')
    .dropna()
    .set_index('id')
)
#df of secret sales
secret_sales_df = (
    pd.DataFrame.from_dict(secret_sales)
    .transpose()
    .rename_axis('id')
)
#combining products_in_stock_df and secret_sales_df to get all the extra columns from secret_sales_df but only get the rows in common with products_in_stock_df
secret_sales_in_stock = (
    products_in_stock_df
    .pipe(pd.merge, secret_sales_df, on='id', how='left')
    .pipe(pd.merge, old_secret_sales_in_stock.rename(columns={'old_price' : 'old_secret_sales_old_price'})[['old_secret_sales_old_price']], on='id', how='left')
    .pipe(pd.merge, old_ulta_df.rename(columns={'old_price' : 'old_ulta_df_price'})[['old_ulta_df_price']], on='id', how='left')
    .rename(columns={'price' : 'ulta_df_price'})
    .fillna(value={'old_secret_sales_old_price': '$0.00', 'old_ulta_df_price': '$0.00', 'ulta_df_price': '$0.00', 'options': ' '})
    .pipe(ulta.add_old_price)
    .pipe(ulta.add_age)
    .drop(columns={'old_secret_sales_old_price', 'old_ulta_df_price', 'ulta_df_price', 'options', 'sale', 'secret_sale', 'sale_price'})
    .rename(columns={'price2' : 'price', 'options2' : 'options'})
    .pipe(ulta.convert_price_to_float)
    .pipe(ulta.add_precent_off)
    .query('percent_off != -1')
)
#list of hyperlinks; will use to populate google doc
hyperlink_urls = secret_sales_in_stock['url'].tolist()
#df that will be put on google doc
df = (
    secret_sales_in_stock
    .pipe(copy.deepcopy)
    .loc[:, ['main_category', 'sub_category', 'sub_sub_category', 'name', 'brand', 'product', 'price', 'old_price', 'percent_off', 'options', 'offers', 'rating', 'no_of_reviews', 'age']]
    .fillna(' ')
)

#update the sheet hosted on the mod's google drive
gapi.Create_Service(creds.get_credentials_file('main_mod'), creds.get_token_write_file('main_mod'), 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
gapi.Clear_Sheet(creds.get_sheet_id('main_mod'))
gapi.Export_Data_To_Sheets(creds.get_sheet_id('main_mod'), df)
gapi.Update_Filter(creds.get_sheet_id('main_mod'), creds.get_filter_id('main_mod'), len(df), len(df.columns))
gapi.Add_Hyperlinks(creds.get_sheet_id('main_mod'), df, hyperlink_urls)

#update the sheet hosted on my google drive
gapi.Create_Service(creds.get_credentials_file('main_local'), creds.get_token_write_file('main_local'), 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
gapi.Clear_Sheet(creds.get_sheet_id('main_local'))
gapi.Export_Data_To_Sheets(creds.get_sheet_id('main_local'), df)
gapi.Update_Filter(creds.get_sheet_id('main_local'), creds.get_filter_id('main_local'), len(df), len(df.columns))
gapi.Add_Hyperlinks(creds.get_sheet_id('main_local'), df, hyperlink_urls)

secret_sales_in_stock.to_csv('data/secret_sales_in_stock.csv')
ulta_df.to_csv('data/ulta_df.csv')