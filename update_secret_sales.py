import pandas as pd
import requests
import copy
import json
import concurrent.futures
import ulta_functions as ulta
import google_api_functions as gapi
import google_sheets_credentials as creds
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

session = requests.Session()
all_url_info = {}
products = {}

#I saved it to a file so I wouldn't have to waste time making requests to get the same data
f = open("data/all_url_info_dict.json","r")
all_url_info = json.loads(f.read())
f.close()

urls = list(all_url_info.keys())

#I'm using threading to make the code run faster
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

#cleaning the data
#df of every product on ulta's website
ulta_df = pd.DataFrame.from_dict(products).transpose()
#df of only the secret sales
secret_sales_df = copy.deepcopy(ulta_df.query('secret_sale == 1 & sale == 0'))
#I'm dropping the columns that have NA in in the options column so I know which products have multiple options so I can 
#check which of those options are in stock
drop_na_options = pd.DataFrame.to_dict(secret_sales_df.dropna(subset=['options']).transpose())
#a couple of the items had an incorrect url for some reason so I turned secret_sales back into a dictionary so that,
#when I need to update a product's url, it's easier for me to do so. you could probably do this using the dataframe 
#instead but I didn't feel like it
secret_sales = pd.DataFrame.to_dict(secret_sales_df.transpose())

#starting a headless selenium driver
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(r'C:\Users\elerm\Downloads\chromedriver_win32\chromedriver.exe', options = chrome_options)
products_in_stock = {}

products_in_stock, secret_sales = ulta.get_products_in_stock(secret_sales, driver)

driver.close()
driver.quit()

products_in_stock_df = pd.DataFrame.from_dict(products_in_stock).transpose().reset_index().rename(columns={'index' : 'name'})
products_in_stock_df = pd.melt(products_in_stock_df, id_vars=['name'], var_name='price2', value_name='options2').dropna().set_index('name')
secret_sales_df = pd.DataFrame.from_dict(secret_sales).transpose()

#getting closer...
secret_sales_in_stock = products_in_stock_df.join(secret_sales_df)
secret_sales_in_stock = secret_sales_in_stock.drop(columns={'price', 'options'}).reset_index().rename(columns={'price2' : 'price', 'options2' : 'options', 'desc' : 'product', 'index' : 'name'})
secret_sales_in_stock['price'] = pd.to_numeric(secret_sales_in_stock['price'])

#final dataframe!!! yay!!!!!
df = secret_sales_in_stock[['main_category', 'sub_category', 'sub_sub_category', 'name', 'brand', 'product', 'price', 'options', 'offers', 'rating', 'number_of_reviews', 'url']].fillna(' ')

#update the sheet hosted on the mod's google drive
gapi.Create_Service(creds.get_credentials_file('main_mod'), creds.get_token_write_file('main_mod'), 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
gapi.Clear_Sheet(creds.get_sheet_id('main_mod'))
gapi.Export_Data_To_Sheets(creds.get_sheet_id('main_mod'), df)
gapi.Update_Filter(creds.get_sheet_id('main_mod'), creds.get_filter_id('main_mod'), len(df), len(df.columns))

#update the sheet hosted on my google drive
gapi.Create_Service(creds.get_credentials_file('main_local'), creds.get_token_write_file('main_local'), 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
gapi.Clear_Sheet(creds.get_sheet_id('main_local'))
gapi.Export_Data_To_Sheets(creds.get_sheet_id('main_local'), df)
gapi.Update_Filter(creds.get_sheet_id('main_local'), creds.get_filter_id('main_local'), len(df), len(df.columns))

#saving it as a .csv for funsies/ just in case idk
#secret_sales_in_stock.to_csv('data/secret_sales_in_stock.csv')
#ulta_df.to_csv('data/ulta_df.csv')