import pandas as pd
import requests
import copy
import concurrent.futures
import ulta_functions as ulta
import google_api_functions as gapi
import google_sheets_credentials as creds
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

print("\nstarting...\n")

session = requests.Session()
all_url_info = {}
products = {}

all_url_info = ulta.get_url_dict(session)
urls = list(all_url_info.keys())

print('scraping ulta...')
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

print('data cleaning...')
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

print('getting available options...')
#starting a headless selenium driver
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(r'C:\Users\elerm\Downloads\chromedriver_win32\chromedriver.exe', chrome_options = chrome_options)
products_in_stock = {}

products_in_stock, secret_sales = ulta.get_products_in_stock(drop_na_options, secret_sales, driver)

driver.close()
driver.quit()

print('more data cleaning...')
products_in_stock_df = pd.DataFrame.from_dict(products_in_stock).transpose().reset_index().rename(columns={'index' : 'name'})
products_in_stock_df = pd.melt(products_in_stock_df, id_vars=['name'], var_name='price2', value_name='options2').dropna().set_index('name')
secret_sales_df = secret_sales_df.join(products_in_stock_df).reset_index().rename(columns={'index' : 'name'}).rename(columns = {'price' : 'price1'})

#combining the og price column from the ulta_df and the price column from the products_in_stock df to get a more alpha
#column with more accurate data muwhahaha
#(basically I want every single price option to have its own column so I can then make the price column numerical instead of 
#a string/character)
price = []
for i in range(len(secret_sales_df)):
    if '-' in secret_sales_df.iloc[i]['price1']:
        price.append(secret_sales_df.iloc[i]['price2'])
    elif '$' in secret_sales_df.iloc[i]['price1']:
        price.append(secret_sales_df.iloc[i]['price1'][1:])

secret_sales_df['price'] = pd.to_numeric(price)
secret_sales_df = secret_sales_df.drop(secret_sales_df[(secret_sales_df['options'] != ' ') & (secret_sales_df['options2'] == ' ')].index)
secret_sales_df = secret_sales_df.drop(columns={'price1', 'price2', 'options'}).rename(columns={'options2' : 'options'})
df = secret_sales_df.fillna(' ').reset_index().rename(columns={'desc' : 'product'})
#selecting the columns I want in the google sheet as well as the order in which they appear
df = df[['main_category', 'sub_category', 'sub_sub_category', 'name', 'brand', 'product', 'price', 'options', 'offers', 'rating', 'number_of_reviews', 'url']]

print("updating sheet hosted on mod's google drive...")
#update the sheet hosted on the mod's google drive
gapi.Create_Service(creds.get_credentials_file('main_mod'), creds.get_token_write_file('main_mod'), 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
gapi.Clear_Sheet(creds.get_sheet_id('main_mod'))
gapi.Export_Data_To_Sheets(creds.get_sheet_id('main_mod'), df)
gapi.Update_Filter(creds.get_sheet_id('main_mod'), creds.get_filter_id('main_mod'), len(df), len(df.columns))

print('updating sheet hosted on my google drive...')
#update the sheet hosted on my google drive
gapi.Create_Service(creds.get_credentials_file('main_local'), creds.get_token_write_file('main_local'), 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
gapi.Clear_Sheet(creds.get_sheet_id('main_local'))
gapi.Export_Data_To_Sheets(creds.get_sheet_id('main_local'), df)
gapi.Update_Filter(creds.get_sheet_id('main_local'), creds.get_filter_id('main_local'), len(df), len(df.columns))

#saving it as a .csv for funsies/ just in case idk
df.to_csv('current_data.csv')

print('DONE')