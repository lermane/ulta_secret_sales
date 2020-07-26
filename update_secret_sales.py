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
ulta_df = pd.DataFrame.from_dict(products).transpose()
#loading in yesterday's data to check for price changes
old_ulta_df = pd.read_csv('data/ulta_df.csv').rename(columns={'price' : 'old_price', 'sale' : 'old_sale', 'secret_sale' : 'old_secret_sale'}).set_index('id')
old_ulta_df = old_ulta_df[['old_price', 'old_sale', 'old_secret_sale']]
changed_prices_df = pd.merge(ulta_df.reset_index().rename(columns={'index' : 'name'}).set_index('id'), old_ulta_df, on='id', how='inner').query('price != old_price').dropna(subset=['price', 'old_price']).query('sale == 0 & old_sale == 0')

df = copy.deepcopy(changed_prices_df)
for i in range(len(changed_prices_df)):
    #checking if an item that was on sale yesterday is still on sale and dropping those that aren't
    if '-' in changed_prices_df.iloc[i]['old_price'] and '-' not in changed_prices_df.iloc[i]['price'] and changed_prices_df.iloc[i]['old_price'].split(' - ')[1] == changed_prices_df.iloc[i]['price']:
        df = df.drop([changed_prices_df.iloc[i].name])
    #if the new price is a hyphenated price and the old price is not, make sure that the second price in the hyphenated one is actually less than the unhyphenated price to make sure it's actually a sale
    elif '-' in changed_prices_df.iloc[i]['price'] and '-' not in changed_prices_df.iloc[i]['old_price'] and changed_prices_df.iloc[i]['price'].split(' - ')[1] > changed_prices_df.iloc[i]['old_price']:
        df = df.drop([changed_prices_df.iloc[i].name])
    #if neither of the prices are hyphenated, make sure the current price is lower than the old price
    elif '-' not in changed_prices_df.iloc[i]['price'] and '-' not in changed_prices_df.iloc[i]['old_price'] and float(changed_prices_df.iloc[i]['price'][1:]) >= float(changed_prices_df.iloc[i]['old_price'][1:]):
        df = df.drop([changed_prices_df.iloc[i].name])
#drop the old data
changed_prices_df = copy.deepcopy(df).drop(columns={'old_price', 'old_sale', 'old_secret_sale'})

#combining those which I marked as secret sale already with the products that have changed in price
secret_sales_df = pd.DataFrame.drop_duplicates(pd.concat([copy.deepcopy(ulta_df.query('secret_sale == 1 & sale == 0')).reset_index().rename(columns={'index' : 'name'}).set_index('id'), changed_prices_df]))
#a couple of the items had an incorrect url for some reason so I turned secret_sales into a dictionary so I can fix a product's url if needed
secret_sales = pd.DataFrame.to_dict(secret_sales_df.reset_index().set_index('name').transpose())

#starting a headless selenium driver
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(r'C:\Users\elerm\Downloads\chromedriver_win32\chromedriver.exe', options = chrome_options)
products_in_stock = {}

products_in_stock, secret_sales = ulta.get_products_in_stock(secret_sales, driver)

driver.close()
driver.quit()
session.close()

#more data cleaning...
products_in_stock_df = pd.DataFrame.from_dict(products_in_stock).transpose().reset_index().rename(columns={'index' : 'id'})
products_in_stock_df = pd.melt(products_in_stock_df, id_vars=['id'], var_name='price2', value_name='options2').dropna().set_index('id')
secret_sales_df = pd.DataFrame.from_dict(secret_sales).transpose().reset_index().rename(columns={'index' : 'name'}).set_index('id')

secret_sales_in_stock = pd.merge(products_in_stock_df, secret_sales_df, on='id', how='left')
secret_sales_in_stock = secret_sales_in_stock.drop(columns={'price', 'options'}).reset_index().rename(columns={'price2' : 'price', 'options2' : 'options', 'desc' : 'product', 'index' : 'id'})

#saving urls for hyperlinks in sheet
hyperlink_urls = secret_sales_in_stock['url'].tolist()
#final dataframe!!! yay!!!!!
df = secret_sales_in_stock[['main_category', 'sub_category', 'sub_sub_category', 'name', 'brand', 'product', 'price', 'options', 'offers', 'rating', 'number_of_reviews']].fillna(' ')

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

for i in range(len(df)):
    gapi.Add_Hyperlink('"' + hyperlink_urls[i] + '"', '"' + df.iloc[i]['name'] + '"', creds.get_sheet_id('main_local'), i + 1, 3)
    gapi.Add_Hyperlink('"' + hyperlink_urls[i] + '"', '"' + df.iloc[i]['name'] + '"', creds.get_sheet_id('main_mod'), i + 1, 3)

secret_sales_in_stock.to_csv('data/secret_sales_in_stock.csv')
ulta_df.to_csv('data/ulta_df.csv')