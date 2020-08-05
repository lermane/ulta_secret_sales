import pandas as pd
import numpy as np
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

ulta_df = pd.DataFrame.from_dict(products).transpose().reset_index().rename(columns={'index' : 'name'}).set_index('id')

#cleaning the data
ulta_df = pd.DataFrame.from_dict(products).transpose().reset_index().rename(columns={'index' : 'name'}).set_index('id')\

#loading in yesterday's data to check for price changes
old_ulta_df = pd.read_csv('data/ulta_df.csv').rename(columns={'price' : 'old_price', 'sale' : 'old_sale', 'secret_sale' : 'old_secret_sale', 'options' : 'old_options'}).set_index('id')
old_ulta_df = old_ulta_df[['old_price', 'old_sale', 'old_secret_sale', 'old_options']]
old_secret_sales_in_stock = pd.read_csv('data/secret_sales_in_stock.csv').set_index('id')

#checking for products whose price has changed since yesterday
changed_prices_df = pd.merge(ulta_df, old_ulta_df, on='id', how='inner').dropna(subset=['price', 'old_price']).query('price != old_price').query('sale == 0 & old_sale == 0')
changed_prices_df['old_options'] = changed_prices_df[['old_options']].fillna(value=str(0))
changed_prices_df['options'] = changed_prices_df[['options']].fillna(value=str(0))

df = copy.deepcopy(changed_prices_df)
for i in range(len(changed_prices_df)):
    #checking if an item that was on sale yesterday is still on sale and dropping those that aren't
    if '-' in changed_prices_df.iloc[i]['old_price'] and '-' not in changed_prices_df.iloc[i]['price'] and changed_prices_df.iloc[i]['old_price'].split(' - ')[1] <= changed_prices_df.iloc[i]['price']:
        df = df.drop([changed_prices_df.iloc[i].name])
    elif '-' in changed_prices_df.iloc[i]['old_price'] and '-' in changed_prices_df.iloc[i]['price'] and float(changed_prices_df.iloc[i]['price'].split(' - ')[1][1:]) >= float(changed_prices_df.iloc[i]['old_price'].split(' - ')[1][1:]):
        df = df.drop([changed_prices_df.iloc[i].name])
    elif '-' in changed_prices_df.iloc[i]['old_price'] and '-' in changed_prices_df.iloc[i]['price'] and float(changed_prices_df.iloc[i]['price'].split(' - ')[0][1:]) >= float(changed_prices_df.iloc[i]['old_price'].split(' - ')[0][1:]):
        df = df.drop([changed_prices_df.iloc[i].name])
    #if the new price is a hyphenated price and the old price is not, make sure that the second price in the hyphenated one is actually less than the unhyphenated price to make sure it's actually a sale
    elif '-' in changed_prices_df.iloc[i]['price'] and '-' not in changed_prices_df.iloc[i]['old_price'] and float(changed_prices_df.iloc[i]['price'].split(' - ')[0][1:]) > float(changed_prices_df.iloc[i]['old_price'][1:]):
        print(i)
        df = df.drop([changed_prices_df.iloc[i].name])
    #if neither of the prices are hyphenated, make sure the current price is lower than the old price
    elif '-' not in changed_prices_df.iloc[i]['price'] and '-' not in changed_prices_df.iloc[i]['old_price'] and float(changed_prices_df.iloc[i]['price'][1:]) >= float(changed_prices_df.iloc[i]['old_price'][1:]):
        df = df.drop([changed_prices_df.iloc[i].name])
    elif 'Sizes' in changed_prices_df.iloc[i]['options'] and 'Sizes' in changed_prices_df.iloc[i]['old_options'] and changed_prices_df.iloc[i]['options'] != changed_prices_df.iloc[i]['old_options']:
        df = df.drop([changed_prices_df.iloc[i].name])
    elif changed_prices_df.iloc[i]['old_options'] == str(0) and 'Sizes' in changed_prices_df.iloc[i]['options']:
        df = df.drop([changed_prices_df.iloc[i].name])
    elif changed_prices_df.iloc[i]['old_options'] == '2 Sizes' and changed_prices_df.iloc[i]['options'] == str(0) and '.97' not in changed_prices_df.iloc[i]['price']:
        df = df.drop([changed_prices_df.iloc[i].name])
changed_prices_df = copy.deepcopy(df).drop(columns={'old_price', 'old_sale', 'old_secret_sale', 'old_options'})

#checking prices of some items that I suspect might be a secret sale
ulta_df_t = ulta_df.dropna(subset=['price', 'options'])
check_prices_df = ulta_df_t[ulta_df_t['options'].str.contains("Colors") & ulta_df_t['price'].str.contains("-")].query('sale == 0 & secret_sale == 0')

df = copy.deepcopy(check_prices_df)
for i in range(len(check_prices_df)):
    if float(check_prices_df.iloc[i]['price'].split(' - ')[0][1:])/float(check_prices_df.iloc[i]['price'].split(' - ')[1][1:]) > .95:
        df = df.drop([check_prices_df.iloc[i].name])
    elif check_prices_df.iloc[i].name in changed_prices_df.index.tolist():
        df = df.drop([check_prices_df.iloc[i].name])
check_prices_df = copy.deepcopy(df)

#getting products with .97 in their price
price_97_df = copy.deepcopy(ulta_df.query('secret_sale == 1 & sale == 0')).reset_index().rename(columns={'index' : 'name'}).set_index('id')

#putting them all together and removing duplicates
secret_sales_df = pd.DataFrame.drop_duplicates(pd.concat([changed_prices_df, check_prices_df, price_97_df]))

#making sure I'm not forgetting any products that were in yesterday's google sheet
query = "id not in {}".format(secret_sales_df.index.tolist())
old_secret_sales_in_stock = pd.read_csv('data/secret_sales_in_stock.csv').set_index('id').groupby('id').first()
not_in_secret_sales = old_secret_sales_in_stock.query(query).index.tolist()

ulta_df_t = ulta_df.reset_index().rename(columns={'index' : 'id'})
not_in_secret_sales_df = ulta_df_t[ulta_df_t['id'].isin(not_in_secret_sales)].set_index('id')

secret_sales_df = pd.DataFrame.drop_duplicates(pd.concat([secret_sales_df, not_in_secret_sales_df])).groupby('id').first()
secret_sales_df = secret_sales_df.query('sale == 0')
secret_sales = pd.DataFrame.to_dict(secret_sales_df.reset_index().rename(columns={'index' : 'id'}).set_index('name').transpose())

#finding out which products are in stock
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(r'C:\Users\elerm\Downloads\chromedriver_win32\chromedriver.exe', options = chrome_options)
products_in_stock, secret_sales = ulta.get_products_in_stock(secret_sales, driver)
driver.close()
driver.quit()

#data cleaning
products_in_stock_df = pd.DataFrame.from_dict(products_in_stock).transpose().reset_index().rename(columns={'index' : 'id'})
products_in_stock_df = pd.melt(products_in_stock_df, id_vars=['id'], var_name='price2', value_name='options2').dropna().set_index('id')
secret_sales_df = pd.DataFrame.from_dict(secret_sales).transpose().reset_index().rename(columns={'index' : 'name'}).set_index('id')
in_stock_and_secret_sales = pd.merge(products_in_stock_df, secret_sales_df, on='id', how='left')
add_old_secret_stock = pd.merge(in_stock_and_secret_sales, old_secret_sales_in_stock.rename(columns={'old_price' : 'old_secret_sales_old_price'})[['old_secret_sales_old_price']], on='id', how='left')
add_old_ulta_df = pd.merge(add_old_secret_stock, old_ulta_df.rename(columns={'old_price' : 'old_ulta_df_price'})[['old_ulta_df_price']], on='id', how='left')
secret_sales_in_stock = copy.deepcopy(add_old_ulta_df).rename(columns={'price' : 'ulta_df_price'})

secret_sales_in_stock = secret_sales_in_stock.fillna(value={'old_secret_sales_old_price': '$0.00', 'old_ulta_df_price': '$0.00', 'ulta_df_price': '$0.00', 'options': ' '}).dropna(subset=['name'])

#creating old_price column
old_price = []
for i in range(len(secret_sales_in_stock)):
    if '-' not in secret_sales_in_stock.iloc[i]['ulta_df_price'] and '-' not in secret_sales_in_stock.iloc[i]['old_secret_sales_old_price'] and '-' not in secret_sales_in_stock.iloc[i]['old_ulta_df_price']:
        max_price = max(float(secret_sales_in_stock.iloc[i]['ulta_df_price'][1:]), float(secret_sales_in_stock.iloc[i]['old_secret_sales_old_price'][1:]), float(secret_sales_in_stock.iloc[i]['old_ulta_df_price'][1:]))
        max_price_str = ('$' + str(format(max_price, '.2f')))
        old_price.append(max_price_str)
    else:
        if '-' in secret_sales_in_stock.iloc[i]['ulta_df_price']:
            ulta_df_price = float(secret_sales_in_stock.iloc[i]['ulta_df_price'].split(' - ')[1][1:])
        else:
            ulta_df_price = float(secret_sales_in_stock.iloc[i]['ulta_df_price'][1:])
        if '-' in secret_sales_in_stock.iloc[i]['old_secret_sales_old_price']:
            old_secret_sales_old_price = float(secret_sales_in_stock.iloc[i]['old_secret_sales_old_price'].split(' - ')[1][1:])
        else:
            old_secret_sales_old_price = float(secret_sales_in_stock.iloc[i]['old_secret_sales_old_price'][1:])
        if '-' in secret_sales_in_stock.iloc[i]['old_ulta_df_price']:
            old_ulta_df_price = float(secret_sales_in_stock.iloc[i]['old_ulta_df_price'].split(' - ')[1][1:])
        else:
            old_ulta_df_price = float(secret_sales_in_stock.iloc[i]['old_ulta_df_price'][1:])
        max_price = max(ulta_df_price, old_secret_sales_old_price, old_ulta_df_price)
        if 'Sizes' not in secret_sales_in_stock.iloc[i]['options']:
            max_price_str = ('$' + str(format(max_price, '.2f')))
            old_price.append(max_price_str)
        else:
            if max_price == old_secret_sales_old_price:
                old_price.append(secret_sales_in_stock.iloc[i]['old_secret_sales_old_price'])
            elif max_price == old_ulta_df_price:
                old_price.append(secret_sales_in_stock.iloc[i]['old_ulta_df_price'])
            else:
                old_price.append(secret_sales_in_stock.iloc[i]['ulta_df_price'])

#creating age column
age = []
for i in range(len(secret_sales_in_stock)):
    if secret_sales_in_stock.iloc[i].name in old_secret_sales_in_stock.index.tolist():
        age.append('old')
    else:
        age.append('new')
secret_sales_in_stock['age'] = age

secret_sales_in_stock = secret_sales_in_stock.drop(columns={'old_secret_sales_old_price', 'old_ulta_df_price', 'ulta_df_price', 'options', 'sale', 'secret_sale', 'sale_price'}).rename(columns={'price2' : 'price', 'options2' : 'options', 'desc' : 'product'})

secret_sales_in_stock['price'] = pd.to_numeric(secret_sales_in_stock['price'])
secret_sales_in_stock['age'] = age
secret_sales_in_stock['old_price'] = old_price

#adding percent off column
percent_off = []
for i in range(len(secret_sales_in_stock)):
    if '-' not in secret_sales_in_stock.iloc[i]['old_price']:
        percent = secret_sales_in_stock.iloc[i]['price'] / float(secret_sales_in_stock.iloc[i]['old_price'][1:])
        if percent == 1:
            percent = math.nan
        elif percent > 1:
            #if it's above 1 then the sale price is more than the normal price. this shouldn't happen but just in case lol.
            percent = -1
    else:
        percent = math.nan
    percent_off.append(round(percent, 4))

secret_sales_in_stock['percent_off'] = percent_off
secret_sales_in_stock = secret_sales_in_stock.query('percent_off != -1')

df = copy.deepcopy(secret_sales_in_stock)
secret_sales_in_stock = secret_sales_in_stock.drop(columns={'percent_off'})
hyperlink_urls = df['url'].tolist()
df['percent_off'] = percent_off
df = df[['main_category', 'sub_category', 'sub_sub_category', 'name', 'brand', 'product', 'price', 'old_price', 'percent_off', 'options', 'offers', 'rating', 'number_of_reviews', 'age']].fillna(' ')

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