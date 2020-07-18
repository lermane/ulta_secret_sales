import pandas as pd
import requests
import concurrent.futures
import ulta_functions as ulta

print("\nstarting...\n")

session = requests.Session()
all_url_info = {}
products = {}

all_url_info = ulta.get_url_dict(session)
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

#cleaning the data
ulta_df = pd.DataFrame.from_dict(products).transpose()
secret_sales = ulta_df.query('secret_sale == 1 & sale == 0')
df = secret_sales.fillna(' ').reset_index().rename(columns={'index' : 'name', 'desc' : 'product'})
#selecting the columns I want in the google sheet as well as the order in which they appear
df = df[['main_category', 'sub_category', 'name', 'brand', 'product', 'price', 'offers', 'options', 'rating', 'number_of_reviews', 'url']]

#clearing the data from the document to replease it with the new data
ulta.Create_Service('unmindful_credentials.json', 'sheets', 'v4', ['https://www.googleapis.com/auth/spreadsheets'])
ulta.Clear_Sheet()
ulta.Export_Data_To_Sheets(df)
ulta.Update_Filter(len(df))

#saving it as a .csv for funsies/ just in case idk
df.to_csv('current_data.csv')

print('DONE')