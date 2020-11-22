import concurrent.futures
import time
import pandas as pd
import requests
import datetime
import os
from bs4 import BeautifulSoup
import re

def create_url_df():
    all_url_info = {}
    #I'm pulling the list of urls straight from ulta's sidebar
    front_page = requests.get('https://www.ulta.com/')
    front_page_soup = BeautifulSoup(front_page.text, features="lxml")
    #anchors = list of links in the side bar
    anchors = front_page_soup.find_all('a', {'class' : 'Anchor'})
    for anchor in anchors:
        #make sure there's a description; I'm getting the categories from the description
        if anchor.get('data-nav-description') is not None and re.search(r'[a-z]*:[a-z]*', anchor.get('data-nav-description')) is not None:
            #split up url path into pieces
            url_path = anchor.get('data-nav-description')[4:].split(':')
            #I do not want urls from these anchors
            if url_path[0] not in ['shop by brand', 'new arrivals', 'ulta beauty collection', 'gifts', 'sale & coupons', 'beauty tips'] and url_path[1] != 'featured':
                page = requests.get(anchor.get('href'))
                soup = BeautifulSoup(page.text, features="lxml")
                #get the number of total products from each id so we can create a different url for each set of 500 products in the url so there isn't too much data loaded into one url at once
                num_results = int(re.findall(r'\b\d+\b', soup.find('h2', {'class' : 'search-res-title'}).find('span', {'class' : 'sr-only'}).text)[0])
                for i in range(math.ceil(num_results / 500)):
                    #creating a dictionary to have each url be linked to its id, main category, and sub category
                    url_info = {}
                    url_info['main_category'] = url_path[0]
                    url_info['sub_category'] = url_path[1]
                    if len(url_path) == 2: #if the length != 2 then the url path has at least 3 parts which means we can get a sub sub sub category from it 
                        url_info['sub_sub_category'] = ' '
                    else:
                        url_info['sub_sub_category'] = url_path[2]
                    #the &No= tag is the number of products on that page starting from 0 and &Nrpp=500 means there will be at most 500 products on each page
                    url = anchor.get('href') + '&No=' + str(i * 500) + '&Nrpp=500'
                    all_url_info[url] = url_info
    url_df = (
        pd.DataFrame.from_dict(all_url_info)
        .transpose()
        .reset_index()
        .rename(columns={'index' : 'url'})
        .rename_axis('url_pkey')
    )
    url_df.to_csv('data/url_df.csv')

def get_url_df():
    #getting the last modified date of my url_df.csv file
    last_mod_time = os.path.getmtime('data/url_df.csv')
    #getting number of days since last file modification date
    days_since_urls_update = (datetime.datetime.today() - datetime.datetime.fromtimestamp(last_mod_time)).days
    #if it has been at least 5 days since the last time the all_url_info_dict.json file was modified, then update
    if days_since_urls_update >= 5:
        create_url_df()
    #return url_df
    url_df = pd.read_csv('data/url_df.csv')
    return(url_df)

def scrape_url(row):
    products = {}
    #going to the url
    page = requests.get(row['url'])
    #getting the page's content and using the package BeautifulSoup to extract data from it
    soup = BeautifulSoup(page.text, features="lxml")
    #each product on ulta's website has a container with the class "productQvContainer" so I'm getting every element that has that as a class to pull every product
    product_containers = soup.find_all('div', {'class' : 'productQvContainer'})
    #applying the function get_single_product for each product in the url. if it throws an exception, I'm having it print the url and index so I can tell what product is having a problem.
    for product_container in product_containers:
        try:
            product, product_id = get_single_product(soup, product_container, row.name)
            products[product_id] = product
        except Exception as exc:
            print(row['url'], product_containers.index(product_container))
            print(exc, '\n')
    products_df = (
        pd.DataFrame.from_dict(products)
        .transpose()
    )
    return(products_df)

def get_single_product(soup, product_container, url_pkey):
    product = {}
    #get general product data from each product
    product_id = product_container.find('span', {'class' : 'prod-id'}).text.strip()
    product['sku_id'] = str(product_container.find('a', {'class' : 'qShopbutton'}).get('data-skuidrr'))
    product['brand'] = product_container.find('h4', {'class' : 'prod-title'}).text.strip()
    #description is the name of the product. so if there's a product called "ULTA Fabulous Concealer", "ULTA" would be the brand and "Fabulous Concealer" would be the description.
    product['product'] = product_container.find('p', {'class' : 'prod-desc'}).text.strip()
    #sometimes the https://www.ulta.com is already in the url and sometimes (most of the time) it's not.
    if product_container.find('a', {'class' : 'product'}).get('href')[0] != '/':
        product_url = product_container.find('a', {'class' : 'product'}).get('href')
    else:
        product_url = 'https://www.ulta.com' + product_container.find('a', {'class' : 'product'}).get('href')
    #if the correct product id isn't in the url then the url is wrong. if it's wrong, then we need to fix it.
    if product_url.split('productId=')[1] != product_id:
        product_url = 'https://www.ulta.com/' + product['product'].replace(' ', '-').lower() + '?productId=' + product_id
    product['url'] = product_url
    #getting the rating information for each product; using if statements in case a product doesn't have a rating for whatever reason
    if product_container.find('label', {'class' : 'sr-only'}) is not None:
        rating = product_container.find('label', {'class' : 'sr-only'}).text.split(' ')[0]
        if rating == 'Price':
            rating = 0.00
        product['rating'] = rating
    if product_container.find('span', {'class' : 'prodCellReview'}) is not None:
        product['no_of_reviews'] = re.findall(r'\b\d+\b', product_container.find('span', {'class' : 'prodCellReview'}).text)[0]
    #the prices are labeled differently in the code depending on whether the product is for sale or not (for sale as in marked as sale not a secret sale)
    if product_container.find('div', {'class' : 'productSale'}) is None:
        product['sale'] = 0
        product['price'] = product_container.find('span', {'class' : 'regPrice'}).text.strip()
    else:
        product['sale'] = 1
        product['price'] = product_container.find('span', {'class' : 'pro-old-price'}).text.strip()
        product['sale_price'] = product_container.find('span', {'class' : 'pro-new-price'}).text.strip()
    #getting the available offers and number of options/colors of the product if they're listed
    if product_container.find('div', {'class' : 'product-detail-offers'}) is not None:
        product['offers'] = product_container.find('div', {'class' : 'product-detail-offers'}).text.strip()
    if product_container.find('span', {'class' : 'pcViewMore'}) is not None:
        product['options'] = re.sub('\xa0', ' ', product_container.find('span', {'class' : 'pcViewMore'}).text.strip())
    product['url_pkey_foreign'] = url_pkey
    return(product, product_id)

def multithreading():
    print('MULTITHREADING')
    
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        url_df = get_url_df().set_index('url_pkey')
    
        rows = []
        for index, row in url_df.iterrows():
            rows.append(row)
            
        results = executor.map(scrape_url, rows)

    finish = time.perf_counter()

    print(f'Finished in {round(finish-start, 2)} second(s)')
    
def multiprocessing():
    print('MULTIPROCESSING')
    
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        url_df = get_url_df().set_index('url_pkey')
    
        rows = []
        for index, row in url_df.iterrows():
            rows.append(row)
            
        results = executor.map(scrape_url, rows)

    finish = time.perf_counter()

    print(f'Finished in {round(finish-start, 2)} second(s)')

if __name__ == '__main__':
    
    multithreading()
    print('\n\n')
    multiprocessing()
        
