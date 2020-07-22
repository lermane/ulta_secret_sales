import pandas as pd
import numpy as np
import requests
from retrying import retry
import re
from bs4 import BeautifulSoup
import time
import math
from selenium.webdriver.chrome.options import Options
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle
import google_sheets_id

#using the ids to create real urls
def get_url_dict(session):
    all_url_info = {}
    #I'm pulling the list of urls straight from ulta's sidebar
    front_page = session.get('https://www.ulta.com/')
    front_page_soup = BeautifulSoup(front_page.content)
    #anchors = list of links in side bar
    anchors = front_page_soup.find_all('a', {'class' : 'Anchor'})
    for anchor in anchors:
        #make sure there's a description; I'm getting the categories from the description
        if anchor.get('data-nav-description') is not None and re.search(r'[a-z]*:[a-z]*', anchor.get('data-nav-description')) is not None:
            navigation = anchor.get('data-nav-description')[4:].split(':')
            if navigation[0] not in ['shop by brand', 'new arrivals', 'ulta beauty collection', 'gifts', 'sale & coupons', 'beauty tips']:
                if navigation[1] != 'featured':
                    page = session.get(anchor.get('href'))
                    soup = BeautifulSoup(page.content)
                    #get the number of total products from each id
                    num_results = int(re.findall(r'\b\d+\b', soup.find('h2', {'class' : 'search-res-title'}).find('span', {'class' : 'sr-only'}).text)[0])
                    #create a url for each 500 products
                    for i in range(math.ceil(num_results / 500)):
                        #creating a dictionary to have each url be linked to its id, main category, and sub category
                        url_info = {}
                        url_info['main_category'] = navigation[0]
                        url_info['sub_category'] = navigation[1]
                        if len(directories) == 2:
                            url_info['sub_sub_category'] = ' '
                        else:
                            url_info['sub_sub_category'] = directories[2]
                        #the &No= tag is the number of products on that page starting from 0 and &Nrpp=500 means there will be at most 500 products on each page
                        url = anchor.get('href') + '&No=' + str(i * 500) + '&Nrpp=500'
                        all_url_info[url] = url_info
    return(all_url_info)

#if this function throws an exception, retry it at least 5 times with 2 seconds in between each retry before failing
@retry(wait_fixed=2000, stop_max_attempt_number=5)
def scrape_url(url, session, products, all_url_info):
    #going to the url
    page = session.get(url)
    #getting the page's content and using the package BeautifulSoup to extract data from it
    soup = BeautifulSoup(page.content, features="lxml")
    #each product on ulta's website has a container with the class "productQvContainer" so I'm getting every element that has that as a class to pull every product
    product_containers = soup.find_all('div', {'class' : 'productQvContainer'})
    main_category = all_url_info[url]['main_category']
    sub_category = all_url_info[url]['sub_category']
    sub_sub_category = all_url_info[url]['sub_sub_category']
    #applying the function get_single_product for each product in the url. if it throws an exception, I'm having it print the url and index so I can tell what product is having a problem.
    for product_container in product_containers:
        try:
            product, product_name = get_single_product(soup, product_container, main_category, sub_category, sub_sub_category)
            products[product_name] = product
        except:
            print(url, product_containers.index(product_container))
             print(exc, '\n')
    return(products)
 
def get_single_product(soup, product_container, main_category, sub_category, sub_sub_category):
    product = {}
    #get general product data from each product
    product['url'] = 'https://www.ulta.com' + product_container.find('a', {'class' : 'product'}).get('href')
    product['id'] = product_container.find('span', {'class' : 'prod-id'}).text.strip()
    product['brand'] = product_container.find('h4', {'class' : 'prod-title'}).text.strip()
    #description is the name of the product. so if there's a product called "ULTA Fabulous Concealer", "ULTA" would be the brand and "Fabulous Concealer" would be the description.
    product['desc'] = product_container.find('p', {'class' : 'prod-desc'}).text.strip()
    product_name = product['brand'] + ' ' + product['desc']
    #getting the rating information for each product; using if statements in case a product doesn't have a rating for whatever reason
    if product_container.find('label', {'class' : 'sr-only'}) is not None:
        product['rating'] = product_container.find('label', {'class' : 'sr-only'}).text.split(' ')[0]
    if product_container.find('span', {'class' : 'prodCellReview'}) is not None:
        product['number_of_reviews'] = re.findall(r'\b\d+\b', product_container.find('span', {'class' : 'prodCellReview'}).text)[0]
    #the prices are labeled differently in the code depending on whether the product is for sale or not (for sale as in marked as sale not a secret sale)
    if product_container.find('div', {'class' : 'productSale'}) is None:
        product['sale'] = 0
        product['price'] = product_container.find('span', {'class' : 'regPrice'}).text.strip()
    else:
        product['sale'] = 1
        product['price'] = product_container.find('span', {'class' : 'pro-old-price'}).text.strip()
        product['sale_price'] = product_container.find('span', {'class' : 'pro-new-price'}).text.strip()
    #marking it as secret sale if the price has .97
    if '.97' in product['price']:
        product['secret_sale'] = 1
    else:
        product['secret_sale'] = 0
    #getting the available offers and number of options/colors of the product if they're listed
    if product_container.find('div', {'class' : 'product-detail-offers'}) is not None:
        product['offers'] = product_container.find('div', {'class' : 'product-detail-offers'}).text.strip()
    if product_container.find('span', {'class' : 'pcViewMore'}) is not None:
        product['options'] = re.sub('\xa0', ' ', product_container.find('span', {'class' : 'pcViewMore'}).text.strip())
    product['main_category'] = main_category
    product['sub_category'] = sub_category
    product['sub_sub_category'] = sub_sub_category
    return(product, product_name)

#variables used in the following functions
gsheetId = google_sheets_id.get_sheet_id()
SAMPLE_RANGE_NAME = 'A1:AA20000'

#copied the next 3 functions from
#https://medium.com/analytics-vidhya/how-to-read-and-write-data-to-google-spreadsheet-using-python-ebf54d51a72c

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        #return service
    except Exception as e:
        print(e)
        #return None

def Clear_Sheet():
    result_clear = service.spreadsheets().values().clear(
        spreadsheetId=gsheetId,
        range=SAMPLE_RANGE_NAME,
        body = {}
    ).execute()
    print('Sheet successfully cleared')

def Export_Data_To_Sheets(df):
    response_date = service.spreadsheets().values().update(
        spreadsheetId=gsheetId,
        valueInputOption='RAW',
        range=SAMPLE_RANGE_NAME,
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully updated')

#updates the sale_filter view so it will change when the numbers of rows change
def Update_Filter(i):
    my_range = {
    'sheetId': 0,
    'startRowIndex': 0,
    'startColumnIndex': 0,
    'endRowIndex': i + 1,
    'endColumnIndex': 11
    }
    
    updateFilterViewRequest = {
        'updateFilterView': {
            'filter': {
                'filterViewId': '2092242562',
                'range': my_range
            },
            'fields': {
                'paths': 'range'
            }
        }
    }
    
    body = {'requests': [updateFilterViewRequest]}
    service.spreadsheets().batchUpdate(spreadsheetId=gsheetId, body=body).execute()
    print('Filter successfully updated')
