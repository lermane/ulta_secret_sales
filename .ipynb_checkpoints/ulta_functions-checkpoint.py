import pandas as pd
import numpy as np
import requests
from retrying import retry
import re
from bs4 import BeautifulSoup
import time
import math
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle
import google_sheets_id

#the ids of the urls I'm scraping from
ids = [
    'makeup-face?N=26y3',
    'makeup-eyes?N=26yd',
    'makeup-lips?N=26yq',
    'tools-brushes-makeup-brushes-tools?N=27hn',
    'makeup-bags-cases?N=26zp',
    'makeup-travel-size?N=2782',
    'makeup-gifts-value-sets?N=26zo',
    'nail-polish?N=278s',
    'nails-gel-manicure?N=278j',
    'nails-top-base-coats?N=27iq',
    'nail-polish-stickers?N=y5vfsk',
    'press-on-nails?N=lrb2l2',
    'nail-art-design?N=27br',
    'nail-care?N=27bs',
    'nails-manicure-pedicure-tools?N=27ir',
    'nails-gifts-value-sets?N=27c1',
    'skin-care-cleansers?N=2794',
    'skin-care-moisturizers?N=2796',
    'skin-care-treatment-serums?N=27cs',
    'skin-care-eye-treatments?N=270k',
    'skin-care-suncare?N=27fe',
    'skin-care-supplements?N=2712',
    'skin-care-tools?N=2718',
    'skin-care-travel-size?N=279d',
    'skin-care-gifts-value-sets?N=2717',
    'hair-shampoo-conditioner?N=27ih',
    'hair-treatment?N=26xy',
    'hair-styling-products?N=26xf',
    'hair-color?N=26xs',
    'hair-kids-haircare?N=26xz',
    'hair-travel-size?N=27ci',
    'hair-gifts-value-sets?N=277l',
    'tools-brushes-hair-styling-tools?N=27gc',
    'tools-brushes-hair-brushes-combs?N=27gj',
    'tools-brushes-accessories?N=27gk',
    'tools-brushes-hair-removal-tools?N=27g9',
    'tools-brushes-travel-size?N=447sb8',
    'tools-brushes-gifts-value-sets?N=27gr',
    'womens-fragrance?N=26wn',
    'mens-fragrance?N=26wf',
    'fragrance-gift-sets?N=26wc',
    'candles-home-fragrance?N=26wb',
    'bath-body-bath-shower?N=26uy',
    'bath-body-body-moisturizers?N=26v3',
    'bath-body-hand-foot-care?N=27ic',
    'bath-body-self-care-wellness?N=27i3',
    'bath-body-accessories?N=27i8',
    'bath-body-suncare?N=276b',
    'bath-body-travel-size?N=276l',
    'bath-body-gifts-value-sets?N=26vq'
]

#by hand, I went and put a main and sub category for each url just to make it easier for people to sort the data in the document
main_categories = [
    'makeup',
    'makeup',
    'makeup',
    'makeup',
    'makeup',
    'makeup',
    'makeup',
    'nails',
    'nails',
    'nails',
    'nails',
    'nails',
    'nails',
    'nails',
    'nails',
    'nails',
    'skincare',
    'skincare',
    'skincare',
    'skincare',
    'skincare',
    'skincare',
    'skincare',
    'skincare',
    'skincare',
    'hair',
    'hair',
    'hair',
    'hair',
    'hair',
    'hair',
    'hair',
    'tools & brushes',
    'tools & brushes',
    'tools & brushes',
    'tools & brushes',
    'tools & brushes',
    'tools & brushes',
    'fragrance',
    'fragrance',
    'fragrance',
    'fragrance',
    'body & body',
    'body & body',
    'body & body',
    'body & body',
    'body & body',
    'body & body',
    'body & body',
    'body & body'
]

sub_categories = [
    'face',
    'eyes',
    'lips',
    'makeup brushes & tools',
    'makeup bags & cases',
    'travel size',
    'gifts & value sets',
    'nail polish',
    'gel polish',
    'top & base coats',
    'nail stickers',
    'press on nails',
    'nail art & design',
    'nail care',
    'manicure & pedicure tools',
    'gift & value sets',
    'face cleansers',
    'face moisturizers',
    'face treatments',
    'eye treatments',
    'suncare',
    'supplements',
    'skin tools',
    'travel size',
    'gifts & value sets',
    'shampoo & conditioner',
    'hair treatments',
    'hair stylers',
    'hair color',
    'kids haircare',
    'travel size',
    'gifts & value sets',
    'hair styling tools',
    'brushes & combs',
    'hair accessories',
    'hair removal tools',
    'travel size',
    'gifts & value sets',
    "women's perfume",
    "men's cologne",
    'gift sets',
    'candles & home fragrance',
    'bath & shower',
    'body moisturizers',
    'hand & foot care',
    'self care & wellness',
    'bath & body accessories',
    'suncare',
    'travel size',
    'gifts & value sets'
]

#using the ids to create real urls
def get_url_dict(session):
    all_url_info = {}
    for i in range(len(ids)):
        #I'm trying to create urls with at most 500 products on each page. Why 500? The max is 1000 and 500 seemed like a good number.
        page = session.get('https://www.ulta.com/' + ids[i])
        soup = BeautifulSoup(page.content, features="lxml")
        #get the number of total products from each id
        num_results = int(re.findall(r'\b\d+\b', soup.find('h2', {'class' : 'search-res-title'}).find('span', {'class' : 'sr-only'}).text)[0])
        #create a url for each 500 products
        for j in range(math.ceil(num_results / 500)):
            #creating a dictionary to have each url be linked to its id, main category, and sub category. this could probably be accomplished using
            #object oriented stuff by creating classes and objects but I don't have much experience doing that in python and dictionaries are easier
            url_info = {}
            url_info['id'] = ids[i]
            url_info['main_category'] = main_categories[i]
            url_info['sub_category'] = sub_categories[i]
            #the &No= tag is the number of products on that page starting from 0 and &Nrpp=500 means there will be at most 500 products on each page
            url = 'https://www.ulta.com/' + url_info['id'] + '&No=' + str(j * 500) + '&Nrpp=500'
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
    #applying the function get_single_product for each product in the url. if it throws an exception, I'm having it print the url and index so I can tell what product is having a problem.
    for product_container in product_containers:
        try:
            product, product_name = get_single_product(soup, product_container, main_category, sub_category)
            products[product_name] = product
        except:
            print(url, product_containers.index(product_container))
    return(products)
 
def get_single_product(soup, product_container, main_category, sub_category):
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
                'filterViewId': '511550738',
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
