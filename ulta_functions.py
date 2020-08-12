import pandas as pd
import numpy as np
import requests
from retrying import retry
import re
import json
from bs4 import BeautifulSoup
import time
import os
import math
import copy
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

#using the ids to create real urls
def create_url_dict(session):
    all_url_info = {}
    #I'm pulling the list of urls straight from ulta's sidebar
    front_page = session.get('https://www.ulta.com/')
    front_page_soup = BeautifulSoup(front_page.content, features="lxml")
    #anchors = list of links in the side bar
    anchors = front_page_soup.find_all('a', {'class' : 'Anchor'})
    for anchor in anchors:
        #make sure there's a description; I'm getting the categories from the description
        if anchor.get('data-nav-description') is not None and re.search(r'[a-z]*:[a-z]*', anchor.get('data-nav-description')) is not None:
            #split up url path into pieces
            url_path = anchor.get('data-nav-description')[4:].split(':')
            #I do not want urls from these anchors
            if url_path[0] not in ['shop by brand', 'new arrivals', 'ulta beauty collection', 'gifts', 'sale & coupons', 'beauty tips'] and url_path[1] != 'featured':
                page = session.get(anchor.get('href'))
                soup = BeautifulSoup(page.content, features="lxml")
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
    #we're saving the results into a .json file
    f = open('data/all_url_info_dict.json', 'w')
    json.dump(all_url_info, f)
    f.close()
    
def get_url_dict(session):
    #getting the last modified date of my all_url_info_dict.json file
    last_mod_time = os.path.getmtime('data/all_url_info_dict.json')
    #getting number of days since last file modification date
    days_since_urls_update = (datetime.datetime.today() - datetime.datetime.fromtimestamp(last_mod_time)).days
    #if it has been at least 5 days since the last time the all_url_info_dict.json file was modified, then update
    if days_since_urls_update >= 5:
        create_url_dict(session)
    #return dictionary in all_url_info_dict.json
    f = open("data/all_url_info_dict.json","r")
    all_url_info = json.loads(f.read())
    f.close()
    return(all_url_info)

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
            product, product_id = get_single_product(soup, product_container, main_category, sub_category, sub_sub_category)
            products[product_id] = product
        except Exception as exc:
            print(url, product_containers.index(product_container))
            print(exc, '\n')
    return(products)
 
def get_single_product(soup, product_container, main_category, sub_category, sub_sub_category):
    product = {}
    #get general product data from each product
    product_id = product_container.find('span', {'class' : 'prod-id'}).text.strip()
    product['url'] = 'https://www.ulta.com' + product_container.find('a', {'class' : 'product'}).get('href')
    product['brand'] = product_container.find('h4', {'class' : 'prod-title'}).text.strip()
    #description is the name of the product. so if there's a product called "ULTA Fabulous Concealer", "ULTA" would be the brand and "Fabulous Concealer" would be the description.
    product['product'] = product_container.find('p', {'class' : 'prod-desc'}).text.strip()
    product['name'] = product['brand'] + product['product']
    #getting the rating information for each product; using if statements in case a product doesn't have a rating for whatever reason
    if product_container.find('label', {'class' : 'sr-only'}) is not None:
        rating = product_container.find('label', {'class' : 'sr-only'}).text.split(' ')[0]
        if rating == 'Price':
            rating = 0
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
    #marking it as secret sale if the price does not end in 0 or 9 (trying to catch more potential secret sales)
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
    return(product, product_id)

def clean_changed_prices_df(changed_prices_df):
    df = copy.deepcopy(changed_prices_df)
    for i in range(len(changed_prices_df)):
        old_price = changed_prices_df.iloc[i]['old_price'] #price from yesterday
        current_price = changed_prices_df.iloc[i]['price'] #current price on ulta's website
        old_options = changed_prices_df.iloc[i]['old_options']
        current_options = changed_prices_df.iloc[i]['options']
        #if there's a dash (-) in a price, then the price is in the format $x.xx - $y.yy), where $x.xx is lower than $y.yy. if there isn't a dash, then the price is in the format $x.xx
        #checking if an item that was on sale yesterday is still on sale and dropping those that aren't
        if ('Sizes' in old_options) or ('Sizes' in current_options):
            if ('Sizes' in old_options) and (current_options == ' '): #there used to be more than 1 size option and now there's only 1; old_price format $a.aa - $b.bb current_price format $x.xx
                if current_price in old_price: #if $x.xx = $a.aa or $x.xx = $b.bb
                    df = df.drop([changed_prices_df.iloc[i].name])
            elif (old_options == ' ') and ('Sizes' in current_options): #there used to be only 1 size option but now there are more; old_price format $a.aa current_price format $x.xx - $y.yy
                if old_price in current_price: #if $a.aa = $x.xx or $a.aa = $y.yy
                    df = df.drop([changed_prices_df.iloc[i].name])
            elif ('Sizes' in old_options) and ('Sizes' in current_options): #size options changed both having 2+ options; old_price format $a.aa - $b.bb current_price format $x.xx - $y.yy
                #if .aa = .bb = .xx = .yy and none of them equal .97
                if (old_price.split(' - ')[0][-2:] == old_price.split(' - ')[1][-2:]) and (old_price.split(' - ')[0][-2:] == current_price.split(' - ')[0][-2:]) and (old_price.split(' - ')[0][-2:] == current_price.split(' - ')[1][-2:]) and (old_price.split(' - ')[0][-2:] != 97):
                    df = df.drop([changed_prices_df.iloc[i].name])
        else:
            if ('-' in old_price) and ('-' not in current_price): #old_price in format $a.aa - $b.bb and current_price in format $x.xx
                if float(old_price.split(' - ')[1][1:]) <= float(current_price[1:]): #if $b.bb <= $x.xx then current_price is not in the price range of old_price because it's more expensive
                    df = df.drop([changed_prices_df.iloc[i].name])
            elif ('-' in old_price) and ('-' in current_price): #old_price in format $a.aa - $b.bb and current_price in format $x.xx - $y.yy
                if float(current_price.split(' - ')[1][1:]) > float(old_price.split(' - ')[1][1:]): #if $y.yy > $b.bb 
                    df = df.drop([changed_prices_df.iloc[i].name])
                elif float(current_price.split(' - ')[0][1:]) > float(old_price.split(' - ')[0][1:]): #if $x.xx > $a.aa
                    df = df.drop([changed_prices_df.iloc[i].name])
            elif ('-' not in old_price) and ('-' in current_price): #old_price in format $a.aa and current_price in format $x.xx - $y.yy
                if float(current_price.split(' - ')[0][1:]) > float(old_price[1:]): #if $x.xx > $a.aa
                    df = df.drop([changed_prices_df.iloc[i].name])
            elif ('-' not in current_price) and ('-' not in old_price): #old_price in format $a.aa and current_price in format $x.xx
                if float(current_price[1:]) >= float(old_price[1:]): #if $x.xx >= $a.aa
                    df = df.drop([changed_prices_df.iloc[i].name])
    changed_prices_df = (
        df
        .pipe(copy.deepcopy)
        .drop(columns={'old_price', 'old_sale', 'old_secret_sale', 'old_options'})
    )
    return(changed_prices_df)

def get_secret_sales_not_in_df(secret_sales_df, old_secret_sales_in_stock, ulta_df):
    query = "id not in {}".format(secret_sales_df.index.tolist())
    #make list of the products that are in the old ulta secret sales doc but aren't in the current secret sales dictionary
    not_in_secret_sales = old_secret_sales_in_stock.query(query).index.tolist()

    ulta_df_t = ulta_df.reset_index().rename(columns={'index' : 'id'})
    #make df containing the products from the not_in_secret_sales list
    not_in_secret_sales_df = (
        ulta_df_t[ulta_df_t['id'].isin(not_in_secret_sales)]
        .set_index('id')
    )
    return(not_in_secret_sales_df)

def get_products_in_stock(secret_sales, driver):
    products_in_stock = {}
    for product_id in secret_sales:
        temp = {} #used to temporarily store product data until 
        #opening product url in the driver/browser
        driver.get(secret_sales[product_id]['url'])
        #if the product doesn't exist anymore ulta wil take you to this site
        if driver.current_url == 'https://www.ulta.com/404.jsp':
            next
        #making sure that the url is correct
        elif driver.current_url.split('productId=')[1] != product_id:
            time.sleep(1)
            driver.find_element_by_xpath("//*[@id='navigation__wrapper--sticky']/div/div[1]/div[2]/div/a").click()
            driver.find_element_by_xpath("//*[@id='searchInput']").send_keys(product_id)
            driver.find_element_by_xpath("//*[@id='js-mobileHeader']/div/div/div/div[1]/div/div[1]/form/button").click()
            if driver.current_url == 'https://www.ulta.com/404.jsp':
                next
            elif driver.current_url.split('productId=')[1] == product_id:
                secret_sales[product]['url'] = driver.current_url
                time.sleep(1)
        #getting all the product variants from the page
        product_variants = driver.find_elements_by_class_name('ProductSwatchImage__variantHolder')
        if len(product_variants) == 0:
            #products that only have one color or one size or whatever have their product variant information in a different lcoation
            product_variants = driver.find_elements_by_class_name('ProductDetail__productSwatches')
        for product_variant in product_variants:
            try:
                product_variant.click() #clicking on each variant at a time to get their price and availability
            except:         
                next #if I can't click on it I want to go to the next variant
            else:
                time.sleep(1)
                #creating a BeautifulSoup object to extract data
                soup = BeautifulSoup(driver.page_source, features="lxml")
                #there are products that only a couple of shades are labeled as sale so I'm removing those to make sure no sale items slip through
                if soup.find('img', {'src' : 'https://images.ulta.com/is/image/Ulta/badge-sale?fmt=png-alpha'}) is not None:
                    next
                #getting price
                price = soup.find('meta', {'property' : 'product:price:amount'}).get('content')
                keep = bool_keep(price, product_id, secret_sales[product_id]['price'])
                if keep == True:
                    option = get_option(soup)
                    #only adding the product variant if it's available
                    if soup.find('div', {'class' : 'ProductDetail__availabilitySection ProductDetail__availabilitySection--error'}) is None:
                        temp[option] = price
        #checking if the temp dictionary is empty to make sure if there are indeed product variants in stock
        if bool(temp):
            variants_in_stock = rearrange_product_dict(temp)
            products_in_stock[product_id] = variants_in_stock
        else:
            #if there aren't any product variants in stock, I don't want them in the document
            next
    return(products_in_stock, secret_sales)

def bool_keep(price, product_id, old_price):
    keep = False
    #attempting to catch other secret sale items that don't end with .97
    if '.97' in price:
        keep = True
    elif '-' not in old_price:
        if float(price) <= float(old_price[1:]):
            keep = True
    elif '-' in old_price:
        if price == old_price.split(' - ')[1][1:] and '0' != price[-1]and '9' != price[-1]:
            keep = True
        elif price == old_price.split(' - ')[0][1:]:
            keep = True
        elif price != old_price.split(' - ')[0][1:] and float(price) < float(old_price.split(' - ')[1][1:]) and '0' != price[-1]:
            keep = True
    return(keep)

def get_option(soup):
    option = soup.find('meta', {'property' : 'product:color'}).get('content')
    #checking other possible locations of option
    if option == '' and soup.find('div', {'class' : 'ProductDetail__colorPanel'}) is not None:
        option_tag = soup.find('div', {'class' : 'ProductDetail__colorPanel'}).find_all('span')[1]
        if option_tag is not None:
            option = option_tag.text
    if option == '' and soup.find('span', {'class' : 'ProductVariantSelector__description'}) is not None:
        option = soup.find('span', {'class' : 'ProductVariantSelector__description'}).text
    #putting the option as 'NA' if I can't find its label
    if option == '':
        option = 'NA'
    return(option)

def rearrange_product_dict(temp):
    #rearranging the dictionary to group variants with the same size together and putting the different options in a single string
    #so that, in the end, for each product, there is a dictionary including the different price options and, for each price option, 
    #a string containing the options (colors, sizes) available for that price point. 
    variants_in_stock = {}
    for key, value in temp.items():
        variants_in_stock.setdefault(value, set()).add(key)
    for key, value in variants_in_stock.items():
        new_value = ", ".join(value)
        variants_in_stock[key] = new_value
    return(variants_in_stock)

def add_old_price(secret_sales_in_stock):
    old_price = []
    for i in range(len(secret_sales_in_stock)):
        ulta_df_price = secret_sales_in_stock.iloc[i]['ulta_df_price'] #price in ulta_df
        old_secret_sales_old_price = secret_sales_in_stock.iloc[i]['old_secret_sales_old_price'] #old_price from yesterday's secret_sales_in_stock df
        old_ulta_df_price = secret_sales_in_stock.iloc[i]['old_ulta_df_price'] #price in old_ulta_df aka yesterday's ulta_df
        if '-' not in ulta_df_price and '-' not in old_secret_sales_old_price and '-' not in old_ulta_df_price: #ulta_df_price in format $a.aa, old_secret_sales_old_price in format $f.ff, old_ulta_df_price in format #$x.xx
            max_price = max(float(ulta_df_price[1:]), float(old_secret_sales_old_price[1:]), float(old_ulta_df_price[1:])) #get max of $a.aa, $f.ff, and $x.xx
            max_price_str = ('$' + str(format(max_price, '.2f')))
            old_price.append(max_price_str)
        else:
            #if price in format $x.xx, price_float = x.xx. if price in format $x.xx - $y.yy, price_float = y.yy, the max of x.xx and y.yy.
            if '-' in ulta_df_price:
                ulta_df_price_float = float(ulta_df_price.split(' - ')[1][1:])
            else:
                ulta_df_price_float = float(ulta_df_price[1:])
            if '-' in old_secret_sales_old_price:
                old_secret_sales_old_price_float = float(old_secret_sales_old_price.split(' - ')[1][1:])
            else:
                old_secret_sales_old_price_float = float(old_secret_sales_old_price[1:])
            if '-' in old_ulta_df_price:
                old_ulta_df_price_float = float(old_ulta_df_price.split(' - ')[1][1:])
            else:
                old_ulta_df_price_float = float(old_ulta_df_price[1:])
            max_price = max(ulta_df_price_float, old_secret_sales_old_price_float, old_ulta_df_price_float)
            if 'Sizes' not in secret_sales_in_stock.iloc[i]['options']: #for products with different sizes, if the price in format $x.xx - $y.yy, it doesn't mean $x.xx is min and $y.yy is max
                max_price_str = ('$' + str(format(max_price, '.2f')))
                old_price.append(max_price_str)
            else:
                print(secret_sales_in_stock.iloc[i].name, ulta_df_price, old_secret_sales_old_price, old_ulta_df_price) #for investigating
                if max_price == old_secret_sales_old_price_float:
                    old_price.append(old_secret_sales_old_price)
                elif max_price == old_ulta_df_price:
                    old_price.append(old_ulta_df_price)
                else:
                    old_price.append(ulta_df_price)
    secret_sales_in_stock['old_price'] = old_price
    return(secret_sales_in_stock)

def add_age(secret_sales_in_stock):
    age = []
    for i in range(len(secret_sales_in_stock)):
        if secret_sales_in_stock.iloc[i].name in set(old_secret_sales_in_stock.index):
            age.append('old')
        else:
            age.append('new')
    secret_sales_in_stock['age'] = age
    return(secret_sales_in_stock)

def convert_price_to_float(secret_sales_in_stock):
    secret_sales_in_stock['price'] = pd.to_numeric(secret_sales_in_stock['price'])
    return(secret_sales_in_stock)

def add_precent_off(secret_sales_in_stock):
    percent_off = []
    for i in range(len(secret_sales_in_stock)):
        if '-' not in secret_sales_in_stock.iloc[i]['old_price']:
            percent = 1 - (secret_sales_in_stock.iloc[i]['price'] / float(secret_sales_in_stock.iloc[i]['old_price'][1:]))
            if percent == 0:
                if '.97' not in str(secret_sales_in_stock.iloc[i]['price']):
                    percent = -1
                else:
                    percent = math.nan
            elif percent > 1:
                percent = -1
        else:
            percent = math.nan
        percent_off.append(round(percent, 4))
    secret_sales_in_stock['percent_off'] = percent_off
    return(secret_sales_in_stock)