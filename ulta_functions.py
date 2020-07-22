import pandas as pd
import numpy as np
import requests
from retrying import retry
import re
from bs4 import BeautifulSoup
import time
import math
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

#using the ids to create real urls
def get_url_dict(session):
    all_url_info = {}
    #I'm pulling the list of urls straight from ulta's sidebar
    front_page = session.get('https://www.ulta.com/')
    front_page_soup = BeautifulSoup(front_page.content, features="lxml")
    #anchors = list of links in side bar
    anchors = front_page_soup.find_all('a', {'class' : 'Anchor'})
    for anchor in anchors:
        #make sure there's a description; I'm getting the categories from the description
        if anchor.get('data-nav-description') is not None and re.search(r'[a-z]*:[a-z]*', anchor.get('data-nav-description')) is not None:
            navigation = anchor.get('data-nav-description')[4:].split(':')
            if navigation[0] not in ['shop by brand', 'new arrivals', 'ulta beauty collection', 'gifts', 'sale & coupons', 'beauty tips']:
                if navigation[1] != 'featured':
                    page = session.get(anchor.get('href'))
                    soup = BeautifulSoup(page.content, features="lxml")
                    #get the number of total products from each id
                    num_results = int(re.findall(r'\b\d+\b', soup.find('h2', {'class' : 'search-res-title'}).find('span', {'class' : 'sr-only'}).text)[0])
                    #create a url for each 500 products
                    for i in range(math.ceil(num_results / 500)):
                        #creating a dictionary to have each url be linked to its id, main category, and sub category
                        url_info = {}
                        url_info['main_category'] = navigation[0]
                        url_info['sub_category'] = navigation[1]
                        if len(navigation) == 2:
                            url_info['sub_sub_category'] = ' '
                        else:
                            url_info['sub_sub_category'] = navigation[2]
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
        rating = product_container.find('label', {'class' : 'sr-only'}).text.split(' ')[0]
        if rating == 'Price':
            rating = 0
        product['rating'] = rating
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

def get_products_in_stock(drop_na_options, secret_sales, driver):
    products_in_stock = {}
    for product in drop_na_options:
        variants_in_stock = {}
        temp = {}
        #opening product url in the driver/browser
        driver.get(drop_na_options[product]['url'])
        #making sure that the url is correct! it wasn't for a couple of the products for some reason idk why. but I'm 
        #fixing it in this step.
        if driver.current_url.split('productId=')[1] != drop_na_options[product]['id']:
            driver.find_element_by_xpath("//*[@id='navigation__wrapper--sticky']/div/div[1]/div[2]/div/a").click()
            driver.find_element_by_xpath("//*[@id='searchInput']").send_keys(drop_na_options[product]['id'])
            driver.find_element_by_xpath("//*[@id='js-mobileHeader']/div/div/div/div[1]/div/div[1]/form/button").click()
            secret_sales[product]['url'] = driver.current_url
        #if I don't add this sleep, the page doesn't finish loading. tried to use implicit waits but this just worked better.
        time.sleep(1)
        #getting all the product variants from the page
        product_variants = driver.find_elements_by_class_name('ProductSwatchImage__variantHolder')
        for product_variant in product_variants:
            try:
                #clicking on each variant at a time to get their price and availability
                product_variant.click()
            except:
                #if I can't click on it I want to go to the next variant
                next
            else:
                #if I don't add this sleep, the page doesn't finish loading. tried to use implicit waits but this just worked better.
                time.sleep(1)
                #creating a BeautifulSoup object to extract data
                soup = BeautifulSoup(driver.page_source, features="lxml")
                #getting price
                price = soup.find('meta', {'property' : 'product:price:amount'}).get('content')
                #only getting other information if it's a secret sale item
                if price.endswith('.97'):
                    #color and size are in different locations
                    #getting color
                    option = soup.find('meta', {'property' : 'product:color'}).get('content')
                    #if there's no color, checking if there's a size
                    if option == '':
                        option_tag = soup.find('div', {'class' : 'ProductDetail__colorPanel'}).find_all('span')[1]
                        if option_tag is not None:
                            option = option_tag.text
                    #if there's no color or size I'm putting 'NA' to represent that there's still a swatch there even if we can't find
                    #information about it. like 99.99% of the time this shouldn't happen but just in case.
                    if option == '':
                        option = 'NA'
                    #only adding the product variant if it's available
                    if soup.find('div', {'class' : 'ProductDetail__availabilitySection ProductDetail__availabilitySection--error'}) is None:
                        temp[option] = price
        #checking if the temp dictionary is empty to make sure if there are indeed product variants in stock
        if bool(temp):
            #rearranging the dictionary to group variants with the same size together and putting the different options in a single string
            #so that, in the end, for each product, there is a dictionary including the different price options and, for each price option, 
            #a string containing the options (colors, sizes) available for that price point. 
            for key, value in temp.items():
                variants_in_stock.setdefault(value, set()).add(key)
            for key, value in variants_in_stock.items():
                new_value = ", ".join(value)
                variants_in_stock[key] = new_value
            products_in_stock[driver.title[:-14]] = variants_in_stock
        else:
            #if there aren't any product variants in stock, I don't want them in the document
            next
    return(products_in_stock, secret_sales)