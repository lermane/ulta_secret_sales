import concurrent.futures
import time
import pandas as pd
import requests
import datetime
import os
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import ulta_functions as ulta

chrome_options = Options()
chrome_options.add_argument("--headless")

def get_product_in_stock(product_id, prod_dict):    
    with webdriver.Chrome(r'C:\Users\emily\Downloads\chromedriver_win32\chromedriver.exe', options=chrome_options) as driver:
        wait = WebDriverWait(driver, 30)
        temp = {}

        driver.get(prod_dict['url'])

        if driver.current_url == 'https://www.ulta.com/404.jsp': #if the product doesn't exist anymore ulta wil take you to this site
            next
        #making sure that the url is correct
        elif driver.current_url.split('productId=')[1] != product_id:
            wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='navigation__wrapper--sticky']/div/div[1]/div[2]/div/a")))
            driver.find_element_by_xpath("//*[@id='navigation__wrapper--sticky']/div/div[1]/div[2]/div/a").click()
            driver.find_element_by_xpath("//*[@id='searchInput']").send_keys(product_id)
            driver.find_element_by_xpath("//*[@id='js-mobileHeader']/div/div/div/div[1]/div/div[1]/form/button").click()
            if driver.current_url == 'https://www.ulta.com/404.jsp':
                next
            elif driver.current_url.split('productId=')[1] == product_id:
                prod_dict['url'] = driver.current_url

        time.sleep(1)

        try:
            soup = BeautifulSoup(driver.page_source, features="lxml")
            if soup.find('div', {'class' : 'ProductSwatches__viewOptionsHolder'}) != None:
                driver.find_element_by_xpath("/html/body/div[1]/div[4]/div/div/div/div/div/div/section[1]/div[3]/div/div[2]/div/button").click()
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
                    wait.until(EC.presence_of_element_located((By.XPATH, "/html/head/meta[10]")))
                    time.sleep(1)
                    #creating a BeautifulSoup object to extract data
                    soup = BeautifulSoup(driver.page_source, features="lxml")
                    #there are products that only a couple of shades are labeled as sale so I'm removing those to make sure no sale items slip through
                    if soup.find('img', {'src' : 'https://images.ulta.com/is/image/Ulta/badge-sale?fmt=png-alpha'}) is not None:
                        next
                    #getting price
                    price = soup.find('meta', {'property' : 'product:price:amount'}).get('content')
                    keep = ulta.bool_keep(price, product_id, prod_dict['price'])
                    if keep == True:
                        option = ulta.get_option(soup)
                        #only adding the product variant if it's available
                        if soup.find('div', {'class' : 'ProductDetail__availabilitySection ProductDetail__availabilitySection--error'}) is None:
                            temp[option] = price
            if bool(temp):
                variants_in_stock = ulta.rearrange_product_dict(temp)
            else:
                variants_in_stock = {}
        except Exception as exc:
            print(product_id, exc)
            variants_in_stock = {}
        finally:
            return(variants_in_stock)    

def multithreading_5(secret_sales):
    print('START MULTITHREADING...')
    prod_stock = {}
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_product_id = {executor.submit(get_product_in_stock, product_id, prod_dict):product_id for product_id, prod_dict in secret_sales.items()}
        for future in concurrent.futures.as_completed(future_to_product_id):
            product_id = future_to_product_id[future]
            try:
                data = future.result()
                prod_stock[product_id] = data
            except Exception as exc:
                print('%r generated an exception: %s' % (product_id, exc))

    finish = time.perf_counter()

    return(round(finish-start, 2))
    
def multithreading_10(secret_sales):
    print('START MULTITHREADING...')
    prod_stock = {}
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_product_id = {executor.submit(get_product_in_stock, product_id, prod_dict):product_id for product_id, prod_dict in secret_sales.items()}
        for future in concurrent.futures.as_completed(future_to_product_id):
            product_id = future_to_product_id[future]
            try:
                data = future.result()
                prod_stock[product_id] = data
            except Exception as exc:
                print('%r generated an exception: %s' % (product_id, exc))

    finish = time.perf_counter()

    return(round(finish-start, 2))

def multithreading(secret_sales):
    print('START MULTITHREADING...')
    prod_stock = {}
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_product_id = {executor.submit(get_product_in_stock, product_id, prod_dict):product_id for product_id, prod_dict in secret_sales.items()}
        for future in concurrent.futures.as_completed(future_to_product_id):
            product_id = future_to_product_id[future]
            try:
                data = future.result()
                prod_stock[product_id] = data
            except Exception as exc:
                print('%r generated an exception: %s' % (product_id, exc))

    finish = time.perf_counter()

    return(round(finish-start, 2))

def multiprocessing(secret_sales):
    print('START MULTIPROCESSING...')
    
    prod_stock = {}
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for product_id, data in zip(secret_sales.keys(), executor.map(get_product_in_stock, secret_sales.keys(), secret_sales.values())):
            try:
                prod_stock[product_id] = data
            except Exception as exc:
                print('%r generated an exception: %s' % (product_id, exc))

    finish = time.perf_counter()

    return(round(finish-start, 2))
    
def forloop(secret_sales):
    print('START FOR LOOP...')
    
    prod_stock = {}
    start = time.perf_counter()
    for product_id, prod_dict in secret_sales.items():
        try:
            data = get_product_in_stock(product_id, prod_dict)
            prod_stock[product_id] = data
        except Exception as exc:
            print('%r generated an exception: %s' % (product_id, exc))

    finish = time.perf_counter()

    return(round(finish-start, 2))

def main():
    secret_sales = pd.read_csv('data/secret_sales_df.csv')[0:20].set_index('product_id').to_dict(orient='index')
    multithreading_time = multithreading(secret_sales)
    time.sleep(10)
    multithreading_5_time = multithreading_5(secret_sales)
    time.sleep(10)
    multithreading_10_time = multithreading_10(secret_sales)
    #multiprocessing_time = multiprocessing(secret_sales)
    #forloop_time = forloop(secret_sales)
    
    print("\n\nMULTITHREADING with no max workers given:")
    print(f'Finished in {multithreading_time} second(s)')
    
    print("\n\nMULTITHREADING with 5 max workers:")
    print(f'Finished in {multithreading_5_time} second(s)')
    
    print("\n\nMULTITHREADING with 10 max workers:")
    print(f'Finished in {multithreading_10_time} second(s)')
    
    #print("\n\nMULTIPROCESSING:")
    #print(f'Finished in {multiprocessing_time} second(s)')
    
    #print("\n\nFORLOOP:")
    #print(f'Finished in {forloop_time} second(s)')
    
if __name__ == '__main__':
    main()
