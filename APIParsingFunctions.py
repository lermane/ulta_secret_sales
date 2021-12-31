""" these functions are used to both get data from the ulta api and parse its json output. """

import pandas as pd
from APIHandler import APIHandler
from APIHandler import APIEndpoint
from flatdict import FlatDict
import datetime
import json
import numpy as np

month = datetime.date.today().month
day = datetime.date.today().day
year = datetime.date.today().year


""" PRODUCT FUNCTIONS """


def _get_today_ulta():
    """ returns today's ulta json file from scrapy """
    file = 'UltaScraper/data/ulta/ulta_%d_%d_%d.json'%(month, day, year)
    with open(file,'r') as f:
        ultaJson = json.loads(f.read())
        
    ulta = pd.DataFrame(ultaJson).drop_duplicates()
    
    return(ulta)


def get_product_urls():
    """ returns a dataframe with 2 columns: product_id and url """
    ulta = _get_today_ulta()
    return(ulta.loc[:, ['product_id', 'url']].drop_duplicates())


def get_product_ids(i = -1):
    """ returns i number of product ids from today's ulta json file """
    ulta = _get_today_ulta()
    productIds = ulta['product_id'].tolist()
    if i == -1 or i == 0:
        return(productIds)
    else:
        return(productIds[0:i])
    

def parse_product_data(jsonDict):
    """ the json dictionary returned by the api contains product information and sku 
    information about the default sku, the default product variant that appears first
    on the product's page. this function takes the json dictionary and parses out 2
    dictionaries: 1 dictionary with product data, and 1 dictionary with the default sku
    data. """
    
    jsonDict['data'].pop('swatches', None)
    jsonDict['data']['product'].pop('altImages', None)
    #using pop will return the sku dict and remove it from jsonDict
    skuDict = jsonDict['data'].pop('sku', None)
    productDict = jsonDict
    
    flatProductDict = FlatDict(productDict, '_')
    flatSkuDict = FlatDict(skuDict, '_')
    
    return(dict(flatProductDict), dict(flatSkuDict))


def get_product_data(productId):
    """ takes a product id, gets its data dictionary from the product api, and applies the 
    returned dictionary to the two main product functions- get_skus and parse_product_data- 
    and returns 3 dictionaries. it returns a dictionary containing the product id and all 
    its sku ids, a dictionary containing the product id and its product data, and a dictionary 
    containing the default sku and its associated sku data. """
    
    #get product data from api
    productHandler = APIHandler(APIEndpoint.PRODUCT, productId)
    jsonDict = productHandler.get()
    
    skuDirectory = get_skus(jsonDict)
    productDict, skuDict = parse_product_data(jsonDict)

    return({productId: skuDirectory}, {productId: productDict}, {skuDict['id']: skuDict})


def get_skus(jsonDict):
    """ takes a json dictionary containing product data and returns a list of all skus, aka ids 
    for each of the product variants, associated with the product """
    
    skus = []
    if isinstance(jsonDict, dict):
        swatches = jsonDict['data']['swatches'] 
        if swatches == None:
            skus = [jsonDict['data']['product']['defaultSku']]
        else:
            variants = swatches['items']
            skus = [variant['skuImages']['mainImage'].split('/')[-1] for variant in variants]
    return(skus)


def parse_category(productsDict):
    """ returns a dataframe containing every existing category with its id, name,
    and action url. """
    
    categoryDict = {}
    for key, value in productsDict.items():
        for d in (value['data_product_categoryPath_items']):
            categoryDict.update({d['actionUrl'].split('?N=')[1]: d})
    return(pd.DataFrame.from_dict(categoryDict, orient='index'))


def get_category_directory(productsDict):
    """ the product api returns a value which is a list of dictionaries containing the 
    categories into which the products fall and its action url. this funtion takes the 
    parameter productsDict which contains all of the product data and returns a dataframe
    containing each product id and category id pair. """
    
    categoryDirectoryDict = {}
    i = 0

    #note: because there will be a many-to-many relationship between this linked table
    #and the product and category tables, neither product id or category id is unique.
    #that is why a separate id, i, is being used.
    for productId, productDict in productsDict.items():
        for d in productDict['data_product_categoryPath_items']:
            categoryDirectoryDict.update({i: {'productId': productId, 'categoryId': d['actionUrl'].split('?N=')[1]}})
            i = i+1
    
    categoryDirectory = pd.DataFrame.from_dict(categoryDirectoryDict, orient='index')
    
    return(categoryDirectory)


def clean_products(productsDict):
    """ takes in the full product data dictionary and returns a cleaned up product dataframe 
    with only the deisred columns """
    products = (
            pd.DataFrame.from_dict(productsDict, orient='index')
            .loc[:, 
            [
                'data_product_id', 
                'data_product_displayName', 
                'data_brand_brandName', 
                'data_product_live', 
                'data_reviewSummary_rating', 
                'data_reviewSummary_reviewCount', 
                'meta_lastFetchedTime'
            ]
        ]
        .rename(columns=
            {
                'data_product_displayName': 'displayName', 
                'data_product_id': 'productId', 
                'data_product_live': 'isLive', 
                'data_reviewSummary_rating': 'rating', 
                'data_reviewSummary_reviewCount': 'reviewCount', 
                'data_brand_brandName': 'brandName', 
                'meta_lastFetchedTime': 'lastFetchedTime'

            }
        )
    )

    return(products)




""" SKU FUNCTIONS """

def get_sku_ids(skusDirectory):
    """ takes in the skusDirectory and returns a list of every sku """
    allSkuIds = []
    
    if skusDirectory != {}:
        for key, value in skusDirectory.items():
            allSkuIds = value + allSkuIds
            
    return(allSkuIds)
    

def get_skus_to_scrape(skusDirectory, productsDict):
    """ the product api returns sku data for the default sku. because we already have that
    data, we do not want to use the sku api to fetch it again. thus, this function takes 
    the full sku list and the list of default skus and removes the default skus from the 
    full sku list. """
    allSkuIds = []
    
    if skusDirectory != {} and productsDict != {}:
        allSkuIds = get_sku_ids(skusDirectory)    
        defaultSkus = pd.DataFrame.from_dict(productsDict, orient='index')['data_product_defaultSku'].tolist()

        for skuId in defaultSkus:
            try:
                allSkuIds.remove(skuId)
            except ValueError:
                print('%s not in list'%skuId)

    return(allSkuIds)

    
def get_sku_data(skuId):
    """takes an sku id, gets its data dictoinary from the sku api, and flattens the data
    dictionary which turns the nested dictionary into a single dictionary. the resulting 
    dictionary is returned with its associated sku id. """
    
    #get sku data from api
    skuHandler = APIHandler(APIEndpoint.SKU, skuId)
    jsonDict = skuHandler.get()

    skuDict = FlatDict(jsonDict['data']['sku'], '_')

    return({skuId: dict(skuDict)})
    
    
def get_sku_directory(skusDirectoryDict):
    """ the skusDirectoryDict is created when the product data is being gathered for 
    simplicity's sake. then, that dictionary is turned into a dataframe containing
    every product id and sku id pair. """
    
    temp = {}
    i = 0

    for productId, skuList in skusDirectoryDict.items():
        for skuId in skuList:
            temp.update({i: {'productId': productId, 'skuId': skuId}})
            i = i + 1

    skusDirectory = pd.DataFrame.from_dict(temp, orient='index')
    
    return(skusDirectory)


def get_badges(row):
    """ the sku data dictionary contains a list of dictionaries containing each badge 
    assigned to the given sku id. this function takes in a row of the sku dataframe 
    and returns a condensed single string version of that dictionary list. """
    
    if isinstance(row['badges_items'], list):
        temp = []
        for d in row['badges_items']:
            temp.append(d['badgeName'])
        return(", ".join(temp))
    else:
        return('')
    
    
def clean_skus(skusDict, skusDirectoryDict={}):
    """ takes in the full sku data dictionary and returns a cleaned up sku dataframe with 
    only the deisred columns """
    
    skus = pd.DataFrame.from_dict(skusDict, orient='index')

    if 'price_salePrice_amount' not in skus:
        skus['price_salePrice_amount'] = [np.nan] * len(skus)
        
    skus = (
        skus
        .loc[:, 
            [
                'id',
                'UPC', 
                'displayName', 
                'storeOnly',
                'onlineOnlyStatus',  
                'price_onlineOnlySalePrice', 
                'price_listPrice_amount',
                'price_salePrice_amount',
                'variant_variantType',
                'variant_variantDesc', 
                'size', 
                'UOM',
                'inventoryStatus',
                'couponEligible',
                'badges_items'    
            ]
        ]
        .rename(columns=
            {
                'id': 'skuId',
                'price_onlineOnlySalePrice': 'onlineOnlySalePrice', 
                'price_listPrice_amount': 'listPrice',
                'price_salePrice_amount': 'salePrice',
                'variant_variantType': 'variantType',
                'variant_variantDesc': 'variantDesc' 
            }
        )
    )

    skus['badge'] = skus.apply(get_badges, axis=1)
    
    if skusDirectoryDict == {}:
            skus = (
            skus
            .drop(columns={'badges_items'})
            .dropna(subset=['listPrice'])
            .astype({'skuId': 'str', 'listPrice': 'float', 'salePrice': 'float'})
        )
    else:    
        skus = (
            skus
            .drop(columns={'badges_items'})
            .merge(get_sku_directory(skusDirectoryDict))
            .dropna(subset=['listPrice'])
            .astype({'skuId': 'str', 'listPrice': 'float', 'salePrice': 'float'})
        )
    
    return(skus)




""" DYNAMIC DATA FUNCTIONS"""

def get_dynamic_data(skuId):
    """takes an sku id, gets its data dictoinary from the dynamic api, and flattens the data
    dictionary which turns the nested dictionary into a single dictionary. the resulting 
    dictionary is returned with its associated sku id. """
    
    #get sku data from api
    dynamicHandler = APIHandler(APIEndpoint.DYNAMIC, skuId)
    jsonDict = dynamicHandler.get()

    dynamicDict = FlatDict(jsonDict['data'], '_')

    return({skuId: dict(dynamicDict)})


def get_promotions(row):
    """ the dynamic data dictionary contains a list of dictionaries containing each promotion 
    assigned to the given sku id. this function takes in a row of the dynamic data dataframe 
    and returns a condensed single string version of that dictionary list. """
    if isinstance(row['promotions_items'], list):
        temp = []
        for d in row['promotions_items']:
            temp.append(d['displayName'])
        return(", ".join(temp))
    else:
        return('')
    
    
def clean_dynamic_data(dynamicDict):
    """ takes in the dynamic data dictionary and returns a clean dataframe """
    dynamicData = (
        pd.DataFrame.from_dict(dynamicDict, orient='index')
        .reset_index()
        .rename(columns=
            {
                'index': 'skuId',
                'price_listPrice_amount': 'listPrice',
                'price_salePrice_amount': 'salePrice'
            }
        )
    )
    
    dynamicData['promotions'] = dynamicData.apply(get_promotions, axis=1)
    
    dynamicData = dynamicData.loc[:, ['skuId', 'listPrice', 'salePrice', 'promotions']]
    
    return(dynamicData)