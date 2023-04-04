""" these functions are used in updating the ulta_db database """

import json
import datetime
import pandas as pd
import glob
import os.path



def _get_today_ulta():
    """ returns today's ulta json file from scrapy """
    folder_path = r'UltaScraper/data/ulta'
    file_type = '/*json'
    files = glob.glob(folder_path + file_type)
    file = max(files, key=os.path.getctime)

    with open(file,'r') as f:
        ultaJson = json.loads(f.read())
        
    ulta = pd.DataFrame(ultaJson).drop_duplicates()
    
    return(ulta)


def _get_yesterday_ulta():
    """ returns yesterday's ulta json file from scrapy """
    folder_path = r'UltaScraper/data/ulta'
    file_type = '/*json'
    files = glob.glob(folder_path + file_type)
    file = sorted(files, key=os.path.getctime)[-2]
    
    with open(file,'r') as f:
        ultaJson = json.loads(f.read())
            
    yestUlta = (
        pd.DataFrame(ultaJson)
        .rename(columns=
            {
                'options': 'yest_options', 
                'sale_price': 'yest_sale_price', 
                'old_price': 'yest_old_price', 
                'url': 'yest_url', 
                'price': 'yest_price'
            }
        )
        .drop_duplicates()
    )
    
    return(yestUlta)


def get_product_ids():
    """ merges today's ulta file and yesterday's ulta file to figure out which products
    need updated data """
    ultaMerge = (
        pd.merge(_get_today_ulta(), _get_yesterday_ulta(), how='left', on='product_id', indicator=True)
        .fillna(
        {
            'product_id': '', 
            'price': 'NA', 
            'url': "", 
            'options': 'NA', 
            'sale_price': 'NA', 
            'old_price': 'NA', 
            'yest_options': 'NA', 
            'yest_sale_price': 'NA', 
            'yest_old_price': 'NA', 
            'yest_url': '', 
            'yest_price': 'NA'
        })
    )
    ultaMerge['sale_price'] = ultaMerge.get('sale_price', 'NA')
    ultaMerge['old_price'] = ultaMerge.get('old_price', 'NA')    
    
    newProducts = ultaMerge.query("_merge != 'both'")['product_id'].tolist()
    changedPrice = ultaMerge.query("price != yest_price & _merge == 'both'")['product_id'].tolist()
    changedSalePrice = ultaMerge.query("sale_price != yest_sale_price & _merge == 'both'")['product_id'].tolist()
    changedOldPrice = ultaMerge.query("old_price != yest_old_price & _merge == 'both'")['product_id'].tolist()

    # make sure an option, aka a new SKU, wasn't added
    newSkus = []
    for index, row in ultaMerge.iterrows():
        if (row['options'] != row['yest_options']) and (row['options'] != 'NA') and (row['yest_options'] != 'NA'):
            if int(row['options'].split(' ')[0]) > int(row['yest_options'].split(' ')[0]):
                newSkus.append(row['product_id'])

    productIds = newProducts + changedPrice + changedSalePrice + changedOldPrice + newSkus
    productIds = list(set(productIds))
    
    return(productIds)