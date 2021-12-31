""" functions used in SecretSales.py """

import database.UltaDBHandler as UltaDBHandler
import APIParsingFunctions as apf
import pandas as pd




def get_data_for_excel(skusDict, testing=0):
    with UltaDBHandler.UltaDBHandler() as ulta_db:
        secretSales = ulta_db.get_secret_sales()
    
    skus = apf.clean_skus(skusDict).rename(columns={'skuId': 'sku_id'})

    secretSalesInStock = (
        pd.merge(pd.merge(secretSales, skus, how='left', on='sku_id'), apf.get_product_urls(), how='left', on='product_id')
        .query("inventoryStatus == 'InStock'")
        .loc[:, 
            [
                'product_id', 
                'sku_id', 
                'product_name', 
                'brand_name', 
                'variant_type', 
                'variant_desc', 
                'current_price', 
                'max_price', 
                'percent_off',
                'badge',
                'url'
            ]
        ]
        .rename(columns={'badge': 'offers'})
    )

    groupVariants = (
        secretSalesInStock
        .fillna('')
        .groupby(['product_id', 'current_price', 'offers'])
        .apply(lambda x: ', '.join(x.variant_desc))
        .reset_index()
        .rename(columns={0: 'variants'})
    )

    if testing == 1:
        df = (
            pd.merge(secretSalesInStock, groupVariants, how='left', on=['product_id', 'current_price', 'offers'])
            .sort_values('product_id')
            .loc[:, ['product_id', 'product_name', 'brand_name', 'current_price', 'max_price', 'percent_off', 'variants', 'offers', 'url']]
            .drop_duplicates()
            .reset_index(drop=True)
            .dropna(subset=['url'])
        )
    else:
        df = (
            pd.merge(secretSalesInStock, groupVariants, how='left', on=['product_id', 'current_price', 'offers'])
            .sort_values('product_id')
            .loc[:, ['product_name', 'brand_name', 'current_price', 'max_price', 'percent_off', 'variants', 'offers', 'url']]
            .drop_duplicates()
            .reset_index(drop=True)
            .dropna(subset=['url'])
        )

    urls = df['url'].tolist()

    df = df.drop(columns={'url'})

    df = df[~df['offers'].str.contains("Sale")]
    
    return(df, urls)