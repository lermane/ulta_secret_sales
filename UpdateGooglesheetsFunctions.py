""" functions used in SecretSales.py """

import database.UltaDBHandler as UltaDBHandler
import UltaScraper.APIParsingFunctions as apf
import pandas as pd
import numpy as np




def get_secret_sales_in_stock(skusDict):
    """ I want there to be one row for each product/current_price/offers grouping*. first I am creating
    the dataframe groupVariants which contains just that. I am then rejoining that dataframe to the 
    main sku dataframe to include all wanted secret sales data. then I am returning the dataframe. Note
    that this function is not called directly, but is used in the below function, get_data_for_excel. 
    
    *the reason why I am grouping the products this way instead of just one row per product is because
    1) some skus within a single product have different prices and 2) some skus within a single product 
    have different offers 
    """
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
    
    return(secretSalesInStock)


def get_group_variants(secretSalesInStock):
    """ I want there to be one row for each product/current_price/offers grouping*. To achieve this, 
    this function takes in the newly created secretSalesInStock dataframe from get_secret_sales_in_stock,
    groups it by product id, current price, and offers, and then returns it.
    
    *the reason why I am grouping the products this way instead of just one row per product is because
    1) some skus within a single product have different prices and 2) some skus within a single product 
    have different offers 
    """
    groupVariants = (
        secretSalesInStock
        .fillna('')
        .groupby(['product_id', 'current_price', 'offers'])
        .apply(lambda x: ', '.join(x.variant_desc))
        .reset_index()
        .rename(columns={0: 'variants'})
    )

    return(groupVariants)


def get_group_category_names(groupVariants):
    """ I want each row to list the product's category, sub_category, and sub_sub_category. To do this,
    the function first gets the needed category information from ulta_db. The returned dataframe is then
    joined to the groupVariants dataframe. The resulting dataframe is in long format, with one row for
    each category. To turn it into wide format, a new column, category, is created, to assign each category
    within each product/current_price/offers group the id category_1, category_2, or category_3, depending
    on whether the category is the first, second, or third category listed in the grouping. these categories 
    are then used as column names, with each grouping's category_name being the column values. these columns 
    are the category, sub_category, and sub_sub_category columns. the function then returns the dataframe. """    
    with UltaDBHandler.UltaDBHandler() as ulta_db:
        cd = ulta_db.execute("SELECT cd.fk_product_id, c.category_name FROM category_directory cd LEFT JOIN category c ON cd.fk_category_id = c.category_id WHERE c.category_name NOT LIKE 'By%'ORDER BY c.category_id")

    categoryDirectory = pd.DataFrame(cd).rename(columns={0: 'product_id', 1: 'category_name'})

    groupCategoryNames = pd.merge(groupVariants, categoryDirectory, how='left', on='product_id')

    category = []
    for index, row in groupCategoryNames.groupby(['product_id']).size().reset_index().iterrows():
        n = row[0]
        for i in range(n):
            category.append('category_' + str(i+1))

    groupCategoryNames['category'] = category
    
    groupCategoryNames = (
        groupCategoryNames
        .pivot_table(index='product_id', columns='category', values='category_name', aggfunc='first')
        .reset_index()
        .loc[:, ['product_id', 'category_1', 'category_2', 'category_3']]
        .rename(columns={'category_1': 'category', 'category_2': 'sub_category', 'category_3': 'sub_sub_category'})
        .fillna(' ')
    )
    
    return(groupCategoryNames)


def get_data_for_excel(skusDict, testing=0):
    """ this function takes in the full sku data dictionary from the ulta sku api. then, it applies the
    three above functions, get_secret_sales_in_stock, get_group_variants, and get_group_category names, 
    to get secretSalesInStock, groupVariants, and groupCategoryNames respectively. afterwards, the 
    dataframes are merged together and returned in the format needed for googlesheets, along with the 
    urls to create the hyperlinks. """
    secretSalesInStock = get_secret_sales_in_stock(skusDict)
    groupVariants = get_group_variants(secretSalesInStock)
    groupCategoryNames = get_group_category_names(groupVariants)

    secretSales = (
        pd.merge(groupVariants, groupCategoryNames, how='left', on='product_id')
        .pipe(pd.merge, secretSalesInStock, how='right', on=['product_id', 'current_price', 'offers'], indicator=True)
        .loc[:, ['product_id', 'category', 'sub_category', 'sub_sub_category', 'brand_name', 'product_name', 'current_price', 'max_price' ,'percent_off', 'variants', 'offers', 'url']]
        .drop_duplicates()
        .dropna(subset=['url'])
        .fillna(' ')
    )
    
    offers = []
    for index, row in secretSales.iterrows():
        if 'sale' in row['offers'].lower():
            offers.append(np.nan)
        else:
            offers.append(row['offers'])

    productNames = []
    for index, row in secretSales.iterrows():
        if '"' in row['product_name']:
            productNames.append(row['product_name'].replace('"', "'"))
        else:
            productNames.append(row['product_name'])

    secretSales['product_name'] = productNames
    secretSales['offers'] = offers

    urls = secretSales['url'].tolist()

    secretSales = (
        secretSales
        .drop(columns={'url'})
        .dropna(subset=['offers'])
        .fillna(' ')
        .reset_index(drop=True)
        .sort_values('product_id')
    )

    if testing == 0:
        secretSales = secretSales.drop(columns={'product_id'})

    return(secretSales, urls)