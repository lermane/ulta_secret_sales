import UltaScraper.APIParsingFunctions as apf
import database.UpdateDBFunctions as udf
import concurrent.futures
import database.UltaDBHandler as UltaDBHandler
import time


#get product ids to scrape
productIds = udf.get_product_ids()

#scrape product and sku data using their respective apis
if productIds != []:
    print('scraping product api...')
    start = time.time()

    productsDict = {}
    skusDict = {}
    skusDirectoryDict = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future2productId = {executor.submit(apf.get_product_data, productId):productId for productId in productIds}
        for future in concurrent.futures.as_completed(future2productId):
            productId = future2productId[future]
            try:
                skuDirectory, productDict, skuDict = future.result()
                skusDirectoryDict.update(skuDirectory)
                productsDict.update(productDict)
                skusDict.update(skuDict)
            except Exception as exc:
                if hasattr(exc, 'msg'):
                    print('%r generated an exception: %s' % (productId, exc.msg))
                else:
                    print('%r generated an exception: %s' % (productId, exc))
                
    skuIds = apf.get_skus_to_scrape(skusDirectoryDict, productsDict)
    
    print('scraping sku api...')
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future2skuId = {executor.submit(apf.get_sku_data, skuId):skuId for skuId in skuIds}
        for future in concurrent.futures.as_completed(future2skuId):
            skuId = future2skuId[future]
            try:
                skuDict = future.result()
                skusDict.update(skuDict)
            except Exception as exc:
                if hasattr(exc, 'msg'):
                    print('%r generated an exception: %s' % (skuId, exc.msg))
                else:
                    print('%r generated an exception: %s' % (skuId, exc))
            
    category = apf.parse_category(productsDict)
    categoryDirectory = apf.get_category_directory(productsDict)  
    products = apf.clean_products(productsDict)
    skus = apf.clean_skus(skusDict, skusDirectoryDict)
    
    end = time.time()
    
    print('total time:', (end-start)/60)
    print('per product_id:', ((end-start)/60)/len(productIds))
    
    #update ultadb
    print('update ultadb...')
    
    with UltaDBHandler.UltaDBHandler() as ulta_db:
        ulta_db.add_products(products)
        ulta_db.add_category(category)
        ulta_db.add_category_directory(categoryDirectory)
        ulta_db.add_skus(skus)
        ulta_db.add_prices(skus)
        
    print('update is complete.')
        
else:
    print('ultadb is up to date!')