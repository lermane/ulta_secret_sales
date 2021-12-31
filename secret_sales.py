import GoogleSheetsHandler
import database.UltaDBHandler as UltaDBHandler
import SecretSalesFunctions as ssf
import concurrent.futures
import APIParsingFunctions as apf


with UltaDBHandler.UltaDBHandler() as ulta_db:
    skuIds = ulta_db.get_secret_sales()['sku_id'].tolist()
    
    
skusDict = {}

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    future2skuId = {executor.submit(apf.get_sku_data, skuId):skuId for skuId in skuIds}
    for future in concurrent.futures.as_completed(future2skuId):
        skuId = future2skuId[future]
        try:
            skuDict = future.result()
            skusDict.update(skuDict)
        except Exception as exc:
            print('%r generated an exception: %s' % (skuId, exc))    

            
secretSalesInStock, hyperlinkUrls = ssf.get_data_for_excel(skusDict)


with GoogleSheetsHandler.GoogleSheetsHandler('dev') as g:
    g.clear_sheet()
    g.export_dataframe_to_sheets(secretSalesInStock)
    g.update_filter(len(secretSalesInStock), len(secretSalesInStock.columns))
    g.add_hyperlinks(secretSalesInStock, hyperlinkUrls)
    g.add_percent_format(len(secretSalesInStock))
    g.resize_columns(len(secretSalesInStock))
    g.resize_rows(len(secretSalesInStock))