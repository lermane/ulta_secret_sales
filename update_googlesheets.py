import googlesheets.GoogleSheetsHandler as GoogleSheetsHandler
import database.UltaDBHandler as UltaDBHandler
import UpdateGooglesheetsFunctions as ugf
import concurrent.futures
import UltaScraper.APIParsingFunctions as apf


#get the sku ids of the secret_sales table from ulta_db
with UltaDBHandler.UltaDBHandler() as ulta_db:
    skuIds = ulta_db.get_secret_sales()['sku_id'].tolist()
    

#scrape the sku ids from the sku api to get the most up-to-date information, 
#along with stock and promotion/offers data that is not saved in the database
print('scraping sku api for secret sales data...')

skusDict = {}
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


#get the data into the format needed to be posted to googlesheets
secretSalesInStock, hyperlinkUrls = ugf.get_data_for_excel(skusDict)


#update googlesheets
print('updating googlesheets...')

with GoogleSheetsHandler.GoogleSheetsHandler('dev') as g:
    g.clear_sheet()
    g.export_dataframe_to_sheets(secretSalesInStock)
    g.update_filter(len(secretSalesInStock), len(secretSalesInStock.columns))
    g.add_hyperlinks(secretSalesInStock, hyperlinkUrls)
    g.add_percent_format(len(secretSalesInStock))
    g.resize_columns(len(secretSalesInStock))
    g.resize_rows(len(secretSalesInStock))
    g.add_currency_format(len(secretSalesInStock))
    g.add_header()
    
with GoogleSheetsHandler.GoogleSheetsHandler('prod') as g:
    g.clear_sheet()
    g.export_dataframe_to_sheets(secretSalesInStock)
    g.update_filter(len(secretSalesInStock), len(secretSalesInStock.columns))
    g.add_hyperlinks(secretSalesInStock, hyperlinkUrls)
    g.add_percent_format(len(secretSalesInStock))
    g.resize_columns(len(secretSalesInStock))
    g.resize_rows(len(secretSalesInStock))
    g.add_currency_format(len(secretSalesInStock))
    g.add_header()