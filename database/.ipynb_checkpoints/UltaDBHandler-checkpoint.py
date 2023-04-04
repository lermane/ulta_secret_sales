import pandas as pd
import psycopg2
from configparser import ConfigParser
#

def config(filename='/home/lermane/Documents/ulta_secret_sales/database/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


#code used as example: https://stackoverflow.com/questions/37648667/python-how-to-initiate-and-close-a-mysql-connection-inside-a-class
class UltaDBHandler:
    _server: str
    _params: dict
    _conn: psycopg2.extensions.connection
    _cur: psycopg2.extensions.cursor
        
    def __init__(self, server='postgresql'):
        self._server = server
        self._params = None
        self._conn = None
        self._cur = None

    def __enter__(self):
        # This ensure, whenever an object is created using "with"
        # this magic method is called, where you can create the connection.
        print('Connecting to the PostgreSQL database...')
        self._params = config(section=self._server)
        self._conn = psycopg2.connect(**self._params)
        self._cur = self._conn.cursor()
        return self

    def __exit__(self, exception_type, exception_val, trace):
        # once the with block is over, the __exit__ method would be called
        # with that, you close the connnection
        try:
            self._cur.close()
            self._conn.commit()
            self._conn.close()
            print('Connection to PostgreSQL database successfully closed.')
        except AttributeError: # isn't closable
            print('Not closable.')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return True # exception handled successfully

    def add_products(self, products):
        productsTuple = products.reset_index().loc[:, ['productId', 'displayName', 'brandName', 'index']].to_records(index=False)
        query_str = """
        INSERT INTO product (product_id, product_name, brand_name, insert_timestamp)
        SELECT %s, %s, %s, NOW()
        WHERE NOT EXISTS (SELECT product_id FROM product WHERE product_id = %s)"""
        self._cur.executemany(query_str, productsTuple)


    def add_category(self, category):
        category = category.reset_index()
        category['category_id'] = category['index']
        categoryTuple = category.loc[:, ['category_id', 'actionUrl', 'name', 'index']].to_records(index=False)
        query_str = """
        INSERT INTO category (category_id, action_url, category_name)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (SELECT category_id FROM category WHERE category_id = %s)"""
        self._cur.executemany(query_str, categoryTuple)


    def add_category_directory(self, categoryDirectory):
        categoryDirectory['index1'] = categoryDirectory['productId']
        categoryDirectory['index2'] = categoryDirectory['categoryId']
        categoryDirectoryTuple = categoryDirectory.loc[:, ['productId', 'categoryId', 'index1', 'index2']].to_records(index=False)
        query_str = """
        INSERT INTO category_directory (fk_product_id, fk_category_id)
        SELECT %s, %s
        WHERE NOT EXISTS (SELECT fk_product_id, fk_category_id FROM category_directory WHERE fk_product_id = %s AND fk_category_id = %s)"""
        self._cur.executemany(query_str, categoryDirectoryTuple)


    def add_skus(self, skus):
        skus['index'] = skus['skuId']
        skusTuple = skus.loc[:, ['skuId', 'UPC', 'productId', 'displayName', 'variantType', 'variantDesc', 'size', 'UOM', 'index']].to_records(index=False)
        query_str = """
        INSERT INTO sku (sku_id, upc, fk_product_id, display_name, variant_type, variant_desc, size, uom, insert_timestamp)
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        WHERE NOT EXISTS (SELECT sku_id FROM sku WHERE sku_id = %s)"""
        for tup in skusTuple:
            args_str = self._cur.mogrify(query_str, tup).decode("utf-8")
            args_str = args_str.replace('\'NaN\'::float', 'NULL')
            self._cur.execute(args_str)


    def add_prices(self, skus):
        priceTuple = skus.loc[:, ['skuId', 'listPrice', 'salePrice']].to_records(index=False)
        args_str = ",".join(self._cur.mogrify("(%s,%s,%s,NOW())", x).decode("utf-8") for x in priceTuple)
        args_str = args_str.replace('\'NaN\'::float', 'NULL')
        self._cur.execute("INSERT INTO price (fk_sku_id, list_price, sale_price, insert_timestamp) VALUES " + args_str)
        
        
    def execute(self, query):
        response = ''
        try:
            self._cur.execute(query)
            response = self._cur.fetchall()
        except Exception as exc:
            print('ERROR!', query, exc)
            self._cur.execute('rollback;')
        return response
    
    
    def test(self):
        response = self.execute('SELECT * FROM product')
        return response
    
    
    def get_secret_sales(self):
        """ returns the secret_sales table from ulta_db """
        self._cur.execute("""
            SELECT 
                product_id, 
                sku_id, 
                product_name, 
                brand_name, 
                variant_type, 
                variant_desc, 
                size, 
                current_price, 
                max_price, 
                percent_off 
            FROM secret_sales 
            WHERE percent_off > 0.10 
            OR RIGHT(CAST(current_price AS TEXT),2) = '97'
        """)
        
        ss = self._cur.fetchall()

        secretSales = (
            pd.DataFrame(ss)
            .rename(columns=
                {
                    0: 'product_id', 
                    1: 'sku_id',
                    2: 'product_name', 
                    3: 'brand_name',
                    4: 'variant_type', 
                    5: 'variant_desc', 
                    6: 'size', 
                    7: 'current_price', 
                    8: 'max_price', 
                    9: 'percent_off'
                }
            )
            .astype({'current_price': 'float', 'max_price': 'float', 'percent_off': 'float'})
        )
        
        return(secretSales)