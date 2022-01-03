import scrapy
from UltaScraper.items import UltascraperItem
from scrapy.loader import ItemLoader


def get_urls():
    with open("/home/lermane/Documents/ulta_secret_sales/UltaScraper/data/urls.txt","r") as f:
        u = f.read()
    
    urls = [i for i in u.split('\n') if i != '']   
    
    return (urls)


class UltaSpider(scrapy.Spider):
    name = 'ulta'
    start_urls = get_urls()
    
    def parse(self, response):
        for products in response.css('div.productQvContainer'):
            l = ItemLoader(item = UltascraperItem(), selector = products)
            
            l.add_css('product_id', 'span.prod-id::text')
            l.add_css('options', 'span.pcViewMore')     
            l.add_css('price', 'span.regPrice') #span.pro-old-price
            l.add_css('sale_price', 'span.pro-new-price')
            l.add_css('old_price', 'span.pro-old-price')
            l.add_css('url', 'a::attr(href)')
            
            yield l.load_item()
            
        
        if response.css('a.next') != []:
            next_page = "https://www.ulta.com" + response.css('a.next').attrib['href']
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse)