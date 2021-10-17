import scrapy
from UltaScraper.items import UltascraperItem
from scrapy.loader import ItemLoader


class UltaSpider(scrapy.Spider):
    name = 'ulta'
    start_urls = [
        'https://www.ulta.com/makeup-eyes-eyeshadow?N=26yf'
    ]
    
    def parse(self, response):
        for products in response.css('div.productQvContainer'):
            l = ItemLoader(item = UltascraperItem(), selector = products)
            
            l.add_css('product_id', 'span.prod-id::text')
            l.add_xpath('brand', '/html/body/div[1]/div[6]/div/div[2]/div[6]/div/div/ul/li[1]/div/div[3]/h4/a/text()')
            l.add_xpath('name', '/html/body/div[1]/div[6]/div/div[2]/div[6]/div/div/ul/li[1]/div/div[3]/p/a/text()')
            l.add_css('options', 'span.pcViewMore')     
            l.add_css('price', 'span.regPrice') #span.pro-old-price
            l.add_css('sale_price', 'span.pro-new-price')
            l.add_css('old_price', 'span.pro-old-price')
            l.add_css('url', 'a::attr(href)')
            
            #product_id = products.attrib['id']
            #brand = products.xpath('/html/body/div[1]/div[6]/div/div[2]/div[6]/div/div/ul/li[1]/div/div[3]/h4/a/text()').get().strip()
            #name = products.xpath('/html/body/div[1]/div[6]/div/div[2]/div[6]/div/div/ul/li[1]/div/div[3]/p/a/text()').get().strip()
            #url = "https://www.ulta.com" + products.css('a').attrib['href']

            #if  products.css('span.pcViewMore::text').get() is not None:
            #    options = " ".join(products.css('span.pcViewMore::text').get().split())
            #if products.css('span.pro-new-price::text').get() is not None:
            #    sale_price = products.css('span.pro-new-price::text').get().strip()
            #    price = products.css('span.pro-old-price::text').get().strip()
            #    sale = 1
            #if products.css('span.regPrice::text').get() is not None:
            #    price = products.css('span.regPrice::text').get().strip()
            
            yield l.load_item()
            
                
        next_page = "https://www.ulta.com" + response.css('a.next').attrib['href']
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)