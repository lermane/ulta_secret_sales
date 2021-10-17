# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags

def add_base_url(url):
    return 'https://www.ulta.com' + url

def remove_whitespace(s):
    return s.strip()

def clean_options(options):
    return " ".join(options.split())

class UltascraperItem(scrapy.Item):
    # define the fields for your item here like:
    product_id = scrapy.Field(input_processor = MapCompose(remove_tags, remove_whitespace), output_processor = TakeFirst())
    brand = scrapy.Field(input_processor = MapCompose(remove_tags, remove_whitespace), output_processor = TakeFirst())
    name = scrapy.Field(input_processor = MapCompose(remove_tags, remove_whitespace), output_processor = TakeFirst())
    options = scrapy.Field(input_processor = MapCompose(remove_tags, clean_options), output_processor = TakeFirst())
    price = scrapy.Field(input_processor = MapCompose(remove_tags, remove_whitespace), output_processor = TakeFirst())
    sale_price = scrapy.Field(input_processor = MapCompose(remove_tags, remove_whitespace), output_processor = TakeFirst())
    old_price = scrapy.Field(input_processor = MapCompose(remove_tags, remove_whitespace), output_processor = TakeFirst())
    url = scrapy.Field(input_processor = MapCompose(add_base_url), output_processor = TakeFirst())