import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from UltaScraper.spiders import ultaspider

process = CrawlerProcess(get_project_settings())

process.crawl(ultaspider.UltaSpider)
process.start() # the script will block here until the crawling is finished