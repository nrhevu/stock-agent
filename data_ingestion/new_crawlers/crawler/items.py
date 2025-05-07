# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    title = scrapy.Field()
    publish_date = scrapy.Field()
    content = scrapy.Field()
    # link = scrapy.Field()