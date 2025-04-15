from scrapy import Spider
import scrapy
from scrapy.selector import Selector
from crawler.items import CrawlerItem

class CrawlerSpider(Spider):
    name = "crawler"
    start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?q=Google&od=2&bydaterang=4&newstype=all" for i in range(0,55)]
    start_urls += [f"https://vietnamnet.vn/tim-kiem-p{i}?bydaterang=4&q=nvidia" for i in range(0,21)]
    start_urls += [f"https://vietnamnet.vn/tim-kiem-p{i}?bydaterang=4&newstype=all&od=2&q=Microsoft" for i in range(0,27)]

    def parse(self, response):
        # item = CrawlerItem()
        # item['title'] = response.css('h1.content-detail-title::text').get()
        # item['publish_date'] = response.css('div.bread-crumb-detail__time::text').get()
        # item['content'] = " ".join(response.css('div.content-detail p::text').getall())
        
        
        # yield item
        
        article_links = response.css('a[href*=".html"]::attr(href)').getall()
        # print(article_links)
        for link in article_links:
            full_link = response.urljoin(link)
            # print(full_link)
            yield scrapy.Request(full_link, callback=self.parse_article)

    def parse_article(self, response):
        item = CrawlerItem()
        item['title'] = response.css('h1.content-detail-title::text').get()
        item['publish_date'] = response.css('div.bread-crumb-detail__time::text').get()
        item['content'] = " ".join(response.css('div.content-detail p::text').getall())

        yield item