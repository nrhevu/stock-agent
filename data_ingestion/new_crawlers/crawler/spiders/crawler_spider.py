from scrapy import Spider
import scrapy
from scrapy.selector import Selector
from crawler.items import CrawlerItem
import re

class CrawlerSpider(Spider):
    name = "crawler"
    # start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?q=Google&od=2&bydaterang=1&newstype=all" for i in range(0,2)]
    # start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?bydaterang=1&q=nvidia" for i in range(0,2)]
    # start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?bydaterang=1&newstype=all&od=2&q=Microsoft" for i in range(0,2)]
    def __init__(self, keyword=None, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.keyword = keyword
    
        if keyword == "nvidia":
            self.start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?bydaterang=1&q=nvidia" for i in range(0,2)]
        elif keyword == "google":
            self.start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?q=Google&od=2&bydaterang=1&newstype=all" for i in range(0,2)]
        elif keyword == "microsoft":
            self.start_urls = [f"https://vietnamnet.vn/tim-kiem-p{i}?bydaterang=1&newstype=all&od=2&q=Microsoft" for i in range(0,2)]

    def clean_text(self,text):
    # Loại bỏ các ký tự thừa như \r, \n và khoảng trắng thừa
        if text is None:
            return text
        text = re.sub(r'\s+', ' ', text.strip())
        return text

    def extract_date(self, text):
        # Chỉ giữ lại ngày tháng năm trong publish_date
        if text is None:
            return text
        date_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        return date_match.group(1) if date_match else text
    def start_requests(self):
        
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

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
        title = response.css('h1.content-detail-title::text').get()
        date = response.css('div.bread-crumb-detail__time::text').get()
        content = " ".join(response.css('div.content-detail p::text').getall())
        if self.keyword[1:] in title or self.keyword[1:] in content:
            item['title'] = self.clean_text(title)
            item['publish_date'] = self.extract_date(date)
            item['content'] = self.clean_text(content)
            yield item