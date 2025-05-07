# How to run

```shell
cd data_ingestion/new_crawlers/crawler
scrapy crawl crawler -a keyword=microsoft -o <path-to-save/raw-data.json> 
scrapy crawl crawler -a keyword=google -o <path-to-save/raw-data.json> 
scrapy crawl crawler -a keyword=nvidia -o <path-to-save/raw-data.json> 
```