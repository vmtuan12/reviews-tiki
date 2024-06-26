from typing import Any, Iterable
from unidecode import unidecode
import scrapy
import re
import time
from crawl_reviews.utils.redis_connector import RedisConnector
# from crawl_reviews.utils.tiki_utils import *

from scrapy.http import Request, Response

class BaseSpider(scrapy.Spider):

    redis_conn = RedisConnector()
    redis_client = redis_conn.get_client()

    def start_requests(self):
        ready_scrape_page_key = self.redis_conn.get_ready_scrape_page_by_spider(self.spider_id)

        if ready_scrape_page_key == None:
            self.choose_category()
        else:
            self.crawl_remaining()

    def choose_category(self):
        pass

    def crawl_remaining(self):
        pass

    def parse_list_product(self, response: Response, **kwargs: Any):
        pass

    def parse_product_detail(self, response: Response, **kwargs: Any):
        pass

    def parse_comment(self, response: Response, **kwargs: Any):
        pass

    def parse_shop_info(self, response: Response, **kwargs: Any):
        pass