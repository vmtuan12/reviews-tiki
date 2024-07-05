from typing import Any, Iterable
from unidecode import unidecode
import scrapy
import re
import time
from crawl_reviews.spiders.base_spider import BaseSpider
from scrapy.http import Request, Response

class WorkerSpider0(BaseSpider):
    name = "spider-0"
    spider_id = 0

    def start_requests(self):
        return super().start_requests()