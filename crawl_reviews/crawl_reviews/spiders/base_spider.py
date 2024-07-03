from typing import Any, Iterable
from unidecode import unidecode
import scrapy
import re
import json
from crawl_reviews.utils.redis_connector import RedisConnector
from crawl_reviews.utils.tiki_utils import *
from crawl_reviews.utils.item_builder import generate_item
from crawl_reviews.items import Product, Shop, Review, User, ReviewChild

from scrapy.http import Request, Response

class BaseSpider(scrapy.Spider):

    redis_conn = RedisConnector()
    redis_client = redis_conn.get_client()

    category_page_limit = 50

    set_shop_page = set()
    set_review_page = set()

    # TODO: re-design the flow
    def start_requests(self):
        scraping_category_id = self.redis_conn.get_scraping_category_by_spider(self.spider_id)

        if scraping_category_id == None:
            self.choose_category()
        else:
            ready_scrape_page_set = scraping_category_id
            if len(ready_scrape_page_set) == 0:
                self.redis_conn.delete_category_key(scraping_category_id)

            self.crawl_remaining(ready_scrape_page_set)

    def choose_category(self):
        chosen_category_id = int(self.redis_conn.get_list_remaining_category_id_by_spider(spider_id=self.spider_id)[0])
        url_key = get_url_key_by_cate_id(chosen_category_id)

        current_page = 1

        api, headers = api_headers_list_item_by_category(category_id=chosen_category_id, url_key=url_key, page=current_page)

        while (current_page <= self.category_page_limit):
            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_list_product,
                                meta={"page": current_page, "from_cate": True, "cate_id": chosen_category_id})
            current_page += 1

    def crawl_remaining(self, ready_scrape_page_set: set):
        pass

    def parse_list_product(self, response: Response, **kwargs: Any):
        response_body = json.loads(response.text)
        cate_id = response.meta.get("cate_id")

        if response.meta.get("from_cate") == True:
            self._parse_list_from_cate(response_body=response_body, cate_id=cate_id)
        else:
            self._parse_list_from_shop(response_body=response_body, cate_id=cate_id)

        for item in response_body:
            current_review_page = 1
            self.set_review_page.add(item["id"])

            while (item["id"] in self.set_review_page):
                review_api, review_headers = api_headers_reviews(product_id=item["id"], spid=item["seller_product_id"], page=current_review_page)

                yield scrapy.Request(url=review_api,
                                    headers=review_headers,
                                    callback=self.parse_comment)
                
                current_review_page += 1
            
            self._review_page_remove_id(item["id"])

    def parse_shop_info(self, response: Response, **kwargs: Any):
        response_body = json.loads(response.text)
        response_data = response_body["data"]["seller"]

        yield generate_item(source=response_data, item_type=Shop)

        cursor = 0
        limit = 40
            
        self.set_shop_page.add(response_data["id"])

        while (response_data["id"] in self.set_shop_page):
            api, headers = api_headers_list_product_in_shop(cursor=cursor, shop_id=response_data["id"])

            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_list_product,
                                meta={"from_cate": False})
            cursor += limit

        self._shop_page_remove_id(response_data["id"])

    def parse_comment(self, response: Response, **kwargs: Any):
        response_body = json.loads(response.text)
        response_data = response_body.get("data")

        if response_data == None or len(response_data) == 0:
            self._review_page_remove_id(response_data["id"])
            return
        
        for item in response_data:
            yield generate_item(source=item, item_type=Review)

            if item["comments"] != None:
                for child in item["comments"]:
                    yield generate_item(source=child, item_type=ReviewChild)
                    
            self.redis_conn.delete_scraped_product_wait_comment(spider_id=self.spider_id,
                                                              product_id=item["product_id"],
                                                              sp_id=item["spid"])
            
    def _parse_list_from_cate(self, response_body: dict, cate_id: int):
        response_paging = response_body["paging"]
        response_data = response_body.get("data")

        self.category_page_limit = response_paging["last_page"]

        if response_data == None or len(response_data) == 0:
            return
        
        current_page = response_paging["current_page"]
        self.redis_conn.add_page_to_cate_page_set(cate_id=cate_id, page=current_page)

        for item in response_data:
            yield generate_item(source={"data": item, "from_cate": True}, item_type=Product)
            self.redis_conn.save_scraped_product_wait_comment(spider_id=self.spider_id,
                                                              product_id=item["id"],
                                                              sp_id=item["seller_product_id"])

            api, headers = api_headers_shop_info(seller_id=item.get("seller_id"))
        
            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_shop_info)
            
        self.redis_conn.remove_page_from_set(cate_id=cate_id, page=current_page)
            
    def _parse_list_from_shop(self, response_body: dict, cate_id: int):
        response_data = response_body.get("data")

        if response_data == None or len(response_data) == 0:
            self._shop_page_remove_id(response_data["id"])
            return
        
        for item in response_data:
            yield generate_item(source={"data": item, "from_cate": False}, item_type=Product)

    def _shop_page_remove_id(self, id: int):
        try:
            self.set_shop_page.remove(id)
        except Exception:
            pass

    def _review_page_remove_id(self, id: int):
        try:
            self.set_review_page.remove(id)
        except Exception:
            pass