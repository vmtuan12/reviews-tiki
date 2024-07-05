from typing import Any, Iterable
from unidecode import unidecode
import scrapy
import re
import json
from crawl_reviews.utils.redis_connector import RedisConnector
from crawl_reviews.utils.tiki_utils import *
# from crawl_reviews.utils.save_data_utils import *
from crawl_reviews.utils.item_builder import generate_item
from crawl_reviews.items import Product, Shop, Review, User, ReviewChild
import requests
from scrapy.http import Request, Response

class BaseSpider(scrapy.Spider):

    redis_conn = RedisConnector()
    redis_client = redis_conn.get_client()

    category_page_limit = dict()
    base_category_page_limit = 50
    limit_product_shop_per_page = 40

    set_shop_page = set()
    set_review_page = set()

    # TODO: re-design the flow
    def start_requests(self):
        scraping_category_id = self.redis_conn.get_scraping_category_by_spider(self.spider_id)

        if scraping_category_id == None:
            possible_categories = self.redis_conn.get_list_remaining_category_id_by_spider(spider_id=self.spider_id)
            if len(possible_categories) == 0:
                return
            
            chosen_category_id = int(possible_categories[0])
            url_key = get_url_key_by_cate_id(chosen_category_id)
            self.category_page_limit[str(chosen_category_id)] = self.base_category_page_limit

            current_page = 1

            while (current_page <= self.category_page_limit[str(chosen_category_id)]):
                api, headers = api_headers_list_item_by_category(category_id=chosen_category_id, url_key=url_key, page=current_page)
                yield scrapy.Request(url=api,
                                    headers=headers,
                                    callback=self.parse_list_product,
                                    meta={"page": current_page, "from_cate": True, "cate_id": chosen_category_id})
                current_page += 1
        else:
            ready_scrape_page_set = self.redis_conn.get_in_queue_pages_in_category(cate_id=scraping_category_id)
            if len(ready_scrape_page_set) == 0:
                # self.redis_conn.delete_category_key(scraping_category_id)
                # self.redis_conn.add_category_done(scraping_category_id)

                self.start_requests()
            else:
                self.crawl_remaining(ready_scrape_page_set=ready_scrape_page_set, category_id=scraping_category_id)

    def choose_category(self):
        possible_categories = self.redis_conn.get_list_remaining_category_id_by_spider(spider_id=self.spider_id)
        print(possible_categories)
        if len(possible_categories) == 0:
            return
        
        chosen_category_id = int(possible_categories[0])
        url_key = get_url_key_by_cate_id(chosen_category_id)
        self.category_page_limit[str(chosen_category_id)] = self.base_category_page_limit

        current_page = 1

        while (current_page <= self.category_page_limit[str(chosen_category_id)]):
            api, headers = api_headers_list_item_by_category(category_id=chosen_category_id, url_key=url_key, page=current_page)
            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_list_product,
                                meta={"page": current_page, "from_cate": True, "cate_id": chosen_category_id})
            current_page += 1

    def crawl_remaining(self, ready_scrape_page_set: set, category_id: int):
        # note: product parsed but not parse comments yet
        set_remaining_product_wait_comment = self.redis_conn.get_scraped_product_wait_comment(self.spider_id)
        for product in set_remaining_product_wait_comment:
            product_id, sp_id = product.split("&")

            pages = self.redis_conn.get_comment_page_set(spider_id=self.spider_id, product_id=product_id, sp_id=sp_id)

            if len(pages == 0):
                current_review_page = 1
                self.set_review_page.add(product_id)

                while (product_id in self.set_review_page):
                    review_api, review_headers = api_headers_reviews(product_id=product_id, spid=sp_id, page=current_review_page)

                    yield scrapy.Request(url=review_api,
                                        headers=review_headers,
                                        callback=self.parse_comment,
                                        meta={"from_remaining": False, "page": current_review_page})
                    
                    current_review_page += 1
                
                self._review_page_remove_product_id(product_id)

            else:
                for p in pages:
                    int_page = int(p)
                    review_api, review_headers = api_headers_reviews(product_id=product_id, spid=sp_id, page=int_page)

                    yield scrapy.Request(url=review_api,
                                        headers=review_headers,
                                        callback=self.parse_comment,
                                        meta={"from_remaining": True, "page": int_page})
                    
        # note: shops which products are scraped, not done
        remaining_cursor_shops = self.redis_conn.get_remaining_cursor_shops(spider_id=self.spider_id)
        for shop_cursor in remaining_cursor_shops:
            shop_id, cursor_set = shop_cursor

            for cursor in cursor_set:
                int_cursor = int(cursor)
                api, headers = api_headers_list_product_in_shop(cursor=int_cursor, shop_id=shop_id)

                yield scrapy.Request(url=api,
                                    headers=headers,
                                    callback=self.parse_list_product,
                                    meta={"from_cate": False, "cursor": int_cursor, "shop_id": shop_id, "from_remaining": True})
                
        # note: shops which info has not been parsed
        remaining_shops = self.redis_conn.get_wait_shop(spider_id=self.spider_id)
        for item in remaining_shops:
            api, headers = api_headers_shop_info(seller_id=int(item))
        
            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_shop_info)

        # note: continue scrape not done pages in category
        for page in ready_scrape_page_set:
            page_int = int(page)
            api, headers = api_headers_list_item_by_category(category_id=category_id, url_key="", page=page_int)
            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_list_product,
                                meta={"page": page_int, "from_cate": True, "cate_id": category_id, "from_remaining": True})

    def parse_list_product(self, response: Response, **kwargs: Any):
        response_body = json.loads(response.text)
        response_data = response_body["data"]
        response_body["from_remaining"] = response.meta.get("from_remaining")

        cate_id = response.meta.get("cate_id")

        if response.meta.get("from_cate") == True:
            self._parse_list_from_cate(response_body=response_body, cate_id=cate_id)
        else:
            self._parse_list_from_shop(response_body=response_body,
                                       cursor=response.meta.get("cursor"),
                                       shop_id=response.meta.get("shop_id"))

        # item = product
        for item in response_data:
            # print(item)
            product_id = item["id"]
            sp_id = item["seller_product_id"]
            shop_id = item["seller_id"]
            self.set_review_page.add(product_id)
            current_review_page = 1
            # total_page = 0
            # if total_page == 0:
            review_api, review_headers = api_headers_reviews(product_id=product_id, spid=sp_id, page=current_review_page, seller_id=shop_id)
            pre_get = requests.get(review_api, headers=review_headers)
            total_page = json.loads(pre_get.text)["paging"]["last_page"]

            while (current_review_page <= total_page):
                review_api, review_headers = api_headers_reviews(product_id=product_id, spid=sp_id, page=current_review_page, seller_id=shop_id)
                print(review_api)

                yield scrapy.Request(url=review_api,
                                    headers=review_headers,
                                    callback=self.parse_comment,
                                    meta={"product_id": product_id, "sp_id": sp_id, "page": current_review_page})
                
                current_review_page += 1
            
            self._review_page_remove_product_id(product_id)

    def parse_shop_info(self, response: Response, **kwargs: Any):
        response_body = json.loads(response.text)
        response_data = response_body["data"]["seller"]

        yield generate_item(source=response_data, item_type=Shop)
        # self.redis_conn.delete_wait_shop(spider_id=self.spider_id, shop_id=response_data["id"])

        cursor = 0
        limit = self.limit_product_shop_per_page
            
        self.set_shop_page.add(response_data["id"])

        while (response_data["id"] in self.set_shop_page):
            api, headers = api_headers_list_product_in_shop(cursor=cursor, shop_id=response_data["id"])

            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_list_product,
                                meta={"from_cate": False, "cursor": cursor, "shop_id": response_data["id"]})
            cursor += limit

        self._shop_page_remove_id(response_data["id"])

    def parse_comment(self, response: Response, **kwargs: Any):
        response_body = json.loads(response.text)
        response_data = response_body.get("data")
        response_paging = response_body.get("paging")

        if response_data == None or len(response_data) == 0:
            self._review_page_remove_product_id(response.meta.get("product_id"))
            return
        
        product_id = response.meta.get("product_id")
        sp_id = response.meta.get("sp_id")
        comment_page_set = self.redis_conn.get_comment_page_set(spider_id=self.spider_id, product_id=product_id, sp_id=sp_id)

        # if response.meta.get("from_remaining") != True and len(comment_page_set) == 0:
        #     self.redis_conn.save_comment_page_set(spider_id=self.spider_id, product_id=product_id, sp_id=sp_id, last_page=response_paging["last_page"])
        
        for item in response_data:
            yield generate_item(source=item, item_type=Review)

            if item["comments"] != None:
                for child in item["comments"]:
                    yield generate_item(source=child, item_type=ReviewChild)
                    
        # self.redis_conn.delete_page_in_comment_page_set(spider_id=self.spider_id,
        #                                                 product_id=product_id,
        #                                                 sp_id=sp_id,
        #                                                 page=response.meta.get("page"))
            
    def _parse_list_from_cate(self, response_body: dict, cate_id: int):
        response_paging = response_body["paging"]
        response_data = response_body.get("data")
        from_remaining = response_body.get("from_remaining")

        self.category_page_limit[str(cate_id)] = response_paging["last_page"]

        if response_data == None or len(response_data) == 0:
            return
            
        # if from_remaining != True and len(self.redis_conn.get_in_queue_pages_in_category(cate_id=cate_id)) == 0:
        #     self.redis_conn.add_page_to_cate_page_set(cate_id=cate_id, last_page=response_paging["last_page"])
        #     self.redis_conn.add_category_to_in_process(cate_id=cate_id)
        
        current_page = response_paging["current_page"]

        for item in response_data:
            yield generate_item(source={"data": item, "from_cate": True}, item_type=Product)

        #     self.redis_conn.save_scraped_product_wait_comment(spider_id=self.spider_id,
        #                                                       product_id=item["id"],
        #                                                       sp_id=item["seller_product_id"])
        #     self.redis_conn.save_wait_shop(spider_id=self.spider_id, shop_id=item.get("seller_id"))

        # self.redis_conn.remove_page_from_category_paging_set(cate_id=cate_id, page=current_page)

        for item in response_data:
            api, headers = api_headers_shop_info(seller_id=item.get("seller_id"))
        
            yield scrapy.Request(url=api,
                                headers=headers,
                                callback=self.parse_shop_info)
            
    def _parse_list_from_shop(self, response_body: dict, cursor: int, shop_id: int):
        response_data = response_body.get("data")
        from_remaining = response_body.get("from_remaining")

        # if from_remaining != True and len(self.redis_conn.get_cursor_shop_set(shop_id=shop_id, spider_id=self.spider_id)) == 0:
        #     self.redis_conn.add_cursor_to_shop_set(shop_id=shop_id,
        #                                          spider_id=self.spider_id,
        #                                          total=response_body.get("page").get("total"))

        if response_data == None or len(response_data) == 0:
            self._shop_page_remove_id(response_data["id"])
            return
        
        for item in response_data:
            yield generate_item(source={"data": item, "from_cate": False}, item_type=Product)

        # self.redis_conn.delete_cursor_from_shop_set(shop_id=shop_id, cursor=cursor, spider_id=self.spider_id)

    def _shop_page_remove_id(self, id: int):
        try:
            self.set_shop_page.remove(id)
        except Exception:
            pass

    def _review_page_remove_product_id(self, id: int):
        try:
            self.set_review_page.remove(id)
        except Exception:
            pass