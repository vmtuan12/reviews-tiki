
import multiprocessing
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import requests
import json
from utils.item_builder import PRODUCT, SHOP, REVIEW, REVIEWCHILD, USER, generate_item
from utils.tiki_utils import api_headers_list_item_by_category, api_headers_shop_info, api_headers_list_product_in_shop, api_headers_reviews
from utils.redis_connector import RedisConnector
import time 

class BaseWorker:
 
    def __init__(self, id: int):
        self.id = id

        self.pool = ThreadPoolExecutor(max_workers=5)
        self.scraped_pages = set()
        self.crawl_queue = Queue()
        self.redis_conn = RedisConnector()

        self._start_requests()

    def run_web_crawler(self):
        while True:
            try:
                url_headers_type = self.crawl_queue.get(timeout=120)
                url = url_headers_type[0]
                if url not in self.scraped_pages:
                    print("Scraping URL: {}".format(url))
                    self.scraped_pages.add(url)

                    job = self.pool.submit(self._scrape_page, url_headers_type)
                    job.add_done_callback(self._post_scrape_callback)
 
            except Empty:
                self.redis_conn.add_category_done()
                return
            except Exception as e:
                print(e)
                continue

    def _start_requests(self):
        category_id = self.redis_conn.get_one_in_process_category_id(self.id)
        if category_id == None:
            remaining_cate = self.redis_conn.get_list_remaining_category_id_by_worker(worker_id=self.id)

            if len(remaining_cate) == 0:
                return
            
            category_id = int(remaining_cate[0])

        url_headers_type = api_headers_list_item_by_category(category_id=category_id, url_key="", page=1)

        total_page = self._init_pages(url_headers_type=url_headers_type, type="CATEGORY", metadata={"cate_id": category_id})

        for index in range(1, total_page + 1):
            url_headers_type = api_headers_list_item_by_category(category_id=category_id, url_key="", page=index)
            self.crawl_queue.put(url_headers_type)

    def _init_pages(self, url_headers_type: tuple, type: str, metadata: dict) -> int | list:
        url, headers = url_headers_type[0], url_headers_type[1]
        while (True):
            req = requests.get(url, headers=headers)

            if req.status_code == 200:
                res: dict = json.loads(req.text)

                if type == "CATEGORY" or type == "REVIEW":
                    pages = res.get("paging").get("last_page")

                    if type == "CATEGORY":
                        self.redis_conn.set_category_total_pages(cate_id=metadata.get("cate_id"), last_page=pages)
                    if type == "REVIEW":
                        self.redis_conn.set_review_total_pages(product_id=metadata.get("product_id"),
                                                               spid=metadata.get("spid"),
                                                               last_page=pages)
                        
                    return int(pages)
                        
                elif type == "SHOP":
                    last_item = res.get("page").get("total")
                    list_cursor = self.redis_conn.set_shop_total_cursors(shop_id=metadata.get("shop_id"), last_item=last_item)

                    return list_cursor
                
                break
            else:
                time.sleep(3)
 
    def _scrape_page(self, url_headers_type):
        url, headers, url_type = url_headers_type
        try:
            res = requests.get(url, headers=headers)
            return (res, url, url_type)
        except requests.RequestException:
            return
 
    def _post_scrape_callback(self, res):
        response, url, request_type = res.result()
        if response and response.status_code == 200:
            self._parse_data(response_data=json.loads(response.text), request_type=request_type, url=url)

        else:
            time.sleep(2)
            self.scraped_pages.remove(url)
            self.crawl_queue

    def _parse_data(self, response_data: dict, request_type: str, url: str):
        data = response_data["data"]
        if request_type == PRODUCT:
            self._parse_product(data)
        elif request_type == SHOP:
            self._parse_shop(data)
        elif request_type == REVIEW:
            self._parse_review(data)

    def _parse_product(self, response_data: list):
        for item in response_data:
            parsed_item = generate_item(source=item, item_type=PRODUCT)

    def _parse_shop(self, response_data: dict):
        parsed_shop = generate_item(response_data["seller"], type=SHOP)

        

    def _parse_review(self, response_data: list):
        for item in response_data:
            parsed_review = generate_item(source=item, item_type=REVIEW)

            user = generate_item(source=item["created_by"], item_type=USER)

            for child in item["comments"]:
                parsed_review_child = generate_item(source=child, item_type=REVIEWCHILD)
        
    def product_to_kafka(self, msg: dict):
        pass