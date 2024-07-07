from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import requests
import json
from utils.item_builder import PRODUCT, SHOP, REVIEW, REVIEWCHILD, USER, generate_item
from utils.tiki_utils import api_headers_list_item_by_category, api_headers_shop_info, api_headers_list_product_in_shop, api_headers_reviews
from utils.redis_connector import RedisConnector
from utils.choose_proxy import get_proxy
import time 
from kafka import KafkaProducer
import json
from urllib.parse import urlparse, parse_qs
from utils.log_writer import write_log
import traceback
from threading import current_thread
from threading import enumerate as threading_enumerate

class BaseWorker:
 
    def __init__(self, id: int):
        self.id = id
        self.max_thread = 6
        self.pool = ThreadPoolExecutor(max_workers=self.max_thread)
        self.working_threads = set()
        self.futures_submitted = 0

        self.scraped_pages = set()
        self.crawl_queue = Queue()
        self.redis_conn = RedisConnector()

        self.producer = KafkaProducer(bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'],
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.topic_product = 'product'
        self.topic_review = 'review'
        self.topic_review_child = 'review_child'
        self.topic_user = 'user'
        self.topic_shop = 'shop'

        self._start_requests()

    def run_web_crawler(self):
        while True:
            try:
                if len(self.working_threads) == self.max_thread:
                    time.sleep(0.25)
                    continue

                url_headers_type = self.crawl_queue.get(timeout=360)
                url = url_headers_type[0]

                if url not in self.scraped_pages:
                    print("Submitting URL: {}".format(url))
                    self.scraped_pages.add(url)

                    job = self.pool.submit(self._scrape_page, url_headers_type)
                    job.add_done_callback(self._post_scrape_callback)
                    time.sleep(0.5)
 
            except Empty:
                self.producer.flush()
                return
            except Exception as e:
                print(e)
                return

    def _start_requests(self):
        remaining_cate = self.redis_conn.get_list_remaining_category_id_by_worker(worker_id=self.id)
        if len(remaining_cate) > 0:
            self.__request_product_category(remaining_cate=remaining_cate)

        else:
            remaining_shop = self.redis_conn.get_set_shop_from_shop_waiting_by_worker(worker_id=self.id)
            if len(remaining_shop) > 0:
                self.__request_shop(remaining_shop=remaining_shop)

            else:
                self.__request_reviews()

    def __request_product_category(self, remaining_cate: set):
        for cate in remaining_cate:
            cate_int = int(cate)
            url_headers_type_init = api_headers_list_item_by_category(category_id=cate_int, url_key="", page=1)
            total_page = self._init_pages(url_headers_type=url_headers_type_init, type="CATEGORY", metadata={"cate_id": cate_int})

            for index in range(1, total_page + 1):
                url_headers_type = api_headers_list_item_by_category(category_id=cate_int, url_key="", page=index)
                self.crawl_queue.put(url_headers_type)

    def __request_shop(self, remaining_shop: set):
        for shop in remaining_shop:
            shop_id = int(shop)
            url_headers_type = api_headers_shop_info(seller_id=shop_id)
            self.crawl_queue.put(url_headers_type)

    def __request_reviews(self):
        product_partition = self.redis_conn.get_product_wait_review_set_by_worker(worker_id=self.id)
        if len(product_partition) == 0:
            return
        
        for product in product_partition:
            product_info = product.split("&")
            product_id, spid = int(product_info[0]), int(product_info[1])

            pages = set()
            if self.redis_conn.product_has_done_review(product_id=product_id, spid=spid):
                continue
            elif self.redis_conn.product_has_review_total_pages(product_id=product_id, spid=spid):
                pages = self.redis_conn.get_review_total_pages(product_id=product_id, spid=spid)
            else:
                url_headers_type_review_init = api_headers_reviews(product_id=product_id, spid=spid, page=1)
                last_page = self._init_pages(url_headers_type=url_headers_type_review_init, 
                                             type="REVIEW", 
                                             metadata={ "product_id": product_id, "spid": spid })
                pages = [x for x in range(1, last_page + 1)]

            for p in pages:
                page_int = int(p)
                url_headers_type_review = api_headers_reviews(product_id=product_id, spid=spid, page=page_int)
                self.crawl_queue.put(url_headers_type_review)

    def _init_pages(self, url_headers_type: tuple, type: str, metadata: dict) -> int | list:
        url, headers = url_headers_type[0], url_headers_type[1]
        proxy = get_proxy()
        while (True):
            req = requests.get(url, headers=headers, proxies=proxy)

            if req.status_code == 200:
                try:
                    res: dict = json.loads(req.text)
                except Exception:
                    err = traceback.format_exc() + "\n" + url + "\n" + req.text
                    write_log(err=err, file_name="init_pages.log")

                    time.sleep(4)

                    return self._init_pages(url_headers_type, type, metadata)

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
        proxy = get_proxy()
        try:
            res = requests.get(url, headers=headers, proxies=proxy)
            # print(res)
            return (res, url, url_type)
        except requests.RequestException:
            return
 
    def _post_scrape_callback(self, res):
        self.working_threads.add(current_thread().getName())
        # print("\n Name of the current executing thread: ",
        #     current_thread().getName(), '\n')
        
        response, url, request_type = res.result()
        if response and response.status_code == 200:
            self._parse_data(response_data=json.loads(response.text), request_type=request_type, url=url)

        else:
            time.sleep(2)
            self.scraped_pages.remove(url)

        self.working_threads.remove(current_thread().getName())
        # print("THREAD " + current_thread().getName() + " DONE")

    def _parse_data(self, response_data: dict, request_type: str, url: str):
        if request_type == PRODUCT:
            self._parse_product(response_data, url=url)
        elif request_type == SHOP:
            self._parse_shop(response_data, url=url)
        elif request_type == REVIEW:
            self._parse_review(response_data, url=url)

    def _parse_product(self, response_data: dict, url: str):
        data = response_data["data"]

        for item in data:
            parsed_item = generate_item(source=item, item_type=PRODUCT)

            if parsed_item == None:
                continue

            shop_id = parsed_item["shop_id"]
            product_id = parsed_item["id"]
            spid = parsed_item["spid"]

            self.redis_conn.add_shop_waiting(shop_id=shop_id)

            if not self.redis_conn.product_has_been_produced(product_id=product_id, spid=spid):
                self.produce_to_kafka(msg=parsed_item, topic=self.topic_product)
                self.redis_conn.add_scraped_product(product_id=product_id, spid=spid)

        url_parsed = urlparse(url)
        query_dict = parse_qs(url_parsed.query)
        
        if response_data.get("paging") != None:
            self.redis_conn.remove_page_in_category_total_pages(worker_id=self.id,
                                                                cate_id=int(query_dict["category"][0]),
                                                                page=int(query_dict["page"][0]))
        else:
            self.redis_conn.remove_cursor_in_shop_total_cursor(shop_id=int(query_dict["seller_id"][0]),
                                                               cursor=int(query_dict["cursor"][0]))

    def _parse_shop(self, response_data: dict, url: str):
        data = response_data["data"]

        parsed_shop = generate_item(data["seller"], item_type=SHOP)

        if parsed_shop == None:
            return
        
        shop_id = parsed_shop["id"]

        if self.redis_conn.shop_can_be_scraped(shop_id=shop_id):
            self.produce_to_kafka(msg=parsed_shop, topic=self.topic_shop)

            if self.redis_conn.shop_has_total_cursor(shop_id=shop_id):
                list_cursor = self.redis_conn.get_shop_total_cursors(shop_id=shop_id)
            else:                
                url_header_type_init = api_headers_list_product_in_shop(shop_id=parsed_shop["id"], cursor=0)
                list_cursor = self._init_pages(url_headers_type=url_header_type_init, type="SHOP", metadata={ "shop_id": parsed_shop["id"] })
            

            for cursor in list_cursor:
                api_headers_type = api_headers_list_product_in_shop(shop_id=parsed_shop["id"], cursor=int(cursor))
                self.crawl_queue.put(api_headers_type)

    def _parse_review(self, response_data: dict, url: str):
        data = response_data["data"]

        for item in data:
            parsed_review = generate_item(source=item, item_type=REVIEW)
            self.produce_to_kafka(msg=parsed_review, topic=self.topic_review)
            # print(parsed_review)

            user = generate_item(source=item["created_by"], item_type=USER)
            self.produce_to_kafka(msg=user, topic=self.topic_user)

            for child in item["comments"]:
                parsed_review_child = generate_item(source=child, item_type=REVIEWCHILD)
                self.produce_to_kafka(msg=parsed_review_child, topic=self.topic_review_child)
        
        url_parsed = urlparse(url)
        query_dict = parse_qs(url_parsed.query)

        self.redis_conn.remove_page_in_review_total_pages(product_id=int(query_dict["product_id"][0]),
                                                          spid=int(query_dict["spid"][0]),
                                                          page=response_data["paging"]["current_page"])

    def produce_to_kafka(self, msg: dict | None, topic: str):
        if msg == None:
            return
        
        self.producer.send(topic, value=msg)
        self.producer.flush()
        print(topic, "---------------------------------------")
        print(msg)