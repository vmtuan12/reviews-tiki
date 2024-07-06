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

class BaseWorker:
 
    def __init__(self, id: int):
        self.id = id

        self.pool = ThreadPoolExecutor(max_workers=5)
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

        self.count_msg_buffer_kafka = 0

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
                    time.sleep(0.5)
 
            except Empty:
                self.producer.flush()
                self.redis_conn.add_category_done()
                return
            except Exception as e:
                print(e)
                continue

    def _start_requests(self):
        self.current_category_id = self.redis_conn.get_one_in_process_category_id(self.id)
        if self.current_category_id == None:
            remaining_cate = self.redis_conn.get_list_remaining_category_id_by_worker(worker_id=self.id)

            if len(remaining_cate) == 0:
                return
            
            self.current_category_id = int(remaining_cate[0])

        url_headers_type = api_headers_list_item_by_category(category_id=self.current_category_id, url_key="", page=1)

        total_page = self._init_pages(url_headers_type=url_headers_type, type="CATEGORY", metadata={"cate_id": self.current_category_id})

        for index in range(1, total_page + 1):
            url_headers_type = api_headers_list_item_by_category(category_id=self.current_category_id, url_key="", page=index)
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
        proxy = get_proxy()
        try:
            res = requests.get(url, headers=headers, proxies=proxy)
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

    def _parse_data(self, response_data: dict, request_type: str, url: str):
        if request_type == PRODUCT:
            self._parse_product(response_data, url=url)
        elif request_type == SHOP:
            self._parse_shop(response_data)
        elif request_type == REVIEW:
            self._parse_review(response_data)

    def _parse_product(self, response_data: dict, url: str):
        data = response_data["data"]

        for item in data:
            parsed_item = generate_item(source=item, item_type=PRODUCT)

            shop_id = parsed_item["shop_id"]
            product_id = parsed_item["id"]
            spid = parsed_item["spid"]

            if not self.redis_conn.product_has_been_produced(product_id=product_id, spid=spid):
                self.produce_to_kafka(msg=parsed_item, topic=self.topic_product)

            api_header_type = api_headers_shop_info(seller_id=shop_id)
            self.crawl_queue.put(api_header_type)
            self.redis_conn.add_scraped_product_wait_shop(shop_id=shop_id, product_id=product_id, spid=spid)

            if self.redis_conn.product_in_scraped_product_wait_review(product_id=product_id, spid=spid):
                pages = self.redis_conn.get_review_total_pages(product_id=product_id, spid=spid)

            else:
                api_header_type_init_review = api_headers_reviews(product_id=product_id, spid=spid, page=1)
                pages = self._init_pages(url_headers_type=api_header_type_init_review, type="REVIEW", metadata={ "product_id": product_id, "spid": spid })

                self.redis_conn.add_scraped_product_wait_review(product_id=product_id, spid=spid)

            for p in pages:
                api_headers_type_review = api_headers_reviews(product_id=product_id, spid=spid, page=int(p))
                self.crawl_queue.put(api_headers_type_review)

        url_parsed = urlparse(url)
        query_dict = parse_qs(url_parsed.query)
        
        if response_data.get("paging") != None:
            self.redis_conn.remove_page_in_category_total_pages(cate_id=int(query_dict["category"][0]),
                                                                page=int(query_dict["page"][0]))
        else:
            self.redis_conn.remove_cursor_in_shop_total_cursor(shop_id=int(query_dict["seller_id"][0]),
                                                               cursor=int(query_dict["cursor"][0]))

    def _parse_shop(self, response_data: dict, url: str):
        data = response_data["data"]

        parsed_shop = generate_item(data["seller"], type=SHOP)
        
        shop_id = parsed_shop["id"]

        if self.redis_conn.shop_can_be_scraped(shop_id=shop_id):
            self.produce_to_kafka(msg=parsed_shop, topic=self.topic_shop)
            self.redis_conn.remove_scraped_product_wait_shop(shop_id=parsed_shop["id"])

            url_header_type_init = api_headers_list_product_in_shop(shop_id=parsed_shop["id"], cursor=0)
            list_cursor = self._init_pages(url_headers_type=url_header_type_init, type="SHOP", metadata={ "shop_id": parsed_shop["id"] })

            self.redis_conn.add_shop_wait_product(shop_id=parsed_shop["id"])
        
        else:
            list_cursor = self.redis_conn.get_shop_total_cursors(shop_id=shop_id)

        for cursor in list_cursor:
            api_headers_type = api_headers_list_product_in_shop(shop_id=parsed_shop["id"], cursor=int(cursor))
            self.crawl_queue.put(api_headers_type)

    def _parse_review(self, response_data: dict, url: str):
        data = response_data["data"]

        for item in data:
            parsed_review = generate_item(source=item, item_type=REVIEW)
            self.produce_to_kafka(msg=parsed_review, topic=self.topic_review)

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

    def produce_to_kafka(self, msg: dict, topic: str):
        self.producer.send(topic, value=msg)
        self.producer.flush()