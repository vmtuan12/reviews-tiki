from redis import Redis
from utils.tiki_utils import get_full_category_id, get_category_range

class RedisConnector:
    _instance = None

    CATEGORY_DONE = "category-done"
    IN_PROCESS_CATEGORY = "in-process-category:"
    DONE_PRODUCT = "done-product"
    SCRAPED_PRODUCT_WAIT_SHOP = "scraped-product-wait-shop:"
    SCRAPED_PRODUCT_WAIT_REVIEW = "scraped-product-wait-review"
    SCRAPED_SHOP_WAIT_PRODUCT = "scraped-shop-wait-product"

    CATEGORY_TOTAL_PAGES = "category-total-page:"
    REVIEW_TOTAL_PAGES = "review-total-page:"
    SHOP_PRODUCT_CURSORS = "shop-product-cursor:"

    DONE_SHOP = "done-shop"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisConnector, cls).__new__(cls)
            cls._instance.redis_client = Redis(host='localhost', port=6379, decode_responses=True)

        return cls._instance

    def get_client(self) -> Redis:
        return self.redis_client
    
    def close(self):
        self.redis_client.close()

    def delete_key(self, key):
        self.redis_client.delete(key)

    def add_in_process_category(self, worker_id: int, cate_id: int):
        self.redis_client.sadd(self.IN_PROCESS_CATEGORY + str(worker_id), cate_id)

    def get_one_in_process_category_id(self, worker_id: int) -> int | None:
        ids = self.redis_client.smembers(self.IN_PROCESS_CATEGORY + str(worker_id))
        for id in ids:
            return id
        
        return None

    def add_category_done(self, cate_id: int, worker_id: int):
        self.redis_client.sadd(self.CATEGORY_DONE, cate_id)
        self.redis_client.srem(self.IN_PROCESS_CATEGORY + str(worker_id), cate_id)

    def get_list_remaining_category_id_by_worker(self, worker_id: int) -> list:
        done_category_set = self.redis_client.smembers(self.CATEGORY_DONE)
        in_process_category_set = self.redis_client.smembers(self.IN_PROCESS_CATEGORY + str(worker_id))

        cate_range = get_category_range(worker_id=worker_id)
        set_category = set(get_full_category_id(is_set=False)[cate_range[0]:cate_range[1]])

        return list((set_category - done_category_set) - in_process_category_set)
    
    def add_done_product(self, product_id: int, spid: int):
        self.redis_client.sadd(self.DONE_PRODUCT, f"{product_id}&{spid}")
    
    def product_has_done(self, product_id: int, spid: int) -> bool:
        return f"{product_id}&{spid}" in self.redis_client.smembers(self.DONE_PRODUCT)
    
    def add_scraped_product_wait_shop(self, shop_id: int, product_id: int, spid: int):
        self.redis_client.set(self.SCRAPED_PRODUCT_WAIT_SHOP + str(shop_id), f"{product_id}&{spid}")
    
    def remove_scraped_product_wait_shop(self, shop_id: int):
        info = self.redis_client.get(self.SCRAPED_PRODUCT_WAIT_SHOP + str(shop_id)).split("&")
        self.redis_client.delete(self.SCRAPED_PRODUCT_WAIT_SHOP + str(shop_id))
        self._check_product_has_done(product_id=int(info[0]), spid=int(info[1]))
    
    def add_scraped_product_wait_review(self, product_id: int, spid: int):
        self.redis_client.sadd(self.SCRAPED_PRODUCT_WAIT_REVIEW, f"{product_id}&{spid}")
    
    def remove_scraped_product_wait_review(self, product_id: int, spid: int):
        self.redis_client.srem(self.SCRAPED_PRODUCT_WAIT_REVIEW, f"{product_id}&{spid}")
        self._check_product_has_done(product_id=product_id, spid=spid)
    
    def product_in_scraped_product_wait_review(self, product_id: int, spid: int) -> bool:
        return f"{product_id}&{spid}" in self.redis_client.smembers(self.SCRAPED_PRODUCT_WAIT_REVIEW)
        
    def add_shop_wait_product(self, shop_id: int):
        self.redis_client.sadd(self.SCRAPED_SHOP_WAIT_PRODUCT, shop_id)
        
    def shop_waiting_product(self, shop_id: int) -> bool:
        return shop_id in self.redis_client.sadd(self.SCRAPED_SHOP_WAIT_PRODUCT)
    
    def remove_shop_wait_product(self, shop_id: int):
        self.redis_client.srem(self.SCRAPED_SHOP_WAIT_PRODUCT, shop_id)
        self.add_shop_done(shop_id=shop_id)
        
    def add_shop_done(self, shop_id: int):
        self.redis_client.sadd(self.DONE_SHOP, shop_id)
    
    def shop_can_be_scraped(self, shop_id: int):
        shop_wait_set = self.redis_client.smembers(self.SCRAPED_SHOP_WAIT_PRODUCT)
        shop_done_set = self.redis_client.smembers(self.DONE_SHOP)

        return (shop_id not in shop_wait_set) and (shop_id not in shop_done_set)

    def set_category_total_pages(self, cate_id: int, last_page: int):
        for page in range(1, last_page + 1):
            self.redis_client.sadd(f"{self.CATEGORY_TOTAL_PAGES}{cate_id}", page)

    def remove_page_in_category_total_pages(self, cate_id: int, page: int):
        self.redis_client.srem(f"{self.CATEGORY_TOTAL_PAGES}{cate_id}", page)

    def set_review_total_pages(self, product_id: int, spid: int, last_page: int):
        for page in range(1, last_page + 1):
            self.redis_client.sadd(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}", page)

    def get_review_total_pages(self, product_id: int, spid: int) -> set:
        return self.redis_client.smembers(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}")

    def remove_page_in_review_total_pages(self, product_id: int, spid: int, page: int):
        self.redis_client.srem(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}", page)

        if len(self.redis_client.smembers(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}")) == 0:
            self.delete_key(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}")
            self.remove_scraped_product_wait_review(product_id=product_id, spid=spid)

    def set_shop_total_cursors(self, shop_id: int, last_item: int) -> list:
        list_cursor = []
        for x in range(0, last_item, 40):
            list_cursor.append(x)
            self.redis_client.sadd(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}", x)

        return list_cursor

    def get_shop_total_cursors(self, shop_id: int) -> set:
        return self.redis_client.smembers(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}")

    def remove_cursor_in_shop_total_cursor(self, shop_id: int, cursor: int):
        self.redis_client.srem(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}", cursor)

        if len(self.redis_client.smembers(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}")) == 0:
            self.delete_key(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}")
            self.remove_shop_wait_product(shop_id=shop_id)

    def product_has_been_produced(self, product_id: int, spid: int) -> bool:
        in_wait_review = f"{product_id}&{spid}" in self.redis_client.smembers(self.SCRAPED_PRODUCT_WAIT_REVIEW)
        in_done = f"{product_id}&{spid}" in self.redis_client.smembers(self.DONE_PRODUCT)

        return (in_wait_review == True) or (in_done == True)

    def _check_product_has_done(self, product_id: int, spid: int):
        not_in_wait_review = f"{product_id}&{spid}" not in self.redis_client.smembers(self.SCRAPED_PRODUCT_WAIT_REVIEW)

        if (not_in_wait_review == True):
            self.add_done_product(product_id=product_id, spid=spid)