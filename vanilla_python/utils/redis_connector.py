from redis import Redis
from utils.tiki_utils import get_full_category_id, get_category_range

NUMBER_OF_WORKER = 5

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

    SCRAPED_PRODUCT = "scraped-product"
    DONE_SHOP = "done-shop"
    SHOP_WAITING = "shop-waiting"
    PRODUCT_DONE_REVIEWS = "scraped-product-done-reviews"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisConnector, cls).__new__(cls)
            cls._instance.redis_client = Redis(host='localhost', port=6379, decode_responses=True)

        return cls._instance

    def get_client(self) -> Redis:
        return self.redis_client
    
    def close(self):
        self.redis_client.close()

    def delete_key(self, key: str):
        self.redis_client.delete(key)

    def add_category_done(self, cate_id: int, worker_id: int):
        self.redis_client.sadd(self.CATEGORY_DONE, cate_id)

    def get_list_remaining_category_id_by_worker(self, worker_id: int) -> set:
        done_category_set = self.redis_client.smembers(self.CATEGORY_DONE)

        cate_range = get_category_range(worker_id=worker_id)
        set_category = set(get_full_category_id(is_set=False)[cate_range[0]:cate_range[1]])

        return set_category - done_category_set

    def set_category_total_pages(self, cate_id: int, last_page: int):
        for page in range(1, last_page + 1):
            self.redis_client.sadd(f"{self.CATEGORY_TOTAL_PAGES}{cate_id}", page)

    def remove_page_in_category_total_pages(self, worker_id: int, cate_id: int, page: int):
        key = f"{self.CATEGORY_TOTAL_PAGES}{cate_id}"

        self.redis_client.srem(key, page)

        if len(self.redis_client.smembers(key)) == 0:
            self.add_category_done(cate_id=cate_id, worker_id=worker_id)
            self.delete_key(key=key)

    def set_shop_total_cursors(self, shop_id: int, last_item: int) -> list:
        list_cursor = []
        for x in range(0, last_item, 40):
            list_cursor.append(x)
            self.redis_client.sadd(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}", x)

        return list_cursor

    def get_shop_total_cursors(self, shop_id: int) -> set:
        return self.redis_client.smembers(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}")
    
    def shop_has_total_cursor(self, shop_id: int) -> bool:
        return self.redis_client.exists(f"{self.SHOP_PRODUCT_CURSORS}{shop_id}") == 1

    def remove_cursor_in_shop_total_cursor(self, shop_id: int, cursor: int):
        key = f"{self.SHOP_PRODUCT_CURSORS}{shop_id}"

        self.redis_client.srem(key, cursor)

        if len(self.redis_client.smembers(key)) == 0:
            self.delete_key(key)
            self.add_shop_done(shop_id=shop_id)

    def product_has_been_produced(self, product_id: int, spid: int) -> bool:
        return f"{product_id}&{spid}" in self.redis_client.smembers(self.SCRAPED_PRODUCT)
    
    def add_scraped_product(self, product_id: int, spid: int):
        self.redis_client.sadd(self.SCRAPED_PRODUCT, f"{product_id}&{spid}")
    
    def get_full_scraped_product(self) -> set:
        return self.redis_client.smembers(self.SCRAPED_PRODUCT)
    
    def add_shop_waiting(self, shop_id: int):
        self.redis_client.sadd(self.SHOP_WAITING, shop_id)
    
    def get_set_shop_from_shop_waiting_by_worker(self, worker_id: int) -> set:
        list_shop = list(self.redis_client.smembers(self.SHOP_WAITING))
        chosen = set(self._choose_partition(worker_id=worker_id, data_list=list_shop))

        done_shop = self.redis_client.smembers(self.DONE_SHOP)

        return chosen - done_shop
        
    def add_shop_done(self, shop_id: int):
        self.redis_client.sadd(self.DONE_SHOP, shop_id)
    
    def shop_can_be_scraped(self, shop_id: int):
        return shop_id not in self.redis_client.smembers(self.DONE_SHOP)

    def get_product_wait_review_set_by_worker(self, worker_id: int) -> set:
        full_list_product = list(self.get_full_scraped_product())
        partition = set(self._choose_partition(worker_id=worker_id, data_list=full_list_product))

        set_product_done_review = self.get_set_product_done_review()

        return partition - set_product_done_review

    def set_review_total_pages(self, product_id: int, spid: int, last_page: int):
        for page in range(1, last_page + 1):
            self.redis_client.sadd(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}", page)

    def get_review_total_pages(self, product_id: int, spid: int) -> set:
        return self.redis_client.smembers(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}")

    def product_has_review_total_pages(self, product_id: int, spid: int) -> bool:
        return self.redis_client.exists(f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}") == 1

    def remove_page_in_review_total_pages(self, product_id: int, spid: int, page: int):
        key = f"{self.REVIEW_TOTAL_PAGES}{product_id}_{spid}"
        self.redis_client.srem(key, page)

        if len(self.redis_client.smembers(key)) == 0:
            self.delete_key(key)
            self.add_product_done_review(product_id=product_id, spid=spid)
        
    def add_product_done_review(self, product_id: int, spid: int):
        self.redis_client.sadd(self.PRODUCT_DONE_REVIEWS, f"{product_id}&{spid}")
        
    def get_set_product_done_review(self) -> set:
        return self.redis_client.smembers(self.PRODUCT_DONE_REVIEWS)
        
    def product_has_done_review(self, product_id: int, spid: int) -> bool:
        return f"{product_id}&{spid}" in self.redis_client.smembers(self.PRODUCT_DONE_REVIEWS)

    def _choose_partition(self, worker_id: int, data_list: list) -> list:
        """
        tuple (int, int)\n
        category id in range [start, end)
        """

        mod = len(data_list) % NUMBER_OF_WORKER

        chunk_size = [0]
        if mod == 0:
            chunk_size += [len(data_list) / NUMBER_OF_WORKER for _ in range(NUMBER_OF_WORKER)]
        else:
            for _ in range(NUMBER_OF_WORKER):
                if mod != 0:
                    chunk_size.append((len(data_list) // NUMBER_OF_WORKER) + 1)
                    mod -= 1
                else:
                    chunk_size.append(len(data_list) // NUMBER_OF_WORKER)
        
        return data_list[chunk_size[worker_id]:(chunk_size[worker_id] + chunk_size[worker_id + 1])]