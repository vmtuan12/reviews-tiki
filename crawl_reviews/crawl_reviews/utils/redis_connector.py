from redis import Redis
from crawl_reviews.utils.tiki_utils import get_full_category_id, get_category_range

class RedisConnector:
    _instance = None

    SCRAPED_PRODUCT_WAIT_COMMENT = "scraped-products-wait-comment:"
    IN_PROCESS_CATEGORY = "in-process-category"
    READY_CATEGORY_PREFIX = "category-scraping-page:"
    CATEGORY_DONE = "category-done"
    SHOP_PRODUCT_PAGE = "shop-product:"
    SHOP_LAST_CURSOR = "shop-last-cursor:"
    LAST_PAGE_CATEGORY = "last-page-category:"
    IN_QUEUE_COMMENT_PAGE = "review:"
    WAIT_SHOP = "shop-wait:"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisConnector, cls).__new__(cls)
            cls._instance.redis_client = Redis(host='localhost', port=6379, decode_responses=True)

        return cls._instance

    def get_client(self) -> Redis:
        return self.redis_client
    
    def close(self):
        self.redis_client.close()

    def get_list_remaining_category_id_by_spider(self, spider_id: int) -> list:
        done_category_set = self.redis_client.smembers(self.CATEGORY_DONE)
        in_process_category_set = self.get_category_in_process()

        cate_range = get_category_range(spider_id=spider_id)
        set_category = set(get_full_category_id(is_set=False)[cate_range[0]:cate_range[1]])

        return list((set_category - done_category_set) - in_process_category_set)
    
    def get_scraping_category_by_spider(self, spider_id: int) -> int | None:
        cate_range = get_category_range(spider_id=spider_id)
        all_ready_scrape_page_key = self.redis_client.keys(pattern=f"{self.READY_CATEGORY_PREFIX}*")

        for key in all_ready_scrape_page_key:
            cate_id = int(key.split(":")[1])
            if cate_range[0] <= cate_id and cate_id < cate_range[1]:
                return int(cate_id)

        return None
    
    def add_category_to_in_process(self, cate_id: int):
        self.redis_client.sadd(self.IN_PROCESS_CATEGORY, cate_id)
    
    def delete_category_from_in_process(self, cate_id: int):
        self.redis_client.srem(self.IN_PROCESS_CATEGORY, cate_id)
    
    def get_category_in_process(self) -> set:
        return self.redis_client.smembers(self.IN_PROCESS_CATEGORY)
    
    def add_page_to_cate_page_set(self, cate_id: int, last_page: int):
        for page in range(1, last_page +):
            self.redis_client.sadd(f"{self.READY_CATEGORY_PREFIX}{cate_id}", page)

    def remove_page_from_category_paging_set(self, cate_id: int, page: int):
        self.redis_client.srem(f"{self.READY_CATEGORY_PREFIX}{cate_id}", page)

        if len(self.get_in_queue_pages_in_category(cate_id=cate_id)) == 0:
            self.delete_category_page_key(cate_id=cate_id)
            self.add_category_done(cate_id=cate_id)

    def get_in_queue_pages_in_category(self, cate_id: int) -> set:
        return self.redis_client.smembers(f"{self.READY_CATEGORY_PREFIX}{cate_id}")
    
    def delete_category_page_key(self, cate_id: int):
        self.redis_client.delete(f"{self.READY_CATEGORY_PREFIX}{cate_id}")

    def add_category_done(self, cate_id: int):
        self.redis_client.sadd(self.CATEGORY_DONE, cate_id)
        self.delete_category_from_in_process(cate_id=cate_id)
    
    def save_scraped_product_wait_comment(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.sadd(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}{spider_id}", f"{product_id}&{sp_id}")
    
    def delete_scraped_product_wait_comment(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.srem(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}{spider_id}", f"{product_id}&{sp_id}")
    
    def get_scraped_product_wait_comment(self, spider_id: int) -> set:
        return self.redis_client.smembers(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}{spider_id}")
    
    def save_wait_shop(self, spider_id: int, shop_id: int):
        self.redis_client.sadd(f"{self.WAIT_SHOP}{spider_id}", shop_id)
    
    def delete_wait_shop(self, spider_id: int, shop_id: int):
        self.redis_client.srem(f"{self.WAIT_SHOP}{spider_id}", shop_id)
    
    def get_wait_shop(self, spider_id: int) -> set:
        return self.redis_client.smembers(f"{self.WAIT_SHOP}{spider_id}")

    def add_cursor_to_shop_set(self, shop_id: int, spider_id: int, total: int):
        for x in range(0, total, 40):
            self.redis_client.sadd(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}", x)

    def get_cursor_shop_set(self, shop_id: int, spider_id: int) -> set:
        return self.redis_client.smembers(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}")

    def delete_cursor_from_shop_set(self, shop_id: int, spider_id: int, cursor: int):
        self.redis_client.srem(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}", cursor)

        if len(self.get_cursor_shop_set(shop_id=shop_id, spider_id=spider_id)) == 0:
            self.delete_cursor_shop_set(shop_id=shop_id, spider_id=spider_id)

    def delete_cursor_shop_set(self, shop_id: int, spider_id: int):
        self.redis_client.delete(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}")

    def get_remaining_cursor_shops(self, spider_id: int) -> list[tuple[int, set]]:
        keys = self.redis_client.keys(f"{self.SHOP_PRODUCT_PAGE}{spider_id}*")
        result = []

        for key in keys:
            shop_id = int(key.split(":")[2])
            cursor_set = self.get_cursor_shop_set(shop_id=shop_id, spider_id=spider_id)
            result.append((shop_id, cursor_set))

        return result
    
    def save_comment_page_set(self, spider_id: int, product_id: int, sp_id: int, last_page: int):
        for index in range(1, last_page):
            self.redis_client.sadd(f"{self.IN_QUEUE_COMMENT_PAGE}{spider_id}:{product_id}:{sp_id}", index)
    
    def delete_page_in_comment_page_set(self, spider_id: int, product_id: int, sp_id: int, page: int):
        self.redis_client.srem(f"{self.IN_QUEUE_COMMENT_PAGE}{spider_id}:{product_id}:{sp_id}", page)

        if len(self.get_comment_page_set(spider_id=spider_id, product_id=product_id, sp_id=sp_id)) == 0:
            self.delete_comment_page_set(spider_id=spider_id, product_id=product_id, sp_id=sp_id)
            self.delete_scraped_product_wait_comment(spider_id=spider_id, product_id=product_id, sp_id=sp_id)

    def get_comment_page_set(self, spider_id: int, product_id: int, sp_id: int) -> set:
        return self.redis_client.smembers(f"{self.IN_QUEUE_COMMENT_PAGE}{spider_id}:{product_id}:{sp_id}")
    
    def delete_comment_page_set(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.delete(f"{self.IN_QUEUE_COMMENT_PAGE}{spider_id}:{product_id}:{sp_id}")
    

    