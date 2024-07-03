from redis import Redis
from crawl_reviews.utils.tiki_utils import get_full_category_id, get_category_range

class RedisConnector:
    _instance = None

    SCRAPED_PRODUCT_WAIT_COMMENT = "scraped-products-wait-comment:"
    READY_CATEGORY_PREFIX = "category-scraping-page:"
    CATEGORY_DONE = "category-done"
    CURRENT_PAGE_NUMBER_CATEGORY = "page-number:"
    SHOP_PRODUCT_PAGE = "shop-product:"
    SHOP_LAST_CURSOR = "shop-last-cursor:"
    LAST_PAGE_CATEGORY = "last-page-category:"

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

        cate_range = get_category_range(spider_id=spider_id)
        set_category = get_full_category_id(is_set=False)[cate_range[0]:cate_range[1]]

        return list(set_category - done_category_set)
    
    def get_scraping_category_by_spider(self, spider_id: int) -> int | None:
        cate_range = get_category_range(spider_id=spider_id)
        all_ready_scrape_page_key = self.redis_client.keys(pattern=f"{self.READY_CATEGORY_PREFIX}*")

        for key in all_ready_scrape_page_key:
            cate_id = int(key.split(":")[1])
            if cate_range[0] <= cate_id and cate_id < cate_range[1]:
                return int(cate_id)

        return None
    
    def set_last_page_category(self, cate_id: int, last_page: int):
        self.redis_client.set(f"{self.LAST_PAGE_CATEGORY}{cate_id}", last_page)
    
    def get_last_page_category(self, cate_id: int) -> int:
        result = int(self.redis_client.get(f"{self.LAST_PAGE_CATEGORY}{cate_id}"))

        if result == None:
            return result
        
        return int(result)
    
    def delete_last_page_category(self, cate_id: int):
        self.redis_client.delete(f"{self.LAST_PAGE_CATEGORY}{cate_id}")
    
    def add_page_to_cate_page_set(self, cate_id: int, page: int):
        self.redis_client.sadd(f"{self.READY_CATEGORY_PREFIX}{cate_id}", page)

        if len(self.get_scraped_pages_in_category()) == self.get_last_page_category(cate_id=cate_id):
            self.delete_category_page_key(cate_id=cate_id)
            self.delete_last_page_category(cate_id=cate_id)
            self.add_category_done(cate_id=cate_id)

    # def remove_page_from_set(self, cate_id: int, page: int):
    #     self.redis_client.srem(f"{self.READY_CATEGORY_PREFIX}:{cate_id}", page)

    def get_scraped_pages_in_category(self, cate_id: int) -> set:
        return self.redis_client.smembers(f"{self.READY_CATEGORY_PREFIX}{cate_id}")

    def delete_category_page_key(self, cate_id: int):
        self.redis_client.delete(f"{self.READY_CATEGORY_PREFIX}{cate_id}")

    def add_category_done(self, cate_id: int):
        self.redis_client.sadd(self.CATEGORY_DONE, cate_id)
    
    def save_scraped_product_wait_comment(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.sadd(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}{spider_id}", f"{product_id}&{sp_id}")
    
    def delete_scraped_product_wait_comment(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.srem(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}{spider_id}", f"{product_id}&{sp_id}")
    
    def get_scraped_product_wait_comment(self, spider_id: int) -> set:
        self.redis_client.smembers(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}{spider_id}")

    def add_cursor_to_shop_set(self, shop_id: int, spider_id: int, cursor: int):
        self.redis_client.sadd(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}", cursor)

        len_cursor_set = len(self.get_cursor_shop_set(shop_id=shop_id, spider_id=spider_id))
        last_cursor = self.get_last_cursor_shop(shop_id=shop_id, spider_id=spider_id)

        if len_cursor_set == (last_cursor / 40 + 1):
            self.delete_cursor_shop_set(shop_id=shop_id, spider_id=spider_id)
            self.delete_last_cursor_shop(shop_id=shop_id, spider_id=spider_id)

    def get_cursor_shop_set(self, shop_id: int, spider_id: int) -> set:
        return self.redis_client.smembers(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}")

    def delete_cursor_shop_set(self, shop_id: int, spider_id: int):
        self.redis_client.delete(f"{self.SHOP_PRODUCT_PAGE}{spider_id}:{shop_id}")

    def set_last_cursor_shop(self, shop_id: int, spider_id: int, cursor: int):
        self.redis_client.set(f"{self.SHOP_LAST_CURSOR}{spider_id}:{shop_id}", cursor)

    def delete_last_cursor_shop(self, shop_id: int, spider_id: int):
        self.redis_client.delete(f"{self.SHOP_LAST_CURSOR}{spider_id}:{shop_id}")

    def get_last_cursor_shop(self, shop_id: int, spider_id: int) -> int | None:
        result = self.redis_client.get(f"{self.SHOP_LAST_CURSOR}{spider_id}:{shop_id}")

        if result == None:
            return result
        
        return int(result)

    