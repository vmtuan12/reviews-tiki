from redis import Redis
from crawl_reviews.utils.tiki_utils import get_full_category_id, get_category_range

class RedisConnector:
    _instance = None

    SCRAPED_PRODUCT_WAIT_COMMENT = "scraped-products-wait-comment:"
    READY_CATEGORY_PREFIX = "category-scraping-page:"
    CATEGORY_DONE = "category-done:"
    CURRENT_PAGE_NUMBER_CATEGORY = "page-number:"
    SHOP_PRODUCT_PAGE = "shop:"

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
    
    def add_page_to_cate_page_set(self, cate_id: int, page: int):
        self.redis_client.sadd(f"{self.READY_CATEGORY_PREFIX}:{cate_id}", page)

    def remove_page_from_set(self, cate_id: int, page: int):
        self.redis_client.srem(f"{self.READY_CATEGORY_PREFIX}:{cate_id}", page)

    def get_remaining_pages_in_category(self, cate_id: int) -> set:
        return self.redis_client.smembers(f"{self.READY_CATEGORY_PREFIX}:{cate_id}")

    def delete_category_page_key(self, cate_id: int):
        self.redis_client.delete(f"{self.READY_CATEGORY_PREFIX}:{cate_id}")
    
    def save_scraped_product_wait_comment(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.sadd(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}:{spider_id}", f"{product_id}&{sp_id}")
    
    def delete_scraped_product_wait_comment(self, spider_id: int, product_id: int, sp_id: int):
        self.redis_client.srem(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}:{spider_id}", f"{product_id}&{sp_id}")
    
    def get_scraped_product_wait_comment(self, spider_id: int) -> set:
        self.redis_client.smembers(f"{self.SCRAPED_PRODUCT_WAIT_COMMENT}:{spider_id}")