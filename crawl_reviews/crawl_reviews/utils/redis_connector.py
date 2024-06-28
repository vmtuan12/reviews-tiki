from redis import Redis
from crawl_reviews.utils.tiki_utils import get_full_category_id, get_category_range

class RedisConnector:
    _instance = None

    SCRAPED_PAGES = "scraped-pages:"
    CURRENT_SCRAPING_PAGES = "current-scraping-pages:"
    READY_SCRAPE_PAGES = "ready-scrape-pages:"
    CATEGORY_DONE = "category-done:"
    CURRENT_PAGE_NUMBER_CATEGORY = "page-number:"

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
    
    def get_ready_scrape_page_by_spider(self, spider_id: int) -> int | None:
        cate_range = get_category_range(spider_id=spider_id)
        all_ready_scrape_page_key = self.redis_client.keys(pattern=f"{self.READY_SCRAPE_PAGES}*")

        for key in all_ready_scrape_page_key:
            cate_id = int(key.split(":")[0])
            if cate_range[0] <= cate_id and cate_id < cate_range[1]:
                return cate_id

        return None