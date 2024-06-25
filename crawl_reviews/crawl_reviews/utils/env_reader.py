
import yaml

class EnvReader():
    def __init__(self) -> None:
        with open('/home/mhtuan/work/reviews/reviews-tiki/crawl_reviews/crawl_reviews/env.yaml', 'r') as f:
            self.config = yaml.load(f, Loader=yaml.SafeLoader)

    def get_redis_scraped_pages_key(self) -> str:
        return self.config["redis-key"]["scraped-pages"]

    def get_redis_current_scraping_pages_key(self) -> str:
        return self.config["current-scraping-pages"]["scraped-pages"]

    def get_ready_scrape_pages_key(self) -> str:
        return self.config["ready-scrape-pages"]["scraped-pages"]

    def get_redis_category_done_key(self) -> str:
        return self.config["category-done"]["scraped-pages"]