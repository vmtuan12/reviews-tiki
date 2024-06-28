from crawl_reviews.items import Product, Shop, Review, User, ReviewChild
from scrapy import Item

def build_item(source: dict, item_type: Item) -> Item:
    if item_type == Product:
        return product(source)
    elif item_type == Shop:
        return shop(source)
    elif item_type == Review:
        return review(source)
    elif item_type == ReviewChild:
        return review_child(source)
    else:
        return user(source)

def product(source: dict) -> Product:
    result = Product()
    pass

def shop(source: dict) -> Shop:
    result = Shop()
    pass

def review(source: dict) -> Review:
    result = Review()
    pass

def user(source: dict) -> User:
    result = User()
    pass

def review_child(source: dict) -> ReviewChild:
    result = ReviewChild()
    pass