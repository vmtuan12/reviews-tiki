# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Product(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    shop_id = scrapy.Field()
    price = scrapy.Field()
    spid = scrapy.Field()
    sold = scrapy.Field()
    brand = scrapy.Field()

class Shop(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    follower = scrapy.Field()
    url = scrapy.Field()
    chat_response = scrapy.Field()
    join_time = scrapy.Field()

class User(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    total_review_written = scrapy.Field()
    total_like_received = scrapy.Field()
    join_time = scrapy.Field()

class Review(scrapy.Item):
    id = scrapy.Field()
    rating = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    created_at = scrapy.Field()
    spid = scrapy.Field()
    product_id = scrapy.Field()
    owner_id = scrapy.Field()
    like = scrapy.Field()
    images = scrapy.Field()

class ReviewChild(scrapy.Item):
    id = scrapy.Field()
    parent_id = scrapy.Field()
    owner_id = scrapy.Field()
    content = scrapy.Field()
    created_at = scrapy.Field()
    commentator = scrapy.Field()