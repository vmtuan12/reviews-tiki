from crawl_reviews.items import Product, Shop, Review, User, ReviewChild
from scrapy import Item
from datetime import date, timedelta

BASE_URL = "https://tiki.vn/"

def generate_item(source: dict, item_type: Item) -> Item:
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
    
def _handle_exception_retrieving_value(data, *args):
    if len(args) == 0:
        return None
    
    try:
        result = data.get(args[0])
        for index in range(1, len(args)):
            result = result.get(args[index])
    
    except Exception:
        return None
    
    return result

def product(source: dict) -> Product:
    result = Product()
    data = source["data"]

    result["id"] = data["id"]
    result["name"] = data["name"]
    result["url"] = BASE_URL + data["url_path"]
    result["price"] = data["original_price"]

    if source.get("from_detail") == True:
        result["shop_id"] = _handle_exception_retrieving_value(data, "current_seller", "id")
        result["spid"] = _handle_exception_retrieving_value(data, "current_seller", "product_id")
        result["sold"] = data["all_time_quantity_sold"]
        result["brand"] = _handle_exception_retrieving_value(data, "brand", "name")

    else:
        result["shop_id"] = data["seller_id"]
        result["spid"] = data["seller_product_id"]
        result["sold"] = _handle_exception_retrieving_value(data, "quantity_sold", "value")
        result["brand"] = _handle_exception_retrieving_value(data, "brand_name")

    return result
        
def shop(source: dict) -> Shop:
    result = Shop()

    result["id"] = source["id"]
    result["name"] = source["name"]
    result["follower"] = source["total_follower"]
    result["url"] = source["url"]

    for info in source["info"]:
        if info["type"] == "chat":
            result["chat_response"] = info["title"]

    result["join_time"] = str(date.today() - timedelta(days=source["days_since_joined"]))

    return result

def review(source: dict) -> Review:
    result = Review()

    result["id"] = source["id"]
    result["rating"] = source["rating"]
    result["title"] = source["title"]
    result["content"] = source["content"]
    result["created_at"] = source["created_at"]
    result["spid"] = source["spid"]
    result["product_id"] = source["product_id"]
    result["owner_id"] = _handle_exception_retrieving_value(source, "customer_id")
    result["like"] = source["thank_count"]

    images = _handle_exception_retrieving_value(source, "images")
    if (images != None):
        result["images"] = [i["full_path"] for i in images]

    return result

def user(source: dict) -> User:
    result = User()

    result["id"] = source["id"]
    result["name"] = source["name"]
    result["total_review_written"] = _handle_exception_retrieving_value(source, "contribute_info", "summary", "total_review")
    result["total_like_received"] = _handle_exception_retrieving_value(source, "contribute_info", "summary", "total_thank")
    result["join_time"] = source["created_time"]

    return result

def review_child(source: dict) -> ReviewChild:
    result = ReviewChild()

    result["id"] = source["id"]
    result["parent_id"] = source["review_id"]
    result["owner_id"] = source["customer_id"]
    result["content"] = source["content"]
    result["created_at"] = source["create_at"]
    result["commentator"] = source.get("commentator")

    return result