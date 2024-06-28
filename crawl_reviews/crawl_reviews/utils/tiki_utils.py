import pandas as pd

NUMBER_OF_SPIDER = 5
CATEGORY_CSV_PATH = "tiki_cate_new.csv"

def get_full_category_id(is_set=False) -> set | list:
    df = pd.read_csv(CATEGORY_CSV_PATH)
    df["cate_id"] = df["cate_id"].astype(str)
    result_category = set(df["cate_id"]) if is_set == True else list(df["cate_id"])

    return result_category

def get_url_key_by_cate_id(category_id: int) -> str:
    df = pd.read_csv(CATEGORY_CSV_PATH)
    url_key = df[df["category_id"] == category_id]["url_key"].name

    return url_key

def get_category_range(spider_id: int) -> tuple:
    """
    tuple (int, int)\n
    category id in range [start, end)
    """
    df = pd.read_csv(CATEGORY_CSV_PATH)
    set_category = set(df["category_id"])

    list_cate = sorted(set_category)

    mod = len(list_cate) % NUMBER_OF_SPIDER

    chunk_size = [0]
    if mod == 0:
        chunk_size += [len(list_cate) / NUMBER_OF_SPIDER for x in range(NUMBER_OF_SPIDER)]
    else:
        for x in range(NUMBER_OF_SPIDER):
            if mod != 0:
                chunk_size.append((len(list_cate) // NUMBER_OF_SPIDER) + 1)
                mod -= 1
            else:
                chunk_size.append(len(list_cate) // NUMBER_OF_SPIDER)
    
    return (chunk_size[spider_id], chunk_size[spider_id] + chunk_size[spider_id + 1])

def api_list_item_by_category(category_id: int | str, url_key: str, page: int, limit=40) -> str:
    return f"https://tiki.vn/api/personalish/v1/blocks/listings?limit={limit}&include=advertisement&aggregations=2&version=home-persionalized&urlKey={url_key}&category={category_id}&page={page}"

def api_shop_info(seller_id: int) -> str:
    return f'https://api.tiki.vn/product-detail/v2/widgets/seller?seller_id={seller_id}&platform=desktop&version=3'

def api_list_product_in_shop(shop_name: str, page: int) -> str:
    return f'https://api.tiki.vn/v2/seller/stores/{shop_name}/products?limit=1&page={page}'

def api_product_detail(product_id: int) -> str:
    return f'https://tiki.vn/api/v2/products/{product_id}?platform=web'

def api_reviews(product_id: int, spid: int, seller_id: int, page: int, limit=5) -> str:
    f'https://tiki.vn/api/v2/reviews?limit={limit}&include=comments,contribute_info,attribute_vote_summary&sort=score%7Cdesc,id%7Cdesc,stars%7Call&page={page}&spid={spid}&product_id={product_id}&seller_id={seller_id}'

def basic_headers() -> dict:
    return {
        "Cookie": "_trackity=90940b0c-9bbe-4d2d-55ee-cd38fcd67404; TIKI_GUEST_TOKEN=NF6svy7tmlhkQY3aTKqUZEP4WGxcVi8B; TOKENS={%22access_token%22:%22NF6svy7tmlhkQY3aTKqUZEP4WGxcVi8B%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1877163290212%2C%22guest_token%22:%22NF6svy7tmlhkQY3aTKqUZEP4WGxcVi8B%22}; delivery_zone=Vk4wMzQwMjQwMTM=; tiki_client_id=; _ga_S9GLR1RQFJ=GS1.1.1719483290.1.0.1719483290.60.0.0; _ga=GA1.1.1837829046.1719483291; amp_99d374=akKFaQrENWTrSLxTlQU3oZ...1i1cj1kka.1i1cj1mvd.3.7.a; _gcl_au=1.1.1125274050.1719483294",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Linux\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }