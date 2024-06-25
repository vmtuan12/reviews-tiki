import pandas as pd
from redis_connector import RedisConnector
from env_reader import EnvReader
import scrapy

env_reader = EnvReader()

def get_list_category_id() -> list:
    redis_conn = RedisConnector()
    redis_client = redis_conn.get_client()
    done_category_set = redis_client.smembers(env_reader.get_redis_category_done_key())

    df = pd.read_csv("tiki_category.csv")
    df["cate_id"] = df["cate_id"].astype(str)
    set_category = set(df["cate_id"])

    return list(set_category - done_category_set)

def api_list_item_by_category(limit: int, category_id: int | str, page: int) -> str:
    return f"https://tiki.vn/api/v2/products?limit={limit}&include=advertisement&aggregations=1&category={category_id}&page={page}"

def api_list_product_in_shop(store_name: str, page: int) -> str:
    return f'https://api.tiki.vn/v2/seller/stores/{store_name}/products?limit=1&page={page}'

def api_product_detail(product_id: int) -> str:
    return f'https://tiki.vn/api/v2/products/{product_id}?platform=web'

def api_reviews(product_id: int, spid: int, seller_id: int, page: int) -> str:
    f'https://tiki.vn/api/v2/reviews?limit=5&include=comments,contribute_info,attribute_vote_summary&sort=score%7Cdesc,id%7Cdesc,stars%7Call&page={page}&spid={spid}&product_id={product_id}&seller_id={seller_id}'

def basic_headers() -> dict:
    return {
        "Cookie": "_trackity=ca57a0b3-de38-8724-d82b-35e19686f058; _ga=GA1.1.230656080.1718013190; TIKI_RECOMMENDATION=d25a36d92f15631f08b14d5d148c404f; _gcl_au=1.1.1566143782.1718013193; __uidac=016666cd09df96efee29cb60013930f7; __RC=4; __R=1; __uif=__uid%3A7731489831246331069; dtdz=270260d2-4929-577b-bd0c-fba36d0f1450; __iid=749; __iid=749; __su=0; __su=0; _hjSessionUser_522327=eyJpZCI6ImI2Yzc0MmI2LTBmODMtNTVlNi1hNzAwLTYyMzZkMjQ5Mzg4NCIsImNyZWF0ZWQiOjE3MTgwMTMxOTM0MDUsImV4aXN0aW5nIjp0cnVlfQ==; _fbp=fb.1.1718013197827.204674149304355791; __tb=0; __IP=1984373581; TOKENS={%22access_token%22:%22JaxCkNq0wQvPYi5BSmGbrc4nUp7of6Ks%22}; __UF=1%252C6; delivery_zone=Vk4wMzQwMjQwMTM=; tiki_client_id=230656080.1718013190; TKSESSID=d6142effdf7730e2e37e1d838222d060; _hjSession_522327=eyJpZCI6IjkzYzk0MDljLTkzY2YtNGEzYy05NTJlLTdlZDE4OGEwNGQ3NiIsImMiOjE3MTgyNjc5MjU0NTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; __adm_upl=eyJ0aW1lIjoxNzE4MjcxNzM5LCJfdXBsIjoiMC03NzMxNDg5ODMxMjQ2MzMxMDY5In0=; cto_bundle=ey7-Hl9oVzI2Rm51T1pYcVVjSllOQWxZRVpJOWRhVm5tJTJGRHlVVFVsOTVRalBVQ29md1NncTVDNUs1RiUyQkZTQktZb1dxWE5HN2Y2MTViSHBBT2ZQTG1mNEVmTDRiSE1QR2Naam9GNWlDQlZraFlleTFQYXdDNVlQaVRhS0pTVVhrY0VGM3Jxb3RzS2E4SVZGNzNzJTJCUDhWN01ybHFQU0p1YjRTYXJCMlBSSTJZbGdFVlklMkJKejlUdUNOTmE4VEsyMTZFJTJGRTlxZUtpTkJhNEJ2bVBJRURyJTJCR0F0RW94ODJPcTFlRk84aURTRFZ1R2g4SkFFeFJ2YmNUM1RJNnFpNXQlMkJrVnl0dnp5N3U3dVhlZzk0WWJHNG03SXN3NHhFUjAzTGs1YzdoWDBERTdyb243WnY4V0w2Z2slMkZnTTZBQkJick1ia3BNMno; _ga_S9GLR1RQFJ=GS1.1.1718267916.3.1.1718270223.59.0.0; amp_99d374=epf0jbMlb74Ev4spVD2GoK...1i08bvc2g.1i08e5pfs.95.a6.jb",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Linux\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }