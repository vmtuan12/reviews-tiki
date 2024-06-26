import pandas as pd

NUMBER_OF_SPIDER = 5
CATEGORY_CSV_PATH = "tiki_cate_new.csv"

def get_full_set_category_id() -> set:
    df = pd.read_csv(CATEGORY_CSV_PATH)
    df["cate_id"] = df["cate_id"].astype(str)
    set_category = set(df["cate_id"])

    return set_category

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
        "Cookie": "_trackity=ca57a0b3-de38-8724-d82b-35e19686f058; _ga=GA1.1.230656080.1718013190; _gcl_au=1.1.1566143782.1718013193; __uidac=016666cd09df96efee29cb60013930f7; __RC=4; __R=1; __uif=__uid%3A7731489831246331069; dtdz=270260d2-4929-577b-bd0c-fba36d0f1450; __iid=749; __iid=749; __su=0; __su=0; _hjSessionUser_522327=eyJpZCI6ImI2Yzc0MmI2LTBmODMtNTVlNi1hNzAwLTYyMzZkMjQ5Mzg4NCIsImNyZWF0ZWQiOjE3MTgwMTMxOTM0MDUsImV4aXN0aW5nIjp0cnVlfQ==; _fbp=fb.1.1718013197827.204674149304355791; __tb=0; TIKI_RECOMMENDATION=1cf7b314c3f3758abffa9407a9435aae; __UF=1%252C6; fblo_220558114759707=y; TOKENS={%22access_token%22:%22eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyOTk5OTk1MSIsImlhdCI6MTcxOTMxMTAzNSwiZXhwIjoxNzE5Mzk3NDM1LCJpc3MiOiJodHRwczovL3Rpa2kudm4iLCJjdXN0b21lcl9pZCI6IjI5OTk5OTUxIiwiZW1haWwiOiIiLCJjbGllbnRfaWQiOiJ0aWtpLXNzbyIsIm5hbWUiOiIyOTk5OTk1MSIsInNjb3BlIjoic3NvIiwiZ3JhbnRfdHlwZSI6InBob25lIn0.Y_LoG3G8XW63CtPTkSyk-SQlfMD3gjGg9FOzdL3IizU48ReGH0GuprGWTynEW97H17zMe8VEzhNcJD-Ku8fCDd6EuhTNYBo0kABYmt_5WlHC_Uh4JDVtJd2GP-19c8eqnN_rJ3F0NaaPGuVo3mPTe2jmaGoU7WCI-QTmXyDUigLvJe4ZkitLQTjHdh1Il3p6jKEqQhSL8wJ_vpaoimISu8U-F-fbmKeMQbm9N29HM7Eko77_QLNiVj5m9uD-6514K1204G2lEF-LvD9fnzF5PC-AFvMq_OF6kjJ0-OIFGW0ONvP0HvZhGjZVXdsueGRyztuvNoINLTINwa9nh2rL3pHS7Ja__ZpKwf6scN5gS-qfZIc6lh1-huhozxN608Mneg3TiU_i5VJhr758HPfA-EDbapUNdb4vA6DIXkg3OlD1LdUkRqPn5mO2aWNu6F1FnThIRfCie6jsl_2aL8fEpNViAlfexFgWIAmmJQHGDTnUv7E5wXgDwQ99vDwhGXXgc3377fC4Wo2HHnRwy3FPtLmNEX7QH1vzDWtW5SddJY7J8t0GVSuB7tQdGUYdxeU6FQldTZwovqfnVa7o_H01Y4FwLFIalrxTQjaWmIyocWdMy0qSUReR_UYDhW4gjGrzNCWjILkM0BP7wYs8PBCebTeoqq0HOhOZfEvbrPo8w5o%22%2C%22refresh_token%22:%22TKIAHKIcOZxsQhr78J8l91-KqIJAd28g78AWXFD1cLQpvfVI4vdKSoc6jhl6fFUUPv0FTLVYh1rAeQiEInat%22%2C%22token_type%22:%22bearer%22%2C%22expires_in%22:86400%2C%22expires_at%22:1719397435134%2C%22customer_id%22:29999951}; TIKI_ACCESS_TOKEN=eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyOTk5OTk1MSIsImlhdCI6MTcxOTMxMTAzNSwiZXhwIjoxNzE5Mzk3NDM1LCJpc3MiOiJodHRwczovL3Rpa2kudm4iLCJjdXN0b21lcl9pZCI6IjI5OTk5OTUxIiwiZW1haWwiOiIiLCJjbGllbnRfaWQiOiJ0aWtpLXNzbyIsIm5hbWUiOiIyOTk5OTk1MSIsInNjb3BlIjoic3NvIiwiZ3JhbnRfdHlwZSI6InBob25lIn0.Y_LoG3G8XW63CtPTkSyk-SQlfMD3gjGg9FOzdL3IizU48ReGH0GuprGWTynEW97H17zMe8VEzhNcJD-Ku8fCDd6EuhTNYBo0kABYmt_5WlHC_Uh4JDVtJd2GP-19c8eqnN_rJ3F0NaaPGuVo3mPTe2jmaGoU7WCI-QTmXyDUigLvJe4ZkitLQTjHdh1Il3p6jKEqQhSL8wJ_vpaoimISu8U-F-fbmKeMQbm9N29HM7Eko77_QLNiVj5m9uD-6514K1204G2lEF-LvD9fnzF5PC-AFvMq_OF6kjJ0-OIFGW0ONvP0HvZhGjZVXdsueGRyztuvNoINLTINwa9nh2rL3pHS7Ja__ZpKwf6scN5gS-qfZIc6lh1-huhozxN608Mneg3TiU_i5VJhr758HPfA-EDbapUNdb4vA6DIXkg3OlD1LdUkRqPn5mO2aWNu6F1FnThIRfCie6jsl_2aL8fEpNViAlfexFgWIAmmJQHGDTnUv7E5wXgDwQ99vDwhGXXgc3377fC4Wo2HHnRwy3FPtLmNEX7QH1vzDWtW5SddJY7J8t0GVSuB7tQdGUYdxeU6FQldTZwovqfnVa7o_H01Y4FwLFIalrxTQjaWmIyocWdMy0qSUReR_UYDhW4gjGrzNCWjILkM0BP7wYs8PBCebTeoqq0HOhOZfEvbrPo8w5o; TIKI_USER=q6MfAjNMdK7Gz5fjjz6TuV9JhmMakIFXGanp6AYOAS7wdrl1p0d2HEybRQPMhxNLVH0RwBathdGl; bnpl_whitelist_info={%22content%22:%22Mua%20tr%C6%B0%E1%BB%9Bc%20tr%E1%BA%A3%20sau%22%2C%22is_enabled%22:true%2C%22icon%22:%22https://salt.tikicdn.com/ts/tmp/95/15/2d/4b3d64b220f55f42885c86ac439d6d62.png%22%2C%22deep_link%22:%22https://tiki.vn/mua-truoc-tra-sau/dang-ky?src=account_page%22}; cto_bundle=b-Hc3l9oVzI2Rm51T1pYcVVjSllOQWxZRVpDcXladEhZVm9mJTJGenJnS1VFcHVkWWRUJTJGSmRYQTc5U0V0clI0bmJDYmRsYzNuZ1FSSCUyQnBFUHM1dlZWRU55bGtXZ29BYjNROEM1SGZvNUhSZW1YWExpbnpOZVZKVmJnSEdDbk1rMnE0R1Y4dEtycW5yQSUyRlZRaUZ6QlROWTk2Y3JBUDRZQXFPaEtndjZKcXpQUlZESWc2NmdmYzRZR0c2ZFFtenFQQ0I2dnh2SHBhQXY5UUlVTTR3ZiUyRjRjak1jSjRNSHZSQ2Jya25qNDY3MzUyb3BZcVBuV1RPSUpXeWd2NzhaaEkyM3BLUHRIYw; TKSESSID=ecabe02083237094c9b00ce0f52a5543; _tuid=29999951; tiki_client_id=230656080.1718013190; delivery_zone=Vk4wMzQwMjQwMTM=; _hjSession_522327=eyJpZCI6IjUzOWVlMzhjLThiYmQtNGFiMC1iMzVkLWU4NjI5MmFiZjY4OSIsImMiOjE3MTkzNjUwNTAzMTUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; __IP=1963319918; __adm_upl=eyJ0aW1lIjoxNzE5MzY2ODUwLCJfdXBsIjoiMC03NzMxNDg5ODMxMjQ2MzMxMDY5In0=; _ga_S9GLR1RQFJ=GS1.1.1719365046.17.1.1719365051.55.0.0; amp_99d374=epf0jbMlb74Ev4spVD2GoK.Mjk5OTk5NTE=..1i19293p0.1i1929bsp.mr.qu.1hp",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Linux\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }