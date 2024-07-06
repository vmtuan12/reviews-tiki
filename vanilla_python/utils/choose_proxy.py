import random

PROXY_LIST = [
    "38.154.227.167:5868:naepplog:nkm332zst3hc",
    "185.199.229.156:7492:naepplog:nkm332zst3hc",
    "185.199.228.220:7300:naepplog:nkm332zst3hc",
    "185.199.231.45:8382:naepplog:nkm332zst3hc",
    "188.74.210.207:6286:naepplog:nkm332zst3hc",
    "188.74.183.10:8279:naepplog:nkm332zst3hc",
    "188.74.210.21:6100:naepplog:nkm332zst3hc",
    "45.155.68.129:8133:naepplog:nkm332zst3hc",
    "154.95.36.199:6893:naepplog:nkm332zst3hc",
    "45.94.47.66:8110:naepplog:nkm332zst3hc"
]

def get_proxy() -> dict:
    """
    proxies = { "http": "http://user:pass@10.10.1.10:3128/" }
    """
    index = random.randint(0, len(PROXY_LIST) - 1)
    proxy_info = PROXY_LIST[index].split(":")

    user = proxy_info[2]
    pwd = proxy_info[3]
    ip = proxy_info[0]
    port = proxy_info[1]

    return { "http": f"http://{user}:{pwd}@{ip}:{port}/" }