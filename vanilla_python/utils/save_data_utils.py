import json
import os

SAVE_DATA_FOLDER_PATH = "/home/mhtuan/work/reviews/reviews-tiki/crawl_reviews/crawl_reviews/save_data/category/"

def _file_exist(path: str) -> bool:
    return os.path.exists(path)

def _category_page_file_path(category_id: int) -> str:
    return f"{SAVE_DATA_FOLDER_PATH}paging_category_{category_id}.json"

def write_category_last_page(category_id: int, last_page: int):
    file_path = _category_page_file_path(category_id)

    if _file_exist(file_path):
        return
    
    try:
        data = {
            "last_page": last_page
        }
        with open(file_path, 'w+', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except Exception:
        pass