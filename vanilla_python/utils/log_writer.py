from datetime import datetime

def write_log(err: str, file_name: str):
    f = open(f"/home/mhtuan/work/reviews/reviews-tiki/vanilla_python/logs/{file_name}", "a+")

    msg = f"ERROR {str(datetime.now())}\n{err}\n----------------------------------------------------------------------------------------------------------------------\n"
    f.write(msg)
    f.close()