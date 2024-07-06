from worker.base_crawler import BaseWorker

if __name__ == '__main__':
    cc = BaseWorker(id=0)
    cc.run_web_crawler()