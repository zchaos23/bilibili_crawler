import requests
from threading import Thread
import json


# 请求代理 ip
class Worker(Thread):

    def __init__(self, name):
        super().__init__(name=name)
        self.proxy_addr = name
        self.availability = None

    def run(self):
        self.availability = self._test_proxy_availability(self)

    @staticmethod
    def _test_proxy_availability(self):
        try:
            response = requests.get("https://www.bilibili.com", proxies={"http": self.proxy_addr, "https": self.proxy_addr}, timeout=1)
            if response.status_code == 200:
                return True
            else:
                return False
        except:
            return False


def get_proxy(proxy_addr):
    proxy_dict = {}

    try:
        response = requests.get(
            proxy_addr
        )
        if response.status_code == 200 and response.text:
            proxy_dict = json.loads(response.text)
        elif response.status_code == 200 and not response.text:
            # 待完成
            print(f"Request failed with no proxy address returned. Please check your IP pool.")
        else:
            print(f"Request failed with status code {response.status_code}.")
    except Exception as e:
        print(f"Request error: {str(e)}")

    proxy_pool = [
        'http://' + proxy_addr for proxy_addr in proxy_dict
    ]

    avail_proxy_pool = []

    threads = []

    for proxy_addr in proxy_pool:
        thread = Worker(proxy_addr)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
        if thread.availability:
            avail_proxy_pool.append(thread.proxy_addr)
        else:
            pass

    return avail_proxy_pool
