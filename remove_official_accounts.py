import asyncio
from bilibili_api import user as bilibili_user
from bilibili_api import settings
from threading import Thread
from proxy import get_proxy
import csv
import queue
import pandas as pd
from conf import config


config = config()

proxy_queue = queue.Queue()


class Worker(Thread):

    def __init__(self, name):
        super().__init__(name=name)
        self.uid = name
        self.proxy_addr = ''
        self.user_info = []

    def run(self) -> None:
        try:
            self.proxy_addr = proxy_queue.get(timeout=1)
        except queue.Empty:
            refresh_proxy_queue()
            self.proxy_addr = proxy_queue.get(timeout=1)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.user_info= loop.run_until_complete(self._crawl(self.uid))
        loop.close()

    async def _crawl(self, uid, max_retries=3):
        for retry in range(max_retries):
            settings.proxy = proxy_queue.get(timeout=1)
            target_user = bilibili_user.User(uid=uid)
            try:
                user_info = await target_user.get_user_info()
                # user_crawled.append(uid)
                return user_info
            except Exception as e:
                if retry < max_retries - 1:
                    await asyncio.sleep(1)
        return None


def refresh_proxy_queue():
    proxy_pool = get_proxy(config.PROXY_ADDR)

    for proxy_addr in proxy_pool:
        proxy_queue.put(proxy_addr)


def remove_official_accounts(user_to_crawl):
    not_official_accounts = []

    refresh_proxy_queue()

    threads = []

    for user in user_to_crawl:
        # 创建多个线程并启动
        thread = Worker(user)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

        user_info = thread.user_info
        if user_info:
            try:
                if user_info['official']['type'] == 1:
                    pass
                else:
                    not_official_accounts.append(user_info['mid'])
            except:
                pass
        else:
            pass

    return not_official_accounts
