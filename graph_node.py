import asyncio
from bilibili_api import user as bilibili_user
from bilibili_api import settings
from threading import Thread
from proxy import get_proxy
import csv
import queue
import pandas as pd
from conf import config


class Worker(Thread):

    def __init__(self, name):
        super().__init__(name=name)
        self.uid = name
        self.proxy_addr = ''
        self.user_info = []

    def run(self) -> None:
        print(f'线程: {self.uid} 开始执行...')
        try:
            self.proxy_addr = proxy_queue.get(timeout=1)
            print(self.proxy_addr)
        except queue.Empty:
            refresh_proxy_queue()
            self.proxy_addr = proxy_queue.get(timeout=1)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.user_info= loop.run_until_complete(self._crawl(self.uid))
        loop.close()

    async def _crawl(self, uid, max_retries=5):
        for retry in range(max_retries):
            settings.proxy = proxy_queue.get(timeout=1)
            target_user = bilibili_user.User(uid=uid)
            try:
                user_info = await target_user.get_user_info()
                # user_crawled.append(uid)
                return user_info
            except Exception as e:
                print(f"Crawl error {uid} (retry {retry + 1}/{max_retries}): {str(e)}")
                if retry < max_retries - 1:
                    await asyncio.sleep(1)
        return {'name': f"已注销用户_uid:_{uid}"}


def refresh_proxy_queue():
    proxy_pool = get_proxy(config.PROXY_ADDR)

    for proxy_addr in proxy_pool:
        proxy_queue.put(proxy_addr)


if __name__ == '__main__':
    # 导入设置
    config = config()

    # 初始化数据库 - To-do: 改为 Neo4j
    with open(f'{config.NODES_OUTPUT_NAME}.csv', 'w', newline='', encoding='utf-8') as f:
        header = ['id', 'label']
        writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
        writer.writeheader()

    with open(f'{config.EDGES_OUTPUT_NAME}.csv', 'r', newline='', encoding='utf-8') as f:
        df = pd.read_csv(f)
        print(df)
        source_column = df['source']
        target_column = df['target']
        # user_to_crawl = pd.concat([source_column, target_column]).unique().tolist()

        combined_column = pd.concat([source_column, target_column])
        element_counts = combined_column.value_counts()

        # 筛选出现一定次数以上的元素
        user_to_crawl = element_counts[element_counts >= config.NODE_CRAWLER_FILTER].index.unique().tolist()

    print(len(user_to_crawl))

    chunk_size = config.NODE_CRAWLER_SPEED  # 每个子列表的大小

    # 分割元素列表为子列表
    nested_lists = [user_to_crawl[i:i + chunk_size] for i in range(0, len(user_to_crawl), chunk_size)]

    # 获取代理ip池，并添加到队列
    proxy_queue = queue.Queue()
    refresh_proxy_queue()

    for sub_lists in nested_lists:
        refresh_proxy_queue()
        # 创建多个线程并启动
        threads = []
        graph_data = []

        for user in sub_lists:
            thread = Worker(user)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

            user_info = thread.user_info
            print(user_info)
            user_name = user_info['name']
            graph_data.append({'id': thread.uid, 'label': user_name})

        with open(f'{config.NODES_OUTPUT_NAME}.csv', 'a', newline='', encoding='utf-8') as f:
            header = ['id', 'label']
            writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
            writer.writerows(graph_data)
