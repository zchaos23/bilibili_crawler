import asyncio
from threading import Thread
from proxy import get_proxy
import queue
import csv
import networkx as nx
import pandas as pd
from bilibili_api import user as bilibili_user
from bilibili_api import settings
from conf import config


class Worker(Thread):

    def __init__(self, name, max_retries):
        super().__init__(name=name)
        self.uid = name
        self.proxy_addr = ''
        self.user_following = []
        self.max_retries = max_retries

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
        self.user_following = loop.run_until_complete(self._crawl(self.uid, self.max_retries))
        loop.close()

    async def _crawl(self, uid, max_retries):
        for retry in range(max_retries):
            settings.proxy = proxy_queue.get(timeout=1)
            target_user = bilibili_user.User(uid=uid)
            try:
                user_following = await target_user.get_all_followings()
                user_crawled.append(uid)
                if user_following:
                    return user_following
                else:
                    return None
            except Exception as e:
                print(f"Crawl error (retry {retry + 1}/{max_retries}): {str(e)}")
                if retry < max_retries - 1:
                    await asyncio.sleep(1)

        return None


def refresh_proxy_queue():
    proxy_pool = get_proxy(config.PROXY_ADDR)

    for proxy_addr in proxy_pool:
        proxy_queue.put(proxy_addr)


def get_user_to_crawl_by_pagerank():
    df = pd.read_csv(f'{config.EDGES_OUTPUT_NAME}.csv')
    G = nx.DiGraph()

    nodes = df['source'].unique()
    G.add_nodes_from(nodes)

    for _, row in df.iterrows():
        source = row['source']
        target = row['target']
        G.add_edge(source, target)

    pagerank_values = nx.pagerank(G)
    for n in nodes:
        print(n)
    pagerank_values = {key: pagerank_values[key] for key in pagerank_values if key not in user_crawled}

    sorted_pagerank_values = list(dict(sorted(pagerank_values.items(), key=lambda item: item[1], reverse=True)).keys())

    return sorted_pagerank_values[:config.MAIN_CRAWLER_SPEED]


if __name__ == '__main__':
    # 导入设置
    config = config()

    seed = config.SEED_LIST

    # 初始化数据库 - To-do: 改为 Neo4j
    with open(f'{config.EDGES_OUTPUT_NAME}.csv', 'w', newline='', encoding='utf-8') as f:
        header = ['source', 'target', 'weight']
        writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
        writer.writeheader()

    # 获取代理ip池，并添加到队列
    proxy_queue = queue.Queue()
    refresh_proxy_queue()

    # 迭代次数
    times = config.MAIN_CRAWLER_SPEED

    user_to_crawl = []
    user_to_crawl_queue = queue.Queue()

    user_crawled = []

    for time in range(1, times):

        graph_data = []

        refresh_proxy_queue()

        print(time)

        # 定义爬取列表，并添加到队列
        if user_to_crawl:
            user_to_crawl = get_user_to_crawl_by_pagerank()
        else:
            user_to_crawl = seed

        for user in user_to_crawl:
            user_to_crawl_queue.put(user)

        print(user_to_crawl)

        # 创建多个线程并启动
        threads = []

        for uid in user_to_crawl:
            thread = Worker(uid, config.MAIN_CRAWLER_MAX_RETRIES)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

            user_following_list = thread.user_following
            print('a'+str(user_following_list))
            if user_following_list:
                for user_following in user_following_list:
                    graph_data.append({'source': thread.uid, 'target': user_following, 'weight': 1})
            else:
                pass

        print(graph_data)
        with open(f'{config.EDGES_OUTPUT_NAME}.csv', 'a', newline='', encoding='utf-8') as f:
            header = ['source', 'target', 'weight']
            writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
            writer.writerows(graph_data)

    f.close()
