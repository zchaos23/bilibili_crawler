import asyncio
from threading import Thread, Lock
from proxy import get_proxy
import queue
import csv
import networkx as nx
import pandas as pd
from bilibili_api import user as bilibili_user
from bilibili_api import settings
from conf import config
from remove_official_accounts import remove_official_accounts
import time as t
import datetime


lock = Lock()

class Worker(Thread):

    def __init__(self, name, max_retries):
        super().__init__(name=name)
        self.uid = name
        self.proxy_addr = ''
        self.user_following = []
        self.max_retries = max_retries

    def run(self) -> None:
        try:
            self.proxy_addr = proxy_queue.get(timeout=1)
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
                else:
                    global this_time_failure
                    global full_time_failure
                    lock.acquire()
                    # 失败计数加一
                    this_time_failure += 1
                    full_time_failure += 1
                    lock.release()

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

    pagerank_values = {key: pagerank_values[key] for key in pagerank_values if key not in user_crawled}

    sorted_pagerank_values = list(dict(sorted(pagerank_values.items(), key=lambda item: item[1], reverse=True)).keys())

    filtered_user_to_crawl = remove_official_accounts(sorted_pagerank_values[:config.MAIN_CRAWLER_SPEED])

    while len(filtered_user_to_crawl) < config.MAIN_CRAWLER_SPEED:
        addon_list = sorted_pagerank_values[config.MAIN_CRAWLER_SPEED + 1:config.MAIN_CRAWLER_SPEED + (
                    config.MAIN_CRAWLER_SPEED + 1 - len(filtered_user_to_crawl))]
        for addon in addon_list:
            filtered_user_to_crawl.append(addon)

    print(filtered_user_to_crawl)

    return filtered_user_to_crawl


if __name__ == '__main__':
    start_time = datetime.datetime.now()

    # 导入设置
    config = config()

    seed = config.SEED_LIST

    # 初始化数据库 - To-do: 改为 Neo4j
    with open(f'{config.EDGES_OUTPUT_NAME}.csv', 'w', newline='', encoding='utf-8') as f:
        header = ['source', 'target', 'weight']
        writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
        writer.writeheader()

    with open('count.csv', 'w', newline='', encoding='utf-8') as f:
        header = ['symbol', 'date', 'price']
        writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
        writer.writeheader()

    with open('count_datetime.csv', 'w', newline='', encoding='utf-8') as f:
        header = ['symbol', 'date', 'price']
        writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
        writer.writeheader()

    with open('start_time.txt', 'w', newline='', encoding='utf-8') as f:
        f.write(str(start_time))

    # 获取代理ip池，并添加到队列
    proxy_queue = queue.Queue()
    refresh_proxy_queue()

    # 迭代次数
    times = config.MAIN_CRAWLER_TIMES

    user_to_crawl = []
    user_to_crawl_queue = queue.Queue()

    user_crawled = []

    # 计数器
    global this_time
    global full_time
    global this_time_failure
    global full_time_failure

    this_time = 0
    full_time = 0
    this_time_failure = 0
    full_time_failure = 0


    for time in range(1, times):

        last_trigger_time = datetime.datetime.now()

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

        # 创建多个线程并启动
        threads = []

        for uid in user_to_crawl:
            thread = Worker(uid, config.MAIN_CRAWLER_MAX_RETRIES)
            threads.append(thread)
            thread.start()

            this_time += 1
            full_time += 1

        for thread in threads:
            thread.join()

            user_following_list = thread.user_following
            if user_following_list:
                for user_following in user_following_list:
                    graph_data.append({'source': thread.uid, 'target': user_following, 'weight': 1})
            else:
                pass

        with open(f'{config.EDGES_OUTPUT_NAME}.csv', 'a', newline='', encoding='utf-8') as f:
            header = ['source', 'target', 'weight']
            writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
            writer.writerows(graph_data)

        if this_time_failure/this_time <= 0.2:
            print(f'本次执行失败率 {(this_time_failure / this_time) * 100}%, 符合容差, 延迟 5 秒后进行下次抓取')
            t.sleep(5)
        else:
            print(f'本次执行失败率 {(this_time_failure / this_time) * 100}%, 不符合容差, 延迟 120 秒后进行下次抓取')
            t.sleep(120)

        this_time = 0
        this_time_failure = 0

        print(f'总次数: {full_time}, 总失败次数: {full_time_failure}, 失败率: {(full_time_failure / full_time) * 100}%')

        current_time = datetime.datetime.now()
        time_difference = current_time - last_trigger_time

        with open('count.csv', 'a', newline='', encoding='utf-8') as f:
            header = ['symbol', 'date', 'price']
            writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
            writer.writerows([{'symbol': 'full_time',
                               'date': time,
                               'price': full_time},
                              {'symbol': 'full_time_failure',
                               'date': time,
                               'price': full_time_failure},
                              {'symbol': 'full_time_ratio',
                               'date': time,
                               'price': (full_time_failure / full_time) * 100}
                              ])

        with open(f'{config.EDGES_OUTPUT_NAME}.csv', 'r', newline='', encoding='utf-8') as f:
            df = pd.read_csv(f)
            source_column = df['source']
            target_column = df['target']
            # user_to_crawl = pd.concat([source_column, target_column]).unique().tolist()

            combined_column = pd.concat([source_column, target_column])
            element_counts = combined_column.value_counts()

            count_length = len(element_counts.index.unique().tolist())

        with open('count_datetime.csv', 'a', newline='', encoding='utf-8') as f:
            header = ['symbol', 'date', 'price']
            writer = csv.DictWriter(f, fieldnames=header, lineterminator='\n')
            writer.writerows([{'symbol': '数据量',
                               'date': current_time,
                               'price': count_length}
                              ])

    f.close()
