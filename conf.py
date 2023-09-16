import configparser
import os
import re


class config:

    def __init__(self):
        self.CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.ini')
        self.CONFIG = configparser.ConfigParser()
        self.CONFIG.read(self.CONFIG_PATH)

        self.MAIN_CRAWLER_SETTINGS = self._get_main_crawler_settings()
        self.MAIN_CRAWLER_TIMES = int(self.MAIN_CRAWLER_SETTINGS['crawl_times'])
        self.MAIN_CRAWLER_SPEED = int(self.MAIN_CRAWLER_SETTINGS['crawl_speed'])
        self.MAIN_CRAWLER_MAX_RETRIES = int(self.MAIN_CRAWLER_SETTINGS['crawl_max_retries'])

        self.NODE_CRAWLER_SETTINGS = self._get_node_crawler_settings()
        self.NODE_CRAWLER_FILTER = int(self.NODE_CRAWLER_SETTINGS['filter'])
        self.NODE_CRAWLER_SPEED = int(self.NODE_CRAWLER_SETTINGS['crawl_speed'])

        self.PROXY_SETTINGS = self._get_proxy_settings()
        self.PROXY_ADDR = self._reformat_proxy_settings(self.PROXY_SETTINGS)

        self.OUTPUT_SETTINGS = self._get_output_settings()
        self.EDGES_OUTPUT_NAME = self.OUTPUT_SETTINGS['edges_output_name']
        self.NODES_OUTPUT_NAME = self.OUTPUT_SETTINGS['nodes_output_name']

        self.SEED = self._get_seed()
        self.SEED_LIST = self._get_seed_list(self.SEED)

    def _get_main_crawler_settings(self):
        return dict(self.CONFIG.items('main_crawler_settings'))

    def _get_node_crawler_settings(self):
        return dict(self.CONFIG.items('node_crawler_settings'))

    def _get_proxy_settings(self):
        return dict(self.CONFIG.items('proxy_settings'))

    def _reformat_proxy_settings(self, proxy_settings):
        proxy_api = proxy_settings['proxy_api']
        proxy_api_key = proxy_settings['proxy_api_key']
        proxy_num = proxy_settings['proxy_num']
        proxy_api = re.sub(r'api_key=', f'api_key={proxy_api_key}', proxy_api)
        proxy_api = re.sub(r'num=', f'num={proxy_num}', proxy_api)

        return proxy_api

    def _get_output_settings(self):
        return dict(self.CONFIG.items('output_settings'))

    def _get_seed(self):
        return dict(self.CONFIG.items('seed'))

    def _get_seed_list(self, seed):
        seed_list = seed['seed'].split(', ')

        return seed_list
