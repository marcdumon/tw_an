# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - proxy_scraper.py
# md
# --------------------------------------------------------------------------------------------------------
# TODO: NEEDS REFACTORING
import multiprocessing as mp
import time
from asyncio import TimeoutError, CancelledError
from datetime import datetime

import pandas as pd
import requests
from aiohttp import ServerDisconnectedError, ClientHttpProxyError, ClientProxyConnectionError, ClientOSError
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from config import LOG_LEVEL
from scraper.business.twitter_scraper import TweetScraper
from scraper.database.proxy_facade import get_proxies, save_a_proxy_test
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()


class ProxyScraper:
    """
    Class to start proxy servers from different websites and send that data to the the scraping_controller for further handeling.
    Currenty implemented websites are:
        - https://free-proxy-list.net/
        - https://hidemy.name/en/proxy-list/
    """

    def __init__(self):
        # Todo: Generalise to more proxy websites.
        #       scrape_free_proxy_list uses request and is very fast
        #       scrape_hide_my_name uses selenium
        pass

    @staticmethod
    def scrape_free_proxy_list():
        proxy_url = 'https://free-proxy-list.net/'
        # logger.debug(f'Start downloading proxy servers from {proxy_url}')
        response = requests.get(proxy_url)
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table', id='proxylisttable')
        list_tr = table.find_all('tr')
        list_td = [elem.find_all('td') for elem in list_tr]
        list_td = list(filter(None, list_td))
        proxies_df = pd.DataFrame()
        proxies_df['ip'] = [elem[0].text for elem in list_td]
        proxies_df['port'] = [elem[1].text for elem in list_td]
        proxies_df['source'] = 'free-proxy-list.net'
        proxies_df['datetime'] = datetime.now()
        proxies_df = proxies_df[['datetime', 'ip', 'port', 'source']]
        return proxies_df

    @staticmethod
    def scrape_hide_my_name():
        # All countries except AUstralia
        proxy_url = 'https://hidemy.name/en/proxy-list/?country=AFALARAMATAZBDBEBJBOBWBRBGBFKHCMCACLCNCOCGCDCRHRCYCZDJECEGFIFRGEDEGHGRGTHNHKHUINIDIRIQIEILITJ' \
                    'PKZKEKRKGLVLBLTLUMKMWMYMVMXMDMNMEMZNPNLNZNINGNOPKPSPAPYPEPHPLPTPRRORUSARSSLSGSKSISOZAESCHTWTHTRUGUAAEGBUSUYUZVEVN&maxtime=1000&type=hs&start='
        pages = 3
        options = Options()
        options.headless = True
        driver = webdriver.Firefox(options=options, service_log_path='/dev/null')
        proxies_df = pd.DataFrame()
        for start in range(0, 64 * (pages - 1) + 1, 64):
            # logger.debug(f'Downloading proxy servers from https://hidemy.name/ starting at {start}')
            driver.get(f'{proxy_url}{start}')
            time.sleep(10)
            table = driver.find_element_by_class_name('table_block')
            body = table.find_element_by_tag_name('tbody')
            for row in body.find_elements_by_tag_name('tr'):
                r = row.find_elements_by_tag_name('td')
                proxies_df = proxies_df.append({'ip': r[0].text,  # Todo: Use list iso DF. Now dict->df->dict ?
                                                'port': r[1].text,
                                                'source': 'hidemy.name',
                                                'datetime': datetime.now()},
                                               ignore_index=True)
                proxies_df = proxies_df[['datetime', 'ip', 'port', 'source']]
        driver.close()
        return proxies_df

    def test_proxies(self, only_blacklisted=False, processes=25):
        logger.info('=' * 100)
        logger.info(f"Start testing {'blacklisted' if only_blacklisted else 'all'} proxy servers")
        logger.info('=' * 100)
        proxies = get_proxies(only_blacklisted)
        proxy_list = []
        for _, row in proxies.iterrows():
            proxy = (row['ip'], row['port'])
            proxy_list.append(proxy)
        with mp.Pool(processes=processes) as pool:
            results = pool.starmap(self._test_and_save_proxy, proxy_list)

    @staticmethod
    def _test_and_save_proxy(ip, port):  # Todo: Rewrite this! Use twitter scraper
        logger.info('-' * 100)
        logger.info(f'Start testing proxy server: {ip}:{port}')
        logger.info('-' * 100)
        info = f'{ip}_{port}'
        delay = -1
        blacklisted = True
        error_code = -1
        # Try and time fetching the last 40 tweets from Donald Trump
        username = 'realDonaldTrump'
        limit = 40
        ts = TweetScraper(username)
        ts.proxy_server = {'ip': ip, 'port': port}
        ts.twint_limit = limit
        ts.twint_hide_terminal_output = True if LOG_LEVEL != 'Debug' else False

        try:
            start_time = time.time()
            ts.execute_scraping()
            delay = time.time() - start_time
            blacklisted, error_code = False, 0
        except ValueError as e:
            delay, blacklisted, error_code = 0, True, 1
            logger.error(f'Error ValueERROR Server: {ip}:{port}')
            logger.error(f'Error message: {e}')
        except (TimeoutError, CancelledError) as e:
            delay, blacklisted, error_code = 0, True, 2
            logger.error(f'Error: asyncio Server:{ip}:{port}')
            logger.error(f'Error message: {e}')
        except (ServerDisconnectedError, ClientHttpProxyError, ClientProxyConnectionError, ClientOSError) as e:
            delay, blacklisted, error_code = 0, True, 3
            logger.error(f'Error aiohttp Server: {ip}:{port}')
            logger.error(f'Error message: {e}')
        except:
            delay, blacklisted, error_code = 0, True, 9
            raise
        finally:
            proxy_test = {'ip': ip, 'port': port,
                          'delay': delay,
                          'blacklisted': blacklisted, 'error_code': error_code}
            logger.info(f'Error aiohttp Server: {ip}:{port}')
            save_a_proxy_test(proxy_test)


if __name__ == '__main__':
    pass
