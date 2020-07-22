# --------------------------------------------------------------------------------------------------------
# 2020/07/22
# src - scraping_proxies.py
# md
# --------------------------------------------------------------------------------------------------------
# TODO: NEEDS REFACTORING

from scraper.business.proxy_scraper import ProxyScraper
from scraper.database.proxy_facade import reset_proxies_stats, save_proxies
from tools.logger import logger


def scrape_proxies():
    ps = ProxyScraper()
    logger.info('=' * 100)
    logger.info('Start scrapping Proxies')
    logger.info('=' * 100)
    logger.info(f'Start scraping proxies from free_proxy_list.net')
    proxies_df = ps.scrape_free_proxy_list()
    save_proxies(proxies_df)
    logger.info(f'Start scraping proxies from hidemy.name')
    proxies_df = ps.scrape_hide_my_name()
    save_proxies(proxies_df)
    ps.test_proxies()


def reset_proxy_servers():
    reset_proxies_stats()


if __name__ == '__main__':
    scrape_proxies()
