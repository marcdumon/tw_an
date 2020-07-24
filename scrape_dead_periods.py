# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - scrape_daily.py
# md
# --------------------------------------------------------------------------------------------------------
import time
from datetime import datetime, timedelta
from pprint import pprint

from config import DATABASE
from scraper.business.scraping_twitter import TwitterScrapingSession
from tools.logger import logger

scraper_cfg = {
    'n_processes': 25,
    'max_fails': 8,
    'scrape_only_missing_dates': False,
    'min_tweets': 1}
session_cfg = {
    'session_begin_date': (datetime.now() - timedelta(days=15)).date(),
    'session_end_date': (datetime.now() - timedelta(days=14)).date(),
    'time_delta': 1}
c = {}
c.update(scraper_cfg)
c.update(session_cfg)

logger.info(f"START DAILY SCRAPING | {DATABASE}")
time.sleep(.5)
pprint(c)

input("Press Enter to continue...")

scrape = TwitterScrapingSession(c)
scrape.rescrape_dead_periods().start()
