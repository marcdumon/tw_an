# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - scrape_daily.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

from scraper.business.scraping_twitter import TwitterScrapingSession

scraper_cfg = {'n_processes': 25,
               'max_fails': 8,
               'scrape_only_missing_dates': False,
               'min_tweets': 1}

session_cfg = {'session_begin_date': datetime(2020, 7, 15).date(),
               'session_end_date': datetime.now().date(),
               'time_delta': 100}
c = {}

c.update(scraper_cfg)
c.update(session_cfg)

scrape = TwitterScrapingSession(c)

# scrape.rescrape_dead_periods(0).start()
scrape.profiles.tweets.users_sample(5).start()
# scrape.profiles.users_all.start()
# scrape.tweets.users_all.start()
# scrape.profiles.users_list(['mauritsvdr','elhammouchiothm'], False).start()
# new_users=['Tom_Demeyer']
# scrape.profiles.tweets.users_list(new_users,True).start()
# scrape.rescrape_dead_periods(7).start()
