# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - twitter_scraper.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime, timedelta

import twint
import twint.output
import twint.storage.panda

# from business.proxy_manager import get_a_proxy_server
# from config import SCRAPE_WITH_PROXY
from config import LOG_LEVEL
from tools.utils import set_pandas_display_options

set_pandas_display_options()

cfg = {
    'twint_debug': False,  # Store information in debug logs.
    'twint_hide_terminal_output': True if LOG_LEVEL != 'Debug' else False,  # Hide termnal output.
    'twint_limit': 200000,  # Number of Tweets to pull (Increments of 20).
    'twint_retries': 10,  # Number of retries of requests (default: 10).
    'twint_show_stats': True,  # Set to True to show Tweet stats (replies, retweets, likes) in the terminal output.
    'twint_show_count': True,  # Count the total number of Tweets fetched.
    'twint_show_hashtags': False,  # Set to True to show hashtags in the terminal output.
    'twint_show_cashtags': False,  # Set to True to show cashtags in the terminal output.
    'twint_pandas_clean': True,  # Automatically clean Pandas dataframe at every start.
    'twint_geo': False

}


class _TwitterScraper:
    """
    Base class to start twitter tweets and profile for a username and send that data to the the scraping_controller for further handeling
    """
    _name = ''
    _twint_command = None  # twint.run.Search for tweets or twint.run.Lookup for profiles

    # def __init__(self, username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    def __init__(self, config=None):
        if config: cfg.update(config)
        self.c = cfg

        self.proxy_server = None

        self.username = None
        self.begin_date = None
        self.end_date = None

        # twint config parameters (see https://github.com/twintproject/twint/wiki/Configuration)
        # Todo: FORK TWINT AND MODIFY IT!!!
        #       The version 2.1.20 (installed in `/home/md/Miniconda3/envs/ai/lib/python3.7/site-packages/twint/`) has problems with downloading profiles.
        #       The patch  [See this issue](https://github.com/twintproject/twint/issues/786#issuecomment-639387864) works.
        #       However it automatically sets c.User_id and that causes problems in case of Connection Error.
        #       A work around for this bug is to manually set c.User_id to None before scraping.

        self.twint_hide_terminal_output = self.c['twint_hide_terminal_output']
        self.twint_debug = self.c['twint_debug']
        self.twint_limit = self.c['twint_limit']
        self.twint_retries = self.c['twint_retries']
        self.twint_show_stats = self.c['twint_show_stats']
        self.twint_show_count = self.c['twint_show_count']
        self.twint_show_hashtags = self.c['twint_show_hashtags']
        self.twint_show_cashtags = self.c['twint_show_cashtags']
        self.twint_pandas_clean = self.c['twint_pandas_clean']
        self.twint_geo = self.c['twint_geo']

    def execute_scraping(self, username, period_begin_date=None, period_end_date=None):
        if not self._twint_command: print('Error. Did you use the base class _TwitterScraper? Try TweetScraper or ProfileScraper instead!')
        self.username = username
        self.begin_date = period_begin_date
        self.end_date = period_end_date

        scraped_df = self._scrape_using_twint()

        return scraped_df

    def _scrape_using_twint(self):
        c = self._make_twint_config()
        twint.storage.panda.clean()

        self._twint_command(c)

        # Get both tweets_df and profile; One of them will be None
        tweets_df = twint.storage.panda.Tweets_df
        profile_df = twint.storage.panda.User_df
        twitter_df = tweets_df if profile_df is None else profile_df
        return twitter_df

    def _make_twint_config(self):
        c = twint.Config()
        c.Limit = self.twint_limit
        c.Geo = self.twint_geo
        c.Show_hashtags = self.twint_show_hashtags
        c.Show_cashtags = self.twint_show_cashtags
        c.Count = self.twint_show_count
        c.Stats = self.twint_show_stats
        c.Debug = self.twint_debug
        c.Hide_output = self.twint_hide_terminal_output
        c.Retries_count = self.twint_retries
        c.Pandas_clean = self.twint_pandas_clean
        c.Username = self.username
        c.Pandas = True
        if self._name == 'tweets':
            # Todo: Patch: if Since and Until are the same date (scraping 1 day) then 0 tweets returned
            if self.begin_date == self.end_date:
                self.end_date += timedelta(days=1)
            c.Since = datetime.strftime(self.begin_date, '%Y-%m-%d')
            c.Until = datetime.strftime(self.end_date, '%Y-%m-%d')
        if self.proxy_server:
            c.Proxy_host, c.Proxy_port = self.proxy_server['ip'], self.proxy_server['port']
            c.Proxy_type = 'http'
        c.User_id = None  # Bugfix twint
        return c


class TweetScraper(_TwitterScraper):
    def __init__(self, config):
        self._name = 'tweets'
        self._twint_command = twint.run.Search
        super(TweetScraper, self).__init__(config)


class ProfileScraper(_TwitterScraper):
    def __init__(self, config):
        self._name = 'profile'
        self._twint_command = twint.run.Lookup
        super(ProfileScraper, self).__init__(config)


if __name__ == '__main__':
    pass
