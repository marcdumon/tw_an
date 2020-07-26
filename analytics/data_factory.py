# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - data_factory.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

import pandas as pd

from database.analytics_facade import get_tweet_datetimes, update_stats, populate_stats, delete_all_stats
from database.twitter_facade import get_usernames
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
cfg = {
    'begin_date': datetime(2006, 1, 1).date(),
    'end_date': datetime.now().date(),
}


class DataFactory:
    def __init__(self, config=None):
        if config: cfg.update(config)
        self.c = cfg
        self.begin_date = self.c['begin_date']
        self.end_date = self.c['end_date']
        self.usernames_df = None
        self.stats_job = False
        self.tokens_job = False
        self.is_populate = False

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # INTERFACE
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @property
    def users_all(self):
        self.usernames_df = get_usernames()
        return self

    def users_sample(self, samples=10):
        usersnames_df = get_usernames()
        self.usernames_df = usersnames_df.sample(samples)
        return self

    def users_list(self, usernames, only_new=True):
        usernames = [u.lower() for u in usernames]
        self.usernames_df = pd.DataFrame(usernames, columns=['username'])
        return self

    @property
    def populate(self):
        self.is_populate = True
        delete_all_stats()
        return self

    @property
    def stats(self):
        self.stats_job = True
        return self

    @property
    def tokens(self):
        self.tokens_job = False
        return self

    def run(self):
        for _, user in self.usernames_df.iterrows():
            username = user['username']
            logger.info(f'Saving stats | {username}')
            if self.stats_job: self.save_stats(username)

    def save_stats(self, username):
        tweet_datetimes = get_tweet_datetimes(username)
        if not tweet_datetimes.empty:
            tweet_datetimes['n_tweets'] = 1
            for freq in ['D', 'W', 'M', 'A']:
                stats = self._calculate_stats(tweet_datetimes, freq)
                if self.is_populate:
                    populate_stats(username, stats, freq)
                else:
                    update_stats(username, stats, freq)
            return stats
        else:
            logger.error(f'User has no tweets | {username}')
            return

    def _calculate_stats(self, tweet_datetimes, freq):
        tweet_datetimes = tweet_datetimes.set_index('datetime')
        tweets_per_day = tweet_datetimes['n_tweets'].resample('D').sum()
        stats = tweet_datetimes.resample(freq).sum()
        idx = pd.date_range(self.begin_date, self.end_date, freq='D')
        tweets_per_day = tweets_per_day.reindex(idx, fill_value=0)
        stats['total'] = stats['n_tweets'].cumsum()
        stats = tweets_per_day.resample(freq).agg(['sum', 'mean', 'max'])
        stats['cumsum'] = stats['sum'].cumsum()
        return stats


if __name__ == '__main__':
    pass
