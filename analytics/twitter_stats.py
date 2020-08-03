# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - data_factory.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

import pandas as pd

from database.analytics_facade import get_tweet_datetimes, update_profile_stats, populate_profile_stats, delete_all_stats
from database.twitter_facade import get_usernames
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
cfg = {
    'begin_date': datetime(2006, 1, 1).date(),
    'end_date': datetime.now().date(),
}


class TwitterStats:
    def __init__(self, config=None):
        if config: cfg.update(config)
        self.c = cfg
        self.begin_date = self.c['begin_date']
        self.end_date = self.c['end_date']
        self.usernames_df = None
        self.profile_stats_job = False
        self.tweet_stat_job = False
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
        return self

    @property
    def profile_stats(self):
        self.profile_stats_job = True
        return self

    @property
    def tweet_stats(self):
        self.tweet_stat_job = False
        return self

    def run(self):
        for _, user in self.usernames_df.iterrows():
            username = user['username']
            logger.info(f'Saving profile_stats | {username}')
            if self.profile_stats_job: self.save_profile_stats(username)

    def save_profile_stats(self, username):
        tweet_datetimes = get_tweet_datetimes(username)
        if not tweet_datetimes.empty:
            tweet_datetimes['n_tweets'] = 1
            for freq in ['D', 'W', 'M', 'A']:
                stats = self._calculate_profile_stats(tweet_datetimes, freq)
                if self.is_populate:
                    populate_profile_stats(username, freq, stats)
                else:
                    update_profile_stats(username, freq, stats)
            return stats
        else:
            logger.error(f'User has no tweets | {username}')
            return

    def save_tweet_stats(self, username):
        # per tweet: tweet_id, concersation_id,rreply_to,nlikes, nreplies,nretweets,is_reply,hashtags, datetime, day, hour,
        # hashtags, stemmed tokens, pos, is_oov, is_emoji

        pass

    def _calculate_profile_stats(self, tweet_datetimes, freq):
        begin, end = tweet_datetimes['datetime'].min(), tweet_datetimes['datetime'].max()
        tweet_datetimes = tweet_datetimes.set_index('datetime')
        tweets_per_day = tweet_datetimes['n_tweets'].resample('D').sum()
        stats = tweet_datetimes.resample(freq).sum()
        idx = pd.date_range(begin, end, freq='D')
        tweets_per_day = tweets_per_day.reindex(idx, fill_value=0)
        stats['total'] = stats['n_tweets'].cumsum()
        stats = tweets_per_day.resample(freq).agg(['sum', 'mean', 'max'])
        stats['cumsum'] = stats['sum'].cumsum()
        return stats


if __name__ == '__main__':
    ts = TwitterStats()
    ts.users_list(['mboudry']).populate.profile_stats.run()
    # ts.users_sample(1).tweet_stats.run()
