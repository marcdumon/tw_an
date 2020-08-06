# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - data_factory.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

import pandas as pd
import spacy
from dateutil.relativedelta import relativedelta
from textacy.preprocessing import normalize_quotation_marks, normalize_unicode, replace_numbers, replace_urls, replace_emojis, replace_hashtags, remove_accents, remove_punctuation, \
    normalize_whitespace

from database.analytics_facade import get_tweet_datetimes, update_profile_stats, populate_profile_stats, get_tweets, upsert_tweet_stat
from database.twitter_facade import get_usernames, get_join_date
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
cfg = {
    'begin_date': datetime(2006, 1, 1).date(),
    'end_date': datetime.now().date()
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
        self.nlp = spacy.load('nl_core_news_lg', disable=[
            'parser',  # Assign dependency labels.
            # 'tagger', # Assign part-of-speech tags.
            'ner'  # Detect and label named entities.
        ])

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
        self.tweet_stat_job = True
        return self

    def run(self):
        for _, user in self.usernames_df.iterrows():
            username = user['username']
            if self.profile_stats_job:
                logger.info(f'Saving profile_stats | {username}')
                self.save_profile_stats(username)
            if self.tweet_stat_job:
                logger.info(f'Saving tweet_stats | {username}')
                self.save_tweet_stats(username)

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

    def save_tweet_stats(self, username):
        join_date = get_join_date(username).date()
        start_date = max(join_date, self.begin_date)
        period_start = start_date
        while period_start <= self.end_date:  # + relativedelta(months=1):
            period_end = period_start + relativedelta(day=31)  # till last day of the month.
            logger.debug(f'Generate tweet_stats for {username} for date {period_start} till {period_end}')
            tweets = get_tweets(username, datetime(period_start.year, period_start.month, period_start.day), datetime(period_end.year, period_end.month, period_end.day))
            if tweets.empty:
                period_start = period_end + relativedelta(days=1)
                continue
            tweets['year'] = tweets['datetime'].dt.year
            tweets['month'] = tweets['datetime'].dt.month
            tweets['day'] = tweets['datetime'].dt.dayofweek
            tweets['hour'] = tweets['datetime'].dt.hour
            text = ''
            for i, tweet in tweets.iterrows():
                tweet_txt = tweet['tweet']
                tweet_txt = self._normalize_tweet(tweet_txt, hashtags=False, emojis=False)
                tweets.loc[i, 'len'] = len(tweet_txt)
                text += ' ' + tweet_txt
            pos_df = pd.DataFrame(self._pos(text))
            pos_dict = {}
            for c in pos_df.columns:
                pos_dict[c.lower()] = pos_df[c].dropna().to_list()
            year, month = period_start.year, period_start.month
            upsert_tweet_stat(username, year, month, tweets, pos_dict)
            period_start = period_end + relativedelta(days=1)

    def _normalize_tweet(self, txt,
                         quotation_marks=True,
                         unicode=True,
                         numbers=True,
                         urls=True,
                         emojis=True,
                         hashtags=True,
                         accents=True,
                         punctuation=True,
                         whitespace=True):
        if quotation_marks: txt = normalize_quotation_marks(txt)
        if unicode: txt = normalize_unicode(txt)
        if numbers: txt = replace_numbers(txt, replace_with='NUMBER')
        if urls: txt = replace_urls(txt, replace_with='')
        if emojis: txt = replace_emojis(txt, replace_with='EMOJI')
        if hashtags: txt = replace_hashtags(txt)
        if accents: txt = remove_accents(txt)
        if punctuation: txt = remove_punctuation(txt, marks='.,?\\/()[];:!*+-*="\'▶•◦⁃∞')  # specify marks otherwise @ will also be removed!
        if whitespace:
            txt = normalize_whitespace(txt)
            txt = ' '.join(txt.split())  # remove excess whitespace
        return txt

    def _pos(self, txt):
        doc = self.nlp(txt)
        pos = [{t.pos_: t.lemma_} for t in doc if (not t.is_stop) and (len(t) > 2) and (t.text[0] != '@')]
        return pos


if __name__ == '__main__':
    ts = TwitterStats()
    # ts.users_list(['mboudry']).populate.profile_stats.run()
    ts.users_list(['mboudry']).tweet_stats.run()
    # ts.users_all.tweet_stats.run()
