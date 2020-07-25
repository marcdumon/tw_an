# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - scraping_twitter.py
# md
# --------------------------------------------------------------------------------------------------------
import multiprocessing as mp
import sys
import time
from concurrent.futures import TimeoutError
from concurrent.futures._base import CancelledError
from datetime import datetime, timedelta
from queue import Empty
from random import random

import pandas as pd
from aiohttp import ServerDisconnectedError, ClientOSError, ClientHttpProxyError, ClientProxyConnectionError

from database.log_facade import log_scraping_profile, log_scraping_tweets, get_max_sesion_id, get_dead_tweets_periods
from database.proxy_facade import get_proxies, update_proxy_stats, update_proxies_ratio
from database.twitter_facade import get_join_date, get_nr_tweets_per_day, save_tweets, save_a_profile, get_a_profile
from database.twitter_facade import get_usernames
from scraper.business.twitter_scraper import TweetScraper, ProfileScraper
from tools.logger import logger

"""
A collection of functions to control scraping and saving proxy servers, Twitter tweets and profiles
"""

####################################################################################################################################################################################
cfg = {
    'n_processes': 25,
    'session_begin_date': datetime(2006, 1, 1).date(),
    'session_end_date': datetime.now().date(),
    'time_delta': 100,
    'max_fails': 8,
    'scrape_only_missing_dates': False,
    'min_tweets': 1,
}


class TwitterScrapingSession:
    def __init__(self, config=None):
        if config: cfg.update(config)
        self.c = cfg
        self.n_processes = self.c['n_processes']
        self.session_begin_date = self.c['session_begin_date']
        self.session_end_date = self.c['session_end_date']
        self.timedelta = self.c['time_delta']
        self.max_fails = self.c['max_fails']
        self.missing_dates = self.c['scrape_only_missing_dates']
        self.min_tweets = self.c['min_tweets']

        self.usersnames_df = pd.DataFrame()
        self.scrape_profiles = False
        self.scrape_tweets = False
        self.rescrape = False

        manager = mp.Manager()
        self.proxy_queue = manager.Queue()
        self.session_id = get_max_sesion_id() + 1
        # if system_cfg.reset_proxies_stat: reset_proxies_stats()

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # INTERFACE
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @property
    def users_all(self):
        self.usersnames_df = get_usernames()
        return self

    def users_sample(self, samples=10):
        self.usersnames_df = get_usernames()
        self.usersnames_df = self.usersnames_df.sample(samples)
        return self

    def users_list(self, usernames, only_new=True):
        usernames = [u.lower() for u in usernames]
        if only_new:
            for username in usernames.copy():
                if get_a_profile(username):
                    logger.debug(f'Username {username} already exists')
                    usernames.remove(username)
                    logger.info(f'Username already exists | {username}')
        self.usersnames_df = pd.DataFrame(usernames, columns=['username'])
        return self

    @property
    def profiles(self):
        self.scrape_profiles = True
        return self

    @property
    def tweets(self):
        self.scrape_tweets = True
        return self

    def rescrape_dead_periods(self, session_id=-1):
        self.rescrape = True
        self.usersnames_df = get_dead_tweets_periods(session_id=session_id)
        logger.warning(f'Rescraping following periods')
        print(self.usersnames_df)
        self.session_id *= -1
        self.scrape_tweets = True
        return self

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ENGINE
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # @property
    def start(self):
        if not (self.scrape_profiles or self.scrape_tweets):
            logger.warning(f'Nothing to do. Did you forget "profiles" or "tweets" instruction?')
            return None
        if self.usersnames_df.empty:
            logger.warning(f'Nothing to do. Did you forget to set "users_all" or "users_list"? Or all users already exist?')
            return None
        else:
            n_processes = min(len(self.usersnames_df), self.n_processes)
            logger.info(
                f'Start Twitter Scraping. | n_processes={n_processes}, session_id={self.session_id}, '
                f'session_begin_date={self.session_begin_date}, session_end_date={self.session_end_date}, timedelta={self.timedelta}, missing_dates={self.missing_dates}')
        if self.scrape_profiles:
            self._populate_proxy_queue()
            mp_iterable = [(username,) for username in self.usersnames_df['username']]
            with mp.Pool(processes=n_processes) as pool:
                pool.starmap(self.scrape_a_user_profile, mp_iterable)
        if self.scrape_tweets:
            self._populate_proxy_queue()
            if self.rescrape:
                mp_iterable = [(username, begin_date, end_date) for _, (username, begin_date, end_date) in self.usersnames_df.iterrows()]
            else:
                mp_iterable = [(username, self.session_begin_date, self.session_end_date) for username in self.usersnames_df['username']]
            with mp.Pool(processes=n_processes) as pool:
                pool.starmap(self.scrape_a_user_tweets, mp_iterable)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # PROFILES
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def scrape_a_user_profile(self, username):  # Todo:  proxy stats
        log_scraping_profile(self.session_id, 'begin', 'profile', username)
        time.sleep(random() * 5)  # Todo: DOESN'T WORKOtherwise other objects in other processes will also start checking proxies populated, filling the queue with the same proxies
        self._check_proxy_queue()
        fail_counter = 0
        while fail_counter < self.max_fails:
            proxy = self.proxy_queue.get()
            profile_scraper = ProfileScraper(self.c)
            profile_scraper.proxy_server = proxy
            logger.info(f'Start scraping profiles | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
            # Todo: When I don't add raise to the get.py / def User(...) / line 197, then fail silently.
            #       No distinction between existing user with proxy failure and canceled account.
            #       When I add raise, twint / asyncio show  error traceback in terminal
            #       ? What happens with proxies when username is canceled? Sometimes TimeoutError or TypeError
            try:  # Todo: Refactor: make method and use also in scrape_a_user_tweets

                profile_df = profile_scraper.execute_scraping(username)

            except:
                print('x' * 100)
                print(sys.exc_info()[0])
                print(sys.exc_info())
                print('x' * 100)
                raise
            else:
                if profile_df.empty:  # ProfileScrapingError
                    logger.error(f'Empty profile | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                    update_proxy_stats('ProfileScrapingError', proxy)
                    fail_counter += 1
                    time.sleep(random() * 5)
                else:  # ok
                    logger.info(f'Saving profile | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                    log_scraping_profile(self.session_id, 'ok', 'profile', username, proxy=proxy)
                    save_a_profile(profile_df)
                    update_proxy_stats('ok', proxy)
                    self._release_proxy_server(proxy)
                    break
            finally:
                if fail_counter >= self.max_fails:  # Dead
                    txt = f'dead | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}'
                    logger.error(txt)
                    log_scraping_profile(self.session_id, 'dead', f'profile', username, proxy=proxy)
        log_scraping_profile(self.session_id, 'end', 'profile', username)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # TWEETS
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def scrape_a_user_tweets(self, username, session_begin_date, session_end_date):
        log_scraping_tweets(self.session_id, 'begin', 'session', username, self.session_begin_date, self.session_end_date)

        periods_to_scrape = self._calculate_scrape_periods(username, session_begin_date, session_end_date)
        for period_begin_date, period_end_date in periods_to_scrape:
            self._check_proxy_queue()
            fail_counter = 0
            while fail_counter < self.max_fails:
                proxy = self.proxy_queue.get()
                tweet_scraper = TweetScraper(self.c)
                tweet_scraper.proxy_server = proxy
                logger.info(
                    f'Start scraping tweets | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                try:
                    tweets_df = tweet_scraper.execute_scraping(username, period_begin_date, period_end_date)
                except ValueError as e:
                    fail_counter += 1
                    self._handle_error('ValueError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except ServerDisconnectedError as e:
                    fail_counter += 1
                    self._handle_error('ServerDisconnectedError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except ClientOSError as e:
                    fail_counter += 1
                    self._handle_error('ClientOSError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except TimeoutError as e:
                    fail_counter += 1
                    self._handle_error('TimeoutError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except ClientHttpProxyError as e:
                    fail_counter += 1
                    self._handle_error('ClientHttpProxyError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except ConnectionRefusedError as e:
                    fail_counter += 1
                    self._handle_error('ConnectionRefusedError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except ClientProxyConnectionError as e:
                    fail_counter += 1
                    self._handle_error('ClientProxyConnectionError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except CancelledError as e:
                    fail_counter += 1
                    self._handle_error('CancelledError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except IndexError as e:
                    fail_counter += 1
                    self._handle_error('IndexError', e, username, proxy, fail_counter, period_begin_date, period_end_date)
                except Empty as e:  # Queue emprt
                    logger.error(
                        f'Empty Error | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                    self._populate_proxy_queue()
                except:
                    print('x' * 100)
                    print(sys.exc_info()[0])
                    print(sys.exc_info())
                    print('x' * 100)
                else:
                    logger.info(
                        f'Saving {len(tweets_df)} tweets | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
                    if not tweets_df.empty: save_tweets(tweets_df)
                    log_scraping_tweets(self.session_id, 'ok', 'period', username, period_begin_date, end_date=period_end_date, n_tweets=len(tweets_df))
                    update_proxy_stats('ok', proxy)
                    self._release_proxy_server(proxy)
                    break  # the wile-loop
                finally:
                    # self._release_proxy_server(proxy)
                    if fail_counter >= self.max_fails:
                        txt = f'Dead | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}'
                        logger.error(txt)
                        log_scraping_tweets(self.session_id, 'dead', 'period', username, period_begin_date, period_end_date, n_tweets=-1)

        # All periods scraped.
        log_scraping_tweets(self.session_id, 'end', 'session', username, self.session_begin_date, self.session_end_date)

    def _handle_error(self, flag, e, username, proxy, fail_counter, period_begin_date=None, period_end_date=None):
        txt = f'{flag} | {username}, {period_begin_date}/{period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}'
        logger.warning(txt)
        logger.warning(e)
        update_proxy_stats(flag, proxy)
        # time.sleep(10)

    def _release_proxy_server(self, proxy):
        logger.info(f'Put back proxy {proxy["ip"]}:{proxy["port"]}')
        self.proxy_queue.put({'ip': proxy['ip'], 'port': proxy['port']})

    def _check_proxy_queue(self):
        if self.proxy_queue.qsize() <= 1:
            logger.debug(f' | proxies queue length: {self.proxy_queue.qsize()}')
            self._populate_proxy_queue()

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _calculate_scrape_periods(self, username, session_begin_date, session_end_date):

        def _get_periods_without_min_tweets(username, session_begin_date, session_end_date):
            """
            Gets all the dates when 'username' has 'min_tweets' nr of tweets stored in the database.
            Returns a list of tuples with session_begin_date and session_end_date date of the period when the 'username' has less or equal amounts of tweets as 'min_tweets' stored in the database.
            We split the periods that are longer than TIME_DELTA.
            """
            # A bit hacky !!! but it works
            # Todo: Refactor: use df.query() here?
            days_with_tweets = get_nr_tweets_per_day(username, session_begin_date, session_end_date)  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
            if not days_with_tweets.empty:  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
                days_with_tweets = days_with_tweets[days_with_tweets['nr_tweets'] >= self.min_tweets]
                days_with_tweets = [d.to_pydatetime() for d in days_with_tweets['date'] if session_begin_date < d.to_pydatetime() < session_end_date]
                # add session_begin_date at the beginning en session_end_date + 1 day at the session_end_date
                days_with_tweets.insert(0, session_begin_date - timedelta(days=1))  # session_begin_date - 1 day because we'log_level add a day when creating the dateranges
                days_with_tweets.append((session_end_date + timedelta(days=1)))

                # Create the periods without min_tweets amount of saved tweets
                missing_tweets_periods = [(b + timedelta(days=1), e - timedelta(days=1))
                                          for b, e in zip(days_with_tweets[:-1], days_with_tweets[1:])  # construct the periods
                                          if e - b > timedelta(days=1)]
                return missing_tweets_periods
            else:
                return [(session_begin_date, session_end_date)]

        def _split_periods(periods):
            # Split the periods into parts with a maximal length of 'TIME_DELTA' days
            td = self.timedelta
            splitted_periods = []
            for b, e in periods:
                if e - b <= timedelta(days=td):
                    splitted_periods.append((b, e))
                else:
                    while e - b >= timedelta(days=self.timedelta):
                        splitted_periods.append((b, b + timedelta(days=td - 1)))
                        b = b + timedelta(days=td)
                        # The last part of the splitting
                        if e - b < timedelta(td):
                            splitted_periods.append((b, e))
            return splitted_periods

        # no need to start before join_date
        join_date = get_join_date(username)
        session_begin_date, session_end_date = max(session_begin_date, join_date.date()), min(session_end_date, datetime.today().date())

        # no need to start same dates again
        if self.missing_dates:
            scrape_periods = _get_periods_without_min_tweets(username, session_begin_date=session_begin_date, session_end_date=session_end_date)
        else:
            scrape_periods = [(session_begin_date, session_end_date)]
        scrape_periods = _split_periods(scrape_periods)
        return scrape_periods

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _populate_proxy_queue(self):

        update_proxies_ratio()
        proxy_df = get_proxies()
        columns = ['datetime', 'ip', 'port', 'source', 'delay', 'blacklisted', 'scrape_n_failed', 'scrape_n_used',
                   'scrape_n_used_total', 'scrape_n_failed_total', 'last_flag', 'fail_ratio']
        # Sort by ratio
        proxy_df.sort_values('fail_ratio', inplace=True)
        print(proxy_df[columns])
        for _, proxy in proxy_df.iterrows():
            self.proxy_queue.put({'ip': proxy['ip'], 'port': proxy['port']})
        logger.warning(f'Proxy queue poulated. Contains {self.proxy_queue.qsize()} servers')


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    pass
