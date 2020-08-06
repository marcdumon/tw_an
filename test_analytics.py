# --------------------------------------------------------------------------------------------------------
# 2020/07/26
# src - test_analytics.py
# md
# --------------------------------------------------------------------------------------------------------
from analytics.twitter_stats import TwitterStats

ts = TwitterStats()
# ts.users_list(['mboudry']).populate.profile_stats.run()
# ts.users_list(['mboudry']).tweet_stats.run()
ts.users_all.tweet_stats.run()

if __name__ == '__main__':
    pass
