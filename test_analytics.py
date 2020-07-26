# --------------------------------------------------------------------------------------------------------
# 2020/07/26
# src - test_analytics.py
# md
# --------------------------------------------------------------------------------------------------------
from analytics.data_factory import DataFactory

df = DataFactory()
x = df.users_list(['franckentheo']).stats.run()
# x = df.users_all.stats.run()

if __name__ == '__main__':
    pass
