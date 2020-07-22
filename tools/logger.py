# --------------------------------------------------------------------------------------------------------
# 2020/06/12
# Twitter_Scraping - logger.py
# md
# --------------------------------------------------------------------------------------------------------
import logging

import coloredlogs

from config import LOG_LEVEL

log_level = LOG_LEVEL

if log_level == 'Debug':
    level = logging.DEBUG
elif log_level == 'Info':
    level = logging.INFO
elif log_level == 'Warning':
    level = logging.WARNING
elif log_level == 'Error':
    level = logging.ERROR
else:
    level = logging.NOTSET

# create logger
logger = logging.getLogger('scraper')
logger.setLevel(level)
logger.propagate = False

# create console handler and set level to debug
handler = logging.StreamHandler()
handler.setLevel(level)

# create formatter
format = '%(asctime)s: [%(filename)-22s | %(funcName)22s:%(lineno)-3s]: [%(levelname)-7s]: %(message)s'
formatter = coloredlogs.ColoredFormatter(format)

# add formatter to console handler
handler.setFormatter(formatter)

# add console handler to logger
logger.addHandler(handler)
