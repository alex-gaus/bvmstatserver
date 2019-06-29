# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
import json
import requests
from operator import itemgetter  
from credentials import botid
import pandas
import logging
import time
import datetime
from cachetools import cached, TTLCache
cache= TTLCache(maxsize=1000, ttl=1000)
logging.basicConfig(level=logging.INFO)

@cached(cache)
def get_reports():
    logging.info("get_reports started")
    cookies = {
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.borderviolence.eu/violence-reports/',
        'X-Requested-With': 'XMLHttpRequest',
        'DNT': '1',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    }

    params = (
        ('action', 'ri_report_query'),
        ('security', botid),
        ('mapQuery', '1'),
        ('ri-incident-location-geo-radius', '50'),
        ('ri-underage-involved', 'all'),
        ('ri-intention-asylum-expressed', 'all'),
        ('ri-page', '1'),
    )

    response = requests.get('https://www.borderviolence.eu/wp-admin/admin-ajax.php', headers=headers, params=params, cookies=cookies)
    reports = response.json()
    return reports