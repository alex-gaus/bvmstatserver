import dataset
from reports import get_reports
import os
import random
import logging
import time
import datetime
from cachetools import cached, TTLCache
cache= TTLCache(maxsize=1000, ttl=100)
logging.basicConfig(level=logging.INFO)

@cached(cache)
def update():
    logging.info("update started")
    # filename = random.randint(100000000,999999999)
    filename = "reports"
    # os.popen('cp reports.db %s.db'%(filename)) 
    db = dataset.connect("sqlite:///%s.db"%(filename))
    reportsdb = db["reports"]
    reports = get_reports()
    for report in reports:
        reportsdb.upsert(report,["report_link"])
    return filename