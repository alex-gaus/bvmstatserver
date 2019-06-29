import dataset
from reports import get_reports
import os
import random
import logging
import time
import datetime
from cachetools import cached, TTLCache
cache= TTLCache(maxsize=1000, ttl=1000)
logging.basicConfig(level=logging.INFO)

@cached(cache)
def update():
    logging.info("update started")
    # filename = "reports"
    # os.popen('cp reports.db %s.db'%(filename)) 
    db = dataset.connect('mysql+mysqlconnector://gobitodic:subotica@gobitodic.mysql.pythonanywhere-services.com/gobitodic$reports')
    # db = dataset.connect("sqlite:///%s.db"%(filename))
    reportsdb = db["reports"]
    reportsdb.delete()
    reports = get_reports()
    db.begin()
    for report in reports:
        reportsdb.upsert(report,["report_link"])
    db.commit()
    return True