import dataset
from reports import get_reports
import os
import random
import logging
import time
import datetime
import gc
from cachetools import cached, TTLCache
cache= TTLCache(maxsize=1000, ttl=1000)
logging.basicConfig(level=logging.INFO)

@cached(cache)
def update():
    logging.info("update started")
    filename = "sqlite:///:reports.db"
    # filename  = "mysql+mysqlconnector://gobitodic:subotica@gobitodic.mysql.pythonanywhere-services.com/gobitodic$reports"
    # os.popen('cp reports.db %s.db'%(filename)) 
    db = dataset.connect(filename)
    db.close()
    # db = dataset.connect("sqlite:///%s.db"%(filename))
    reportsdb = db["reports"]
    reportsdb.delete()
    reports = get_reports()
    for report in reports:
        reportsdb.insert(report)
    db.commit()
    del db
    gc.collect()
    return filename