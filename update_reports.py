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

# @cached(cache)
def update():
    try:
        logging.info("update started")
        filename = "%s.db"%(str(random.randint(1,999999999999999)))
        logging.info("Created new db %s"%(filename))
        # filename = "sqlite:///reports.db"
        # filename  = "mysql+mysqlconnector://gobitodic:subotica@gobitodic.mysql.pythonanywhere-services.com/gobitodic$reports"
        db = dataset.connect("sqlite:///%s"%(filename))
        reportsdb = db["reports"]
        db.begin()
        reports = get_reports()
        for report in reports:
            reportsdb.insert(report)
        db.commit()
        db.engine.dispose()
        logging.info("DB updated")
    except Exception as e:
        logging.error("Update() didnt' work; Error: %s"%(e))
        os.remove(filename)
    return filename