import pandas as pd
import MySQLdb
import logging
logging.basicConfig(level=logging.INFO)

def killer():
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT Id, Command, Time from information_schema.processlist", conn)
    x = 0
    while x < len(df):
        kill_id = df["Id"][x]
        command = df["Command"][x]
        time = str(df["Time"][x])
        if time > 50 or command == "Sleep":
            try:
                pd.read_sql_query("Kill %s"%(kill_id)
                logging.info("Process %s killed!"%(kill_id))
            except:
                logging.info("Couldn't kill process %s"%(kill_id))
        


