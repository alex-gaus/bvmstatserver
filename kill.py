import pandas as pd
import MySQLdb
import logging
import mysql;
import mysql.connector;


logging.basicConfig(level=logging.INFO)

def killer():
    try:
        conn = mysql.connector.connectconn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    except:
        logging.error("Keine Verbindung zum Server")
        exit(0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT Id, Command, Time from information_schema.processlist", conn)
    conn.close()
    x = 0
    while x < len(df):
        kill_id = str(df["Id"][x])
        command = df["Command"][x]
        time = int(df["Time"][x])
        if time > 20 and command == "Sleep":
            try:
                try:
                    connection = mysql.connector.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
                except:
                    logging.error("Keine Verbindung zum Server")
                    exit(0)
                cursor = connection.cursor()
                cursor.execute("Kill %s"%(kill_id))
                logging.info("Process %s killed!"%(kill_id))
                cursor.close()
            except:
                logging.info("Couldn't kill process %s"%(kill_id))
        x = x+1
        connection.close()


