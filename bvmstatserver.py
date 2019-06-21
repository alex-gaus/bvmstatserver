# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
from flask import Flask
from flask import make_response
from update_reports import update
import pandas as pd
import sqlite3
import os
import logging
logging.basicConfig(level=logging.INFO)


# To get server  running:
# $export FLASK_APP=server_flask.py
# $flask run

app = Flask(__name__)
app.config['debug'] = False

@app.route('/')
def hello_world():
    return 'Hello, World!'

# orgas:
# Shows which organization documented how many reports
@app.route('/orgas')
def orgas():
    filename=update()
    conn = sqlite3.connect("%s.db"%(filename))
    df = pd.read_sql_query("SELECT incident_author, COUNT(date) FROM reports GROUP BY incident_author", conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=orgas.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    os.remove("%s.db"%(filename))
    return(output)

# reports
# Shows how many reports were reported by month (+ the group size)
@app.route('/reports')
def reports():
    filename=update()
    conn = sqlite3.connect("%s.db"%(filename))
    df = pd.read_sql_query("SELECT SUBSTR(date,0,8) as date_yyyy_mm, count(date) as counter, sum(group_size) as size FROM reports GROUP BY date_yyyy_mm", conn)
    csv_data = df.to_csv()
    logging.info(csv_data)
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=reports.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    os.remove("%s.db"%(filename))
    logging.info(output)
    return(output)

# reports
# Shows how many pushbacks involed minors
@app.route('/underage')
def underage():
    filename=update()
    conn = sqlite3.connect("%s.db"%(filename))
    df = pd.read_sql_query("""
        SELECT 
        SUBSTR(reports.date,0,5) as date_report,
        reports.underage_involved as underage_involved,
        count(age) as counter,
        CASE SUBSTR(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTR(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTR(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by date_report,reports.underage_involved
    """, conn)
        # SELECT 
        # DISTINCT
        # SUBSTR(reports.date,0,5) as date_report,
        # reports.underage_involved as underage_involved,
        # count(age) as counter,
        # CASE underage_involved
        #     WHEN "yes" THEN count(age)
        #     WHEN "no" THEN  0
        #     WHEN "unknown" THEN 0
        # END as yes,
        # CASE underage_involved
        #     WHEN "yes" THEN 0
        #     WHEN "no" THEN  count(age)
        #     WHEN "unknown" THEN 0
        # END as no,
        # CASE underage_involved
        #     WHEN "yes" THEN 0
        #     WHEN "no" THEN  0
        #     WHEN "unknown" THEN count(age)
        # END as unknown
        # from reports
        # Group by reports.underage_involved, date_report
    
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=underage.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    os.remove("%s.db"%(filename))
    return(output)



if __name__ == '__main__':
    hello_world()