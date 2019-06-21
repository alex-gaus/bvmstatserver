# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
from flask import Flask
from flask import make_response
from update_reports import update
import pandas as pd
import sqlite3
import csv
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

# underage
# Shows how many pushbacks involed minors
@app.route('/underage')
def underage():
    filename=update()
    conn = sqlite3.connect("%s.db"%(filename))
    df = pd.read_sql_query("""
        SELECT 
        a.underage_involved,
        b.year_2017,c.year_2018, a.year_2019
        FROM 
        (SELECT 
        reports.underage_involved,
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
        Group by underage_involved, SUBSTR(reports.date,0,5)) as a,

        (SELECT 
        reports.underage_involved,
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
        Group by underage_involved, SUBSTR(reports.date,0,5)) as b,
        (SELECT 
        reports.underage_involved,
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
        Group by underage_involved, SUBSTR(reports.date,0,5)) as c
        WHERE 
        a.underage_involved =b.underage_involved and a.underage_involved = c.underage_involved
        Group by a.underage_involved
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
    
    csv_data = df.to_csv(quoting=csv.QUOTE_NONNUMERIC)
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=underage.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    os.remove("%s.db"%(filename))
    return(output)

# women
# Shows how many pushbacks involed women
@app.route('/women')
def women():
    filename=update()
    conn = sqlite3.connect("%s.db"%(filename))
    df = pd.read_sql_query("""
        SELECT 
        CASE a.women_involved
        WHEN "yes" THEN "yes"
        WHEN "no" THEN "no"
        ELSE "unknown"
        END as women_involved_c,
        b.year_2017,c.year_2018, a.year_2019
        FROM 
        (SELECT 
        reports.women_involved,
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
        Group by women_involved, SUBSTR(reports.date,0,5)) as a,
        (SELECT 
        reports.women_involved,
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
        Group by women_involved, SUBSTR(reports.date,0,5)) as b,
        (SELECT 
        reports.women_involved,
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
        Group by women_involved, SUBSTR(reports.date,0,5)) as c
        WHERE 
        a.women_involved =b.women_involved and a.women_involved = c.women_involved
        Group by women_involved_c
   """,conn)
    csv_data = df.to_csv(quoting=csv.QUOTE_NONNUMERIC)
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=women.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    os.remove("%s.db"%(filename))
    return(output)

# asylum
# Shows for how many pushbacks the request to ask for asylum was denied
@app.route('/asylum')
def asylum():
    filename=update()
    conn = sqlite3.connect("%s.db"%(filename))
    df = pd.read_sql_query("""
        SELECT 
        a.intention_asylum_expressed,
        b.year_2017,c.year_2018, a.year_2019
        FROM 
        (SELECT 
        reports.intention_asylum_expressed,
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
        Group by intention_asylum_expressed, SUBSTR(reports.date,0,5)) as a,

        (SELECT 
        reports.intention_asylum_expressed,
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
        Group by intention_asylum_expressed, SUBSTR(reports.date,0,5)) as b,
        (SELECT 
        reports.intention_asylum_expressed,
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
        Group by intention_asylum_expressed, SUBSTR(reports.date,0,5)) as c
        WHERE 
        a.intention_asylum_expressed =b.intention_asylum_expressed and a.intention_asylum_expressed = c.intention_asylum_expressed
        Group by a.intention_asylum_expressed
   """,conn)
    csv_data = df.to_csv(quoting=csv.QUOTE_NONNUMERIC)
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=asylun.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    os.remove("%s.db"%(filename))
    return(output)

if __name__ == '__main__':
    hello_world()