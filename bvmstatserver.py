# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
from flask import Flask
from flask import make_response
from update_reports import update
import pandas as pd
import MySQLdb
import pandas.io.sql as psql
import sqlite3
import csv
import os
import logging
import dataset
import time
import datetime
from cachetools import cached, TTLCache
cache= TTLCache(maxsize=1000, ttl=1000)
logging.basicConfig(level=logging.INFO)


# To get server  running:
# $export FLASK_APP=bvmstatserver.py
# $flask run

app = Flask(__name__)
app.config['debug'] = False

@cached(cache)
@app.route('/')
def hello_world():
    return 'Server is running!'

@cached(cache)
@app.route('/csv_export')
def csv_export():
    filename=update()
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT * FROM reports ORDER BY date", conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    now = str(datetime.datetime.now()).replace(" ","-")
    now = now.replace(":","_")
    now = now[:19]
    output.headers["Content-Disposition"] = "attachment; filename=borderviolence_reports_%s.csv"%(now)
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return(output)

# orgas:
# Shows which organization documented how many reports
@cached(cache)
@app.route('/orgas')
def orgas():
    filename=update()
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT incident_author, COUNT(date) FROM reports GROUP BY incident_author", conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=orgas.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return(output)

# reports
# Shows how many reports were reported by month (+ the group size)
@cached(cache)
@app.route('/reports')
def reports():
    filename=update()
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT SUBSTRING(date,1,9) as date_yyyy_mm, count(date) as counter, sum(group_size) as size FROM reports GROUP BY date_yyyy_mm", conn)
    csv_data = df.to_csv()
    logging.info(csv_data)
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=reports.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    logging.info(output)
    return(output)

# underage
# Shows how many pushbacks involed minors
@cached(cache)
@app.route('/underage')
def underage():
    filename=update()
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("""
        SELECT 
        a.underage_involved,
        b.year_2017,c.year_2018, a.year_2019
        FROM 
        (SELECT 
        reports.underage_involved,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by underage_involved, SUBSTRING(reports.date,0,5)) as a,

        (SELECT 
        reports.underage_involved,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by underage_involved, SUBSTRING(reports.date,0,5)) as b,
        (SELECT 
        reports.underage_involved,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
            WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by underage_involved, SUBSTRING(reports.date,0,5)) as c
        WHERE 
        a.underage_involved =b.underage_involved and a.underage_involved = c.underage_involved
        Group by a.underage_involved
    """, conn)
        # SELECT 
        # DISTINCT
        # SUBSTRING(reports.date,0,5) as date_report,
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
    # os.remove("%s.db"%(filename),timeout=30.0)
    return(output)

# women
# Shows how many pushbacks involed women
@cached(cache)
@app.route('/women')
def women():
    filename=update()
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
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
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by women_involved, SUBSTRING(reports.date,0,5)) as a,
        (SELECT 
        reports.women_involved,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by women_involved, SUBSTRING(reports.date,0,5)) as b,
        (SELECT 
        reports.women_involved,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by women_involved, SUBSTRING(reports.date,0,5)) as c
        WHERE 
        a.women_involved =b.women_involved and a.women_involved = c.women_involved
        Group by women_involved_c
   """,conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=women.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return(output)

# asylum
# Shows for how many pushbacks the request to ask for asylum was denied
@cached(cache)
@app.route('/asylum')
def asylum():
    filename=update()
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("""
        SELECT 
        a.intention_asylum_expressed,
        b.year_2017,c.year_2018, a.year_2019
        FROM 
        (SELECT 
        reports.intention_asylum_expressed,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by intention_asylum_expressed, SUBSTRING(reports.date,0,5)) as a,
        (SELECT 
        reports.intention_asylum_expressed,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by intention_asylum_expressed, SUBSTRING(reports.date,0,5)) as b,
        (SELECT 
        reports.intention_asylum_expressed,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2017" THEN count(age)
        END as year_2017,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2018" THEN  count(age)
        END as year_2018,
        CASE SUBSTRING(reports.date,0,5)
        WHEN "2019" THEN count(age)
        END as year_2019
        from reports
        Group by intention_asylum_expressed, SUBSTRING(reports.date,0,5)) as c
        WHERE 
        a.intention_asylum_expressed =b.intention_asylum_expressed and a.intention_asylum_expressed = c.intention_asylum_expressed
        Group by a.intention_asylum_expressed
   """,conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=asylum.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return(output)

@cached(cache)
@app.route('/pushback_from_counter')
def pushback_from_counter():
    filename=update()
    db = dataset.connect("sqlite:///%s.db"%(filename))
    tempdb = db["pushback_from_counter"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, pushback_from FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        pf = df["pushback_from"][x].split(" | ")
        for country in pf:
            if country == "":
                country = "Unknown"
            tempdb.insert(
                {"report_link":report_link, "pushback_from":str(country)}, ["report_link","pushback_from"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT pushback_from, count(report_link) FROM pushback_from_counter GROUP BY pushback_from ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_from_counter.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

@cached(cache)
@app.route('/pushback_to_counter')
def pushback_to_counter():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["pushback_to_counter"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, pushback_to FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        pf = df["pushback_to"][x].split(" | ")
        for country in pf:
            if country == "":
                country = "Unknown"
            tempdb.insert(
                {"report_link":report_link, "pushback_to":str(country)}, ["report_link","pushback_from"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT pushback_to, count(report_link) FROM pushback_to_counter GROUP BY pushback_to ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_to_counter.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

@cached(cache)
@app.route('/pushback_from_date')
def pushback_from_date():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["pushback_from_date"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, SUBSTRING(date,1,9) as date_yyyy_mm, pushback_from FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        date_yyyy_mm = df["date_yyyy_mm"][x]
        pf = df["pushback_from"][x].split(" | ")
        for country in pf:
            if country == "":
                country = "Unknown"
            tempdb.insert(
                {"date_yyyy_mm": date_yyyy_mm, "report_link":str(report_link), "pushback_from":str(country)}, ["report_link","pushback_from"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT date_yyyy_mm, pushback_from, count(report_link) FROM pushback_from_date GROUP BY date_yyyy_mm, pushback_from ORDER BY date_yyyy_mm", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_from_date.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

@cached(cache)
@app.route('/pushback_to_date')
def pushback_to_date():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["pushback_to_date"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, SUBSTRING(date,1,9) as date_yyyy_mm, pushback_to FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        date_yyyy_mm = df["date_yyyy_mm"][x]
        pf = df["pushback_to"][x].split(" | ")
        for country in pf:
            if country == "":
                country = "Unknown"
            tempdb.insert(
                {"date_yyyy_mm": date_yyyy_mm, "report_link":report_link, "pushback_to":str(country)}, ["report_link","pushback_from"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT date_yyyy_mm, pushback_to, count(report_link) FROM pushback_to_date GROUP BY date_yyyy_mm, pushback_to ORDER BY date_yyyy_mm", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_to_date.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

@cached(cache)
@app.route('/chainpushback')
def chainpushback():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["chainpushback"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, pushback_to, pushback_from FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        pf = df["pushback_to"][x].split(" | ")
        pt = df["pushback_from"][x].split(" | ")
        if len(pf)>1 or len(pt)>1:
            chain_pushback = "Yes"
        if len(pf) == 1 or len(pt) == 1:
            chain_pushback = "No"
        tempdb.insert(
            {"report_link":report_link, "chain_pushback" : chain_pushback}, ["report_link"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT count(report_link), chain_pushback FROM chainpushback GROUP BY chain_pushback ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=chainpushback.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

@cached(cache)
@app.route('/violence')
def violence():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["violence"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, types_of_violence_used FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        try: 
            violences = df["types_of_violence_used"][x].split(" | ")
        except AttributeError:
            v = "Unknown"
        for v in violences:
            if v == "":
                v = "Unknown"
            tempdb.insert(
                {"report_link":report_link, "types_of_violence_used":str(v)}, ["report_link","types_of_violence_used"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT types_of_violence_used, count(report_link) FROM violence GROUP BY types_of_violence_used ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=violence.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

@cached(cache)
@app.route('/countries_of_origin')
def countries_of_origin():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["countries_of_origin"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, countries_of_origin FROM reports",conn) 
    x= 0
    tempdb.delete()
    db.begin()
    while x < len(df):
        report_link = df["report_link"][x]
        countries = df["countries_of_origin"][x].split(" | ")
        for country in countries:
            if country == "":
                country = "Unknown"
            tempdb.insert(
                {"report_link":str(report_link), "countries_of_origin":str(country)}, ["report_link","pushback_from"]
                )
        x=x+1
    db.commit()
    df2 = pd.read_sql_query("SELECT countries_of_origin, count(report_link) FROM countries_of_origin GROUP BY countries_of_origin ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=countries_of_origin.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return (output)

    # To Do:
    # Adapt country-coudes
    # Types of violence
    # Country of origin




    
if __name__ == '__main__':
    hello_world()