# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
from flask import Flask
from flask import make_response
from update_reports import update
import pandas as pd
try:
    import MySQLdb
except:
    print("problem importing MYSQLdb")
import pandas.io.sql as psql
import sqlite3
import csv
import os
import logging
import dataset
import time
import datetime
import gc
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
    df = pd.read_sql_query("SELECT SUBSTRING(date,1,7) as date_yyyy_mm, count(date) as counter, sum(group_size) as size FROM reports GROUP BY date_yyyy_mm", conn)
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
    db = dataset.connect(filename)
    # conn = sqlite3.connect("reports.db",timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT underage_involved, SUBSTR(date,1,4) as year, count(id) as counter from reports GROUP BY year, underage_involved ORDER BY underage_involved",conn)
    underage_involved = df["underage_involved"][0]
    templist = []
    templist.append({"underage_involved":underage_involved,df["year"][0]:str(df["counter"][0])})
    x = 1
    y = 0
    while x < len(df):
        if underage_involved == df["underage_involved"][x]:
            templist[y][df["year"][x]] =str(df["counter"][x])
        else :
            underage_involved = df["underage_involved"][x]
            templist.append({"underage_involved":underage_involved,df["year"][x]:str(df["counter"][x])})
            y = y+1
        x= x+1
    tempdb = db["underage"]
    tempdb.delete()
    db.begin()
    for row in templist:
        tempdb.insert(row)
    tempdb.drop_column('id')    
    db.commit()
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT * from underage",conn)
    csv_data = df2.to_csv()
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
    db = dataset.connect(filename)
    # conn = sqlite3.connect("reports.db",timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT women_involved, SUBSTR(date,1,4) as year, count(id) as counter from reports GROUP BY year, women_involved ORDER BY women_involved",conn)
    if df["women_involved"][0] == "":
        women_involved = "unknown"
    else:
        women_involved = df["women_involved"][0]
    templist = []
    templist.append({"women_involved":women_involved,df["year"][0]:str(df["counter"][0])})
    x = 1
    y = 0
    while x < len(df):
        if df["women_involved"][0] == "":
            women_involved2 = "unknown"
        else:
         women_involved2 = df["women_involved"][x]
        if women_involved == women_involved2:
            templist[y][df["year"][x]] =str(df["counter"][x])
        else :
            women_involved = women_involved2
            templist.append({"women_involved":women_involved,df["year"][x]:str(df["counter"][x])})
            y = y+1
        x= x+1
    tempdb = db["women"]
    tempdb.delete()
    db.begin()
    for row in templist:
        tempdb.insert(row)
    tempdb.drop_column('id')    
    db.commit()
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT * from women",conn)
    csv_data = df2.to_csv()
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
    db = dataset.connect(filename)
    # conn = sqlite3.connect("reports.db",timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT intention_asylum_expressed, SUBSTR(date,1,4) as year, count(id) as counter from reports GROUP BY year, intention_asylum_expressed ORDER BY intention_asylum_expressed",conn)
    if df["intention_asylum_expressed"][0] == "":
        intention_asylum_expressed = "unknown"
    else:
        intention_asylum_expressed = df["intention_asylum_expressed"][0]
    templist = []
    templist.append({"intention_asylum_expressed":intention_asylum_expressed,df["year"][0]:str(df["counter"][0])})
    x = 1
    y = 0
    while x < len(df):
        if df["intention_asylum_expressed"][0] == "":
            intention_asylum_expressed2 = "unknown"
        else:
         intention_asylum_expressed2 = df["intention_asylum_expressed"][x]
        if intention_asylum_expressed == intention_asylum_expressed2:
            templist[y][df["year"][x]] =str(df["counter"][x])
        else :
            intention_asylum_expressed = intention_asylum_expressed2
            templist.append({"intention_asylum_expressed":intention_asylum_expressed,df["year"][x]:str(df["counter"][x])})
            y = y+1
        x= x+1
    tempdb = db["intention_asylum_expressed"]
    tempdb.delete()
    db.begin()
    for row in templist:
        tempdb.insert(row)
    tempdb.drop_column('id')    
    db.commit()
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT * from intention_asylum_expressed",conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=intention_asylum_expressed.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    return(output)

@cached(cache)
@app.route('/pushback_from_counter')
def pushback_from_counter():
    gc.collect()
    filename=update()
    db = dataset.connect(filename)
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
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT pushback_from, count(report_link) FROM pushback_from_counter GROUP BY pushback_from ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_from_counter.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

@cached(cache)
@app.route('/pushback_to_counter')
def pushback_to_counter():
    gc.collect()
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
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT pushback_to, count(report_link) FROM pushback_to_counter GROUP BY pushback_to ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_to_counter.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

@cached(cache)
@app.route('/pushback_from_date')
def pushback_from_date():
    gc.collect()
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["pushback_from_date"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, SUBSTRING(date,1,7) as date_yyyy_mm, pushback_from FROM reports",conn) 
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
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT date_yyyy_mm, pushback_from, count(report_link) FROM pushback_from_date GROUP BY date_yyyy_mm, pushback_from ORDER BY date_yyyy_mm", conn)
    df3 = df2.set_index('date_yyyy_mm').transpose()
    csv_data = df3.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_from_date.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

@cached(cache)
@app.route('/pushback_to_date')
def pushback_to_date():
    gc.collect()
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["pushback_to_date"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn =MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df = pd.read_sql_query("SELECT report_link, SUBSTRING(date,1,7) as date_yyyy_mm, pushback_to FROM reports",conn) 
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
                {"date_yyyy_mm": date_yyyy_mm, "report_link":str(report_link), "pushback_to":str(country)}, ["report_link","pushback_to"]
                )
        x=x+1
    db.commit()
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT date_yyyy_mm, pushback_to, count(report_link) FROM pushback_to_date GROUP BY date_yyyy_mm, pushback_to ORDER BY date_yyyy_mm", conn)
    df3 = df2.set_index('date_yyyy_mm').transpose()
    csv_data = df3.to_csv()
    logging.info(csv_data)
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=pushback_to_date.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

@cached(cache)
@app.route('/chainpushback')
def chainpushback():
    filename=update()
    db = dataset.connect(filename)
    tempdb = db["chainpushback"]
    # conn = sqlite3.connect(filename,timeout=30.0)
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
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
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT count(report_link), chain_pushback FROM chainpushback GROUP BY chain_pushback ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=chainpushback.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

@cached(cache)
@app.route('/violence')
def violence():
    gc.collect()
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
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT types_of_violence_used, count(report_link) FROM violence GROUP BY types_of_violence_used ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=violence.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

@cached(cache)
@app.route('/countries_of_origin')
def countries_of_origin():
    gc.collect()
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
    conn.close()
    conn = MySQLdb.connect(host="gobitodic.mysql.pythonanywhere-services.com", user="gobitodic", passwd="subotica", db="gobitodic$reports")
    df2 = pd.read_sql_query("SELECT countries_of_origin, count(report_link) FROM countries_of_origin GROUP BY countries_of_origin ORDER BY count(report_link) DESC", conn)
    csv_data = df2.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=countries_of_origin.csv"
    output.headers["Content-type"] = "text/csv"
    conn.close()
    # os.remove("%s.db"%(filename),timeout=30.0)
    del db
    return (output)

    # To Do:
    # Adapt country-coudes
    # Types of violence
    # Country of origin




    
if __name__ == '__main__':
    hello_world()