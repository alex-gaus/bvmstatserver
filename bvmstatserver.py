# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
from flask import Flask
from flask import make_response
from flask_cors import CORS
from flask_caching import Cache
from update_reports import update
import pandas as pd
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
config = {
    "DEBUG": True,         
    "CACHE_TYPE": "simple",
    "CACHE_DEFAULT_TIMEOUT": 3600
}
app = Flask(__name__)
CORS(app)
app.config.from_mapping(config)
cache = Cache(app)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/')
def hello_world():
    return 'Server is running!'

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/csv_export')
def csv_export():
    try:
        logging.info("csv_export() started")
        filename=update()
        conn = sqlite3.connect(filename,timeout=30.0)
        df = pd.read_sql_query("SELECT * FROM reports ORDER BY date", conn)
        csv_data = df.to_csv()
        output = make_response(csv_data)
        now = str(datetime.datetime.now()).replace(" ","-")
        now = now.replace(":","_")
        now = now[:19]
        output.headers["Content-Disposition"] = "attachment; filename=borderviolence_reports_%s.csv"%(now)
        output.headers["Content-type"] = "text/csv"
        conn.close()
    except Exception as e:
        logging.error("csv_export() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s,"%(filename))
        return(output)

# orgas:
# Shows which organization documented how many reports
@cached(cache)
@cache.cached(timeout=3600)
@app.route('/orgas')
def orgas():
    try:
        logging.info("orgas() started")
        filename=update()
        conn = sqlite3.connect(filename,timeout=30.0)
        df = pd.read_sql_query("SELECT incident_author, COUNT(date) FROM reports GROUP BY incident_author", conn)
        csv_data = df.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=orgas.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
    except Exception as e:
        logging.error("orgas() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

# reports
# Shows how many reports were reported by month (+ the group size)
@cached(cache)
@cache.cached(timeout=3600)
@app.route('/reports')
def reports():
    try:
        logging.info("reports() started")
        filename=update()
        conn = sqlite3.connect(filename,timeout=30.0)
        df = pd.read_sql_query("SELECT SUBSTR(date,1,7) as date_yyyy_mm, count(date) as counter, sum(group_size) as size FROM reports GROUP BY date_yyyy_mm", conn)
        csv_data = df.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=reports.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
    except Exception as e:
        logging.error("reports() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

# underage
# Shows how many pushbacks involed minors
@cached(cache)
@cache.cached(timeout=3600)
@app.route('/underage')
def underage():
    try:
        logging.info("underage() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
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
        # tempdb.drop_column('id')    
        db.commit()
        conn.close()
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT * from underage",conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=underage.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
    except Exception as e:
        logging.error("underage() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)
    

# women
# Shows how many pushbacks involed women
@cached(cache)
@cache.cached(timeout=3600)
@app.route('/women')
def women():
    try:
        logging.info("women() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        df = pd.read_sql_query("SELECT women_involved, SUBSTR(date,1,4) as year, count(id) as counter from reports GROUP BY year, women_involved ORDER BY women_involved",conn)
        print(df)
        if df["women_involved"][0] == "":
            women_involved = "unknown"
        else:
            women_involved = df["women_involved"][0]
        templist = []
        templist.append({"women_involved":women_involved,df["year"][0]:str(df["counter"][0])})
        x = 1
        y = 0
        while x < len(df):
            if df["women_involved"][x] == "":
                women_involved2 = "unknown"
            else:
                women_involved2 = df["women_involved"][x]
            if women_involved == women_involved2:
                templist[y][df["year"][x]] =str(df["counter"][x])
            else :
                women_involved = women_involved2
                templist.append({"women_involved":women_involved,df["year"][x]:str(df["counter"][x])})
                y = y+1
            print(templist)
            x= x+1
        tempdb = db["women"]
        tempdb.delete()
        db.begin()
        for row in templist:
            tempdb.insert(row)
        # tempdb.drop_column('id')    
        db.commit()
        conn.close()
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT * from women GROUP BY women_involved",conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=women.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        # os.remove("%s.db"%(filename),timeout=30.0)
        db.engine.dispose()
    except Exception as e:
        logging.error("women() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)
    
# asylum
# Shows for how many pushbacks the request to ask for asylum was denied
@cached(cache)
@cache.cached(timeout=3600)
@app.route('/asylum')
def asylum():
    try:
        logging.info("asylum() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
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
            if df["intention_asylum_expressed"][x] == "":
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
        db.commit()
        conn.close()
        conn = sqlite3.connect(filename,timeout=30.0) 
        df2 = pd.read_sql_query("SELECT * from intention_asylum_expressed group by intention_asylum_expressed ",conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=intention_asylum_expressed.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
    except Exception as e:
        logging.error("asylum() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/pushback_from_counter')
def pushback_from_counter():
    try:
        logging.info("pushback_from_counter() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["pushback_from_counter"]
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT pushback_from, count(report_link) FROM pushback_from_counter GROUP BY pushback_from ORDER BY count(report_link) DESC", conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=pushback_from_counter.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
    except Exception as e:
        logging.error("pushback_from_counter() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/pushback_to_counter')
def pushback_to_counter():
    try:
        logging.info("pushback_to_counter() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["pushback_to_counter"]
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT pushback_to, count(report_link) FROM pushback_to_counter GROUP BY pushback_to ORDER BY count(report_link) DESC", conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=pushback_to_counter.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
    except Exception as e:
        logging.error("pushback_to_counter() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/pushback_from_date')
def pushback_from_date():
    try:
        logging.info("pushback_from() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["pushback_from_date"]
        df = pd.read_sql_query("SELECT report_link, SUBSTR(date,1,7) as date_yyyy_mm, pushback_from FROM reports",conn) 
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT date_yyyy_mm, pushback_from, count(report_link) FROM pushback_from_date GROUP BY date_yyyy_mm, pushback_from ORDER BY date_yyyy_mm", conn)
        df3 = df2.set_index('date_yyyy_mm').transpose()
        csv_data = df3.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=pushback_from_date.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
    except Exception as e:
        logging.error("pushback_from_date() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/pushback_to_date')
def pushback_to_date():
    try:
        logging.info("pushback_to() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["pushback_to_date"]
        df = pd.read_sql_query("SELECT report_link, SUBSTR(date,1,7) as date_yyyy_mm, pushback_to FROM reports",conn) 
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT date_yyyy_mm, pushback_to, count(report_link) FROM pushback_to_date GROUP BY date_yyyy_mm, pushback_to ORDER BY date_yyyy_mm", conn)
        df3 = df2.set_index('date_yyyy_mm').transpose()
        csv_data = df3.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=pushback_to_date.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
    except Exception as e:
        logging.error("pushback_to_date() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/chainpushback')
def chainpushback():
    try:
        logging.info("chainpushback() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["chainpushback"]
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT count(report_link), chain_pushback FROM chainpushback GROUP BY chain_pushback ORDER BY count(report_link) DESC", conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=chainpushback.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
        del db
    except Exception as e:
        logging.error("chainpushback() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/violence')
def violence():
    try:
        logging.info("violence() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["violence"]
        df = pd.read_sql_query("SELECT report_link, types_of_violence_used FROM reports",conn) 
        length = float(len(df)*0.01)
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT types_of_violence_used, (CAST (count(report_link) AS DECIMAL(5,1))/%f ) FROM violence GROUP BY types_of_violence_used ORDER BY count(report_link) DESC LIMIT 15"%length, conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=violence.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
        del db
    except Exception as e:
        logging.error("violence() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

@cached(cache)
@cache.cached(timeout=3600)
@app.route('/countries_of_origin')
def countries_of_origin():
    try:
        logging.info("countries_of_origin() started")
        filename=update()
        db = dataset.connect("sqlite:///%s"%(filename))
        conn = sqlite3.connect(filename,timeout=30.0)
        tempdb = db["countries_of_origin"]
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
        conn = sqlite3.connect(filename,timeout=30.0)
        df2 = pd.read_sql_query("SELECT countries_of_origin, count(report_link) FROM countries_of_origin GROUP BY countries_of_origin ORDER BY count(report_link) DESC", conn)
        csv_data = df2.to_csv()
        output = make_response(csv_data)
        output.headers["Content-Disposition"] = "attachment; filename=countries_of_origin.csv"
        output.headers["Content-type"] = "text/csv"
        conn.close()
        db.engine.dispose()
        del db
    except Exception as e:
        logging.error("countries_of_origin() didnt' work; Error: %s"%(e))
    finally:
        os.remove(filename)
        logging.info("removed %s"%(filename))
        return(output)

    # To Do:
    # Adapt country-coudes
    # Types of violence
    # Country of origin





if __name__ == '__main__': #für Debug-Mode
    app.run(debug=True)