# - *- coding: utf- 8 - *-
# (c) by Alex (t.me/gobi_todic)
from flask import Flask
from flask import make_response
from update_reports import update
import pandas as pd
import sqlite3


# To get server  running:
# $export FLASK_APP=server_flask.py
# $flask run

app = Flask(__name__)
@app.route('/')
def hello_world():
    return 'Hello, World!'

# orgas:
# Shows which organization documented how many reports
@app.route('/orgas')
def orgas():
    update()
    conn = sqlite3.connect("reports.db")
    df = pd.read_sql_query("SELECT incident_author, COUNT(date) FROM reports GROUP BY incident_author", conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=orgas.csv"
    output.headers["Content-type"] = "text/csv"
    return(output)

# reports
# Shows how many reports were reported by month (+ the group size)
@app.route('/reports')
def reports():
    update()
    conn = sqlite3.connect("reports.db")
    df = pd.read_sql_query("SELECT SUBSTR(date,0,8) as date, count(date) as counter, sum(group_size) as size FROM reports GROUP BY date", conn)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=reports.csv"
    output.headers["Content-type"] = "text/csv"
    return(output)



if __name__ == '__main__':
    hello_world()