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

@app.route('/count_orgas.csv')
def count_orgas():
    update()
    conn = sqlite3.connect("reports.db")
    df = pd.read_sql_query("SELECT incident_author, COUNT(date) FROM reports GROUP BY incident_author", conn)
    print(df)
    csv_data = df.to_csv()
    output = make_response(csv_data)
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return(output)

    # df = pd.read_sql_query("select * from yyyy_mm_overview;", conn)
    # df.to_csv("yyyy_mm_overview.csv", index=True, sep=",")



if __name__ == '__main__':
    hello_world()