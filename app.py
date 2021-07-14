import sqlite3
import os

from flask import Flask, jsonify
import markdown
import markdown.extensions.fenced_code

from TaxiETL import DB_PATH, TIP_STATS_TABLE_NAME, SPEED_STATS_TABLE_NAME

app = Flask(__name__)
ldir = os.path.dirname(__file__)


def db_connection():
    """
    Returns SQLite-DB connection
    :return: <sqlite3.connection>
    	SQLite-DB connection
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as e:
        print(e)
    return conn


@app.route("/api/tip/<int:year>/<int:quarter>/max", methods=["GET"])
def quarter_tip(year, quarter):
    """
    Returns max tip percentage for the quarter of the year
    """
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("SELECT maxTipPercentage FROM {} where year={} and quarter={}".format
                          (TIP_STATS_TABLE_NAME, year, quarter))

    row = cursor.fetchall()
    if row:
        return jsonify({
            'maxTipPercentage': row[0]["maxTipPercentage"]
        })
    else:
        return "Data not present", 404


@app.route("/api/tips/<int:year>/max", methods=["GET"])
def quarter_tip_year(year):
    """
    Returns max tip percentage for the year for all quarters
    """
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT quarter, maxTipPercentage FROM {} where year={}".format(
            TIP_STATS_TABLE_NAME, year)
    )

    rows = cursor.fetchall()
    if rows:
        return jsonify({
            'maxTipPercentages': [
                dict(quarter=row["quarter"], maxTipPercentage=row["maxTipPercentage"])
                for row in rows
            ]
        })
    else:
        return "Data not present", 404


@app.route("/api/speed/<int:year>/<int:month>/<int:day>/max", methods=["GET"])
def speed(year, month, day):
    """
    Return max speed for the given year, month and day
    """
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT hour, maxSpeed FROM {} where year={} and month={} and day={} order by hour asc".format(
            SPEED_STATS_TABLE_NAME, year, month, day)
    )

    rows = cursor.fetchall()
    if rows:
        return jsonify({
            'tripSpeeds': [
                dict(hour=row["hour"] + 1, maxSpeed=row["maxSpeed"])  # hour + 1 as in requirements it is 1-based
                for row in rows
            ]
        })
    else:
        return "Data not present", 404


@app.route("/")
def index():
    readme_file = open("README.md", "r", encoding="utf-8")
    md_template_string = markdown.markdown(
        readme_file.read()
    )

    return md_template_string


if __name__ == "__main__":
    app.run(debug=True)
