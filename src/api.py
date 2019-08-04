import flask
import sqlite3
from db_init import get_maxspan, create_connection
import json

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Cryptocurrency Analysis based on weekly relative spans</h1><p>We currently have only 1 GET API (/maxspan) that results in a json response of max relative span year and week.</p>"

@app.route('/maxspan', methods=['GET'])
def maxspan():
    results_table = 'maxspan'
    sqlite_db = 'crypto.db'
    conn = create_connection(sqlite_db)
    result = get_maxspan(conn, results_table)

    result_json = json.dumps({"year_week": result[0][0]})

    return result_json
app.run()