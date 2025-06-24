from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient

from Routes.demandRoutes import demandApi
from Routes.iexRoutes import iexApi
from Routes.procurementRoutes import procurementAPI
from Routes.plantRoutes import plantAPI
from Routes.BankingRoutes import bankingAPI
from Routes.availibilityfactorRoutes import availabilityAPI
from Routes.regionRoutes import regionApi
from Routes.feederRoutes import feederApi
from Routes.powerTheftRoutes import powerTheftApi
from Routes.divisionRoutes import divisionApi
from Routes.substationRoutes import substationApi
from Routes.consumerRoutes import consumerApi
from Routes.dtrRoutes import dtrApi
from Routes.BackdownRoutes import backDownApi
import mysql.connector
import json
from dotenv import load_dotenv
import os
from datetime import datetime
# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1]  # Using guvnl_consumers for main app
}
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(procurementAPI, url_prefix='/procurement')  # Registering the Procurement API
app.register_blueprint(plantAPI, url_prefix='/plant')  # Registering the Plant API
app.register_blueprint(demandApi, url_prefix='/demand')  # Registering the Demand API
app.register_blueprint(bankingAPI, url_prefix='/banking')  # Registering the Banking API
app.register_blueprint(iexApi, url_prefix='/iex')  # Registering the IEX API
app.register_blueprint(availabilityAPI, url_prefix='/availability')  # Registering the Plant availability factor API
app.register_blueprint(backDownApi, url_prefix='/backdown')  # Registering the Backdown API
app.register_blueprint(regionApi, url_prefix='/region')  # Registering the Region API
app.register_blueprint(divisionApi, url_prefix='/division')  # Registering the Division API
app.register_blueprint(substationApi, url_prefix='/substation')  # Registering the Substation API
app.register_blueprint(feederApi, url_prefix='/feeder')  # Registering the Feeder API
app.register_blueprint(dtrApi, url_prefix='/dtr')  # Registering the DTR API
app.register_blueprint(consumerApi, url_prefix='/consumer')  # Registering the Consumer API
app.register_blueprint(powerTheftApi, url_prefix='/power-theft')  # Registering the Power Theft API

# Mongo config
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = 'powercasting'  # adjust if different
MONGO_COLL = 'Demand'


@app.route("/dashboard", methods=["GET"])
def get_data_with_sum():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    # parse into Python datetimes (strip trailing Z if present)
    try:
        start = datetime.fromisoformat(start_date.rstrip("Z"))
        end = datetime.fromisoformat(end_date.rstrip("Z"))
    except ValueError:
        return jsonify({"error": "Invalid date format. Use ISO 8601"}), 400

    try:
        # ── 1️⃣ Demand from MongoDB ────────────────────────────
        mongo = MongoClient(MONGO_URI)
        coll = mongo[MONGO_DB][MONGO_COLL]

        cursor = coll.find(
            {"TimeStamp": {"$gte": start, "$lte": end}},
            {"_id": False}
        )
        demand_rows = []
        sum_actual = 0.0
        sum_predicted = 0.0

        for doc in cursor:
            # convert Decimal128 if needed
            actual = doc.get("Demand(Actual)")
            pred = doc.get("Demand(Pred)")

            if hasattr(actual, "to_decimal"):
                actual = float(actual.to_decimal())
            if hasattr(pred, "to_decimal"):
                pred = float(pred.to_decimal())

            # accumulate
            sum_actual += actual or 0
            sum_predicted += pred or 0

            # isoformat timestamp
            ts = doc.get("TimeStamp")
            if isinstance(ts, datetime):
                doc["TimeStamp"] = ts.isoformat()
            demand_rows.append(doc)

        mongo.close()

        # ── 2️⃣ IEX data from MySQL ─────────────────────────────
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM price WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        iex_rows = cursor.fetchall()
        # example sum (adjust field name)
        sum_iex = sum(r.get("SomeIexMetric", 0) for r in iex_rows)

        # ── 3️⃣ Procurement data from MySQL ────────────────────
        cursor.execute(
            "SELECT * FROM demand_output WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        procurement_rows = cursor.fetchall()
        # parse JSON-string columns
        for row in procurement_rows:
            for fld in ("iex_data", "must_run", "remaining_plants"):
                if row.get(fld):
                    try:
                        row[fld] = json.loads(row[fld])
                    except json.JSONDecodeError:
                        pass

        cursor.close()
        conn.close()

        return jsonify({
            "demand": {
                "rows": demand_rows,
                "sum_actual": sum_actual,
                "sum_predicted": sum_predicted
            },
            "iex": {
                "rows": iex_rows,
                "sum_metric": sum_iex
            },
            "procurement": procurement_rows
        }), 200

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=True)  # Run the app on all available IP addresses
