from flask import Flask, jsonify, request
from flask_cors import CORS
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


@app.route('/dashboard', methods=['GET'])
def get_data_with_sum():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # ── 1️⃣ Raw demand data ──────────────────────────────
        cursor.execute(
            "SELECT * "
            "FROM demand_data "
            "WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        demand_rows = cursor.fetchall()

        # compute sums for demand_data (adjust field names to your schema)
        total_actual = sum(r.get('Actual_Demand', 0) for r in demand_rows)
        total_predicted = sum(r.get('Predicted_Demand', 0) for r in demand_rows)

        # ── 2️⃣ IEX data ────────────────────────────────────
        cursor.execute(
            "SELECT * "
            "FROM price "
            "WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        iex_rows = cursor.fetchall()

        # example: sum some numeric field in iex_data
        total_iex_value = sum(r.get('SomeIexMetric', 0) for r in iex_rows)

        # ── 3️⃣ Procurement data ────────────────────────────
        cursor.execute(
            "SELECT * FROM demand_output WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        procurement_rows = cursor.fetchall()

        # for each row, parse the JSON-string columns
        for row in procurement_rows:
            # iex_data is a JSON-string: e.g. "{\"Qty_Pred\": 0, …}"
            if row.get("iex_data"):
                try:
                    row["iex_data"] = json.loads(row["iex_data"])
                except json.JSONDecodeError:
                    # leave it as string if it really isn't JSON
                    pass

            # must_run is a JSON array in string form
            if row.get("must_run"):
                try:
                    row["must_run"] = json.loads(row["must_run"])
                except json.JSONDecodeError:
                    pass

            # remaining_plants likewise
            if row.get("remaining_plants"):
                try:
                    row["remaining_plants"] = json.loads(row["remaining_plants"])
                except json.JSONDecodeError:
                    pass

        cursor.close()
        conn.close()

        return jsonify({
            "demand": demand_rows,
            "iex": iex_rows,
            "procurement": procurement_rows,
        })

    except mysql.connector.Error as err:
        print(err)  # Add this line
        return jsonify({"error": str(err)}), 500


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=True)  # Run the app on all available IP addresses
