from flask import Blueprint, jsonify, request
import mysql.connector
from typing import List, Dict, Any, Union
from datetime import datetime
from collections import OrderedDict
from dotenv import load_dotenv
import os

from pymongo import MongoClient

# ----------------------------- Blueprint Setup -----------------------------
procurementAPI = Blueprint('procurement', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1],  # Using guvnl_dev for procurement routes
}

# ——— MongoDB setup ———
# MONGO_URI might look like "mongodb://username:password@host:port/"
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)

# select your database and collection
db = client["powercasting"]
collection = db["Demand_Output"]


def parse_timestamp(ts_str: str) -> datetime:
    """
    Accept either:
      - ISO format: '2023-04-01 00:00:00'
      - RFC-style:  'Sat, 01 Apr 2023 00:00:00 GMT' (or without the ' GMT')
    """
    ts = ts_str.strip()
    # 1) Try ISO "YYYY-MM-DD HH:MM:SS"
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    # 2) Try the RFC-style with day-of-week + month name
    #    remove a trailing " GMT" if present
    ts_clean = ts.replace(" GMT", "")
    try:
        return datetime.strptime(ts_clean, "%a, %d %b %Y %H:%M:%S")
    except ValueError:
        pass

    # 3) Nothing matched
    raise ValueError(f"time data {ts_str!r} does not match any supported format")


# ----------------------------- Helper Functions -----------------------------

def map_and_calculate(alloc: Dict[str, Any], plant_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Maps a single allocation to plant data and calculates PLF and Net Cost.
    """
    plant_code = alloc['plant_code']
    allocated = alloc['allocated_gen']
    plant = plant_dict.get(plant_code, {})

    rated = plant.get('Rated_Capacity', 0.0)
    paf = plant.get('PAF', 0.0)
    aux = plant.get('Aux_Consumption', 0.0)
    var_cost = plant.get('Variable_Cost', 0.0)

    denom = rated * 1000 * 0.25 * paf * (1 - aux) or 1.0
    plf = allocated / denom
    net_cost = allocated * var_cost

    return {
        'plant_name': plant.get('name', 'Unknown'),
        'plant_code': plant_code,
        'rated_capacity': rated,
        'paf': round(paf, 2),
        'Aux_Consumption': aux,
        'plf': round(plf, 4),
        'Variable_Cost': var_cost,
        'max_power': round(alloc['max_gen'], 3),
        'min_power': round(alloc['min_gen'], 3),
        'generated_energy': round(allocated, 3),
        'net_cost': round(net_cost, 2)
    }


def allocate_generation(
        plants: List[Dict[str, Any]],
        net_demand: float,
        backdown_table: List[Dict[str, float]]
) -> Dict[str, Union[float, List[Any]]]:
    if net_demand <= 0:
        raise ValueError("Net demand must be greater than zero")
    sorted_plants = sorted(plants, key=lambda p: p['Variable_Cost'])
    allocation = []
    total_alloc = 0.0
    for plant in sorted_plants:
        code = plant['Code']
        max_p = plant['Max_Power']
        min_p = plant['Min_Power']
        if max_p <= 0:
            continue
        rem = net_demand - total_alloc
        if rem <= 0:
            break
        if rem <= max_p:
            alloc_val = max(min_p, rem)
            allocation.append(
                {'plant_code': code, 'allocated_gen': alloc_val,
                 'min_gen': min_p, 'max_gen': max_p, 'Type': plant['Type']}
            )
            total_alloc += alloc_val
            break
        allocation.append(
            {'plant_code': code, 'allocated_gen': max_p,
             'min_gen': min_p, 'max_gen': max_p, 'Type': plant['Type']}
        )
        total_alloc += max_p
    excess = total_alloc - net_demand
    if excess > 0:
        for alloc in reversed(allocation):
            reducible = alloc['allocated_gen'] - alloc['min_gen']
            if reducible <= 0:
                continue
            red = min(reducible, excess)
            alloc['allocated_gen'] -= red
            excess -= red
            if excess <= 0:
                break
    plant_dict = {p['Code']: p for p in plants}
    final_list = []
    total_cost = 0.0
    for alloc in allocation:
        base = map_and_calculate(alloc, plant_dict)
        plf_pct = base['plf'] * 100
        SHR = Aux = 0.0
        for row in backdown_table:
            if row['lower'] <= plf_pct <= row['upper']:
                SHR = row['SHR']
                Aux = row['Aux_Consumption']
                break
        var_cost = base['Variable_Cost']
        max_gen = base['max_power']
        gen = base['generated_energy']
        backdown_rate = round(var_cost * ((1 + SHR / 100) / (1 - Aux / 100)), 2)
        backdown_cost = round(backdown_rate * (max_gen - gen), 2)
        base['backdown_rate'] = backdown_rate
        base['backdown_cost'] = backdown_cost
        base['backdown_unit'] = backdown_cost / backdown_rate if backdown_cost else 0.0
        final_list.append(base)
        total_cost += base['net_cost']
    final_list.sort(key=lambda x: x['Variable_Cost'])
    return {"other_plant_data": final_list, "total_cost": total_cost}


def get_must_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    if not net_demand:
        return {"error": "Net demand parameters are required"}
    try:
        # Convert timestamp string to datetime for MongoDB query
        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        # Fetch must-run plant details
        cursor.execute(
            """
            SELECT name,
                   Code,
                   Rated_Capacity,
                   PAF,
                   PLF,
                   Type,
                   Technical_Minimum,
                   Aux_Consumption,
                   Variable_Cost,
                   Max_Power,
                   Min_Power
            FROM plant_details
            WHERE Type = 'Must run'
            ORDER BY Variable_Cost
            """
        )
        plants = cursor.fetchall()
        cursor.close()
        conn.close()

        # Get the MongoDB collection for must-run plant consumption
        mustrun_collection = db["mustrunplantconsumption"]

        gen_all = 0.0
        cost_all = 0.0
        data = []

        for plant in plants:
            code = plant['Code']

            # Query MongoDB for the plant's prediction data
            pred_data = mustrun_collection.find_one({
                "TimeStamp": timestamp_dt,
                "Plant_Name": code
            })

            # Default to 0 if no data found
            pred_value = 0.0
            if pred_data and "Pred" in pred_data:
                pred_value = float(pred_data["Pred"])

            gen_kwh = round(pred_value * 1000 * 0.25, 3)
            gen_all += gen_kwh
            var_cost = float(plant['Variable_Cost'])
            cost_all += round(gen_kwh * var_cost, 2)

            data.append({
                'plant_name': plant['name'],
                'plant_code': code,
                'Rated_Capacity': plant['Rated_Capacity'],
                'PAF': plant['PAF'],
                'PLF': plant['PLF'],
                'Type': plant['Type'],
                'Aux_Consumption': plant['Aux_Consumption'],
                'Variable_Cost': var_cost,
                'generated_energy': gen_kwh,
                'max_power': plant['Max_Power'],
                'min_power': plant['Min_Power'],
                'net_cost': round(gen_kwh * var_cost, 2)
            })

        return {'plant_data': data, 'generated_energy_all': gen_all, 'total_cost': cost_all}
    except Exception as e:
        return {'error': str(e)}


def get_exchange_data(timestamp: str, cap_price: float) -> Union[List[Dict[str, Any]], Dict[str, str]]:
    try:
        # Convert timestamp string to datetime object for MongoDB query
        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        # Query MongoDB collection
        iex_collection = db["IEX_Generation"]
        iex_data = iex_collection.find_one({"TimeStamp": timestamp_dt})

        if not iex_data:
            return []

        # Format the data similar to the MySQL version
        result = {
            "TimeStamp": timestamp,
            "Pred_Price": 0.0,
            "Qty_Pred": 0.0
        }

        # Check if we have price and quantity data
        if "Pred_Price" in iex_data:
            pred_price = float(iex_data["Pred_Price"])
            # Apply price cap
            if pred_price > float(cap_price):
                result["Pred_Price"] = 0.0
            else:
                result["Pred_Price"] = pred_price

        if "Qty_Pred" in iex_data:
            # Convert to kWh (MW * 1000 * 0.25)
            result["Qty_Pred"] = round(float(iex_data["Qty_Pred"]) * 1000 * 0.25, 3)

        return [result]
    except Exception as e:
        return {'error': str(e)}


def get_other_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    if net_demand is None or float(net_demand) <= 0:
        return {'error': 'Net demand must be greater than zero'}
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    col = month_map[dt.month]
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT pd.name, pd.Code, pd.Rated_Capacity, pd.PAF, pd.PLF, pd.Type,
               pd.Technical_Minimum, pd.Aux_Consumption, pd.Variable_Cost,
               pd.Max_Power, pd.Min_Power
        FROM plant_details pd
        JOIN PAF_Details pfd ON pd.Code=pfd.Code
        WHERE pd.Type='Other' AND pfd.`{col}`='Y'
        ORDER BY pd.Variable_Cost ASC
    """)
    plants = cursor.fetchall()
    cursor.execute(
        "SELECT Start_Load, End_Load, SHR, Aux_Consumption FROM Back_Down_Table"
    )
    bd_rows = cursor.fetchall()
    backdown_table = [
        {'lower': r['Start_Load'], 'upper': r['End_Load'], 'SHR': r['SHR'], 'Aux_Consumption': r['Aux_Consumption']} for
        r in bd_rows]
    cursor.close()
    conn.close()
    return allocate_generation(plants, float(net_demand), backdown_table)


@procurementAPI.route('/', methods=['GET'])
def get_demand():
    start_date = request.args.get('start_date')
    price_cap = request.args.get('price_cap', 0)

    if not start_date:
        return jsonify({'error': 'Start date parameter is required'}), 400
    start_date = start_date[:19]

    try:
        # Convert start_date string to datetime for MongoDB query
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

        # Fetch demand data from MongoDB
        demand_collection = db["Demand"]
        demand_data = demand_collection.find_one({"TimeStamp": start_date_dt})

        if not demand_data:
            return jsonify({'error': 'No demand data found for the given date'}), 404

        # Create demand_row similar to MySQL version
        demand_row = {
            "TimeStamp": start_date,
            "Demand(Actual)": demand_data.get("Demand(Actual)", 0),
            "Demand(Pred)": demand_data.get("Demand(Pred)", 0)
        }

        # Fetch banking data (still from MySQL)
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT Banking_Unit FROM Banking_Data WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, start_date)
        )
        bank_row = cursor.fetchone() or {'Banking_Unit': 0}
        banking_unit = bank_row.get('Banking_Unit', 0) or 0
        cursor.close()
        conn.close()

        # convert to kWh
        actual_kwh = round(float(demand_row['Demand(Actual)']) * 1000 * 0.25, 3)
        pred_kwh = round(float(demand_row['Demand(Pred)']) * 1000 * 0.25, 3)

        # select which demand to bank: only use pred_kwh if we actually have a non-zero actual_kwh
        base_kwh = pred_kwh if actual_kwh == 0 else actual_kwh
        banked_kwh = base_kwh - banking_unit

        # must-run
        must = get_must_run(banked_kwh, demand_row['TimeStamp'])
        if 'error' in must:
            return jsonify({'error': must['error']}), 500

        # IEX
        iex_list = get_exchange_data(demand_row['TimeStamp'], price_cap)
        if isinstance(iex_list, dict) and 'error' in iex_list:
            return jsonify({'error': iex_list['error']}), 500
        iex = iex_list[0] if iex_list else {'Pred_Price': 0.0, 'Qty_Pred': 0.0}
        iex_cost = iex['Pred_Price'] * iex['Qty_Pred'] if iex['Pred_Price'] else 0.0
        iex_gen = iex['Qty_Pred'] if iex['Pred_Price'] else 0.0

        # remaining
        net1 = banked_kwh - must['generated_energy_all']
        net2 = net1 - iex_gen
        other = get_other_run(net2, start_date)
        if 'error' in other:
            return jsonify({'error': other['error']}), 500

        rem_plants = other['other_plant_data']
        rem_cost = other['total_cost']
        rem_gen = sum(p['generated_energy'] for p in rem_plants)

        # ─────────────── BANKING‐CHECK FOR BACKDOWN ───────────────
        if banking_unit == 0:
            # zero out each plant's backdown_cost
            for p in rem_plants:
                p['backdown_cost'] = 0.0
            total_backdown = 0.0
        else:
            # sum up precomputed backdown_costs
            total_backdown = sum(p['backdown_cost'] for p in rem_plants)
        # ────────────────────────────────────────────────────────────
        iex_price = iex['Pred_Price'] if iex['Qty_Pred'] > 0 else 0.0
        last_price = max(round(rem_plants[-1]['Variable_Cost'], 2), iex_price) if rem_plants else iex_price
        cost_per_block = round((must['total_cost'] + iex_cost + rem_cost) / banked_kwh, 2) if banked_kwh else 0.0
        backdown_unit = sum([p.get('backdown_unit', 0) for p in rem_plants])
        # min_backdown_cost will be minimum of backdown_cost for plants that have a backdown_unit > 0
        min_backdown_cost = min(
            (p['backdown_rate'] for p in rem_plants if p.get('backdown_unit', 0) > 0),
            default=0.0
        )

        result = OrderedDict({
            'TimeStamp': demand_row['TimeStamp'],
            'Demand(Actual)': actual_kwh,
            'Demand(Pred)': pred_kwh,
            'Banking_Unit': banking_unit,
            'Demand_Banked': banked_kwh,
            'Backdown_Cost_Min': round(min_backdown_cost, 2) if banking_unit > 0 else 0.0,
            'Must_Run': must['plant_data'],
            'Must_Run_Total_Gen': must['generated_energy_all'],
            'Must_Run_Total_Cost': must['total_cost'],
            'IEX_Data': iex,
            'IEX_Gen': round(iex_gen, 3),
            'IEX_Cost': round(iex_cost, 2),
            'Remaining_Plants': rem_plants,
            'Remaining_Plants_Total_Gen': round(rem_gen, 3),
            'Remaining_Plants_Total_Cost': round(rem_cost, 2),
            'Last_Price': round(last_price, 2),
            'Cost_Per_Block': round(cost_per_block, 2),
            'Backdown_Cost': round(total_backdown, 2) if banking_unit > 0 else 0.0,
            'Backdown_Unit': round(backdown_unit, 2) if banking_unit > 0 else 0.0
        })

        return jsonify(result), 200

    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()


@procurementAPI.route('/range', methods=['GET'])
def get_demand_output_range():
    """
    GET /range?start=<ts>&end=<ts>
    Returns all Demand_Output docs between start and end (inclusive),
    along with summary (total/average cost_per_block and last_price).
    Expects timestamps in the same format your POST accepts:
    'Sat, 01 Apr 2023 00:00:00 GMT'
    """
    start = request.args.get('start')
    end = request.args.get('end')
    if not start or not end:
        return jsonify({"error": "Both 'start' and 'end' query parameters are required"}), 400

    # parse them into datetimes
    try:
        start_dt = parse_timestamp(start)
        end_dt = parse_timestamp(end)
    except ValueError as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    # fetch matching docs
    cursor = collection.find(
        {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
        {"_id": 0}
    ).sort("TimeStamp", 1)
    docs = list(cursor)

    # build the rows in the same shape as your old MySQL output
    rows = []
    for doc in docs:
        # convert back to the string form for clients
        ts_str = doc["TimeStamp"].strftime("%a, %d %b %Y %H:%M:%S GMT")
        rows.append({
            "timestamp": ts_str,
            "cost_per_block": doc.get("Cost_Per_Block", 0),
            "last_price": doc.get("Last_Price", 0)
        })

    # compute totals & averages
    total_cost_per_block = sum(r["cost_per_block"] for r in rows)
    average_cost_per_block = (
        total_cost_per_block / len(rows)
        if rows else None
    )

    total_mod = sum(r["last_price"] for r in rows)
    average_mod = (
        total_mod / len(rows)
        if rows else None
    )

    return jsonify({
        "data": rows,
        "summary": {
            "total_cost_per_block": total_cost_per_block,
            "average_cost_per_block": round(average_cost_per_block, 2)
            if average_cost_per_block is not None else None,
            "total_mod": total_mod,
            "average_mod": round(average_mod, 2)
            if average_mod is not None else None
        }
    }), 200
