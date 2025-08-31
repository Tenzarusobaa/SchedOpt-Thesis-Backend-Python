import mysql.connector
from flask import Blueprint, request, jsonify
import os
from dotenv import load_dotenv

# Load .env values
load_dotenv()

save_bp = Blueprint('save_bp', __name__)

def get_connection():
    return mysql.connector.connect(
        user=os.getenv("MYSQLUSER", "root"),
        password=os.getenv("MYSQLPASSWORD", ""),
        database=os.getenv("MYSQLDATABASE", "schedopt_db"),
        host=os.getenv("MYSQLHOST", "localhost"),
        port=int(os.getenv("MYSQLPORT", 3306))
    )

# --- Save Forecasted ---
@save_bp.route('/save_forecasted', methods=['POST'])
def save_forecasted():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tbl_forecasted_enrolled")
        for row in data:
            query = """
                INSERT INTO tbl_forecasted_enrolled 
                (fe_program_abbr, fe_department, fe_year_level, fe_enrolled_count) 
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (row['PROGRAM'], row['DEPARTMENT'], int(row['YEAR']), int(row['ENROLLED COUNT'])))
        conn.commit()
        cursor.close(); conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Save Programs ---
@save_bp.route('/save_programs', methods=['POST'])
def save_programs():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tbl_program_department")
        for row in data:
            query = """
                INSERT INTO tbl_program_department
                (pd_program_abbr, pd_program_name, pd_department, pd_priority_index)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (row['PROGRAM ABBREVIATION'], row['PROGRAM NAME'], row['DEPARTMENT'], int(row['PRIORITY INDEX'])))
        conn.commit()
        cursor.close(); conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Save Prospectus ---
@save_bp.route('/save_prospectus', methods=['POST'])
def save_prospectus():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tbl_prospectus_list")
        for row in data:
            query = """
                INSERT INTO tbl_prospectus_list
                (pl_program, pl_department, pl_year, pl_course_code, pl_course_title, pl_units, pl_semester, pl_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (row['PROGRAM ABBREVIATION'], row['DEPARTMENT'], int(row['YEAR']),
                                   row['COURSE CODE'], row['COURSE TITLE'], int(row['UNITS']),
                                   int(row['SEMESTER']), row['TYPE']))
        conn.commit()
        cursor.close(); conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Save Rooms ---
@save_bp.route('/save_rooms', methods=['POST'])
def save_rooms():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tbl_room_data")
        for row in data:
            query = """
                INSERT INTO tbl_room_data
                (rd_room_code, rd_building, rd_capacity, rd_size, rd_type,
                 rd_function, rd_department_owner, rd_program_owner)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (row['ROOM CODE'], row['BUILDING'], int(row['CAPACITY']), row['SIZE'],
                                   row['TYPE'], row['FUNCTION'], row['DEPARTMENT OWNER'], row['PROGRAM OWNER']))
        conn.commit()
        cursor.close(); conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Save Timeslots ---
@save_bp.route('/save_timeslots', methods=['POST'])
def save_timeslots():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tbl_time_slot")
        for row in data:
            query = """
                INSERT INTO tbl_time_slot
                (ts_key, ts_start_time, ts_end_time, ts_duration)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (int(row['KEY']), row['START TIME'], row['END TIME'], int(row['DURATION'])))
        conn.commit()
        cursor.close(); conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Save Days ---
@save_bp.route('/save_days', methods=['POST'])
def save_days():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tbl_day_slot")
        for row in data:
            query = """
                INSERT INTO tbl_day_slot
                (day_key, day_abbr, day_long, day_type)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (int(row['KEY']), row['DAY ABBREVIATION'], row['DAY LONG'], row['DAY TYPE']))
        conn.commit()
        cursor.close(); conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
