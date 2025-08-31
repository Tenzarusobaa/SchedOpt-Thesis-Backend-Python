from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import mysql.connector
import subprocess
import os
from flask import send_file
from flask import Flask, jsonify, send_from_directory, request
import traceback

from upload_scripts import upload_bp
from save_scripts import save_bp

app = Flask(__name__)
CORS(app)

@app.route('/download_templates', methods=['GET'])
def download_templates():
    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        template_dir = os.path.join(BASE_DIR, "template")
        return send_from_directory(template_dir, "Import_Templates.zip", as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/')
def home():
    return "Hello Admin! API works"

# --- Table check function (reusable) ---
def check_all_tables():
    conn = mysql.connector.connect(
        user='root',
        password='',
        database='schedopt_db',
        host='localhost'
    )
    cursor = conn.cursor()

    table_map = {
        "Forecasting": "tbl_forecasted_enrolled",
        "Programs": "tbl_program_department",
        "Prospectus": "tbl_prospectus_list",
        "Rooms": "tbl_room_data",
        "Timeslots": "tbl_time_slot",
        "Days": "tbl_day_slot"
    }

    results = {}
    for name, table in table_map.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        results[name] = "success" if count > 0 else "failed"
        if results[name] == "failed":
            break   # stop at first failure

    cursor.close()
    conn.close()
    return results

# --- Check if tables have data ---
@app.route('/check_tables', methods=['GET'])
def check_tables():
    try:
        results = check_all_tables()
        return jsonify(results)
    except Exception as e:
        print("Error checking tables:", str(e))
        return jsonify({'error': str(e)}), 500

# --- Run scheduling scripts ---
@app.route('/run_scheduling', methods=['POST'])
def run_scheduling():
    try:
        data = request.get_json(silent=True) or {}
        semester = data.get("semester", 1)


        results = check_all_tables()
        if "failed" in results.values():
            failed_table = [k for k,v in results.items() if v=="failed"][0]
            return jsonify({
                "status": "failed",
                "message": f"Failed. {failed_table} values not verified. Please check one more time."
            }), 400

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        scripts = [
            os.path.join(BASE_DIR, "section.py"),
            os.path.join(BASE_DIR, "course_section.py"),
            os.path.join(BASE_DIR, "final_assignment.py")
        ]

        for script in scripts:
            result = subprocess.run(
                ["python", script, str(semester)],  # ✅ pass semester as arg
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                return jsonify({
                    "status": "error",
                    "message": f"Script {os.path.basename(script)} failed:\n{result.stderr}"
                }), 500

        return jsonify({
            "status": "success",
            "message": f"All scheduling scripts executed successfully for semester {semester}."
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

        
# Update the export route in app.py
@app.route('/export', methods=['POST'])
def run_export():
    try:
        print("Starting export process...")
        
        # Get the absolute path to the export script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        export_script = os.path.join(BASE_DIR, "export.py")
        
        # Run the export script
        result = subprocess.run(
            ["python", export_script],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("Export completed successfully")
            return jsonify({
                "status": "success",
                "message": "Export completed successfully."
            })
        else:
            print(f"Export script error: {result.stderr}")
            return jsonify({
                "status": "error",
                "message": f"Export script failed: {result.stderr}"
            }), 500
            
    except Exception as e:
        print(f"Error in export endpoint: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Export endpoint failed: {str(e)}"
        }), 500

# Update the download route with better error handling
@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    try:
        print(f"Attempting to download file: {filename}")
        
        # Security check to prevent directory traversal
        if '..' in filename or filename.startswith('/'):
            return jsonify({"error": "Invalid filename"}), 400
            
        # Check if file exists
        if not os.path.exists(filename):
            print(f"File not found: {filename}")
            return jsonify({"error": f"File not found: {filename}"}), 404
            
        print(f"Sending file: {filename}")
        return send_file(filename, as_attachment=True)
    except Exception as e:
        print(f"Error downloading file {filename}: {str(e)}")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500
    




# Register routers
app.register_blueprint(upload_bp)
app.register_blueprint(save_bp)

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
