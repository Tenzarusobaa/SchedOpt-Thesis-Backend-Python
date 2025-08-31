import pandas as pd
from flask import Blueprint, request, jsonify
import os
from dotenv import load_dotenv
# Load .env values
load_dotenv()

upload_bp = Blueprint('upload_bp', __name__)
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# --- Upload & Preview Excel for Forecasted ---
@upload_bp.route('/upload_forecasted', methods=['POST'])
def upload_forecasted():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded in request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    try:
        df = pd.read_excel(file_path)
        required_columns = ["PROGRAM", "DEPARTMENT", "YEAR", "ENROLLED COUNT"]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            return jsonify({'error': f"Excel missing required columns: {', '.join(missing)}"}), 400
        return jsonify({'output': df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': f"Failed to process Excel file: {str(e)}"}), 500


# --- Upload & Preview Excel for Programs ---
@upload_bp.route('/upload_programs', methods=['POST'])
def upload_programs():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded in request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    try:
        df = pd.read_excel(file_path)
        required_columns = ["PROGRAM ABBREVIATION", "PROGRAM NAME", "DEPARTMENT", "PRIORITY INDEX"]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            return jsonify({'error': f"Excel missing required columns: {', '.join(missing)}"}), 400
        return jsonify({'output': df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': f"Failed to process Excel file: {str(e)}"}), 500


# --- Upload & Preview Excel for Prospectus ---
@upload_bp.route('/upload_prospectus', methods=['POST'])
def upload_prospectus():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded in request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    try:
        df = pd.read_excel(file_path)
        required_columns = [
            "PROGRAM ABBREVIATION", "DEPARTMENT", "YEAR", "COURSE CODE",
            "COURSE TITLE", "UNITS", "SEMESTER", "TYPE"
        ]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            return jsonify({'error': f"Excel missing required columns: {', '.join(missing)}"}), 400
        return jsonify({'output': df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': f"Failed to process Excel file: {str(e)}"}), 500


# --- Upload & Preview Excel for Rooms ---
@upload_bp.route('/upload_rooms', methods=['POST'])
def upload_rooms():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded in request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    try:
        df = pd.read_excel(file_path)
        required_columns = [
            "ROOM CODE", "BUILDING", "CAPACITY", "SIZE", "TYPE",
            "FUNCTION", "DEPARTMENT OWNER", "PROGRAM OWNER"
        ]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            return jsonify({'error': f"Excel missing required columns: {', '.join(missing)}"}), 400
        return jsonify({'output': df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': f"Failed to process Excel file: {str(e)}"}), 500


# --- Upload & Preview Excel for Timeslots ---
@upload_bp.route('/upload_timeslots', methods=['POST'])
def upload_timeslots():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded in request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    try:
        df = pd.read_excel(file_path)
        required_columns = ["KEY", "START TIME", "END TIME", "DURATION"]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            return jsonify({'error': f"Excel missing required columns: {', '.join(missing)}"}), 400

        df['START TIME'] = df['START TIME'].apply(lambda x: x.strftime("%H:%M") if pd.notnull(x) else "")
        df['END TIME'] = df['END TIME'].apply(lambda x: x.strftime("%H:%M") if pd.notnull(x) else "")

        return jsonify({'output': df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': f"Failed to process Excel file: {str(e)}"}), 500


# --- Upload & Preview Excel for Days ---
@upload_bp.route('/upload_days', methods=['POST'])
def upload_days():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded in request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    try:
        df = pd.read_excel(file_path)
        required_columns = ["KEY", "DAY ABBREVIATION", "DAY LONG", "DAY TYPE"]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            return jsonify({'error': f"Excel missing required columns: {', '.join(missing)}"}), 400
        return jsonify({'output': df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': f"Failed to process Excel file: {str(e)}"}), 500
