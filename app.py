from flask import Flask, request, jsonify, render_template, send_file
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import io
import os
from xlsxwriter import Workbook
from flask_mail import Mail
from itsdangerous import URLSafeTimedSerializer

app = Flask(__name__, template_folder='templates', static_folder='static')

app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET', '$#$^%%*')

# Database Configuration
def _build_fallback_mysql_uri():
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    pwd  = os.getenv('DB_PASSWORD')
    name = os.getenv('DB_NAME')
    port = os.getenv('DB_PORT')
    return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"

db_uri = (
    os.getenv('SQLALCHEMY_DATABASE_URI') or
    os.getenv('MYSQL_URL') or    
    os.getenv('DATABASE_URL') or   
    _build_fallback_mysql_uri()
)

app.config['SQLALCHEMY_DATABASE_URI'] = db_uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {"pool_pre_ping": True}

db = SQLAlchemy(app)

# ---- Email Configuration ----
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')

mail = Mail(app)

# ---- Token Serializer for password reset ----
TOKEN_SERIALIZER = URLSafeTimedSerializer(app.config['SECRET_KEY'])
RESET_TOKEN_SALT = os.environ.get('RESET_TOKEN_SALT', 'reset-password-salt')
# SQLAlchemy engine with error handling
try:
    engine = create_engine(db_uri)
    with engine.connect() as conn:
        pass
except Exception as e:
    raise Exception(f"Database connection failed: {str(e)}")

# Routes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400

    try:
        # Loading and validating Excel file
        df = pd.read_excel(file)
        required_columns = ['Phone_Number', 'EFD', 'Job Category', 'Employment Status', 'Sex', 'Status', 'Q1', 'Q2', 'Q3']
        if not all(col in df.columns for col in required_columns):
            return jsonify({'error': 'Missing required columns'}), 400

        # Rename columns
        df = df.rename(columns={
            'Phone_Number': 'phone_number',
            'EFD': 'efd',
            'Job Category': 'job_category',
            'Employment Status': 'employment_status',
            'Sex': 'sex',
            'Status': 'status',
            'Q1': 'q1',
            'Q2': 'q2',
            'Q3': 'q3'
        })

        # Search for duplicates and sort
        df = df.sort_values(by=['phone_number', 'efd', 'job_category', 'sex'])

        # Clear old data and insert new data (exclude is_duplicate)
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE survey_responses"))
            batch_size = 1000
            for start in range(0, len(df), batch_size):
                batch = df[start:start + batch_size][['phone_number', 'efd', 'job_category', 'employment_status', 'sex', 'status', 'q1', 'q2', 'q3']]
                batch.to_sql('survey_responses', con=engine, if_exists='append', index=False)

        return jsonify({'message': 'File uploaded and data saved to database successfully'}), 200

    except Exception as e:
        return jsonify({'error': f'Error processing file: {str(e)}'}), 500

@app.route('/download', methods=['GET'])
def download_file():
    try:
        df = pd.read_sql("SELECT * FROM survey_responses", con=engine)
        # Compute is_duplicate on the fly
        df['is_duplicate'] = df.duplicated(subset=['phone_number', 'efd', 'job_category', 'sex'], keep=False)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='SurveyData')
            workbook = writer.book
            worksheet = writer.sheets['SurveyData']
            format1 = workbook.add_format({'bg_color': '#FFFF00'})
            for idx, row in df.iterrows():
                if row['is_duplicate']:
                    worksheet.set_row(idx + 1, cell_format=format1)
        output.seek(0)
        return send_file(output, download_name='survey_data.xlsx', as_attachment=True)
    except Exception as e:
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 500


@app.route('/api/data', methods=['GET'])
def get_data():
    try:
        df = pd.read_sql("SELECT * FROM survey_responses", con=engine)

        # Prepare Q1 counts
        q1_options = [
            "1. SEAH awareness", "2. Disciplinary action", "5. SemaUsikike",
            "6. SEAH engagements", "7. Risk assessment", "8. MD Communications",
            "9. Visible welfare"
        ]
        q1_counts = {option: 0 for option in q1_options}
        for q1 in df['q1'].dropna():
            for option in q1.split(', '):
                if option in q1_options:
                    q1_counts[option] += 1

        # Q1 distribution
        total_respondents = len(df['q1'].dropna().unique())
        q1_dist = {'Q1': 0.6, 'Q2': 0.2, 'Q3': 0.2} if total_respondents > 0 else {'Q1': 0, 'Q2': 0, 'Q3': 0}
        return jsonify({
            "q1_counts": q1_counts,
            "q1_dist": q1_dist,
            "col2": df['job_category'].fillna("Unknown").tolist(),
            "col3": df['employment_status'].fillna("Unknown").tolist(),
            "col4": df['sex'].fillna("Unknown").tolist(),
            "col5": df['efd'].fillna("Unknown").tolist(),
            "q2": df['q2'].fillna("N/A").tolist(),
            "q3": df['q3'].fillna("N/A").tolist()
        })
    except Exception as e:
        return jsonify({'error': f'Error fetching data: {str(e)}'}), 500

# Routes
@app.route('/api/data', methods=['POST'])
def add_data():
    try:
        data = request.json
        df = pd.DataFrame([data])
        df.to_sql('survey_responses', con=engine, if_exists='append', index=False)
        return jsonify({'message': 'Data added successfully'}), 201
    except Exception as e:
        return jsonify({'error': f'Error adding data: {str(e)}'}), 500

@app.route('/api/data/<int:id>', methods=['PUT'])
def update_data(id):
    try:
        data = request.json
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE survey_responses SET phone_number=:phone_number, efd=:efd, job_category=:job_category, "
                     "employment_status=:employment_status, sex=:sex, status=:status, q1=:q1, q2=:q2, q3=:q3 "
                     "WHERE id=:id"),
                {
                    'id': id,
                    'phone_number': data.get('phone_number'),
                    'efd': data.get('efd'),
                    'job_category': data.get('job_category'),
                    'employment_status': data.get('employment_status'),
                    'sex': data.get('sex'),
                    'status': data.get('status'),
                    'q1': data.get('q1'),
                    'q2': data.get('q2'),
                    'q3': data.get('q3')
                }
            )
        return jsonify({'message': 'Data updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': f'Error updating data: {str(e)}'}), 500

@app.route('/api/data/<int:id>', methods=['DELETE'])
def delete_data(id):
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM survey_responses WHERE id=:id"), {'id': id})
        return jsonify({'message': 'Data deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': f'Error deleting data: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)