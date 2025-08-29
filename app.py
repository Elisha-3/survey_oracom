from flask import Flask, request, jsonify, render_template, send_file
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from sqlalchemy import create_engine, text
import io, os, logging
from xlsxwriter import Workbook
from flask_mail import Mail
from itsdangerous import URLSafeTimedSerializer
from contextlib import contextmanager

# ----------------------
# App & Logging
# ----------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------
# Config
# ----------------------
app.config.update(
    SECRET_KEY=os.getenv("FLASK_SECRET", "changeme"),
    MAIL_SERVER=os.getenv("MAIL_SERVER", "smtp.gmail.com"),
    MAIL_PORT=int(os.getenv("MAIL_PORT", 587)),
    MAIL_USE_TLS=os.getenv("MAIL_USE_TLS", "True").lower() in ("true", "1", "yes"),
    MAIL_USE_SSL=os.getenv("MAIL_USE_SSL", "False").lower() in ("true", "1", "yes"),
    MAIL_USERNAME=os.getenv("MAIL_USERNAME"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD"),
    MAIL_DEFAULT_SENDER=os.getenv("MAIL_DEFAULT_SENDER"),
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    SQLALCHEMY_ENGINE_OPTIONS={
        "pool_pre_ping": True,
        "pool_size": 5,
        "max_overflow": 10,
        "pool_timeout": 30,
        "pool_recycle": 300,
    },
)

mail = Mail(app)

# ----------------------
# Database
# ----------------------
user = os.getenv("MYSQLUSER")
pwd = os.getenv("MYSQLPASSWORD")
host = os.getenv("MYSQLHOST")
port = os.getenv("MYSQLPORT", "3306")
name = os.getenv("MYSQLDATABASE")

if all([user, pwd, host, port, name]):
    db_uri = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"
    app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
    logger.info(f"✅ Using DB: mysql+pymysql://{user}:***@{host}:{port}/{name}")
else:
    db_uri = None
    logger.error("❌ Missing MySQL environment variables")

db = SQLAlchemy(app)

def get_db_engine():
    if not db_uri:
        raise RuntimeError("Database URI not configured")
    return create_engine(db_uri, **app.config["SQLALCHEMY_ENGINE_OPTIONS"])

@contextmanager
def db_connection():
    engine = get_db_engine()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()

# Token Serializer
TOKEN_SERIALIZER = URLSafeTimedSerializer(app.config["SECRET_KEY"])
RESET_TOKEN_SALT = os.getenv("RESET_TOKEN_SALT", "reset-password-salt")

# ----------------------
# Helpers
# ----------------------
def handle_db_error(e, operation):
    logger.exception(f"Error in {operation}")
    return jsonify({"error": f"Error {operation}: {str(e)}"}), 500

# ----------------------
# Routes
# ----------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        df = pd.read_excel(file)
        required_columns = ["Phone_Number", "EFD", "Job Category", "Employment Status", "Sex", "Status", "Q1", "Q2", "Q3"]
        if not all(col in df.columns for col in required_columns):
            return jsonify({"error": "Missing required columns"}), 400

        df = df.rename(columns={
            "Phone_Number": "phone_number",
            "EFD": "efd",
            "Job Category": "job_category",
            "Employment Status": "employment_status",
            "Sex": "sex",
            "Status": "status",
            "Q1": "q1",
            "Q2": "q2",
            "Q3": "q3",
        }).sort_values(by=["phone_number", "efd", "job_category", "sex"])

        with db_connection() as conn:
            conn.execute(text("TRUNCATE TABLE survey_responses"))
            batch_size = 1000
            cols = ["phone_number", "efd", "job_category", "employment_status", "sex", "status", "q1", "q2", "q3"]
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start:start + batch_size][cols]
                batch.to_sql("survey_responses", con=get_db_engine(), if_exists="append", index=False)

        return jsonify({"message": "File uploaded and data saved successfully"}), 200
    except Exception as e:
        return handle_db_error(e, "processing upload")

@app.route("/download", methods=["GET"])
def download_file():
    try:
        with db_connection() as conn:
            df = pd.read_sql("SELECT * FROM survey_responses", con=conn)
        df["is_duplicate"] = df.duplicated(subset=["phone_number", "efd", "job_category", "sex"], keep=False)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="SurveyData")
            workbook, worksheet = writer.book, writer.sheets["SurveyData"]
            highlight = workbook.add_format({"bg_color": "#FFFF00"})
            for idx, row in df.iterrows():
                if row.get("is_duplicate"):
                    worksheet.set_row(idx + 1, cell_format=highlight)

        output.seek(0)
        return send_file(output, download_name="survey_data.xlsx", as_attachment=True)
    except Exception as e:
        return handle_db_error(e, "generating download")

@app.route("/api/data", methods=["GET"])
def get_data():
    try:
        with db_connection() as conn:
            df = pd.read_sql("SELECT * FROM survey_responses", con=conn)

        q1_options = [
            "1. SEAH awareness", "2. Disciplinary action", "5. SemaUsikike",
            "6. SEAH engagements", "7. Risk assessment", "8. MD Communications",
            "9. Visible welfare"
        ]
        q1_counts = {opt: 0 for opt in q1_options}
        for q1 in df["q1"].dropna():
            for opt in q1.split(", "):
                if opt in q1_options:
                    q1_counts[opt] += 1

        total = len(df)
        q1_dist = {
            "Q1": len(df[df["q1"].notna()]) / total if total else 0,
            "Q2": len(df[df["q2"].notna()]) / total if total else 0,
            "Q3": len(df[df["q3"].notna()]) / total if total else 0,
        }

        return jsonify({
            "q1_counts": q1_counts,
            "q1_dist": q1_dist,
            "col2": df["job_category"].fillna("Unknown").tolist(),
            "col3": df["employment_status"].fillna("Unknown").tolist(),
            "col4": df["sex"].fillna("Unknown").tolist(),
            "col5": df["efd"].fillna("Unknown").tolist(),
            "q2": df["q2"].fillna("N/A").tolist(),
            "q3": df["q3"].fillna("N/A").tolist(),
        })
    except Exception as e:
        return handle_db_error(e, "fetching data")

@app.route("/api/data", methods=["POST"])
def add_data():
    try:
        data = request.json
        pd.DataFrame([data]).to_sql("survey_responses", con=get_db_engine(), if_exists="append", index=False)
        return jsonify({"message": "Data added successfully"}), 201
    except Exception as e:
        return handle_db_error(e, "adding data")

@app.route("/api/data/<int:id>", methods=["PUT"])
def update_data(id):
    try:
        data = request.json
        with db_connection() as conn:
            conn.execute(
                text("""UPDATE survey_responses SET phone_number=:phone_number, efd=:efd, job_category=:job_category, 
                        employment_status=:employment_status, sex=:sex, status=:status, q1=:q1, q2=:q2, q3=:q3 
                        WHERE id=:id"""),
                {**data, "id": id},
            )
        return jsonify({"message": "Data updated successfully"}), 200
    except Exception as e:
        return handle_db_error(e, "updating data")

@app.route("/api/data/<int:id>", methods=["DELETE"])
def delete_data(id):
    try:
        with db_connection() as conn:
            conn.execute(text("DELETE FROM survey_responses WHERE id=:id"), {"id": id})
        return jsonify({"message": "Data deleted successfully"}), 200
    except Exception as e:
        return handle_db_error(e, "deleting data")

@app.route("/health")
def health():
    try:
        with db_connection() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return handle_db_error(e, "health check")

# ----------------------
# Entry
# ----------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "false").lower() in ("true", "1", "yes"))
