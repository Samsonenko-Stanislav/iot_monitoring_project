
from flask import Flask, request, jsonify
import psycopg2
import os

app = Flask(__name__)

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data_ext (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            timestamp TIMESTAMP,
            sensor TEXT,
            parameter TEXT,
            value DOUBLE PRECISION,
            unit TEXT
        );
    """)

@app.route('/api/v1/data', methods=['POST'])
def receive_bulk_data():
    payload = request.json

    telegram_id = payload.get("telegram_id")
    measurements = payload.get("data")

    if not telegram_id or not isinstance(measurements, list):
        return jsonify({"error": "Missing telegram_id or invalid data"}), 400

    records = []
    for record in measurements:
        timestamp = record.get("timestamp")
        for sensor, content in record.items():
            if sensor == "timestamp":
                continue
            records.extend(flatten_data(telegram_id, timestamp, sensor, content))

    with conn.cursor() as cur:
        for row in records:
            cur.execute(
                """
                INSERT INTO sensor_data_ext (telegram_id, timestamp, sensor, parameter, value, unit)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, row
            )

    return jsonify({"inserted": len(records)}), 201

def flatten_data(telegram_id, timestamp, sensor, nested, path=""):
    result = []
    for key, val in nested.items():
        full_path = f"{path}.{key}" if path else key
        if isinstance(val, dict) and "value" in val and "unit" in val:
            result.append((telegram_id, timestamp, sensor, full_path, val["value"], val["unit"]))
        elif isinstance(val, dict):
            result.extend(flatten_data(telegram_id, timestamp, sensor, val, full_path))
    return result


@app.route('/')
def index():
    return 'Sensor API running!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
