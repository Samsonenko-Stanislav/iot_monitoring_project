from flask import Flask, request, jsonify
import psycopg2
import os
import requests

app = Flask(__name__)

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
BOT_TOKEN = os.environ['BOT_TOKEN']

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
conn.autocommit = True

# Создание таблиц
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_thresholds (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            sensor TEXT,
            parameter TEXT,
            lower DOUBLE PRECISION,
            upper DOUBLE PRECISION
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
    alerts = []

    for record in measurements:
        timestamp = record.get("timestamp")
        for sensor, content in record.items():
            if sensor == "timestamp":
                continue
            rows = flatten_data(telegram_id, timestamp, sensor, content)
            records.extend(rows)

            # Проверка на аномалии
            for _, ts, s, param, value, _ in rows:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT lower, upper FROM alert_thresholds
                        WHERE telegram_id = %s AND sensor = %s AND parameter = %s
                    """, (telegram_id, s, param))
                    threshold = cur.fetchone()
                    if threshold:
                        lower, upper = threshold
                        if value < lower or value > upper:
                            alerts.append((telegram_id, s, param, value, lower, upper))

    # Вставка данных
    with conn.cursor() as cur:
        for row in records:
            cur.execute("""
                INSERT INTO sensor_data_ext (telegram_id, timestamp, sensor, parameter, value, unit)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row)

    # Отправка предупреждений
    for tg_id, s, p, val, low, high in alerts:
        msg = f"⚠️ <b>Аномалия!</b>\nДатчик: <b>{s}</b>\nПараметр: <b>{p}</b>\nЗначение: <b>{val}</b> вне диапазона [{low} - {high}]"
        send_telegram_alert(tg_id, msg)

    return jsonify({"inserted": len(records), "alerts": len(alerts)}), 201

def flatten_data(telegram_id, timestamp, sensor, nested, path=""):
    result = []
    for key, val in nested.items():
        full_path = f"{path}.{key}" if path else key
        if isinstance(val, dict) and "value" in val and "unit" in val:
            result.append((telegram_id, timestamp, sensor, full_path, val["value"], val["unit"]))
        elif isinstance(val, dict):
            result.extend(flatten_data(telegram_id, timestamp, sensor, val, full_path))
    return result

def send_telegram_alert(chat_id, message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=data, timeout=3)
    except Exception as e:
        print("Failed to send alert:", e)

@app.route('/')
def index():
    return 'Sensor API running!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
