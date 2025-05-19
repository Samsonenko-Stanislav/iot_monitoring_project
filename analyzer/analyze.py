
import os
import time
import psycopg2
import pandas as pd
from sklearn.ensemble import IsolationForest
import requests
from flask import Flask, request, jsonify

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
BOT_TOKEN = os.environ.get('BOT_TOKEN')

max_retries = 10
for i in range(max_retries):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        break
    except psycopg2.OperationalError:
        print("⏳ Ожидаем готовности PostgreSQL...")
        time.sleep(3)
else:
    raise RuntimeError("❌ Не удалось подключиться к PostgreSQL")

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

app = Flask(__name__)

@app.route('/api/v1/data/<int:telegram_id>', methods=['POST'])
def receive_bulk_data(telegram_id):
    data = request.json
    if not isinstance(data, list):
        data = [data]
    records = []
    for record in data:
        timestamp = record.get("timestamp")
        for sensor, content in record.items():
            if sensor == "timestamp":
                continue
            records.extend(flatten_data(telegram_id, timestamp, sensor, content))

    with conn.cursor() as cur:
        for row in records:
            cur.execute(
                "INSERT INTO sensor_data_ext (telegram_id, timestamp, sensor, parameter, value, unit) VALUES (%s, %s, %s, %s, %s, %s)",
                row
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

model = IsolationForest(contamination=0.05, random_state=42)
last_id = 0

while True:
    with conn.cursor() as cur:
        cur.execute("SELECT id, telegram_id, timestamp, sensor, parameter, value FROM sensor_data_ext WHERE id > %s ORDER BY id", (last_id,))
        rows = cur.fetchall()
    if not rows:
        time.sleep(5)
        continue
    df = pd.DataFrame(rows, columns=["id", "telegram_id", "timestamp", "sensor", "parameter", "value"])
    last_id = df["id"].max()
    X = df[["value"]].values
    if len(X) > 5:
        preds = model.fit_predict(X)
        for i, pred in enumerate(preds):
            if pred == -1:
                row = df.iloc[i]
                message = f"""🚨 Аномалия!
            Сенсор: {row['sensor']}.{row['parameter']}
            Значение: {row['value']}
            Время: {row['timestamp']}
            """
                requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                              data={"chat_id": row['telegram_id'], "text": message})
    time.sleep(5)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
