import requests
import json

TELEGRAM_ID = 902075408  # ← замените на свой
SERVER_URL = "http://45.12.134.3:5000/api/v1/data"

with open("output2.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

payload = {
    "telegram_id": TELEGRAM_ID,
    "data": raw_data
}

response = requests.post(SERVER_URL, json=payload)
print("Status:", response.status_code)
print("Response:", response.json())
