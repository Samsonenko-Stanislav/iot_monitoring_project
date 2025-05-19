import asyncio
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State

# Подключение к БД и боту
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
conn.autocommit = True

# FSM состояния
class ParamSelect(StatesGroup):
    sensor = State()
    parameter = State()
    count = State()

# Главное меню
main_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
    [types.KeyboardButton(text="🔎 Статус")],
    [types.KeyboardButton(text="📊 Последние показания")]
])

@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    await msg.answer("Привет! Выберите действие:", reply_markup=main_kb)

# 🔎 Статус — выбор датчика
@dp.message(F.text == "🔎 Статус")
async def status_sensors(msg: types.Message):
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (user_id,))
        sensors = cur.fetchall()
    if not sensors:
        await msg.answer("Нет данных.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"status:{s[0]}")] for s in sensors
    ])
    await msg.answer("Выберите датчик:", reply_markup=kb)

# 📊 Последние показания — выбор датчика
@dp.message(F.text == "📊 Последние показания")
async def latest_sensor_select(msg: types.Message, state: FSMContext):
    user_id = msg.from_user.id
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (user_id,))
        sensors = cur.fetchall()
    if not sensors:
        await msg.answer("Нет данных.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"param_sensor:{s[0]}")] for s in sensors
    ])
    await msg.answer("Выберите датчик:", reply_markup=kb)

# Статус — после выбора датчика
@dp.callback_query(F.data.startswith("status:"))
async def status_sensor_selected(callback: types.CallbackQuery):
    sensor = callback.data.split(":")[1]
    user_id = callback.from_user.id
    with conn.cursor() as cur:
        cur.execute("""
            SELECT parameter, value, unit, timestamp
            FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
            ORDER BY timestamp DESC
        """, (sensor, user_id))
        rows = cur.fetchall()

    result = {}
    for param, value, unit, ts in rows:
        if param not in result:
            result[param] = (value, unit, ts)

    if not result:
        await callback.message.answer("Нет данных.")
        return

    text = f"📟 <b>{sensor}</b>:\n"
    for param, (val, unit, ts) in result.items():
        text += f"{param} = {val} {unit}\n"
    await callback.message.answer(text)

# Последние показания — выбор параметра
@dp.callback_query(F.data.startswith("param_sensor:"))
async def choose_parameter(callback: types.CallbackQuery, state: FSMContext):
    sensor = callback.data.split(":")[1]
    user_id = callback.from_user.id
    await state.update_data(sensor=sensor)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT parameter FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
        """, (sensor, user_id))
        params = cur.fetchall()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=p[0], callback_data=f"param_select:{p[0]}")] for p in params
    ])
    await callback.message.answer("Выберите параметр:", reply_markup=kb)

@dp.callback_query(F.data.startswith("param_select:"))
async def ask_count(callback: types.CallbackQuery, state: FSMContext):
    param = callback.data.split(":")[1]
    await state.update_data(parameter=param)
    await state.set_state(ParamSelect.count)
    await callback.message.answer(f"Сколько последних значений показать для <b>{param}</b>?")

@dp.message(ParamSelect.count)
async def show_last_values(msg: types.Message, state: FSMContext):
    user_id = msg.from_user.id
    try:
        n = int(msg.text)
        if n <= 0:
            raise ValueError
    except:
        await msg.answer("Введите положительное целое число.")
        return

    data = await state.get_data()
    sensor = data['sensor']
    parameter = data['parameter']

    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, value
            FROM sensor_data_ext
            WHERE telegram_id = %s AND sensor = %s AND parameter = %s
            ORDER BY timestamp DESC LIMIT %s
        """, (user_id, sensor, parameter, n))
        rows = cur.fetchall()

    if not rows:
        await msg.answer("Нет данных.")
        return

    df = pd.DataFrame(rows, columns=["timestamp", "value"]).sort_values("timestamp")
    plt.figure(figsize=(8, 4))
    plt.plot(df["timestamp"], df["value"], marker="o")
    plt.title(f"{sensor}.{parameter}")
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    path = "plot.png"
    plt.savefig(path)
    plt.close()

    photo = FSInputFile(path)
    await msg.answer_photo(photo)
    await state.clear()

# Запуск
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
