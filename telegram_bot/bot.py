import asyncio
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State

class ParamSelect(StatesGroup):
    telegram_id = State()
    sensor = State()
    parameter = State()
    count = State()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

conn = None

def get_main_kb(is_admin=False):
    keyboard = [
        [types.KeyboardButton(text="🔎 Статус")],
        [types.KeyboardButton(text="📊 Последние показания")]
    ]
    if is_admin:
        keyboard.append([types.KeyboardButton(text="👥 Пользователи")])
        keyboard.append([types.KeyboardButton(text="🔑 Выдать админа"), types.KeyboardButton(text="🔄 Понизить")])
    return types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=keyboard)

@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    global conn
    user_id = msg.from_user.id
    username = msg.from_user.username or ""
    full_name = msg.from_user.full_name or ""

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (user_id,))
        user = cur.fetchone()
        if not user:
            cur.execute("""
                INSERT INTO users (telegram_id, full_name, username, role)
                VALUES (%s, %s, %s, %s)
            """, (user_id, full_name, username, 'operator'))
            role = 'operator'
        else:
            role = user[4]

    await msg.answer("👋 Добро пожаловать!", reply_markup=get_main_kb(is_admin=(role == 'admin')))

async def get_user_role(user_id):
    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE telegram_id = %s", (user_id,))
        row = cur.fetchone()
    return row[0] if row else 'operator'

@dp.message(F.text == "🔎 Статус")
async def status_command(msg: types.Message, state: FSMContext):
    role = await get_user_role(msg.from_user.id)
    if role == 'admin':
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT telegram_id, username FROM users")
            users = cur.fetchall()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"@{u[1]}", callback_data=f"admin_status:{u[0]}")] for u in users if u[1]
        ])
        await msg.answer("Выберите пользователя:", reply_markup=kb)
    else:
        await show_sensors_status(msg.from_user.id, msg)

@dp.callback_query(F.data.startswith("admin_status:"))
async def admin_select_status(callback: types.CallbackQuery):
    telegram_id = int(callback.data.split(":")[1])
    await show_sensors_status(telegram_id, callback.message)

async def show_sensors_status(telegram_id, message):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (telegram_id,))
        sensors = cur.fetchall()
    if not sensors:
        await message.answer("Нет данных.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"status:{telegram_id}:{s[0]}")] for s in sensors
    ])
    await message.answer("Выберите датчик:", reply_markup=kb)

@dp.callback_query(F.data.startswith("status:"))
async def show_status_for_sensor(callback: types.CallbackQuery):
    _, telegram_id, sensor = callback.data.split(":")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT parameter, value, unit, timestamp
            FROM sensor_data_ext
            WHERE sensor = %s AND telegram_id = %s
            ORDER BY timestamp DESC
        """, (sensor, telegram_id))
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

@dp.message(F.text == "📊 Последние показания")
async def last_values_command(msg: types.Message, state: FSMContext):
    role = await get_user_role(msg.from_user.id)
    if role == 'admin':
        with conn.cursor() as cur:
            cur.execute("SELECT telegram_id, username FROM users")
            users = cur.fetchall()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"@{u[1]}", callback_data=f"admin_data:{u[0]}")] for u in users if u[1]
        ])
        await msg.answer("Выберите пользователя:", reply_markup=kb)
    else:
        await show_sensor_selection(msg.from_user.id, msg, state)

@dp.callback_query(F.data.startswith("admin_data:"))
async def admin_choose_user_data(callback: types.CallbackQuery, state: FSMContext):
    telegram_id = int(callback.data.split(":")[1])
    await state.update_data(telegram_id=telegram_id)
    await show_sensor_selection(telegram_id, callback.message, state)

async def show_sensor_selection(telegram_id, message, state):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor FROM sensor_data_ext WHERE telegram_id = %s", (telegram_id,))
        sensors = cur.fetchall()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s[0], callback_data=f"param_sensor:{telegram_id}:{s[0]}")] for s in sensors
    ])
    await message.answer("Выберите датчик:", reply_markup=kb)

@dp.callback_query(F.data.startswith("param_sensor:"))
async def choose_param(callback: types.CallbackQuery, state: FSMContext):
    _, telegram_id, sensor = callback.data.split(":")
    await state.update_data(sensor=sensor, telegram_id=int(telegram_id))
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT parameter FROM sensor_data_ext WHERE sensor = %s AND telegram_id = %s", (sensor, telegram_id))
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
async def show_plot(msg: types.Message, state: FSMContext):
    try:
        count = int(msg.text)
    except ValueError:
        await msg.answer("Введите число")
        return

    data = await state.get_data()
    telegram_id = data["telegram_id"]
    sensor = data["sensor"]
    parameter = data["parameter"]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, value FROM sensor_data_ext
            WHERE telegram_id = %s AND sensor = %s AND parameter = %s
            ORDER BY timestamp DESC LIMIT %s
        """, (telegram_id, sensor, parameter, count))
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
    plt.savefig("plot.png")
    plt.close()

    photo = FSInputFile("plot.png")
    await msg.answer_photo(photo)
    await state.clear()

async def main():
    global conn
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            full_name TEXT,
            username TEXT,
            role TEXT,
            registered_at TIMESTAMP DEFAULT NOW()
        );
        """)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
